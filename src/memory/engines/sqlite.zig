//! SQLite-backed persistent memory — the brain.
//!
//! Features:
//! - Core memories table with CRUD
//! - FTS5 full-text search with BM25 scoring
//! - FTS5 sync triggers (insert/update/delete)
//! - Upsert semantics (ON CONFLICT DO UPDATE)
//! - Session-scoped memory isolation via session_id
//! - Session message storage
//! - KV store for settings

const std = @import("std");
const builtin = @import("builtin");
const json_util = @import("../../json_util.zig");
const root = @import("../root.zig");
const key_codec = @import("../vector/key_codec.zig");
const Memory = root.Memory;
const MemoryCategory = root.MemoryCategory;
const MemoryEntry = root.MemoryEntry;
const MemoryEvent = root.MemoryEvent;
const MemoryEventFeedInfo = root.MemoryEventFeedInfo;
const MemoryEventInput = root.MemoryEventInput;
const MemoryEventOp = root.MemoryEventOp;
const MemoryValueKind = root.MemoryValueKind;
const log = std.log.scoped(.memory_sqlite);

pub const c = @cImport({
    @cInclude("sqlite3.h");
});

pub const SQLITE_STATIC: c.sqlite3_destructor_type = null;
const BUSY_TIMEOUT_MS: c_int = 5000;

/// Detect whether the filesystem backing `path` supports WAL mode (which
/// requires mmap).  On Linux, 9p / NFS / CIFS do not support mmap, so we
/// fall back to DELETE journal mode for those.  On non-Linux or on statfs
/// failure we default to WAL (the common case).
///
/// Primary detection: parse `/proc/self/mountinfo` to find the fs_type for
/// the longest matching mount_point prefix.  This correctly identifies 9p
/// even when statfs reports the host's backing filesystem magic (e.g. ZFS).
///
/// Fallback: statfs syscall (catches cases where mountinfo is unavailable).
pub fn shouldUseWal(path: [*:0]const u8) bool {
    if (comptime builtin.os.tag != .linux) return true;

    const path_span = std.mem.span(path);
    if (path_span.len == 0 or std.mem.eql(u8, path_span, ":memory:")) return true;

    // Primary: /proc/self/mountinfo
    if (checkMountinfo(path_span)) |use_wal| return use_wal;

    // Fallback: statfs syscall
    return checkStatfs(path);
}

/// Parse /proc/self/mountinfo to find the filesystem type for `path`.
/// Returns `false` if the fs is 9p/nfs/cifs/smb3, `true` for others,
/// `null` if mountinfo is unavailable or unparseable.
fn checkMountinfo(path: []const u8) ?bool {
    const file = std.fs.openFileAbsolute("/proc/self/mountinfo", .{}) catch return null;
    defer file.close();

    var best_len: usize = 0;
    var best_is_network = false;
    var buf: [4096]u8 = undefined;
    var carry: [512]u8 = undefined;
    var carry_len: usize = 0;

    while (true) {
        const n = file.read(buf[carry_len..]) catch break;
        if (carry_len > 0) {
            // Prepend leftover bytes from the previous read
            @memcpy(buf[0..carry_len], carry[0..carry_len]);
        }
        const total = carry_len + n;
        carry_len = 0;
        if (total == 0) break;

        var data = buf[0..total];
        while (data.len > 0) {
            if (std.mem.indexOfScalar(u8, data, '\n')) |nl| {
                const line = data[0..nl];
                data = data[nl + 1 ..];
                parseMountinfoLine(line, path, &best_len, &best_is_network);
            } else {
                // Incomplete line -- carry over to next read
                const leftover = data.len;
                if (leftover <= carry.len) {
                    @memcpy(carry[0..leftover], data[0..leftover]);
                    carry_len = leftover;
                }
                break;
            }
        }

        if (n == 0) break; // EOF
    }

    if (best_len == 0) return null;
    return !best_is_network;
}

/// Parse one mountinfo line and update best match if mount_point is a
/// longer prefix of `path`.
///
/// Format: mount_id parent_id major:minor root mount_point flags [opts]* - fs_type source super_opts
fn parseMountinfoLine(line: []const u8, path: []const u8, best_len: *usize, best_is_network: *bool) void {
    // Fields are space-separated.  We need field index 4 (mount_point)
    // and the field after the " - " separator (fs_type).
    var it = std.mem.splitScalar(u8, line, ' ');

    // Skip mount_id (0), parent_id (1), major:minor (2), root (3)
    inline for (0..4) |_| {
        _ = it.next() orelse return;
    }
    const mount_point = it.next() orelse return;

    // mount_point in mountinfo uses octal escapes (for example "\040" for space).
    // Decode while matching against `path` to avoid allocating.
    const mount_point_len = mountPointDecodedPrefixLen(path, mount_point) orelse return;
    // Ensure it's a proper prefix (exact match, or next char is '/')
    if (mount_point_len != path.len and mount_point_len > 1 and
        (mount_point_len >= path.len or path[mount_point_len] != '/'))
        return;
    if (mount_point_len <= best_len.*) return;

    // Find " - " separator to locate fs_type
    const sep = " - ";
    const sep_pos = std.mem.indexOf(u8, line, sep) orelse return;
    const after_sep = line[sep_pos + sep.len ..];
    var sep_it = std.mem.splitScalar(u8, after_sep, ' ');
    const fs_type = sep_it.next() orelse return;

    best_len.* = mount_point_len;
    best_is_network.* = isNetworkFs(fs_type);
}

fn mountPointDecodedPrefixLen(path: []const u8, mount_point: []const u8) ?usize {
    var path_idx: usize = 0;
    var mp_idx: usize = 0;
    while (mp_idx < mount_point.len) {
        if (mount_point[mp_idx] == '\\' and mp_idx + 3 < mount_point.len) {
            const d1 = octalDigit(mount_point[mp_idx + 1]);
            const d2 = octalDigit(mount_point[mp_idx + 2]);
            const d3 = octalDigit(mount_point[mp_idx + 3]);
            if (d1 != null and d2 != null and d3 != null) {
                const decoded: u8 = (@as(u8, d1.?) << 6) | (@as(u8, d2.?) << 3) | d3.?;
                if (path_idx >= path.len or path[path_idx] != decoded) return null;
                path_idx += 1;
                mp_idx += 4;
                continue;
            }
        }

        if (path_idx >= path.len or path[path_idx] != mount_point[mp_idx]) return null;
        path_idx += 1;
        mp_idx += 1;
    }
    return path_idx;
}

fn octalDigit(ch: u8) ?u8 {
    if (ch < '0' or ch > '7') return null;
    return ch - '0';
}

fn isNetworkFs(fs_type: []const u8) bool {
    const network_types = [_][]const u8{ "9p", "nfs", "nfs4", "cifs", "smb3" };
    for (network_types) |nt| {
        if (std.mem.eql(u8, fs_type, nt)) return true;
    }
    return false;
}

/// Fallback: use statfs to check f_type. If the DB file does not exist yet,
/// retry on its parent directory.
fn checkStatfs(path: [*:0]const u8) bool {
    if (statfsSupportsWal(path)) |use_wal| return use_wal;

    const path_span = std.mem.span(path);
    if (path_span.len == 0 or std.mem.eql(u8, path_span, ":memory:")) return true;

    const dir_path = std.fs.path.dirname(path_span) orelse ".";
    var dir_buf: [std.fs.max_path_bytes]u8 = undefined;
    if (dir_path.len + 1 > dir_buf.len) return true;
    @memcpy(dir_buf[0..dir_path.len], dir_path);
    dir_buf[dir_path.len] = 0;
    const dir_z: [*:0]const u8 = @ptrCast(&dir_buf[0]);

    if (statfsSupportsWal(dir_z)) |use_wal| return use_wal;
    return true;
}

fn statfsSupportsWal(path: [*:0]const u8) ?bool {
    var buf: [15]usize = undefined;
    const rc = std.os.linux.syscall2(
        .statfs,
        @intFromPtr(path),
        @intFromPtr(&buf),
    );
    const signed_rc: isize = @bitCast(rc);
    if (signed_rc < 0) return null;

    const f_magic: u32 = @truncate(buf[0]);
    return switch (f_magic) {
        0x01021997, // V9FS_MAGIC
        0x6969, // NFS_SUPER_MAGIC
        0xFF534D42, // CIFS_MAGIC_NUMBER
        => false,
        else => true,
    };
}

pub const SqliteMemory = struct {
    db: ?*c.sqlite3,
    allocator: std.mem.Allocator,
    instance_id: []const u8 = "default",
    owns_instance_id: bool = false,
    owns_self: bool = false,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, db_path: [*:0]const u8) !Self {
        return initWithInstanceId(allocator, db_path, "default");
    }

    pub fn initWithInstanceId(allocator: std.mem.Allocator, db_path: [*:0]const u8, instance_id: []const u8) !Self {
        const use_wal = shouldUseWal(db_path);

        var db: ?*c.sqlite3 = null;
        const rc = c.sqlite3_open(db_path, &db);
        if (rc != c.SQLITE_OK) {
            if (db) |d| _ = c.sqlite3_close(d);
            return error.SqliteOpenFailed;
        }
        if (db) |d| {
            // Reduce startup flakiness when multiple runtimes touch the same DB.
            _ = c.sqlite3_busy_timeout(d, BUSY_TIMEOUT_MS);
        }

        const effective_instance_id = if (instance_id.len > 0) instance_id else "default";
        var self_ = Self{
            .db = db,
            .allocator = allocator,
            .instance_id = try allocator.dupe(u8, effective_instance_id),
            .owns_instance_id = true,
        };
        errdefer if (self_.owns_instance_id) allocator.free(self_.instance_id);
        try self_.configurePragmas(use_wal);
        try self_.migrate();
        try self_.migrateSessionId();
        try self_.migrateEventStream();
        try self_.migrateAgentNamespace();
        return self_;
    }

    pub fn deinit(self: *Self) void {
        if (self.db) |db| {
            _ = c.sqlite3_close(db);
            self.db = null;
        }
        if (self.owns_instance_id) {
            self.allocator.free(self.instance_id);
            self.owns_instance_id = false;
        }
    }

    fn logExecFailure(self: *Self, context: []const u8, sql: []const u8, rc: c_int, err_msg: [*c]u8) void {
        if (err_msg) |msg| {
            const msg_text = std.mem.span(msg);
            log.warn("sqlite {s} failed (rc={d}, sql={s}): {s}", .{ context, rc, sql, msg_text });
            return;
        }
        if (self.db) |db| {
            const msg_text = std.mem.span(c.sqlite3_errmsg(db));
            log.warn("sqlite {s} failed (rc={d}, sql={s}): {s}", .{ context, rc, sql, msg_text });
            return;
        }
        log.warn("sqlite {s} failed (rc={d}, sql={s})", .{ context, rc, sql });
    }

    fn configurePragmas(self: *Self, use_wal: bool) !void {
        // Pragmas are tuning knobs; failure should not prevent startup.
        const journal_pragma: [:0]const u8 = if (use_wal)
            "PRAGMA journal_mode = WAL;"
        else
            "PRAGMA journal_mode = DELETE;";
        if (!use_wal) {
            log.info("filesystem does not support mmap; using DELETE journal mode instead of WAL", .{});
        }
        const pragmas = [_][:0]const u8{
            journal_pragma,
            "PRAGMA synchronous  = NORMAL;",
            "PRAGMA temp_store   = MEMORY;",
            "PRAGMA cache_size   = -2000;",
        };
        for (pragmas) |pragma| {
            var err_msg: [*c]u8 = null;
            const rc = c.sqlite3_exec(self.db, pragma, null, null, &err_msg);
            if (rc != c.SQLITE_OK) {
                self.logExecFailure("pragma", pragma, rc, err_msg);
            }
            if (err_msg) |msg| c.sqlite3_free(msg);
        }
    }

    fn migrate(self: *Self) !void {
        const sql =
            \\-- Core memories table
            \\CREATE TABLE IF NOT EXISTS memories (
            \\  id         TEXT PRIMARY KEY,
            \\  key        TEXT NOT NULL UNIQUE,
            \\  content    TEXT NOT NULL,
            \\  category   TEXT NOT NULL DEFAULT 'core',
            \\  value_kind TEXT,
            \\  session_id TEXT,
            \\  event_timestamp_ms INTEGER NOT NULL DEFAULT 0,
            \\  event_origin_instance_id TEXT NOT NULL DEFAULT 'default',
            \\  event_origin_sequence INTEGER NOT NULL DEFAULT 0,
            \\  created_at TEXT NOT NULL,
            \\  updated_at TEXT NOT NULL
            \\);
            \\CREATE INDEX IF NOT EXISTS idx_memories_category ON memories(category);
            \\CREATE INDEX IF NOT EXISTS idx_memories_key ON memories(key);
            \\CREATE INDEX IF NOT EXISTS idx_memories_session ON memories(session_id);
            \\-- FTS5 full-text search (BM25 scoring)
            \\CREATE VIRTUAL TABLE IF NOT EXISTS memories_fts USING fts5(
            \\  key, content, content=memories, content_rowid=rowid
            \\);
            \\
            \\-- FTS5 triggers: keep in sync with memories table
            \\CREATE TRIGGER IF NOT EXISTS memories_ai AFTER INSERT ON memories BEGIN
            \\  INSERT INTO memories_fts(rowid, key, content)
            \\  VALUES (new.rowid, new.key, new.content);
            \\END;
            \\CREATE TRIGGER IF NOT EXISTS memories_ad AFTER DELETE ON memories BEGIN
            \\  INSERT INTO memories_fts(memories_fts, rowid, key, content)
            \\  VALUES ('delete', old.rowid, old.key, old.content);
            \\END;
            \\CREATE TRIGGER IF NOT EXISTS memories_au AFTER UPDATE ON memories BEGIN
            \\  INSERT INTO memories_fts(memories_fts, rowid, key, content)
            \\  VALUES ('delete', old.rowid, old.key, old.content);
            \\  INSERT INTO memories_fts(rowid, key, content)
            \\  VALUES (new.rowid, new.key, new.content);
            \\END;
            \\
            \\-- Legacy tables for backward compat
            \\CREATE TABLE IF NOT EXISTS messages (
            \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
            \\  session_id TEXT NOT NULL,
            \\  role TEXT NOT NULL,
            \\  content TEXT NOT NULL,
            \\  created_at TEXT DEFAULT (datetime('now'))
            \\);
            \\CREATE TABLE IF NOT EXISTS sessions (
            \\  id TEXT PRIMARY KEY,
            \\  provider TEXT,
            \\  model TEXT,
            \\  created_at TEXT DEFAULT (datetime('now')),
            \\  updated_at TEXT DEFAULT (datetime('now'))
            \\);
            \\CREATE TABLE IF NOT EXISTS session_usage (
            \\  session_id TEXT PRIMARY KEY,
            \\  total_tokens INTEGER NOT NULL DEFAULT 0,
            \\  updated_at TEXT DEFAULT (datetime('now'))
            \\);
            \\CREATE TABLE IF NOT EXISTS kv (
            \\  key TEXT PRIMARY KEY,
            \\  value TEXT NOT NULL
            \\);
            \\
            \\-- Embedding cache for vector search
            \\CREATE TABLE IF NOT EXISTS embedding_cache (
            \\  content_hash TEXT PRIMARY KEY,
            \\  embedding    BLOB NOT NULL,
            \\  created_at   TEXT NOT NULL DEFAULT (datetime('now'))
            \\);
            \\
            \\-- Embeddings linked to memory entries
            \\CREATE TABLE IF NOT EXISTS memory_embeddings (
            \\  memory_key  TEXT PRIMARY KEY,
            \\  embedding   BLOB NOT NULL,
            \\  updated_at  TEXT NOT NULL DEFAULT (datetime('now')),
            \\  FOREIGN KEY (memory_key) REFERENCES memories(key) ON DELETE CASCADE
            \\);
        ;
        var err_msg: [*c]u8 = null;
        const rc = c.sqlite3_exec(self.db, sql, null, null, &err_msg);
        if (rc != c.SQLITE_OK) {
            self.logExecFailure("schema migration", "CREATE TABLE/FTS/triggers", rc, err_msg);
            if (err_msg) |msg| c.sqlite3_free(msg);
            return error.MigrationFailed;
        }
    }

    /// Migration: add session_id column to existing databases that lack it.
    /// Safe to run repeatedly — ALTER TABLE fails gracefully if column already exists.
    pub fn migrateSessionId(self: *Self) !void {
        var err_msg: [*c]u8 = null;
        const rc = c.sqlite3_exec(
            self.db,
            "ALTER TABLE memories ADD COLUMN session_id TEXT;",
            null,
            null,
            &err_msg,
        );
        if (rc != c.SQLITE_OK) {
            // "duplicate column name" is expected on databases that already have the column.
            var ignore_error = false;
            if (err_msg) |msg| {
                const msg_text = std.mem.span(msg);
                ignore_error = std.mem.indexOf(u8, msg_text, "duplicate column name") != null;
            }
            if (!ignore_error) {
                self.logExecFailure("session_id migration", "ALTER TABLE memories ADD COLUMN session_id TEXT", rc, err_msg);
            }
            if (err_msg) |msg| c.sqlite3_free(msg);
        }
        // Ensure index exists regardless
        var err_msg2: [*c]u8 = null;
        const rc2 = c.sqlite3_exec(
            self.db,
            "CREATE INDEX IF NOT EXISTS idx_memories_session ON memories(session_id);",
            null,
            null,
            &err_msg2,
        );
        if (rc2 != c.SQLITE_OK) {
            self.logExecFailure("session_id migration", "CREATE INDEX IF NOT EXISTS idx_memories_session", rc2, err_msg2);
            if (err_msg2) |msg| c.sqlite3_free(msg);
        }
    }

    pub fn migrateAgentNamespace(self: *Self) !void {
        {
            const check_sql = "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_memories_key_session'";
            var stmt: ?*c.sqlite3_stmt = null;
            var rc = c.sqlite3_prepare_v2(self.db, check_sql, -1, &stmt, null);
            if (rc != c.SQLITE_OK) return error.PrepareFailed;
            defer _ = c.sqlite3_finalize(stmt);

            rc = c.sqlite3_step(stmt);
            if (rc == c.SQLITE_ROW and c.sqlite3_column_int64(stmt.?, 0) > 0) return;
        }

        var needs_rebuild = false;
        {
            const check_sql = "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name LIKE 'sqlite_autoindex_memories_%'";
            var stmt: ?*c.sqlite3_stmt = null;
            var rc = c.sqlite3_prepare_v2(self.db, check_sql, -1, &stmt, null);
            if (rc != c.SQLITE_OK) return error.PrepareFailed;
            defer _ = c.sqlite3_finalize(stmt);

            rc = c.sqlite3_step(stmt);
            if (rc == c.SQLITE_ROW) {
                needs_rebuild = c.sqlite3_column_int64(stmt.?, 0) > 0;
            }
        }

        if (needs_rebuild) {
            const rebuild_sql =
                \\BEGIN;
                \\CREATE TABLE memories_new (
                \\  id         TEXT PRIMARY KEY,
                \\  key        TEXT NOT NULL,
                \\  content    TEXT NOT NULL,
                \\  category   TEXT NOT NULL DEFAULT 'core',
                \\  value_kind TEXT,
                \\  session_id TEXT,
                \\  event_timestamp_ms INTEGER NOT NULL DEFAULT 0,
                \\  event_origin_instance_id TEXT NOT NULL DEFAULT 'default',
                \\  event_origin_sequence INTEGER NOT NULL DEFAULT 0,
                \\  created_at TEXT NOT NULL,
                \\  updated_at TEXT NOT NULL
                \\);
                \\INSERT INTO memories_new (id, key, content, category, value_kind, session_id, event_timestamp_ms, event_origin_instance_id, event_origin_sequence, created_at, updated_at)
                \\SELECT id, key, content, category, NULL, session_id,
                \\       COALESCE(event_timestamp_ms, 0),
                \\       COALESCE(event_origin_instance_id, 'default'),
                \\       COALESCE(event_origin_sequence, 0),
                \\       created_at, updated_at
                \\FROM memories;
                \\DROP TABLE memories;
                \\ALTER TABLE memories_new RENAME TO memories;
                \\CREATE INDEX IF NOT EXISTS idx_memories_category ON memories(category);
                \\CREATE INDEX IF NOT EXISTS idx_memories_key ON memories(key);
                \\CREATE INDEX IF NOT EXISTS idx_memories_session ON memories(session_id);
                \\CREATE INDEX IF NOT EXISTS idx_memories_event_order ON memories(event_timestamp_ms, event_origin_instance_id, event_origin_sequence);
                \\CREATE TRIGGER IF NOT EXISTS memories_ai AFTER INSERT ON memories BEGIN
                \\  INSERT INTO memories_fts(rowid, key, content)
                \\  VALUES (new.rowid, new.key, new.content);
                \\END;
                \\CREATE TRIGGER IF NOT EXISTS memories_ad AFTER DELETE ON memories BEGIN
                \\  INSERT INTO memories_fts(memories_fts, rowid, key, content)
                \\  VALUES ('delete', old.rowid, old.key, old.content);
                \\END;
                \\CREATE TRIGGER IF NOT EXISTS memories_au AFTER UPDATE ON memories BEGIN
                \\  INSERT INTO memories_fts(memories_fts, rowid, key, content)
                \\  VALUES ('delete', old.rowid, old.key, old.content);
                \\  INSERT INTO memories_fts(rowid, key, content)
                \\  VALUES (new.rowid, new.key, new.content);
                \\END;
                \\COMMIT;
            ;
            var err_msg: [*c]u8 = null;
            const rc = c.sqlite3_exec(self.db, rebuild_sql, null, null, &err_msg);
            if (rc != c.SQLITE_OK) {
                self.logExecFailure("agent namespace migration (rebuild)", "CREATE TABLE memories_new / rename", rc, err_msg);
                if (err_msg) |msg| c.sqlite3_free(msg);
                return error.MigrationFailed;
            }

            var fts_err_msg: [*c]u8 = null;
            const fts_rc = c.sqlite3_exec(
                self.db,
                "INSERT INTO memories_fts(memories_fts) VALUES('rebuild');",
                null,
                null,
                &fts_err_msg,
            );
            if (fts_rc != c.SQLITE_OK) {
                self.logExecFailure("agent namespace migration (fts rebuild)", "INSERT INTO memories_fts(memories_fts) VALUES('rebuild')", fts_rc, fts_err_msg);
                if (fts_err_msg) |msg| c.sqlite3_free(msg);
            }
        }

        {
            var err_msg: [*c]u8 = null;
            const rc = c.sqlite3_exec(
                self.db,
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_memories_key_session ON memories(key, COALESCE(session_id, '__global__'));",
                null,
                null,
                &err_msg,
            );
            if (rc != c.SQLITE_OK) {
                self.logExecFailure("agent namespace migration (composite index)", "CREATE UNIQUE INDEX idx_memories_key_session", rc, err_msg);
                if (err_msg) |msg| c.sqlite3_free(msg);
                return error.MigrationFailed;
            }
        }
    }

    fn execSql(self: *Self, context: []const u8, sql: [:0]const u8) !void {
        var err_msg: [*c]u8 = null;
        const rc = c.sqlite3_exec(self.db, sql, null, null, &err_msg);
        if (rc != c.SQLITE_OK) {
            self.logExecFailure(context, sql, rc, err_msg);
            if (err_msg) |msg| c.sqlite3_free(msg);
            return error.MigrationFailed;
        }
    }

    fn execSqlAllowDuplicateColumn(self: *Self, context: []const u8, sql: [:0]const u8) !void {
        var err_msg: [*c]u8 = null;
        const rc = c.sqlite3_exec(self.db, sql, null, null, &err_msg);
        if (rc == c.SQLITE_OK) return;

        var ignore_error = false;
        if (err_msg) |msg| {
            const msg_text = std.mem.span(msg);
            ignore_error = std.mem.indexOf(u8, msg_text, "duplicate column name") != null;
        }
        if (!ignore_error) {
            self.logExecFailure(context, sql, rc, err_msg);
            if (err_msg) |msg| c.sqlite3_free(msg);
            return error.MigrationFailed;
        }
    }

    pub fn migrateEventStream(self: *Self) !void {
        try self.execSqlAllowDuplicateColumn("event stream migration", "ALTER TABLE memories ADD COLUMN event_timestamp_ms INTEGER NOT NULL DEFAULT 0;");
        try self.execSqlAllowDuplicateColumn("event stream migration", "ALTER TABLE memories ADD COLUMN event_origin_instance_id TEXT NOT NULL DEFAULT 'default';");
        try self.execSqlAllowDuplicateColumn("event stream migration", "ALTER TABLE memories ADD COLUMN event_origin_sequence INTEGER NOT NULL DEFAULT 0;");
        try self.execSqlAllowDuplicateColumn("event stream migration", "ALTER TABLE memories ADD COLUMN value_kind TEXT;");
        try self.execSql("event stream migration", "CREATE INDEX IF NOT EXISTS idx_memories_event_order ON memories(event_timestamp_ms, event_origin_instance_id, event_origin_sequence);");
        try self.execSql(
            "event stream migration",
            \\CREATE TABLE IF NOT EXISTS memory_events (
            \\  local_sequence INTEGER PRIMARY KEY,
            \\  schema_version INTEGER NOT NULL DEFAULT 1,
            \\  origin_instance_id TEXT NOT NULL,
            \\  origin_sequence INTEGER NOT NULL,
            \\  timestamp_ms INTEGER NOT NULL,
            \\  operation TEXT NOT NULL,
            \\  key TEXT NOT NULL,
            \\  session_id TEXT,
            \\  category TEXT,
            \\  value_kind TEXT,
            \\  content TEXT,
            \\  UNIQUE(origin_instance_id, origin_sequence)
            \\);
        );
        try self.execSql("event stream migration", "CREATE INDEX IF NOT EXISTS idx_memory_events_local_sequence ON memory_events(local_sequence);");
        try self.execSql("event stream migration", "CREATE INDEX IF NOT EXISTS idx_memory_events_origin ON memory_events(origin_instance_id, origin_sequence);");
        try self.execSql(
            "event stream migration",
            \\CREATE TABLE IF NOT EXISTS memory_event_frontiers (
            \\  origin_instance_id TEXT PRIMARY KEY,
            \\  last_origin_sequence INTEGER NOT NULL
            \\);
        );
        try self.execSql(
            "event stream migration",
            \\CREATE TABLE IF NOT EXISTS memory_tombstones (
            \\  key TEXT NOT NULL,
            \\  scope TEXT NOT NULL,
            \\  session_key TEXT NOT NULL,
            \\  session_id TEXT,
            \\  timestamp_ms INTEGER NOT NULL,
            \\  origin_instance_id TEXT NOT NULL,
            \\  origin_sequence INTEGER NOT NULL,
            \\  PRIMARY KEY (key, scope, session_key)
            \\);
        );
        try self.execSql("event stream migration", "CREATE INDEX IF NOT EXISTS idx_memory_tombstones_key ON memory_tombstones(key);");
        try self.execSql(
            "event stream migration",
            \\CREATE TABLE IF NOT EXISTS memory_feed_meta (
            \\  key TEXT PRIMARY KEY,
            \\  value TEXT NOT NULL
            \\);
        );
        try self.bootstrapEventFeedFromExistingMemories();
    }

    fn localInstanceId(self: *Self) []const u8 {
        return self.instance_id;
    }

    fn getCompactedThroughSequence(self: *Self) !u64 {
        const sql = "SELECT value FROM memory_feed_meta WHERE key = 'compacted_through_sequence' LIMIT 1";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);
        rc = c.sqlite3_step(stmt);
        if (rc != c.SQLITE_ROW) return 0;
        const value_ptr = c.sqlite3_column_text(stmt, 0) orelse return 0;
        const value_len: usize = @intCast(c.sqlite3_column_bytes(stmt, 0));
        const value = @as([*]const u8, @ptrCast(value_ptr))[0..value_len];
        return std.fmt.parseInt(u64, value, 10) catch 0;
    }

    fn setCompactedThroughSequenceTx(self: *Self, sequence: u64) !void {
        const sql =
            "INSERT INTO memory_feed_meta (key, value) VALUES ('compacted_through_sequence', ?1) " ++
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        var value_buf: [32]u8 = undefined;
        const value = try std.fmt.bufPrint(&value_buf, "{d}", .{sequence});
        _ = c.sqlite3_bind_text(stmt, 1, value.ptr, @intCast(value.len), SQLITE_STATIC);
        rc = c.sqlite3_step(stmt);
        if (rc != c.SQLITE_DONE) return error.StepFailed;
    }

    fn compareInputToMetadata(input: MemoryEventInput, timestamp_ms: i64, origin_instance_id: []const u8, origin_sequence: u64) i8 {
        if (input.timestamp_ms < timestamp_ms) return -1;
        if (input.timestamp_ms > timestamp_ms) return 1;

        const order = std.mem.order(u8, input.origin_instance_id, origin_instance_id);
        if (order == .lt) return -1;
        if (order == .gt) return 1;

        if (input.origin_sequence < origin_sequence) return -1;
        if (input.origin_sequence > origin_sequence) return 1;
        return 0;
    }

    fn execTxnSql(self: *Self, sql: [:0]const u8) !void {
        var err_msg: [*c]u8 = null;
        const rc = c.sqlite3_exec(self.db, sql, null, null, &err_msg);
        if (rc != c.SQLITE_OK) {
            self.logExecFailure("transaction", sql, rc, err_msg);
            if (err_msg) |msg| c.sqlite3_free(msg);
            return error.StepFailed;
        }
    }

    fn beginImmediate(self: *Self) !bool {
        var err_msg: [*c]u8 = null;
        const sql = "BEGIN IMMEDIATE;";
        const rc = c.sqlite3_exec(self.db, sql, null, null, &err_msg);
        if (rc == c.SQLITE_OK) return true;

        if (err_msg) |msg| {
            const text = std.mem.span(msg);
            if (std.mem.indexOf(u8, text, "cannot start a transaction within a transaction") != null) {
                c.sqlite3_free(msg);
                return false;
            }
        }

        self.logExecFailure("transaction", sql, rc, err_msg);
        if (err_msg) |msg| c.sqlite3_free(msg);
        return error.StepFailed;
    }

    fn commitTxn(self: *Self) !void {
        try self.execTxnSql("COMMIT;");
    }

    fn rollbackTxn(self: *Self) void {
        var err_msg: [*c]u8 = null;
        _ = c.sqlite3_exec(self.db, "ROLLBACK;", null, null, &err_msg);
        if (err_msg) |msg| c.sqlite3_free(msg);
    }

    fn bindNullableText(stmt: ?*c.sqlite3_stmt, index: c_int, value: ?[]const u8) void {
        if (value) |text| {
            _ = c.sqlite3_bind_text(stmt, index, text.ptr, @intCast(text.len), SQLITE_STATIC);
        } else {
            _ = c.sqlite3_bind_null(stmt, index);
        }
    }

    fn getFrontierTx(self: *Self, origin_instance_id: []const u8) !u64 {
        const sql = "SELECT last_origin_sequence FROM memory_event_frontiers WHERE origin_instance_id = ?1";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, origin_instance_id.ptr, @intCast(origin_instance_id.len), SQLITE_STATIC);
        rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_ROW) {
            const frontier = c.sqlite3_column_int64(stmt, 0);
            return if (frontier < 0) 0 else @intCast(frontier);
        }
        return 0;
    }

    fn setFrontierTx(self: *Self, origin_instance_id: []const u8, origin_sequence: u64) !void {
        const sql =
            "INSERT INTO memory_event_frontiers (origin_instance_id, last_origin_sequence) VALUES (?1, ?2) " ++
            "ON CONFLICT(origin_instance_id) DO UPDATE SET last_origin_sequence = MAX(last_origin_sequence, excluded.last_origin_sequence)";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, origin_instance_id.ptr, @intCast(origin_instance_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_int64(stmt, 2, @intCast(origin_sequence));
        rc = c.sqlite3_step(stmt);
        if (rc != c.SQLITE_DONE) return error.StepFailed;
    }

    fn queryCount(self: *Self, sql: [:0]const u8) !u64 {
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self.db, sql.ptr, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_ROW) {
            const value = c.sqlite3_column_int64(stmt, 0);
            return if (value < 0) 0 else @intCast(value);
        }
        return 0;
    }

    fn queryMaxI64(self: *Self, sql: [:0]const u8) !i64 {
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self.db, sql.ptr, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_ROW) {
            return c.sqlite3_column_int64(stmt, 0);
        }
        return 0;
    }

    fn bootstrapEventFeedFromExistingMemories(self: *Self) !void {
        if (try self.getCompactedThroughSequence() > 0) return;
        const event_count = try self.queryCount("SELECT COUNT(*) FROM memory_events");
        if (event_count > 0) return;

        const memory_count = try self.queryCount("SELECT COUNT(*) FROM memories");
        if (memory_count == 0) return;

        var owns_tx = c.sqlite3_get_autocommit(self.db) != 0;
        if (owns_tx) owns_tx = try self.beginImmediate();
        var committed = false;
        errdefer if (owns_tx and !committed) self.rollbackTxn();

        const select_sql =
            "SELECT rowid, key, content, category, session_id, value_kind, " ++
            "COALESCE(CAST(strftime('%s', updated_at) AS INTEGER), 0) " ++
            "FROM memories ORDER BY updated_at ASC, rowid ASC";
        var select_stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self.db, select_sql, -1, &select_stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(select_stmt);

        const update_sql =
            "UPDATE memories SET event_timestamp_ms = ?1, event_origin_instance_id = ?2, event_origin_sequence = ?3 WHERE rowid = ?4";
        var update_stmt: ?*c.sqlite3_stmt = null;
        rc = c.sqlite3_prepare_v2(self.db, update_sql, -1, &update_stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(update_stmt);

        var next_origin_sequence: u64 = 1;
        while (true) {
            rc = c.sqlite3_step(select_stmt);
            if (rc != c.SQLITE_ROW) break;

            const rowid = c.sqlite3_column_int64(select_stmt, 0);
            const key = try dupeColumnText(select_stmt.?, 1, self.allocator);
            defer self.allocator.free(key);
            const content = try dupeColumnText(select_stmt.?, 2, self.allocator);
            defer self.allocator.free(content);
            const category_text = try dupeColumnText(select_stmt.?, 3, self.allocator);
            defer self.allocator.free(category_text);
            const session_id = try dupeColumnTextNullable(select_stmt.?, 4, self.allocator);
            defer if (session_id) |sid| self.allocator.free(sid);
            const value_kind_text = try dupeColumnTextNullable(select_stmt.?, 5, self.allocator);
            defer if (value_kind_text) |value| self.allocator.free(value);
            const updated_at_secs = c.sqlite3_column_int64(select_stmt, 6);
            const timestamp_ms: i64 = if (updated_at_secs > 0) updated_at_secs * 1000 else @intCast(next_origin_sequence);
            const input = MemoryEventInput{
                .origin_instance_id = self.localInstanceId(),
                .origin_sequence = next_origin_sequence,
                .timestamp_ms = timestamp_ms,
                .operation = .put,
                .key = key,
                .session_id = session_id,
                .category = MemoryCategory.fromString(category_text),
                .value_kind = if (value_kind_text) |value| MemoryValueKind.fromString(value) else null,
                .content = content,
            };
            const inserted = try self.insertEventTx(input);
            if (!inserted) return error.StepFailed;

            _ = c.sqlite3_reset(update_stmt);
            _ = c.sqlite3_clear_bindings(update_stmt);
            _ = c.sqlite3_bind_int64(update_stmt, 1, timestamp_ms);
            _ = c.sqlite3_bind_text(update_stmt, 2, self.localInstanceId().ptr, @intCast(self.localInstanceId().len), SQLITE_STATIC);
            _ = c.sqlite3_bind_int64(update_stmt, 3, @intCast(next_origin_sequence));
            _ = c.sqlite3_bind_int64(update_stmt, 4, rowid);
            rc = c.sqlite3_step(update_stmt);
            if (rc != c.SQLITE_DONE) return error.StepFailed;

            next_origin_sequence += 1;
        }

        if (next_origin_sequence > 1) {
            try self.setFrontierTx(self.localInstanceId(), next_origin_sequence - 1);
        }

        if (owns_tx) try self.commitTxn();
        committed = true;
    }

    fn nextLocalOriginSequenceTx(self: *Self) !u64 {
        return (try self.getFrontierTx(self.localInstanceId())) + 1;
    }

    fn nextEventSequenceTx(self: *Self) !u64 {
        const sql = "SELECT COALESCE(MAX(local_sequence), 0) FROM memory_events";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        rc = c.sqlite3_step(stmt);
        const compacted_through = try self.getCompactedThroughSequence();
        if (rc == c.SQLITE_ROW) {
            const value = c.sqlite3_column_int64(stmt, 0);
            const last_in_events = if (value < 0) 0 else @as(u64, @intCast(value));
            return @max(last_in_events, compacted_through) + 1;
        }
        return compacted_through + 1;
    }

    fn insertEventTx(self: *Self, input: MemoryEventInput) !bool {
        const sql =
            "INSERT INTO memory_events (local_sequence, schema_version, origin_instance_id, origin_sequence, timestamp_ms, operation, key, session_id, category, value_kind, content) " ++
            "VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        const next_event_sequence = try self.nextEventSequenceTx();
        const category_str = if (input.category) |category| category.toString() else null;
        const value_kind_str = if (input.value_kind) |value_kind| value_kind.toString() else null;
        _ = c.sqlite3_bind_int64(stmt, 1, @intCast(next_event_sequence));
        _ = c.sqlite3_bind_int64(stmt, 2, 1);
        _ = c.sqlite3_bind_text(stmt, 3, input.origin_instance_id.ptr, @intCast(input.origin_instance_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_int64(stmt, 4, @intCast(input.origin_sequence));
        _ = c.sqlite3_bind_int64(stmt, 5, input.timestamp_ms);
        const op_str = input.operation.toString();
        _ = c.sqlite3_bind_text(stmt, 6, op_str.ptr, @intCast(op_str.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 7, input.key.ptr, @intCast(input.key.len), SQLITE_STATIC);
        bindNullableText(stmt, 8, input.session_id);
        bindNullableText(stmt, 9, category_str);
        bindNullableText(stmt, 10, value_kind_str);
        bindNullableText(stmt, 11, input.content);

        rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) return true;
        if (rc == c.SQLITE_CONSTRAINT) return false;
        return error.StepFailed;
    }

    fn sessionKeyFor(session_id: ?[]const u8) []const u8 {
        return if (session_id) |sid| sid else "__global__";
    }

    fn tombstoneBlocksPutTx(self: *Self, input: MemoryEventInput) !bool {
        const scoped_session_key = sessionKeyFor(input.session_id);
        const sql =
            "SELECT timestamp_ms, origin_instance_id, origin_sequence FROM memory_tombstones " ++
            "WHERE key = ?1 AND ((scope = 'scoped' AND session_key = ?2) OR (scope = 'all' AND session_key = '*'))";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, input.key.ptr, @intCast(input.key.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 2, scoped_session_key.ptr, @intCast(scoped_session_key.len), SQLITE_STATIC);

        while (true) {
            rc = c.sqlite3_step(stmt);
            if (rc != c.SQLITE_ROW) break;
            const timestamp_ms = c.sqlite3_column_int64(stmt, 0);
            const origin_ptr = c.sqlite3_column_text(stmt, 1);
            const origin_len: usize = @intCast(c.sqlite3_column_bytes(stmt, 1));
            const origin = if (origin_ptr == null) "" else @as([*]const u8, @ptrCast(origin_ptr))[0..origin_len];
            const origin_sequence: u64 = @intCast(@max(c.sqlite3_column_int64(stmt, 2), 0));
            if (compareInputToMetadata(input, timestamp_ms, origin, origin_sequence) <= 0) return true;
        }

        return false;
    }

    fn putStateTx(self: *Self, input: MemoryEventInput) !void {
        if (try self.tombstoneBlocksPutTx(input)) return;

        const select_sql = if (input.session_id != null)
            "SELECT content, category, value_kind, event_timestamp_ms, event_origin_instance_id, event_origin_sequence FROM memories WHERE key = ?1 AND session_id = ?2 LIMIT 1"
        else
            "SELECT content, category, value_kind, event_timestamp_ms, event_origin_instance_id, event_origin_sequence FROM memories WHERE key = ?1 AND session_id IS NULL LIMIT 1";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self.db, select_sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, input.key.ptr, @intCast(input.key.len), SQLITE_STATIC);
        if (input.session_id) |sid| {
            _ = c.sqlite3_bind_text(stmt, 2, sid.ptr, @intCast(sid.len), SQLITE_STATIC);
        }

        var existing_content: ?[]u8 = null;
        defer if (existing_content) |value| self.allocator.free(value);
        var existing_category: ?MemoryCategory = null;
        defer if (existing_category) |category| switch (category) {
            .custom => |name| self.allocator.free(name),
            else => {},
        };
        var existing_value_kind: ?MemoryValueKind = null;

        rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_ROW) {
            existing_content = try dupeColumnText(stmt.?, 0, self.allocator);
            const category_text = try dupeColumnText(stmt.?, 1, self.allocator);
            defer self.allocator.free(category_text);
            existing_category = try parseCategoryOwned(self.allocator, category_text);
            const value_kind_text = try dupeColumnTextNullable(stmt.?, 2, self.allocator);
            defer if (value_kind_text) |value| self.allocator.free(value);
            if (value_kind_text) |value| {
                existing_value_kind = MemoryValueKind.fromString(value) orelse return error.InvalidEvent;
            }
            const timestamp_ms = c.sqlite3_column_int64(stmt, 3);
            const origin_ptr = c.sqlite3_column_text(stmt, 4);
            const origin_len: usize = @intCast(c.sqlite3_column_bytes(stmt, 4));
            const origin = if (origin_ptr == null) "" else @as([*]const u8, @ptrCast(origin_ptr))[0..origin_len];
            const origin_sequence: u64 = @intCast(@max(c.sqlite3_column_int64(stmt, 5), 0));
            if (compareInputToMetadata(input, timestamp_ms, origin, origin_sequence) <= 0) return;
        }

        const resolved_state = try root.resolveMemoryEventState(
            self.allocator,
            existing_content,
            existing_category,
            existing_value_kind,
            input,
        ) orelse return error.InvalidEvent;
        defer resolved_state.deinit(self.allocator);

        const now = try std.fmt.allocPrint(self.allocator, "{d}", .{@divTrunc(input.timestamp_ms, 1000)});
        defer self.allocator.free(now);
        const id = try generateId(self.allocator);
        defer self.allocator.free(id);
        const cat_str = resolved_state.category.toString();
        const value_kind_str = if (resolved_state.value_kind) |value_kind| value_kind.toString() else null;

        const sql =
            "INSERT INTO memories (id, key, content, category, value_kind, session_id, event_timestamp_ms, event_origin_instance_id, event_origin_sequence, created_at, updated_at) " ++
            "VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11) " ++
            "ON CONFLICT(key, COALESCE(session_id, '__global__')) DO UPDATE SET " ++
            "id = excluded.id, " ++
            "content = excluded.content, " ++
            "category = excluded.category, " ++
            "value_kind = excluded.value_kind, " ++
            "event_timestamp_ms = excluded.event_timestamp_ms, " ++
            "event_origin_instance_id = excluded.event_origin_instance_id, " ++
            "event_origin_sequence = excluded.event_origin_sequence, " ++
            "updated_at = excluded.updated_at";
        var upsert_stmt: ?*c.sqlite3_stmt = null;
        rc = c.sqlite3_prepare_v2(self.db, sql, -1, &upsert_stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(upsert_stmt);

        _ = c.sqlite3_bind_text(upsert_stmt, 1, id.ptr, @intCast(id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(upsert_stmt, 2, input.key.ptr, @intCast(input.key.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(upsert_stmt, 3, resolved_state.content.ptr, @intCast(resolved_state.content.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(upsert_stmt, 4, cat_str.ptr, @intCast(cat_str.len), SQLITE_STATIC);
        bindNullableText(upsert_stmt, 5, value_kind_str);
        bindNullableText(upsert_stmt, 6, input.session_id);
        _ = c.sqlite3_bind_int64(upsert_stmt, 7, input.timestamp_ms);
        _ = c.sqlite3_bind_text(upsert_stmt, 8, input.origin_instance_id.ptr, @intCast(input.origin_instance_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_int64(upsert_stmt, 9, @intCast(input.origin_sequence));
        _ = c.sqlite3_bind_text(upsert_stmt, 10, now.ptr, @intCast(now.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(upsert_stmt, 11, now.ptr, @intCast(now.len), SQLITE_STATIC);

        rc = c.sqlite3_step(upsert_stmt);
        if (rc != c.SQLITE_DONE) return error.StepFailed;
    }

    fn deleteScopedStateTx(self: *Self, input: MemoryEventInput) !void {
        const select_sql = if (input.session_id != null)
            "SELECT event_timestamp_ms, event_origin_instance_id, event_origin_sequence FROM memories WHERE key = ?1 AND session_id = ?2 LIMIT 1"
        else
            "SELECT event_timestamp_ms, event_origin_instance_id, event_origin_sequence FROM memories WHERE key = ?1 AND session_id IS NULL LIMIT 1";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self.db, select_sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, input.key.ptr, @intCast(input.key.len), SQLITE_STATIC);
        if (input.session_id) |sid| {
            _ = c.sqlite3_bind_text(stmt, 2, sid.ptr, @intCast(sid.len), SQLITE_STATIC);
        }

        rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_ROW) {
            const timestamp_ms = c.sqlite3_column_int64(stmt, 0);
            const origin_ptr = c.sqlite3_column_text(stmt, 1);
            const origin_len: usize = @intCast(c.sqlite3_column_bytes(stmt, 1));
            const origin = if (origin_ptr == null) "" else @as([*]const u8, @ptrCast(origin_ptr))[0..origin_len];
            const origin_sequence: u64 = @intCast(@max(c.sqlite3_column_int64(stmt, 2), 0));
            if (compareInputToMetadata(input, timestamp_ms, origin, origin_sequence) >= 0) {
                const delete_sql = if (input.session_id != null)
                    "DELETE FROM memories WHERE key = ?1 AND session_id = ?2"
                else
                    "DELETE FROM memories WHERE key = ?1 AND session_id IS NULL";
                var delete_stmt: ?*c.sqlite3_stmt = null;
                rc = c.sqlite3_prepare_v2(self.db, delete_sql, -1, &delete_stmt, null);
                if (rc != c.SQLITE_OK) return error.PrepareFailed;
                defer _ = c.sqlite3_finalize(delete_stmt);

                _ = c.sqlite3_bind_text(delete_stmt, 1, input.key.ptr, @intCast(input.key.len), SQLITE_STATIC);
                if (input.session_id) |sid| {
                    _ = c.sqlite3_bind_text(delete_stmt, 2, sid.ptr, @intCast(sid.len), SQLITE_STATIC);
                }
                rc = c.sqlite3_step(delete_stmt);
                if (rc != c.SQLITE_DONE) return error.StepFailed;
            }
        }
    }

    fn deleteAllStateTx(self: *Self, input: MemoryEventInput) !void {
        const select_sql =
            "SELECT session_id, event_timestamp_ms, event_origin_instance_id, event_origin_sequence FROM memories WHERE key = ?1";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self.db, select_sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, input.key.ptr, @intCast(input.key.len), SQLITE_STATIC);

        var sessions_to_delete: std.ArrayListUnmanaged(?[]u8) = .empty;
        defer {
            for (sessions_to_delete.items) |sid_opt| if (sid_opt) |sid| self.allocator.free(sid);
            sessions_to_delete.deinit(self.allocator);
        }

        while (true) {
            rc = c.sqlite3_step(stmt);
            if (rc != c.SQLITE_ROW) break;
            const sid = try dupeColumnTextNullable(stmt.?, 0, self.allocator);
            errdefer if (sid) |value| self.allocator.free(value);
            const timestamp_ms = c.sqlite3_column_int64(stmt, 1);
            const origin_ptr = c.sqlite3_column_text(stmt, 2);
            const origin_len: usize = @intCast(c.sqlite3_column_bytes(stmt, 2));
            const origin = if (origin_ptr == null) "" else @as([*]const u8, @ptrCast(origin_ptr))[0..origin_len];
            const origin_sequence: u64 = @intCast(@max(c.sqlite3_column_int64(stmt, 3), 0));
            if (compareInputToMetadata(input, timestamp_ms, origin, origin_sequence) >= 0) {
                try sessions_to_delete.append(self.allocator, sid);
            } else if (sid) |value| {
                self.allocator.free(value);
            }
        }

        for (sessions_to_delete.items) |sid_opt| {
            const delete_sql = if (sid_opt != null)
                "DELETE FROM memories WHERE key = ?1 AND session_id = ?2"
            else
                "DELETE FROM memories WHERE key = ?1 AND session_id IS NULL";
            var delete_stmt: ?*c.sqlite3_stmt = null;
            rc = c.sqlite3_prepare_v2(self.db, delete_sql, -1, &delete_stmt, null);
            if (rc != c.SQLITE_OK) return error.PrepareFailed;
            defer _ = c.sqlite3_finalize(delete_stmt);

            _ = c.sqlite3_bind_text(delete_stmt, 1, input.key.ptr, @intCast(input.key.len), SQLITE_STATIC);
            if (sid_opt) |sid| {
                _ = c.sqlite3_bind_text(delete_stmt, 2, sid.ptr, @intCast(sid.len), SQLITE_STATIC);
            }
            rc = c.sqlite3_step(delete_stmt);
            if (rc != c.SQLITE_DONE) return error.StepFailed;
        }
    }

    fn upsertTombstoneTx(self: *Self, input: MemoryEventInput, scope: []const u8, session_key: []const u8, session_id: ?[]const u8) !void {
        const select_sql =
            "SELECT timestamp_ms, origin_instance_id, origin_sequence FROM memory_tombstones WHERE key = ?1 AND scope = ?2 AND session_key = ?3 LIMIT 1";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self.db, select_sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, input.key.ptr, @intCast(input.key.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 2, scope.ptr, @intCast(scope.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 3, session_key.ptr, @intCast(session_key.len), SQLITE_STATIC);

        rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_ROW) {
            const timestamp_ms = c.sqlite3_column_int64(stmt, 0);
            const origin_ptr = c.sqlite3_column_text(stmt, 1);
            const origin_len: usize = @intCast(c.sqlite3_column_bytes(stmt, 1));
            const origin = if (origin_ptr == null) "" else @as([*]const u8, @ptrCast(origin_ptr))[0..origin_len];
            const origin_sequence: u64 = @intCast(@max(c.sqlite3_column_int64(stmt, 2), 0));
            if (compareInputToMetadata(input, timestamp_ms, origin, origin_sequence) <= 0) return;
        }

        const upsert_sql =
            "INSERT INTO memory_tombstones (key, scope, session_key, session_id, timestamp_ms, origin_instance_id, origin_sequence) " ++
            "VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) " ++
            "ON CONFLICT(key, scope, session_key) DO UPDATE SET " ++
            "session_id = excluded.session_id, " ++
            "timestamp_ms = excluded.timestamp_ms, " ++
            "origin_instance_id = excluded.origin_instance_id, " ++
            "origin_sequence = excluded.origin_sequence";
        var upsert_stmt: ?*c.sqlite3_stmt = null;
        rc = c.sqlite3_prepare_v2(self.db, upsert_sql, -1, &upsert_stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(upsert_stmt);

        _ = c.sqlite3_bind_text(upsert_stmt, 1, input.key.ptr, @intCast(input.key.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(upsert_stmt, 2, scope.ptr, @intCast(scope.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(upsert_stmt, 3, session_key.ptr, @intCast(session_key.len), SQLITE_STATIC);
        bindNullableText(upsert_stmt, 4, session_id);
        _ = c.sqlite3_bind_int64(upsert_stmt, 5, input.timestamp_ms);
        _ = c.sqlite3_bind_text(upsert_stmt, 6, input.origin_instance_id.ptr, @intCast(input.origin_instance_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_int64(upsert_stmt, 7, @intCast(input.origin_sequence));
        rc = c.sqlite3_step(upsert_stmt);
        if (rc != c.SQLITE_DONE) return error.StepFailed;
    }

    fn applyEventTx(self: *Self, input: MemoryEventInput) !void {
        const frontier = try self.getFrontierTx(input.origin_instance_id);
        if (input.origin_sequence <= frontier) return;

        const inserted = try self.insertEventTx(input);
        if (!inserted) {
            try self.setFrontierTx(input.origin_instance_id, input.origin_sequence);
            return;
        }

        switch (input.operation) {
            .put, .merge_object, .merge_string_set => try self.putStateTx(input),
            .delete_scoped => {
                try self.deleteScopedStateTx(input);
                try self.upsertTombstoneTx(input, "scoped", sessionKeyFor(input.session_id), input.session_id);
            },
            .delete_all => {
                try self.deleteAllStateTx(input);
                try self.upsertTombstoneTx(input, "all", "*", null);
            },
        }

        try self.setFrontierTx(input.origin_instance_id, input.origin_sequence);
    }

    fn applyEventInternal(self: *Self, input: MemoryEventInput) !void {
        var owns_tx = c.sqlite3_get_autocommit(self.db) != 0;
        if (owns_tx) owns_tx = try self.beginImmediate();
        var committed = false;
        errdefer if (owns_tx and !committed) self.rollbackTxn();
        try self.applyEventTx(input);
        if (owns_tx) try self.commitTxn();
        committed = true;
    }

    fn emitLocalEvent(self: *Self, operation: MemoryEventOp, key: []const u8, session_id: ?[]const u8, category: ?MemoryCategory, content: ?[]const u8) !void {
        var owns_tx = c.sqlite3_get_autocommit(self.db) != 0;
        if (owns_tx) owns_tx = try self.beginImmediate();
        var committed = false;
        errdefer if (owns_tx and !committed) self.rollbackTxn();

        const input = MemoryEventInput{
            .origin_instance_id = self.localInstanceId(),
            .origin_sequence = try self.nextLocalOriginSequenceTx(),
            .timestamp_ms = std.time.milliTimestamp(),
            .operation = operation,
            .key = key,
            .session_id = session_id,
            .category = category,
            .content = content,
        };
        try self.applyEventTx(input);
        if (owns_tx) try self.commitTxn();
        committed = true;
    }

    // ── Memory trait implementation ────────────────────────────────

    fn implName(_: *anyopaque) []const u8 {
        return "sqlite";
    }

    fn implStore(ptr: *anyopaque, key: []const u8, content: []const u8, category: MemoryCategory, session_id: ?[]const u8) anyerror!void {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        try self_.emitLocalEvent(.put, key, session_id, category, content);
    }

    fn implRecall(ptr: *anyopaque, allocator: std.mem.Allocator, query: []const u8, limit: usize, session_id: ?[]const u8) anyerror![]MemoryEntry {
        const self_: *Self = @ptrCast(@alignCast(ptr));

        const trimmed = std.mem.trim(u8, query, " \t\n\r");
        if (trimmed.len == 0) return allocator.alloc(MemoryEntry, 0);

        const results = try fts5Search(self_, allocator, trimmed, limit, session_id);
        if (results.len > 0) return results;

        allocator.free(results);
        return try likeSearch(self_, allocator, trimmed, limit, session_id);
    }

    fn implGet(ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8) anyerror!?MemoryEntry {
        const self_: *Self = @ptrCast(@alignCast(ptr));

        const sql = "SELECT id, key, content, category, created_at, session_id FROM memories WHERE key = ?1 AND session_id IS NULL LIMIT 1";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self_.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, key.ptr, @intCast(key.len), SQLITE_STATIC);

        rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_ROW) {
            return try readEntryFromRow(stmt.?, allocator);
        }
        return null;
    }

    fn implGetScoped(ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8, session_id: ?[]const u8) anyerror!?MemoryEntry {
        const self_: *Self = @ptrCast(@alignCast(ptr));

        const sql = if (session_id != null)
            "SELECT id, key, content, category, created_at, session_id FROM memories WHERE key = ?1 AND session_id = ?2 LIMIT 1"
        else
            "SELECT id, key, content, category, created_at, session_id FROM memories WHERE key = ?1 AND session_id IS NULL LIMIT 1";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self_.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, key.ptr, @intCast(key.len), SQLITE_STATIC);
        if (session_id) |sid| {
            _ = c.sqlite3_bind_text(stmt, 2, sid.ptr, @intCast(sid.len), SQLITE_STATIC);
        }

        rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_ROW) {
            return try readEntryFromRow(stmt.?, allocator);
        }
        return null;
    }

    fn implList(ptr: *anyopaque, allocator: std.mem.Allocator, category: ?MemoryCategory, session_id: ?[]const u8) anyerror![]MemoryEntry {
        const self_: *Self = @ptrCast(@alignCast(ptr));

        var entries: std.ArrayList(MemoryEntry) = .empty;
        errdefer {
            for (entries.items) |*entry| entry.deinit(allocator);
            entries.deinit(allocator);
        }

        if (category) |cat| {
            const cat_str = cat.toString();
            const sql = "SELECT id, key, content, category, created_at, session_id FROM memories " ++
                "WHERE category = ?1 ORDER BY updated_at DESC";
            var stmt: ?*c.sqlite3_stmt = null;
            var rc = c.sqlite3_prepare_v2(self_.db, sql, -1, &stmt, null);
            if (rc != c.SQLITE_OK) return error.PrepareFailed;
            defer _ = c.sqlite3_finalize(stmt);

            _ = c.sqlite3_bind_text(stmt, 1, cat_str.ptr, @intCast(cat_str.len), SQLITE_STATIC);

            while (true) {
                rc = c.sqlite3_step(stmt);
                if (rc == c.SQLITE_ROW) {
                    const entry = try readEntryFromRow(stmt.?, allocator);
                    if (session_id) |sid| {
                        if (entry.session_id == null or !std.mem.eql(u8, entry.session_id.?, sid)) {
                            entry.deinit(allocator);
                            continue;
                        }
                    }
                    try entries.append(allocator, entry);
                } else break;
            }
        } else {
            const sql = "SELECT id, key, content, category, created_at, session_id FROM memories ORDER BY updated_at DESC";
            var stmt: ?*c.sqlite3_stmt = null;
            var rc = c.sqlite3_prepare_v2(self_.db, sql, -1, &stmt, null);
            if (rc != c.SQLITE_OK) return error.PrepareFailed;
            defer _ = c.sqlite3_finalize(stmt);

            while (true) {
                rc = c.sqlite3_step(stmt);
                if (rc == c.SQLITE_ROW) {
                    const entry = try readEntryFromRow(stmt.?, allocator);
                    if (session_id) |sid| {
                        if (entry.session_id == null or !std.mem.eql(u8, entry.session_id.?, sid)) {
                            entry.deinit(allocator);
                            continue;
                        }
                    }
                    try entries.append(allocator, entry);
                } else break;
            }
        }

        return entries.toOwnedSlice(allocator);
    }

    fn implForget(ptr: *anyopaque, key: []const u8) anyerror!bool {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        if (!(try self_.keyExistsAnyScope(key))) return false;
        try self_.emitLocalEvent(.delete_all, key, null, null, null);
        return true;
    }

    fn implForgetScoped(ptr: *anyopaque, key: []const u8, session_id: ?[]const u8) anyerror!bool {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const existing = try implGetScoped(ptr, self_.allocator, key, session_id);
        if (existing == null) return false;
        existing.?.deinit(self_.allocator);
        try self_.emitLocalEvent(.delete_scoped, key, session_id, null, null);
        return true;
    }

    fn implListEvents(ptr: *anyopaque, allocator: std.mem.Allocator, after_sequence: u64, limit: usize) anyerror![]MemoryEvent {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const compacted_through = try self_.getCompactedThroughSequence();
        if (after_sequence < compacted_through) return error.CursorExpired;
        const sql =
            "SELECT local_sequence, schema_version, origin_instance_id, origin_sequence, timestamp_ms, operation, key, session_id, category, value_kind, content " ++
            "FROM memory_events WHERE local_sequence > ?1 ORDER BY local_sequence ASC LIMIT ?2";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self_.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_int64(stmt, 1, @intCast(after_sequence));
        _ = c.sqlite3_bind_int64(stmt, 2, @intCast(limit));

        var events: std.ArrayListUnmanaged(MemoryEvent) = .empty;
        errdefer {
            for (events.items) |*event| event.deinit(allocator);
            events.deinit(allocator);
        }

        while (true) {
            rc = c.sqlite3_step(stmt);
            if (rc != c.SQLITE_ROW) break;
            try events.append(allocator, try readEventFromRow(stmt.?, allocator));
        }

        return events.toOwnedSlice(allocator);
    }

    fn implApplyEvent(ptr: *anyopaque, input: MemoryEventInput) anyerror!void {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        try self_.applyEventInternal(input);
    }

    fn implLastEventSequence(ptr: *anyopaque) anyerror!u64 {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const sql = "SELECT COALESCE(MAX(local_sequence), 0) FROM memory_events";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self_.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);
        rc = c.sqlite3_step(stmt);
        const compacted_through = try self_.getCompactedThroughSequence();
        if (rc == c.SQLITE_ROW) {
            const value = c.sqlite3_column_int64(stmt, 0);
            const last_in_events = if (value < 0) 0 else @as(u64, @intCast(value));
            return @max(last_in_events, compacted_through);
        }
        return compacted_through;
    }

    fn implEventFeedInfo(ptr: *anyopaque, allocator: std.mem.Allocator) anyerror!MemoryEventFeedInfo {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const compacted_through = try self_.getCompactedThroughSequence();
        return .{
            .instance_id = try allocator.dupe(u8, self_.localInstanceId()),
            .last_sequence = try implLastEventSequence(ptr),
            .next_local_origin_sequence = try self_.nextLocalOriginSequenceTx(),
            .supports_compaction = true,
            .compacted_through_sequence = compacted_through,
            .oldest_available_sequence = compacted_through + 1,
        };
    }

    fn implCompactEvents(ptr: *anyopaque) anyerror!u64 {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const compacted_through = try implLastEventSequence(ptr);
        if (compacted_through == 0) {
            try self_.setCompactedThroughSequenceTx(0);
            return 0;
        }

        var owns_tx = c.sqlite3_get_autocommit(self_.db) != 0;
        if (owns_tx) owns_tx = try self_.beginImmediate();
        var committed = false;
        errdefer if (owns_tx and !committed) self_.rollbackTxn();

        const delete_sql = "DELETE FROM memory_events WHERE local_sequence <= ?1";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self_.db, delete_sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);
        _ = c.sqlite3_bind_int64(stmt, 1, @intCast(compacted_through));
        rc = c.sqlite3_step(stmt);
        if (rc != c.SQLITE_DONE) return error.StepFailed;

        try self_.setCompactedThroughSequenceTx(compacted_through);
        if (owns_tx) try self_.commitTxn();
        committed = true;
        return compacted_through;
    }

    fn implExportCheckpoint(ptr: *anyopaque, allocator: std.mem.Allocator) anyerror![]u8 {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        return self_.exportCheckpointPayload(allocator);
    }

    fn implApplyCheckpoint(ptr: *anyopaque, payload: []const u8) anyerror!void {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        try self_.applyCheckpointPayload(payload);
    }

    const CheckpointStateRow = struct {
        key: []u8,
        session_id: ?[]u8,
        category: []u8,
        value_kind: ?[]u8,
        content: []u8,
        timestamp_ms: i64,
        origin_instance_id: []u8,
        origin_sequence: u64,

        fn deinit(self: *const CheckpointStateRow, allocator: std.mem.Allocator) void {
            allocator.free(self.key);
            if (self.session_id) |sid| allocator.free(sid);
            allocator.free(self.category);
            if (self.value_kind) |kind| allocator.free(kind);
            allocator.free(self.content);
            allocator.free(self.origin_instance_id);
        }
    };

    const CheckpointFrontierRow = struct {
        origin_instance_id: []u8,
        origin_sequence: u64,

        fn deinit(self: *const CheckpointFrontierRow, allocator: std.mem.Allocator) void {
            allocator.free(self.origin_instance_id);
        }
    };

    const CheckpointTombstoneRow = struct {
        kind: []const u8,
        key: []u8,
        timestamp_ms: i64,
        origin_instance_id: []u8,
        origin_sequence: u64,

        fn deinit(self: *const CheckpointTombstoneRow, allocator: std.mem.Allocator) void {
            allocator.free(self.key);
            allocator.free(self.origin_instance_id);
        }
    };

    fn appendCheckpointMetaLine(
        allocator: std.mem.Allocator,
        out: *std.ArrayListUnmanaged(u8),
        last_sequence: u64,
        last_timestamp_ms: i64,
    ) !void {
        try out.append(allocator, '{');
        try json_util.appendJsonKeyValue(out, allocator, "kind", "meta");
        try out.append(allocator, ',');
        try json_util.appendJsonInt(out, allocator, "schema_version", 1);
        try out.append(allocator, ',');
        try json_util.appendJsonKey(out, allocator, "last_sequence");
        try out.writer(allocator).print("{d}", .{last_sequence});
        try out.append(allocator, ',');
        try json_util.appendJsonKey(out, allocator, "last_timestamp_ms");
        try out.writer(allocator).print("{d}", .{last_timestamp_ms});
        try out.append(allocator, ',');
        try json_util.appendJsonKey(out, allocator, "compacted_through_sequence");
        try out.writer(allocator).print("{d}", .{last_sequence});
        try out.appendSlice(allocator, "}\n");
    }

    fn appendCheckpointFrontierLine(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), origin_instance_id: []const u8, origin_sequence: u64) !void {
        try out.append(allocator, '{');
        try json_util.appendJsonKeyValue(out, allocator, "kind", "frontier");
        try out.append(allocator, ',');
        try json_util.appendJsonKeyValue(out, allocator, "origin_instance_id", origin_instance_id);
        try out.append(allocator, ',');
        try json_util.appendJsonKey(out, allocator, "origin_sequence");
        try out.writer(allocator).print("{d}", .{origin_sequence});
        try out.appendSlice(allocator, "}\n");
    }

    fn appendCheckpointStateLine(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), row: CheckpointStateRow) !void {
        try out.append(allocator, '{');
        try json_util.appendJsonKeyValue(out, allocator, "kind", "state");
        try out.append(allocator, ',');
        try json_util.appendJsonKeyValue(out, allocator, "key", row.key);
        try out.append(allocator, ',');
        try json_util.appendJsonKey(out, allocator, "session_id");
        if (row.session_id) |sid| {
            try json_util.appendJsonString(out, allocator, sid);
        } else {
            try out.appendSlice(allocator, "null");
        }
        try out.append(allocator, ',');
        try json_util.appendJsonKeyValue(out, allocator, "category", row.category);
        try out.append(allocator, ',');
        try json_util.appendJsonKey(out, allocator, "value_kind");
        if (row.value_kind) |kind| {
            try json_util.appendJsonString(out, allocator, kind);
        } else {
            try out.appendSlice(allocator, "null");
        }
        try out.append(allocator, ',');
        try json_util.appendJsonKeyValue(out, allocator, "content", row.content);
        try out.append(allocator, ',');
        try json_util.appendJsonKey(out, allocator, "timestamp_ms");
        try out.writer(allocator).print("{d}", .{row.timestamp_ms});
        try out.append(allocator, ',');
        try json_util.appendJsonKeyValue(out, allocator, "origin_instance_id", row.origin_instance_id);
        try out.append(allocator, ',');
        try json_util.appendJsonKey(out, allocator, "origin_sequence");
        try out.writer(allocator).print("{d}", .{row.origin_sequence});
        try out.appendSlice(allocator, "}\n");
    }

    fn appendCheckpointTombstoneLine(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), row: CheckpointTombstoneRow) !void {
        try out.append(allocator, '{');
        try json_util.appendJsonKeyValue(out, allocator, "kind", row.kind);
        try out.append(allocator, ',');
        try json_util.appendJsonKeyValue(out, allocator, "key", row.key);
        try out.append(allocator, ',');
        try json_util.appendJsonKey(out, allocator, "timestamp_ms");
        try out.writer(allocator).print("{d}", .{row.timestamp_ms});
        try out.append(allocator, ',');
        try json_util.appendJsonKeyValue(out, allocator, "origin_instance_id", row.origin_instance_id);
        try out.append(allocator, ',');
        try json_util.appendJsonKey(out, allocator, "origin_sequence");
        try out.writer(allocator).print("{d}", .{row.origin_sequence});
        try out.appendSlice(allocator, "}\n");
    }

    fn exportCheckpointPayload(self: *Self, allocator: std.mem.Allocator) ![]u8 {
        var out: std.ArrayListUnmanaged(u8) = .empty;
        errdefer out.deinit(allocator);

        const last_sequence = try self.memory().lastEventSequence();
        const last_timestamp_ms = @max(
            try self.queryMaxI64("SELECT COALESCE(MAX(event_timestamp_ms), 0) FROM memories"),
            try self.queryMaxI64("SELECT COALESCE(MAX(timestamp_ms), 0) FROM memory_tombstones"),
        );
        try appendCheckpointMetaLine(allocator, &out, last_sequence, last_timestamp_ms);

        {
            const sql = "SELECT origin_instance_id, last_origin_sequence FROM memory_event_frontiers ORDER BY origin_instance_id ASC";
            var stmt: ?*c.sqlite3_stmt = null;
            var rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
            if (rc != c.SQLITE_OK) return error.PrepareFailed;
            defer _ = c.sqlite3_finalize(stmt);
            while (true) {
                rc = c.sqlite3_step(stmt);
                if (rc != c.SQLITE_ROW) break;
                const origin_instance_id = try dupeColumnText(stmt.?, 0, allocator);
                defer allocator.free(origin_instance_id);
                const origin_sequence: u64 = @intCast(@max(c.sqlite3_column_int64(stmt, 1), 0));
                try appendCheckpointFrontierLine(allocator, &out, origin_instance_id, origin_sequence);
            }
        }

        {
            const sql =
                "SELECT key, session_id, category, value_kind, content, event_timestamp_ms, event_origin_instance_id, event_origin_sequence " ++
                "FROM memories ORDER BY key ASC, COALESCE(session_id, '') ASC";
            var stmt: ?*c.sqlite3_stmt = null;
            var rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
            if (rc != c.SQLITE_OK) return error.PrepareFailed;
            defer _ = c.sqlite3_finalize(stmt);
            while (true) {
                rc = c.sqlite3_step(stmt);
                if (rc != c.SQLITE_ROW) break;
                const row = CheckpointStateRow{
                    .key = try dupeColumnText(stmt.?, 0, allocator),
                    .session_id = try dupeColumnTextNullable(stmt.?, 1, allocator),
                    .category = try dupeColumnText(stmt.?, 2, allocator),
                    .value_kind = try dupeColumnTextNullable(stmt.?, 3, allocator),
                    .content = try dupeColumnText(stmt.?, 4, allocator),
                    .timestamp_ms = c.sqlite3_column_int64(stmt, 5),
                    .origin_instance_id = try dupeColumnText(stmt.?, 6, allocator),
                    .origin_sequence = @intCast(@max(c.sqlite3_column_int64(stmt, 7), 0)),
                };
                defer row.deinit(allocator);
                try appendCheckpointStateLine(allocator, &out, row);
            }
        }

        {
            const sql =
                "SELECT key, scope, session_id, timestamp_ms, origin_instance_id, origin_sequence " ++
                "FROM memory_tombstones ORDER BY key ASC, scope ASC, COALESCE(session_id, '') ASC";
            var stmt: ?*c.sqlite3_stmt = null;
            var rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
            if (rc != c.SQLITE_OK) return error.PrepareFailed;
            defer _ = c.sqlite3_finalize(stmt);
            while (true) {
                rc = c.sqlite3_step(stmt);
                if (rc != c.SQLITE_ROW) break;
                const scope = try dupeColumnText(stmt.?, 1, allocator);
                defer allocator.free(scope);
                const logical_key = try dupeColumnText(stmt.?, 0, allocator);
                defer allocator.free(logical_key);
                const session_id = try dupeColumnTextNullable(stmt.?, 2, allocator);
                defer if (session_id) |sid| allocator.free(sid);
                const encoded_key = if (std.mem.eql(u8, scope, "all"))
                    try allocator.dupe(u8, logical_key)
                else
                    try key_codec.encode(allocator, logical_key, session_id);
                defer allocator.free(encoded_key);
                const row = CheckpointTombstoneRow{
                    .kind = if (std.mem.eql(u8, scope, "all")) "key_tombstone" else "scoped_tombstone",
                    .key = try allocator.dupe(u8, encoded_key),
                    .timestamp_ms = c.sqlite3_column_int64(stmt, 3),
                    .origin_instance_id = try dupeColumnText(stmt.?, 4, allocator),
                    .origin_sequence = @intCast(@max(c.sqlite3_column_int64(stmt, 5), 0)),
                };
                defer row.deinit(allocator);
                try appendCheckpointTombstoneLine(allocator, &out, row);
            }
        }

        return out.toOwnedSlice(allocator);
    }

    fn applyCheckpointPayload(self: *Self, payload: []const u8) !void {
        var last_sequence: u64 = 0;
        var compacted_through: u64 = 0;
        var saw_meta = false;
        var frontiers: std.ArrayListUnmanaged(CheckpointFrontierRow) = .empty;
        defer {
            for (frontiers.items) |*row| row.deinit(self.allocator);
            frontiers.deinit(self.allocator);
        }
        var states: std.ArrayListUnmanaged(CheckpointStateRow) = .empty;
        defer {
            for (states.items) |*row| row.deinit(self.allocator);
            states.deinit(self.allocator);
        }
        var tombstones: std.ArrayListUnmanaged(CheckpointTombstoneRow) = .empty;
        defer {
            for (tombstones.items) |*row| row.deinit(self.allocator);
            tombstones.deinit(self.allocator);
        }

        var lines = std.mem.splitScalar(u8, payload, '\n');
        while (lines.next()) |raw_line| {
            const line = std.mem.trim(u8, raw_line, " \t\r\n");
            if (line.len == 0) continue;

            var parsed = try std.json.parseFromSlice(std.json.Value, self.allocator, line, .{});
            defer parsed.deinit();

            const kind = checkpointJsonStringField(parsed.value, "kind") orelse return error.InvalidEvent;
            if (std.mem.eql(u8, kind, "meta")) {
                saw_meta = true;
                const schema_version = checkpointJsonUnsignedField(parsed.value, "schema_version") orelse return error.InvalidEvent;
                if (schema_version != 1) return error.InvalidEvent;
                last_sequence = checkpointJsonUnsignedField(parsed.value, "last_sequence") orelse 0;
                compacted_through = checkpointJsonUnsignedField(parsed.value, "compacted_through_sequence") orelse last_sequence;
                continue;
            }
            if (std.mem.eql(u8, kind, "frontier")) {
                const origin_instance_id = checkpointJsonStringField(parsed.value, "origin_instance_id") orelse return error.InvalidEvent;
                const origin_sequence = checkpointJsonUnsignedField(parsed.value, "origin_sequence") orelse return error.InvalidEvent;
                try frontiers.append(self.allocator, .{
                    .origin_instance_id = try self.allocator.dupe(u8, origin_instance_id),
                    .origin_sequence = origin_sequence,
                });
                continue;
            }
            if (std.mem.eql(u8, kind, "state")) {
                const key = checkpointJsonStringField(parsed.value, "key") orelse return error.InvalidEvent;
                const content = checkpointJsonStringField(parsed.value, "content") orelse return error.InvalidEvent;
                const category = checkpointJsonStringField(parsed.value, "category") orelse return error.InvalidEvent;
                const value_kind = checkpointJsonNullableStringField(parsed.value, "value_kind");
                const timestamp_ms = checkpointJsonIntegerField(parsed.value, "timestamp_ms") orelse return error.InvalidEvent;
                const origin_instance_id = checkpointJsonStringField(parsed.value, "origin_instance_id") orelse return error.InvalidEvent;
                const origin_sequence = checkpointJsonUnsignedField(parsed.value, "origin_sequence") orelse return error.InvalidEvent;
                if (value_kind) |kind_text| _ = MemoryValueKind.fromString(kind_text) orelse return error.InvalidEvent;
                try states.append(self.allocator, .{
                    .key = try self.allocator.dupe(u8, key),
                    .session_id = if (checkpointJsonNullableStringField(parsed.value, "session_id")) |sid|
                        try self.allocator.dupe(u8, sid)
                    else
                        null,
                    .category = try self.allocator.dupe(u8, category),
                    .value_kind = if (value_kind) |kind_text|
                        try self.allocator.dupe(u8, kind_text)
                    else
                        null,
                    .content = try self.allocator.dupe(u8, content),
                    .timestamp_ms = timestamp_ms,
                    .origin_instance_id = try self.allocator.dupe(u8, origin_instance_id),
                    .origin_sequence = origin_sequence,
                });
                continue;
            }
            if (std.mem.eql(u8, kind, "scoped_tombstone") or std.mem.eql(u8, kind, "key_tombstone")) {
                const key = checkpointJsonStringField(parsed.value, "key") orelse return error.InvalidEvent;
                const timestamp_ms = checkpointJsonIntegerField(parsed.value, "timestamp_ms") orelse return error.InvalidEvent;
                const origin_instance_id = checkpointJsonStringField(parsed.value, "origin_instance_id") orelse return error.InvalidEvent;
                const origin_sequence = checkpointJsonUnsignedField(parsed.value, "origin_sequence") orelse return error.InvalidEvent;
                try tombstones.append(self.allocator, .{
                    .kind = if (std.mem.eql(u8, kind, "key_tombstone")) "key_tombstone" else "scoped_tombstone",
                    .key = try self.allocator.dupe(u8, key),
                    .timestamp_ms = timestamp_ms,
                    .origin_instance_id = try self.allocator.dupe(u8, origin_instance_id),
                    .origin_sequence = origin_sequence,
                });
                continue;
            }
            return error.InvalidEvent;
        }

        if (!saw_meta) return error.InvalidEvent;

        var owns_tx = c.sqlite3_get_autocommit(self.db) != 0;
        if (owns_tx) owns_tx = try self.beginImmediate();
        var committed = false;
        errdefer if (owns_tx and !committed) self.rollbackTxn();

        try self.execSql("checkpoint import", "DELETE FROM memory_events;");
        try self.execSql("checkpoint import", "DELETE FROM memory_tombstones;");
        try self.execSql("checkpoint import", "DELETE FROM memory_event_frontiers;");
        try self.execSql("checkpoint import", "DELETE FROM memories;");
        try self.execSql("checkpoint import", "DELETE FROM memory_feed_meta;");

        for (frontiers.items) |row| {
            try self.insertCheckpointFrontier(row.origin_instance_id, row.origin_sequence);
        }
        for (states.items) |row| {
            try self.insertCheckpointState(
                row.key,
                row.session_id,
                row.category,
                row.value_kind,
                row.content,
                row.timestamp_ms,
                row.origin_instance_id,
                row.origin_sequence,
            );
        }
        for (tombstones.items) |row| {
            try self.insertCheckpointTombstone(
                row.kind,
                row.key,
                row.timestamp_ms,
                row.origin_instance_id,
                row.origin_sequence,
            );
        }
        try self.setCompactedThroughSequenceTx(compacted_through);
        if (owns_tx) try self.commitTxn();
        committed = true;
    }

    fn keyExistsAnyScope(self: *Self, key: []const u8) !bool {
        const sql = "SELECT 1 FROM memories WHERE key = ?1 LIMIT 1";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);
        _ = c.sqlite3_bind_text(stmt, 1, key.ptr, @intCast(key.len), SQLITE_STATIC);
        rc = c.sqlite3_step(stmt);
        return rc == c.SQLITE_ROW;
    }

    fn insertCheckpointFrontier(self: *Self, origin_instance_id: []const u8, origin_sequence: u64) !void {
        const sql = "INSERT INTO memory_event_frontiers (origin_instance_id, last_origin_sequence) VALUES (?1, ?2)";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);
        _ = c.sqlite3_bind_text(stmt, 1, origin_instance_id.ptr, @intCast(origin_instance_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_int64(stmt, 2, @intCast(origin_sequence));
        rc = c.sqlite3_step(stmt);
        if (rc != c.SQLITE_DONE) return error.StepFailed;
    }

    fn insertCheckpointState(
        self: *Self,
        key: []const u8,
        session_id: ?[]const u8,
        category: []const u8,
        value_kind: ?[]const u8,
        content: []const u8,
        timestamp_ms: i64,
        origin_instance_id: []const u8,
        origin_sequence: u64,
    ) !void {
        const now = try std.fmt.allocPrint(self.allocator, "{d}", .{@divTrunc(timestamp_ms, 1000)});
        defer self.allocator.free(now);
        const id = try generateId(self.allocator);
        defer self.allocator.free(id);

        const sql =
            "INSERT INTO memories (id, key, content, category, value_kind, session_id, event_timestamp_ms, event_origin_instance_id, event_origin_sequence, created_at, updated_at) " ++
            "VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);
        _ = c.sqlite3_bind_text(stmt, 1, id.ptr, @intCast(id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 2, key.ptr, @intCast(key.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 3, content.ptr, @intCast(content.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 4, category.ptr, @intCast(category.len), SQLITE_STATIC);
        bindNullableText(stmt, 5, value_kind);
        bindNullableText(stmt, 6, session_id);
        _ = c.sqlite3_bind_int64(stmt, 7, timestamp_ms);
        _ = c.sqlite3_bind_text(stmt, 8, origin_instance_id.ptr, @intCast(origin_instance_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_int64(stmt, 9, @intCast(origin_sequence));
        _ = c.sqlite3_bind_text(stmt, 10, now.ptr, @intCast(now.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 11, now.ptr, @intCast(now.len), SQLITE_STATIC);
        rc = c.sqlite3_step(stmt);
        if (rc != c.SQLITE_DONE) return error.StepFailed;
    }

    fn insertCheckpointTombstone(
        self: *Self,
        kind: []const u8,
        key: []const u8,
        timestamp_ms: i64,
        origin_instance_id: []const u8,
        origin_sequence: u64,
    ) !void {
        const scope = if (std.mem.eql(u8, kind, "key_tombstone")) "all" else "scoped";
        const decoded = if (std.mem.eql(u8, kind, "key_tombstone"))
            key_codec.DecodedVectorKey{ .logical_key = key, .session_id = null, .is_legacy = false }
        else
            key_codec.decode(key);
        if (std.mem.eql(u8, kind, "scoped_tombstone") and decoded.is_legacy) return error.InvalidEvent;
        const session_key = if (std.mem.eql(u8, kind, "key_tombstone")) "*" else sessionKeyFor(decoded.session_id);
        const session_id: ?[]const u8 = decoded.session_id;
        const sql =
            "INSERT INTO memory_tombstones (key, scope, session_key, session_id, timestamp_ms, origin_instance_id, origin_sequence) " ++
            "VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);
        _ = c.sqlite3_bind_text(stmt, 1, decoded.logical_key.ptr, @intCast(decoded.logical_key.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 2, scope.ptr, @intCast(scope.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 3, session_key.ptr, @intCast(session_key.len), SQLITE_STATIC);
        bindNullableText(stmt, 4, session_id);
        _ = c.sqlite3_bind_int64(stmt, 5, timestamp_ms);
        _ = c.sqlite3_bind_text(stmt, 6, origin_instance_id.ptr, @intCast(origin_instance_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_int64(stmt, 7, @intCast(origin_sequence));
        rc = c.sqlite3_step(stmt);
        if (rc != c.SQLITE_DONE) return error.StepFailed;
    }

    fn checkpointJsonStringField(val: std.json.Value, key: []const u8) ?[]const u8 {
        if (val != .object) return null;
        const field = val.object.get(key) orelse return null;
        return if (field == .string) field.string else null;
    }

    fn checkpointJsonNullableStringField(val: std.json.Value, key: []const u8) ?[]const u8 {
        if (val != .object) return null;
        const field = val.object.get(key) orelse return null;
        if (field == .null) return null;
        return if (field == .string) field.string else null;
    }

    fn checkpointJsonIntegerField(val: std.json.Value, key: []const u8) ?i64 {
        if (val != .object) return null;
        const field = val.object.get(key) orelse return null;
        return switch (field) {
            .integer => field.integer,
            else => null,
        };
    }

    fn checkpointJsonUnsignedField(val: std.json.Value, key: []const u8) ?u64 {
        const value = checkpointJsonIntegerField(val, key) orelse return null;
        if (value < 0) return null;
        return @intCast(value);
    }

    fn implCount(ptr: *anyopaque) anyerror!usize {
        const self_: *Self = @ptrCast(@alignCast(ptr));

        const sql = "SELECT COUNT(*) FROM memories";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self_.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_ROW) {
            const count = c.sqlite3_column_int64(stmt, 0);
            return @intCast(count);
        }
        return 0;
    }

    fn implHealthCheck(ptr: *anyopaque) bool {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        var err_msg: [*c]u8 = null;
        const rc = c.sqlite3_exec(self_.db, "SELECT 1", null, null, &err_msg);
        if (err_msg) |msg| c.sqlite3_free(msg);
        return rc == c.SQLITE_OK;
    }

    fn implDeinit(ptr: *anyopaque) void {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        self_.deinit();
        if (self_.owns_self) {
            self_.allocator.destroy(self_);
        }
    }

    pub const vtable = Memory.VTable{
        .name = &implName,
        .store = &implStore,
        .recall = &implRecall,
        .get = &implGet,
        .getScoped = &implGetScoped,
        .list = &implList,
        .forget = &implForget,
        .forgetScoped = &implForgetScoped,
        .listEvents = &implListEvents,
        .applyEvent = &implApplyEvent,
        .lastEventSequence = &implLastEventSequence,
        .eventFeedInfo = &implEventFeedInfo,
        .compactEvents = &implCompactEvents,
        .exportCheckpoint = &implExportCheckpoint,
        .applyCheckpoint = &implApplyCheckpoint,
        .count = &implCount,
        .healthCheck = &implHealthCheck,
        .deinit = &implDeinit,
    };

    pub fn memory(self: *Self) Memory {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    // ── Legacy helpers ─────────────────────────────────────────────

    pub fn saveMessage(self: *Self, session_id: []const u8, role_str: []const u8, content: []const u8) !void {
        const sql = "INSERT INTO messages (session_id, role, content) VALUES (?, ?, ?)";
        var stmt: ?*c.sqlite3_stmt = null;
        const rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, session_id.ptr, @intCast(session_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 2, role_str.ptr, @intCast(role_str.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 3, content.ptr, @intCast(content.len), SQLITE_STATIC);

        if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.StepFailed;
    }

    /// A single persisted message entry (role + content).
    pub const MessageEntry = root.MessageEntry;

    /// Load all messages for a session, ordered by creation time.
    /// Caller owns the returned slice and all strings within it.
    pub fn loadMessages(self: *Self, allocator: std.mem.Allocator, session_id: []const u8) ![]MessageEntry {
        const sql = "SELECT role, content FROM messages WHERE session_id = ? ORDER BY id ASC";
        var stmt: ?*c.sqlite3_stmt = null;
        const rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, session_id.ptr, @intCast(session_id.len), SQLITE_STATIC);

        var list: std.ArrayListUnmanaged(MessageEntry) = .empty;
        errdefer {
            for (list.items) |entry| {
                allocator.free(entry.role);
                allocator.free(entry.content);
            }
            list.deinit(allocator);
        }

        while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            const role_ptr = c.sqlite3_column_text(stmt, 0);
            const role_len: usize = @intCast(c.sqlite3_column_bytes(stmt, 0));
            const content_ptr = c.sqlite3_column_text(stmt, 1);
            const content_len: usize = @intCast(c.sqlite3_column_bytes(stmt, 1));

            if (role_ptr == null or content_ptr == null) continue;

            try list.append(allocator, .{
                .role = try allocator.dupe(u8, role_ptr[0..role_len]),
                .content = try allocator.dupe(u8, content_ptr[0..content_len]),
            });
        }

        return list.toOwnedSlice(allocator);
    }

    /// Delete all messages for a session.
    pub fn clearMessages(self: *Self, session_id: []const u8) !void {
        const sql = "DELETE FROM messages WHERE session_id = ?";
        var stmt: ?*c.sqlite3_stmt = null;
        const rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, session_id.ptr, @intCast(session_id.len), SQLITE_STATIC);
        if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.StepFailed;

        try self.clearUsage(session_id);
    }

    pub fn saveUsage(self: *Self, session_id: []const u8, total_tokens: u64) !void {
        const sql =
            "INSERT INTO session_usage (session_id, total_tokens, updated_at) VALUES (?1, ?2, datetime('now')) " ++
            "ON CONFLICT(session_id) DO UPDATE SET total_tokens = excluded.total_tokens, updated_at = datetime('now')";
        var stmt: ?*c.sqlite3_stmt = null;
        const rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, session_id.ptr, @intCast(session_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_int64(stmt, 2, @intCast(total_tokens));
        if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.StepFailed;
    }

    pub fn loadUsage(self: *Self, session_id: []const u8) !?u64 {
        const sql = "SELECT total_tokens FROM session_usage WHERE session_id = ?1";
        var stmt: ?*c.sqlite3_stmt = null;
        const rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, session_id.ptr, @intCast(session_id.len), SQLITE_STATIC);
        if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return null;
        const total = c.sqlite3_column_int64(stmt, 0);
        if (total < 0) return 0;
        return @intCast(total);
    }

    fn clearUsage(self: *Self, session_id: []const u8) !void {
        const sql = "DELETE FROM session_usage WHERE session_id = ?1";
        var stmt: ?*c.sqlite3_stmt = null;
        const rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, session_id.ptr, @intCast(session_id.len), SQLITE_STATIC);
        if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.StepFailed;
    }

    /// Delete auto-saved memory entries (autosave_user_*, autosave_assistant_*).
    /// If `session_id` is provided, only entries for that session are removed.
    /// If `session_id` is null, entries are removed globally.
    pub fn clearAutoSaved(self: *Self, session_id: ?[]const u8) !void {
        const sql_scoped = "DELETE FROM memories WHERE key LIKE 'autosave_%' AND session_id = ?1";
        const sql_global = "DELETE FROM memories WHERE key LIKE 'autosave_%'";
        const sql = if (session_id != null) sql_scoped else sql_global;

        var stmt: ?*c.sqlite3_stmt = null;
        const rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        if (session_id) |sid| {
            _ = c.sqlite3_bind_text(stmt, 1, sid.ptr, @intCast(sid.len), SQLITE_STATIC);
        }

        if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.StepFailed;
    }

    // ── History queries ──────────────────────────────────────────────

    pub fn countSessions(self: *Self) !u64 {
        const sql =
            "SELECT COUNT(*) FROM (SELECT 1 FROM messages WHERE role <> '" ++ root.RUNTIME_COMMAND_ROLE ++ "' GROUP BY session_id)";
        var stmt: ?*c.sqlite3_stmt = null;
        const rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return 0;
        const total = c.sqlite3_column_int64(stmt, 0);
        if (total < 0) return 0;
        return @intCast(total);
    }

    /// List sessions with message counts and time bounds.
    pub fn listSessions(self: *Self, allocator: std.mem.Allocator, limit: usize, offset: usize) ![]root.SessionInfo {
        const sql =
            "SELECT session_id, COUNT(*) as msg_count, MIN(created_at) as first_at, MAX(created_at) as last_at " ++
            "FROM messages WHERE role <> '" ++ root.RUNTIME_COMMAND_ROLE ++ "' " ++
            "GROUP BY session_id ORDER BY MAX(created_at) DESC LIMIT ?1 OFFSET ?2";
        var stmt: ?*c.sqlite3_stmt = null;
        const rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_int64(stmt, 1, @intCast(limit));
        _ = c.sqlite3_bind_int64(stmt, 2, @intCast(offset));

        var list: std.ArrayListUnmanaged(root.SessionInfo) = .empty;
        errdefer {
            for (list.items) |info| info.deinit(allocator);
            list.deinit(allocator);
        }

        while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            const sid_ptr = c.sqlite3_column_text(stmt, 0);
            const sid_len: usize = @intCast(c.sqlite3_column_bytes(stmt, 0));
            const count = c.sqlite3_column_int64(stmt, 1);
            const first_ptr = c.sqlite3_column_text(stmt, 2);
            const first_len: usize = @intCast(c.sqlite3_column_bytes(stmt, 2));
            const last_ptr = c.sqlite3_column_text(stmt, 3);
            const last_len: usize = @intCast(c.sqlite3_column_bytes(stmt, 3));

            if (sid_ptr == null) continue;

            try list.append(allocator, .{
                .session_id = try allocator.dupe(u8, sid_ptr[0..sid_len]),
                .message_count = if (count < 0) 0 else @intCast(count),
                .first_message_at = if (first_ptr) |p| try allocator.dupe(u8, p[0..first_len]) else try allocator.dupe(u8, ""),
                .last_message_at = if (last_ptr) |p| try allocator.dupe(u8, p[0..last_len]) else try allocator.dupe(u8, ""),
            });
        }

        return list.toOwnedSlice(allocator);
    }

    pub fn countDetailedMessages(self: *Self, session_id: []const u8) !u64 {
        const sql = "SELECT COUNT(*) FROM messages WHERE session_id = ?1 AND role <> '" ++ root.RUNTIME_COMMAND_ROLE ++ "'";
        var stmt: ?*c.sqlite3_stmt = null;
        const rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, session_id.ptr, @intCast(session_id.len), SQLITE_STATIC);
        if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return 0;
        const total = c.sqlite3_column_int64(stmt, 0);
        if (total < 0) return 0;
        return @intCast(total);
    }

    /// Load messages with timestamps for a session.
    pub fn loadMessagesDetailed(self: *Self, allocator: std.mem.Allocator, session_id: []const u8, limit: usize, offset: usize) ![]root.DetailedMessageEntry {
        const sql =
            "SELECT role, content, created_at FROM messages " ++
            "WHERE session_id = ?1 AND role <> '" ++ root.RUNTIME_COMMAND_ROLE ++ "' " ++
            "ORDER BY id ASC LIMIT ?2 OFFSET ?3";
        var stmt: ?*c.sqlite3_stmt = null;
        const rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, session_id.ptr, @intCast(session_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_int64(stmt, 2, @intCast(limit));
        _ = c.sqlite3_bind_int64(stmt, 3, @intCast(offset));

        var list: std.ArrayListUnmanaged(root.DetailedMessageEntry) = .empty;
        errdefer {
            for (list.items) |entry| {
                allocator.free(entry.role);
                allocator.free(entry.content);
                allocator.free(entry.created_at);
            }
            list.deinit(allocator);
        }

        while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            const role_ptr = c.sqlite3_column_text(stmt, 0);
            const role_len: usize = @intCast(c.sqlite3_column_bytes(stmt, 0));
            const content_ptr = c.sqlite3_column_text(stmt, 1);
            const content_len: usize = @intCast(c.sqlite3_column_bytes(stmt, 1));
            const ts_ptr = c.sqlite3_column_text(stmt, 2);
            const ts_len: usize = @intCast(c.sqlite3_column_bytes(stmt, 2));

            if (role_ptr == null or content_ptr == null) continue;

            try list.append(allocator, .{
                .role = try allocator.dupe(u8, role_ptr[0..role_len]),
                .content = try allocator.dupe(u8, content_ptr[0..content_len]),
                .created_at = if (ts_ptr) |p| try allocator.dupe(u8, p[0..ts_len]) else try allocator.dupe(u8, ""),
            });
        }

        return list.toOwnedSlice(allocator);
    }

    // ── SessionStore vtable ────────────────────────────────────────

    fn implSessionSaveMessage(ptr: *anyopaque, session_id: []const u8, role: []const u8, content: []const u8) anyerror!void {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        return self_.saveMessage(session_id, role, content);
    }

    fn implSessionLoadMessages(ptr: *anyopaque, allocator: std.mem.Allocator, session_id: []const u8) anyerror![]root.MessageEntry {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        return self_.loadMessages(allocator, session_id);
    }

    fn implSessionClearMessages(ptr: *anyopaque, session_id: []const u8) anyerror!void {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        return self_.clearMessages(session_id);
    }

    fn implSessionClearAutoSaved(ptr: *anyopaque, session_id: ?[]const u8) anyerror!void {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        return self_.clearAutoSaved(session_id);
    }

    fn implSessionSaveUsage(ptr: *anyopaque, session_id: []const u8, total_tokens: u64) anyerror!void {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        return self_.saveUsage(session_id, total_tokens);
    }

    fn implSessionLoadUsage(ptr: *anyopaque, session_id: []const u8) anyerror!?u64 {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        return self_.loadUsage(session_id);
    }

    fn implSessionCountSessions(ptr: *anyopaque) anyerror!u64 {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        return self_.countSessions();
    }

    fn implSessionListSessions(ptr: *anyopaque, allocator: std.mem.Allocator, limit: usize, offset: usize) anyerror![]root.SessionInfo {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        return self_.listSessions(allocator, limit, offset);
    }

    fn implSessionCountDetailedMessages(ptr: *anyopaque, session_id: []const u8) anyerror!u64 {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        return self_.countDetailedMessages(session_id);
    }

    fn implSessionLoadMessagesDetailed(ptr: *anyopaque, allocator: std.mem.Allocator, session_id: []const u8, limit: usize, offset: usize) anyerror![]root.DetailedMessageEntry {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        return self_.loadMessagesDetailed(allocator, session_id, limit, offset);
    }

    const session_vtable = root.SessionStore.VTable{
        .saveMessage = &implSessionSaveMessage,
        .loadMessages = &implSessionLoadMessages,
        .clearMessages = &implSessionClearMessages,
        .clearAutoSaved = &implSessionClearAutoSaved,
        .saveUsage = &implSessionSaveUsage,
        .loadUsage = &implSessionLoadUsage,
        .countSessions = &implSessionCountSessions,
        .listSessions = &implSessionListSessions,
        .countDetailedMessages = &implSessionCountDetailedMessages,
        .loadMessagesDetailed = &implSessionLoadMessagesDetailed,
    };

    pub fn sessionStore(self: *Self) root.SessionStore {
        return .{ .ptr = @ptrCast(self), .vtable = &session_vtable };
    }

    pub fn reindex(self: *Self) !void {
        var err_msg: [*c]u8 = null;
        const rc = c.sqlite3_exec(
            self.db,
            "INSERT INTO memories_fts(memories_fts) VALUES('rebuild');",
            null,
            null,
            &err_msg,
        );
        if (rc != c.SQLITE_OK) {
            if (err_msg) |msg| c.sqlite3_free(msg);
            return error.StepFailed;
        }
    }

    // ── Internal search helpers ────────────────────────────────────

    fn fts5Search(self_: *Self, allocator: std.mem.Allocator, query: []const u8, limit: usize, session_id: ?[]const u8) ![]MemoryEntry {
        // Build FTS5 query: wrap each word in quotes joined by OR
        var fts_query: std.ArrayList(u8) = .empty;
        defer fts_query.deinit(allocator);

        var iter = std.mem.tokenizeAny(u8, query, " \t\n\r");
        var first = true;
        while (iter.next()) |word| {
            if (!first) {
                try fts_query.appendSlice(allocator, " OR ");
            }
            try fts_query.append(allocator, '"');
            for (word) |ch_byte| {
                if (ch_byte == '"') {
                    try fts_query.appendSlice(allocator, "\"\"");
                } else {
                    try fts_query.append(allocator, ch_byte);
                }
            }
            try fts_query.append(allocator, '"');
            first = false;
        }

        if (fts_query.items.len == 0) return allocator.alloc(MemoryEntry, 0);

        const sql =
            "SELECT m.id, m.key, m.content, m.category, m.created_at, bm25(memories_fts) as score, m.session_id " ++
            "FROM memories_fts f " ++
            "JOIN memories m ON m.rowid = f.rowid " ++
            "WHERE memories_fts MATCH ?1 " ++
            "ORDER BY score " ++
            "LIMIT ?2";

        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self_.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return allocator.alloc(MemoryEntry, 0);
        defer _ = c.sqlite3_finalize(stmt);

        // Null-terminate the FTS query for sqlite
        try fts_query.append(allocator, 0);
        const fts_z = fts_query.items[0 .. fts_query.items.len - 1];
        _ = c.sqlite3_bind_text(stmt, 1, fts_z.ptr, @intCast(fts_z.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_int64(stmt, 2, @intCast(limit));

        var entries: std.ArrayList(MemoryEntry) = .empty;
        errdefer {
            for (entries.items) |*entry| entry.deinit(allocator);
            entries.deinit(allocator);
        }

        while (true) {
            rc = c.sqlite3_step(stmt);
            if (rc == c.SQLITE_ROW) {
                const score_raw = c.sqlite3_column_double(stmt.?, 5);
                var entry = try readEntryFromRowWithSessionCol(stmt.?, allocator, 6);
                entry.score = -score_raw; // BM25 returns negative (lower = better)
                // Filter by session_id if requested
                if (session_id) |sid| {
                    if (entry.session_id == null or !std.mem.eql(u8, entry.session_id.?, sid)) {
                        entry.deinit(allocator);
                        continue;
                    }
                }
                try entries.append(allocator, entry);
            } else break;
        }

        return entries.toOwnedSlice(allocator);
    }

    fn likeSearch(self_: *Self, allocator: std.mem.Allocator, query: []const u8, limit: usize, session_id: ?[]const u8) ![]MemoryEntry {
        var keywords: std.ArrayList([]const u8) = .empty;
        defer keywords.deinit(allocator);

        var iter = std.mem.tokenizeAny(u8, query, " \t\n\r");
        while (iter.next()) |word| {
            try keywords.append(allocator, word);
        }

        if (keywords.items.len == 0) return allocator.alloc(MemoryEntry, 0);

        var sql_buf: std.ArrayList(u8) = .empty;
        defer sql_buf.deinit(allocator);

        try sql_buf.appendSlice(allocator, "SELECT id, key, content, category, created_at, session_id FROM memories WHERE ");

        for (keywords.items, 0..) |_, i| {
            if (i > 0) try sql_buf.appendSlice(allocator, " OR ");
            try sql_buf.appendSlice(allocator, "(content LIKE ?");
            try appendInt(&sql_buf, allocator, i * 2 + 1);
            try sql_buf.appendSlice(allocator, " ESCAPE '\\' OR key LIKE ?");
            try appendInt(&sql_buf, allocator, i * 2 + 2);
            try sql_buf.appendSlice(allocator, " ESCAPE '\\')");
        }

        try sql_buf.appendSlice(allocator, " ORDER BY updated_at DESC LIMIT ?");
        try appendInt(&sql_buf, allocator, keywords.items.len * 2 + 1);
        try sql_buf.append(allocator, 0);

        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self_.db, sql_buf.items.ptr, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return allocator.alloc(MemoryEntry, 0);
        defer _ = c.sqlite3_finalize(stmt);

        var like_bufs: std.ArrayList([]u8) = .empty;
        defer {
            for (like_bufs.items) |buf| allocator.free(buf);
            like_bufs.deinit(allocator);
        }

        for (keywords.items, 0..) |word, i| {
            const like = try escapeLikePattern(allocator, word);
            try like_bufs.append(allocator, like);
            _ = c.sqlite3_bind_text(stmt, @intCast(i * 2 + 1), like.ptr, @intCast(like.len), SQLITE_STATIC);
            _ = c.sqlite3_bind_text(stmt, @intCast(i * 2 + 2), like.ptr, @intCast(like.len), SQLITE_STATIC);
        }
        _ = c.sqlite3_bind_int64(stmt, @intCast(keywords.items.len * 2 + 1), @intCast(limit));

        var entries: std.ArrayList(MemoryEntry) = .empty;
        errdefer {
            for (entries.items) |*entry| entry.deinit(allocator);
            entries.deinit(allocator);
        }

        while (true) {
            rc = c.sqlite3_step(stmt);
            if (rc == c.SQLITE_ROW) {
                var entry = try readEntryFromRow(stmt.?, allocator);
                entry.score = 1.0;
                // Filter by session_id if requested
                if (session_id) |sid| {
                    if (entry.session_id == null or !std.mem.eql(u8, entry.session_id.?, sid)) {
                        entry.deinit(allocator);
                        continue;
                    }
                }
                try entries.append(allocator, entry);
            } else break;
        }

        return entries.toOwnedSlice(allocator);
    }

    // ── Utility functions ──────────────────────────────────────────

    fn parseCategoryOwned(allocator: std.mem.Allocator, value: []const u8) !MemoryCategory {
        if (std.mem.eql(u8, value, "core")) return .core;
        if (std.mem.eql(u8, value, "daily")) return .daily;
        if (std.mem.eql(u8, value, "conversation")) return .conversation;
        return .{ .custom = try allocator.dupe(u8, value) };
    }

    fn readEventFromRow(stmt: *c.sqlite3_stmt, allocator: std.mem.Allocator) !MemoryEvent {
        const schema_version_raw = c.sqlite3_column_int64(stmt, 1);
        if (schema_version_raw <= 0) return error.InvalidEvent;
        const schema_version: u32 = @intCast(schema_version_raw);
        const operation_str = try dupeColumnText(stmt, 5, allocator);
        defer allocator.free(operation_str);
        const operation = MemoryEventOp.fromString(operation_str) orelse return error.InvalidEvent;

        const category_text = try dupeColumnTextNullable(stmt, 8, allocator);
        errdefer if (category_text) |text| allocator.free(text);
        const category = if (category_text) |text|
            try parseCategoryOwned(allocator, text)
        else
            null;
        if (category_text) |text| allocator.free(text);
        errdefer if (category) |value| switch (value) {
            .custom => |name| allocator.free(name),
            else => {},
        };
        const value_kind_text = try dupeColumnTextNullable(stmt, 9, allocator);
        defer if (value_kind_text) |text| allocator.free(text);

        return .{
            .schema_version = schema_version,
            .sequence = @intCast(@max(c.sqlite3_column_int64(stmt, 0), 0)),
            .origin_instance_id = try dupeColumnText(stmt, 2, allocator),
            .origin_sequence = @intCast(@max(c.sqlite3_column_int64(stmt, 3), 0)),
            .timestamp_ms = c.sqlite3_column_int64(stmt, 4),
            .operation = operation,
            .key = try dupeColumnText(stmt, 6, allocator),
            .session_id = try dupeColumnTextNullable(stmt, 7, allocator),
            .category = category,
            .value_kind = if (value_kind_text) |text|
                MemoryValueKind.fromString(text) orelse return error.InvalidEvent
            else
                null,
            .content = try dupeColumnTextNullable(stmt, 10, allocator),
        };
    }

    fn readEntryFromRow(stmt: *c.sqlite3_stmt, allocator: std.mem.Allocator) !MemoryEntry {
        return readEntryFromRowWithSessionCol(stmt, allocator, 5);
    }

    fn readEntryFromRowWithSessionCol(stmt: *c.sqlite3_stmt, allocator: std.mem.Allocator, session_col: c_int) !MemoryEntry {
        const id = try dupeColumnText(stmt, 0, allocator);
        errdefer allocator.free(id);
        const key = try dupeColumnText(stmt, 1, allocator);
        errdefer allocator.free(key);
        const content = try dupeColumnText(stmt, 2, allocator);
        errdefer allocator.free(content);
        const cat_str = try dupeColumnText(stmt, 3, allocator);
        errdefer allocator.free(cat_str);
        const timestamp = try dupeColumnText(stmt, 4, allocator);
        errdefer allocator.free(timestamp);
        const sid = try dupeColumnTextNullable(stmt, session_col, allocator);
        errdefer if (sid) |s| allocator.free(s);

        const category = blk: {
            const parsed = try parseCategoryOwned(allocator, cat_str);
            allocator.free(cat_str);
            break :blk parsed;
        };

        return MemoryEntry{
            .id = id,
            .key = key,
            .content = content,
            .category = category,
            .timestamp = timestamp,
            .session_id = sid,
            .score = null,
        };
    }

    fn dupeColumnText(stmt: *c.sqlite3_stmt, col: c_int, allocator: std.mem.Allocator) ![]u8 {
        const raw = c.sqlite3_column_text(stmt, col);
        const len: usize = @intCast(c.sqlite3_column_bytes(stmt, col));
        if (raw == null or len == 0) {
            return allocator.dupe(u8, "");
        }
        const slice: []const u8 = @as([*]const u8, @ptrCast(raw))[0..len];
        return allocator.dupe(u8, slice);
    }

    /// Like dupeColumnText but returns null when the column value is SQL NULL.
    fn dupeColumnTextNullable(stmt: *c.sqlite3_stmt, col: c_int, allocator: std.mem.Allocator) !?[]u8 {
        if (c.sqlite3_column_type(stmt, col) == c.SQLITE_NULL) {
            return null;
        }
        const raw = c.sqlite3_column_text(stmt, col);
        const len: usize = @intCast(c.sqlite3_column_bytes(stmt, col));
        if (raw == null) {
            return null;
        }
        const slice: []const u8 = @as([*]const u8, @ptrCast(raw))[0..len];
        return try allocator.dupe(u8, slice);
    }

    /// Escape SQL LIKE wildcards (% and _) in user input, then wrap with %...%.
    /// Uses backslash as escape char (paired with ESCAPE '\' in the query).
    fn escapeLikePattern(allocator: std.mem.Allocator, word: []const u8) ![]u8 {
        var buf: std.ArrayList(u8) = .empty;
        errdefer buf.deinit(allocator);
        try buf.append(allocator, '%');
        for (word) |ch| {
            if (ch == '%' or ch == '_' or ch == '\\') {
                try buf.append(allocator, '\\');
            }
            try buf.append(allocator, ch);
        }
        try buf.append(allocator, '%');
        return buf.toOwnedSlice(allocator);
    }

    fn appendInt(buf: *std.ArrayList(u8), allocator: std.mem.Allocator, value: usize) !void {
        var tmp: [20]u8 = undefined;
        const s = std.fmt.bufPrint(&tmp, "{d}", .{value}) catch return error.PrepareFailed;
        try buf.appendSlice(allocator, s);
    }

    fn getNowTimestamp(allocator: std.mem.Allocator) ![]u8 {
        const ts = std.time.timestamp();
        return std.fmt.allocPrint(allocator, "{d}", .{ts});
    }

    fn generateId(allocator: std.mem.Allocator) ![]u8 {
        const ts = std.time.nanoTimestamp();
        var buf: [16]u8 = undefined;
        std.crypto.random.bytes(&buf);
        const rand_hi = std.mem.readInt(u64, buf[0..8], .little);
        const rand_lo = std.mem.readInt(u64, buf[8..16], .little);
        return std.fmt.allocPrint(allocator, "{d}-{x}-{x}", .{ ts, rand_hi, rand_lo });
    }
};

// ── Tests ──────────────────────────────────────────────────────────

test "mountinfo parser decodes escaped mount point and picks network fs" {
    const path = "/mnt/My Drive/work/memory.db";
    const root_line = "24 1 0:21 / / rw,relatime - ext4 /dev/root rw";
    const share_line = "36 24 0:31 / /mnt/My\\040Drive rw,relatime - 9p drvfs rw";

    var best_len: usize = 0;
    var best_is_network = false;
    parseMountinfoLine(root_line, path, &best_len, &best_is_network);
    parseMountinfoLine(share_line, path, &best_len, &best_is_network);

    try std.testing.expect(best_len > 1);
    try std.testing.expect(best_is_network);
}

test "mountinfo parser enforces directory boundary on prefix matches" {
    const line = "36 24 0:31 / /mnt/share rw,relatime - 9p drvfs rw";
    const path = "/mnt/share2/memory.db";

    var best_len: usize = 0;
    var best_is_network = false;
    parseMountinfoLine(line, path, &best_len, &best_is_network);

    try std.testing.expectEqual(@as(usize, 0), best_len);
    try std.testing.expect(!best_is_network);
}

test "sqlite memory init with in-memory db" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    try mem.saveMessage("test-session", "user", "hello");
}

test "sqlite init configures busy timeout" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();

    var stmt: ?*c.sqlite3_stmt = null;
    const prep_rc = c.sqlite3_prepare_v2(mem.db, "PRAGMA busy_timeout;", -1, &stmt, null);
    try std.testing.expectEqual(@as(c_int, c.SQLITE_OK), prep_rc);
    defer _ = c.sqlite3_finalize(stmt);

    const step_rc = c.sqlite3_step(stmt);
    try std.testing.expectEqual(@as(c_int, c.SQLITE_ROW), step_rc);
    const timeout_ms = c.sqlite3_column_int(stmt, 0);
    try std.testing.expect(timeout_ms >= BUSY_TIMEOUT_MS);
}

test "sqlite name" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();
    try std.testing.expectEqualStrings("sqlite", m.name());
}

test "sqlite health check" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();
    try std.testing.expect(m.healthCheck());
}

test "sqlite store and get" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("user_lang", "Prefers Zig", .core, null);

    const entry = try m.get(std.testing.allocator, "user_lang");
    try std.testing.expect(entry != null);
    defer entry.?.deinit(std.testing.allocator);

    try std.testing.expectEqualStrings("user_lang", entry.?.key);
    try std.testing.expectEqualStrings("Prefers Zig", entry.?.content);
    try std.testing.expect(entry.?.category.eql(.core));
}

test "sqlite store upsert" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("pref", "likes Zig", .core, null);
    try m.store("pref", "loves Zig", .core, null);

    const entry = try m.get(std.testing.allocator, "pref");
    try std.testing.expect(entry != null);
    defer entry.?.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("loves Zig", entry.?.content);

    const cnt = try m.count();
    try std.testing.expectEqual(@as(usize, 1), cnt);
}

test "sqlite recall keyword" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("a", "Zig is fast and safe", .core, null);
    try m.store("b", "Python is interpreted", .core, null);
    try m.store("c", "Zig has comptime", .core, null);

    const results = try m.recall(std.testing.allocator, "Zig", 10, null);
    defer root.freeEntries(std.testing.allocator, results);

    try std.testing.expectEqual(@as(usize, 2), results.len);
    for (results) |entry| {
        try std.testing.expect(std.mem.indexOf(u8, entry.content, "Zig") != null);
    }
}

test "sqlite recall no match" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("a", "Zig rocks", .core, null);

    const results = try m.recall(std.testing.allocator, "javascript", 10, null);
    defer root.freeEntries(std.testing.allocator, results);

    try std.testing.expectEqual(@as(usize, 0), results.len);
}

test "sqlite recall empty query" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("a", "data", .core, null);

    const results = try m.recall(std.testing.allocator, "", 10, null);
    defer root.freeEntries(std.testing.allocator, results);
    try std.testing.expectEqual(@as(usize, 0), results.len);
}

test "sqlite recall whitespace query" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("a", "data", .core, null);

    const results = try m.recall(std.testing.allocator, "   ", 10, null);
    defer root.freeEntries(std.testing.allocator, results);
    try std.testing.expectEqual(@as(usize, 0), results.len);
}

test "sqlite forget" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("temp", "temporary data", .conversation, null);
    try std.testing.expectEqual(@as(usize, 1), try m.count());

    const removed = try m.forget("temp");
    try std.testing.expect(removed);
    try std.testing.expectEqual(@as(usize, 0), try m.count());
}

test "sqlite forget nonexistent" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    const removed = try m.forget("nope");
    try std.testing.expect(!removed);
}

test "sqlite list all" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("a", "one", .core, null);
    try m.store("b", "two", .daily, null);
    try m.store("c", "three", .conversation, null);

    const all = try m.list(std.testing.allocator, null, null);
    defer root.freeEntries(std.testing.allocator, all);
    try std.testing.expectEqual(@as(usize, 3), all.len);
}

test "sqlite list by category" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("a", "core1", .core, null);
    try m.store("b", "core2", .core, null);
    try m.store("c", "daily1", .daily, null);

    const core_list = try m.list(std.testing.allocator, .core, null);
    defer root.freeEntries(std.testing.allocator, core_list);
    try std.testing.expectEqual(@as(usize, 2), core_list.len);

    const daily_list = try m.list(std.testing.allocator, .daily, null);
    defer root.freeEntries(std.testing.allocator, daily_list);
    try std.testing.expectEqual(@as(usize, 1), daily_list.len);
}

test "sqlite count empty" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();
    try std.testing.expectEqual(@as(usize, 0), try m.count());
}

test "sqlite get nonexistent" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    const entry = try m.get(std.testing.allocator, "nope");
    try std.testing.expect(entry == null);
}

test "sqlite category roundtrip" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("k0", "v0", .core, null);
    try m.store("k1", "v1", .daily, null);
    try m.store("k2", "v2", .conversation, null);
    try m.store("k3", "v3", .{ .custom = "project" }, null);

    const e0 = (try m.get(std.testing.allocator, "k0")).?;
    defer e0.deinit(std.testing.allocator);
    try std.testing.expect(e0.category.eql(.core));

    const e1 = (try m.get(std.testing.allocator, "k1")).?;
    defer e1.deinit(std.testing.allocator);
    try std.testing.expect(e1.category.eql(.daily));

    const e2 = (try m.get(std.testing.allocator, "k2")).?;
    defer e2.deinit(std.testing.allocator);
    try std.testing.expect(e2.category.eql(.conversation));

    const e3 = (try m.get(std.testing.allocator, "k3")).?;
    defer e3.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("project", e3.category.custom);
}

test "sqlite forget then recall no ghost results" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("ghost", "phantom memory content", .core, null);
    _ = try m.forget("ghost");

    const results = try m.recall(std.testing.allocator, "phantom memory", 10, null);
    defer root.freeEntries(std.testing.allocator, results);
    try std.testing.expectEqual(@as(usize, 0), results.len);
}

test "sqlite forget and re-store same key" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("cycle", "version 1", .core, null);
    _ = try m.forget("cycle");
    try m.store("cycle", "version 2", .core, null);

    const entry = (try m.get(std.testing.allocator, "cycle")).?;
    defer entry.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("version 2", entry.content);
    try std.testing.expectEqual(@as(usize, 1), try m.count());
}

test "sqlite store empty content" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("empty", "", .core, null);
    const entry = (try m.get(std.testing.allocator, "empty")).?;
    defer entry.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("", entry.content);
}

test "sqlite store empty key" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("", "content for empty key", .core, null);
    const entry = (try m.get(std.testing.allocator, "")).?;
    defer entry.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("content for empty key", entry.content);
}

test "sqlite recall results have scores" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("s1", "scored result test", .core, null);

    const results = try m.recall(std.testing.allocator, "scored", 10, null);
    defer root.freeEntries(std.testing.allocator, results);

    try std.testing.expect(results.len > 0);
    for (results) |entry| {
        try std.testing.expect(entry.score != null);
    }
}

test "sqlite reindex" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("r1", "reindex test alpha", .core, null);
    try m.store("r2", "reindex test beta", .core, null);

    try mem.reindex();

    const results = try m.recall(std.testing.allocator, "reindex", 10, null);
    defer root.freeEntries(std.testing.allocator, results);
    try std.testing.expectEqual(@as(usize, 2), results.len);
}

test "sqlite recall with sql injection attempt" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("safe", "normal content", .core, null);

    const results = try m.recall(std.testing.allocator, "'; DROP TABLE memories; --", 10, null);
    defer root.freeEntries(std.testing.allocator, results);

    try std.testing.expectEqual(@as(usize, 1), try m.count());
}

test "sqlite schema has fts5 table" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();

    const sql = "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='memories_fts'";
    var stmt: ?*c.sqlite3_stmt = null;
    var rc = c.sqlite3_prepare_v2(mem.db, sql, -1, &stmt, null);
    try std.testing.expectEqual(c.SQLITE_OK, rc);
    defer _ = c.sqlite3_finalize(stmt);

    rc = c.sqlite3_step(stmt);
    try std.testing.expectEqual(c.SQLITE_ROW, rc);
    const count = c.sqlite3_column_int64(stmt, 0);
    try std.testing.expectEqual(@as(i64, 1), count);
}

test "sqlite fts5 syncs on insert" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("test_key", "unique_searchterm_xyz", .core, null);

    const sql = "SELECT COUNT(*) FROM memories_fts WHERE memories_fts MATCH '\"unique_searchterm_xyz\"'";
    var stmt: ?*c.sqlite3_stmt = null;
    var rc = c.sqlite3_prepare_v2(mem.db, sql, -1, &stmt, null);
    try std.testing.expectEqual(c.SQLITE_OK, rc);
    defer _ = c.sqlite3_finalize(stmt);

    rc = c.sqlite3_step(stmt);
    try std.testing.expectEqual(c.SQLITE_ROW, rc);
    try std.testing.expectEqual(@as(i64, 1), c.sqlite3_column_int64(stmt, 0));
}

test "sqlite fts5 syncs on delete" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("del_key", "deletable_content_abc", .core, null);
    _ = try m.forget("del_key");

    const sql = "SELECT COUNT(*) FROM memories_fts WHERE memories_fts MATCH '\"deletable_content_abc\"'";
    var stmt: ?*c.sqlite3_stmt = null;
    var rc = c.sqlite3_prepare_v2(mem.db, sql, -1, &stmt, null);
    try std.testing.expectEqual(c.SQLITE_OK, rc);
    defer _ = c.sqlite3_finalize(stmt);

    rc = c.sqlite3_step(stmt);
    try std.testing.expectEqual(c.SQLITE_ROW, rc);
    try std.testing.expectEqual(@as(i64, 0), c.sqlite3_column_int64(stmt, 0));
}

test "sqlite fts5 syncs on update" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("upd_key", "original_content_111", .core, null);
    try m.store("upd_key", "updated_content_222", .core, null);

    {
        const sql = "SELECT COUNT(*) FROM memories_fts WHERE memories_fts MATCH '\"original_content_111\"'";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(mem.db, sql, -1, &stmt, null);
        try std.testing.expectEqual(c.SQLITE_OK, rc);
        defer _ = c.sqlite3_finalize(stmt);
        rc = c.sqlite3_step(stmt);
        try std.testing.expectEqual(c.SQLITE_ROW, rc);
        try std.testing.expectEqual(@as(i64, 0), c.sqlite3_column_int64(stmt, 0));
    }

    {
        const sql = "SELECT COUNT(*) FROM memories_fts WHERE memories_fts MATCH '\"updated_content_222\"'";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(mem.db, sql, -1, &stmt, null);
        try std.testing.expectEqual(c.SQLITE_OK, rc);
        defer _ = c.sqlite3_finalize(stmt);
        rc = c.sqlite3_step(stmt);
        try std.testing.expectEqual(c.SQLITE_ROW, rc);
        try std.testing.expectEqual(@as(i64, 1), c.sqlite3_column_int64(stmt, 0));
    }
}

test "sqlite list custom category" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("c1", "custom1", .{ .custom = "project" }, null);
    try m.store("c2", "custom2", .{ .custom = "project" }, null);
    try m.store("c3", "other", .core, null);

    const project = try m.list(std.testing.allocator, .{ .custom = "project" }, null);
    defer root.freeEntries(std.testing.allocator, project);
    try std.testing.expectEqual(@as(usize, 2), project.len);
}

test "sqlite list empty db" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    const all = try m.list(std.testing.allocator, null, null);
    defer root.freeEntries(std.testing.allocator, all);
    try std.testing.expectEqual(@as(usize, 0), all.len);
}

test "sqlite recall matches by key not just content" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("zig_preferences", "User likes systems programming", .core, null);

    const results = try m.recall(std.testing.allocator, "zig", 10, null);
    defer root.freeEntries(std.testing.allocator, results);

    try std.testing.expect(results.len > 0);
}

test "sqlite recall respects limit" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    for (0..10) |i| {
        var key_buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "key_{d}", .{i}) catch continue;
        var content_buf: [64]u8 = undefined;
        const content = std.fmt.bufPrint(&content_buf, "searchable content number {d}", .{i}) catch continue;
        try m.store(key, content, .core, null);
    }

    const results = try m.recall(std.testing.allocator, "searchable", 3, null);
    defer root.freeEntries(std.testing.allocator, results);

    try std.testing.expect(results.len <= 3);
}

test "sqlite store unicode content" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("unicode_key", "\xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9e\xe3\x81\xae\xe3\x83\x86\xe3\x82\xb9\xe3\x83\x88", .core, null);

    const entry = (try m.get(std.testing.allocator, "unicode_key")).?;
    defer entry.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("\xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9e\xe3\x81\xae\xe3\x83\x86\xe3\x82\xb9\xe3\x83\x88", entry.content);
}

test "sqlite recall unicode query" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("jp", "\xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9e\xe3\x81\xae\xe3\x83\x86\xe3\x82\xb9\xe3\x83\x88", .core, null);

    const results = try m.recall(std.testing.allocator, "\xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9e", 10, null);
    defer root.freeEntries(std.testing.allocator, results);

    try std.testing.expect(results.len > 0);
}

test "sqlite store long content" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    // Build a long string
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(std.testing.allocator);
    for (0..1000) |_| {
        try buf.appendSlice(std.testing.allocator, "abcdefghij");
    }

    try m.store("long", buf.items, .core, null);
    const entry = (try m.get(std.testing.allocator, "long")).?;
    defer entry.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 10000), entry.content.len);
}

test "sqlite multiple categories count" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("a", "one", .core, null);
    try m.store("b", "two", .daily, null);
    try m.store("c", "three", .conversation, null);
    try m.store("d", "four", .{ .custom = "project" }, null);

    try std.testing.expectEqual(@as(usize, 4), try m.count());
}

test "sqlite saveMessage stores messages" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();

    try mem.saveMessage("session-1", "user", "hello");
    try mem.saveMessage("session-1", "assistant", "hi there");
    try mem.saveMessage("session-2", "user", "another session");

    // Verify messages table has data
    const sql = "SELECT COUNT(*) FROM messages";
    var stmt: ?*c.sqlite3_stmt = null;
    var rc = c.sqlite3_prepare_v2(mem.db, sql, -1, &stmt, null);
    try std.testing.expectEqual(c.SQLITE_OK, rc);
    defer _ = c.sqlite3_finalize(stmt);

    rc = c.sqlite3_step(stmt);
    try std.testing.expectEqual(c.SQLITE_ROW, rc);
    try std.testing.expectEqual(@as(i64, 3), c.sqlite3_column_int64(stmt, 0));
}

test "sqlite store and forget multiple keys" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("k1", "v1", .core, null);
    try m.store("k2", "v2", .core, null);
    try m.store("k3", "v3", .core, null);

    try std.testing.expectEqual(@as(usize, 3), try m.count());

    _ = try m.forget("k2");
    try std.testing.expectEqual(@as(usize, 2), try m.count());

    _ = try m.forget("k1");
    _ = try m.forget("k3");
    try std.testing.expectEqual(@as(usize, 0), try m.count());
}

test "sqlite upsert changes category" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("key", "value", .core, null);
    try m.store("key", "new value", .daily, null);

    const entry = (try m.get(std.testing.allocator, "key")).?;
    defer entry.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("new value", entry.content);
    try std.testing.expect(entry.category.eql(.daily));
}

test "sqlite recall multi-word query" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("zig-lang", "Zig is a systems programming language", .core, null);
    try m.store("rust-lang", "Rust is also a systems language", .core, null);
    try m.store("python-lang", "Python is interpreted", .core, null);

    const results = try m.recall(std.testing.allocator, "systems programming", 10, null);
    defer root.freeEntries(std.testing.allocator, results);

    try std.testing.expect(results.len >= 1);
}

test "sqlite list returns all entries" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("first", "first entry", .core, null);
    try m.store("second", "second entry", .core, null);
    try m.store("third", "third entry", .core, null);

    const all = try m.list(std.testing.allocator, null, null);
    defer root.freeEntries(std.testing.allocator, all);

    try std.testing.expectEqual(@as(usize, 3), all.len);

    // All keys should be present
    var found_first = false;
    var found_second = false;
    var found_third = false;
    for (all) |entry| {
        if (std.mem.eql(u8, entry.key, "first")) found_first = true;
        if (std.mem.eql(u8, entry.key, "second")) found_second = true;
        if (std.mem.eql(u8, entry.key, "third")) found_third = true;
    }
    try std.testing.expect(found_first);
    try std.testing.expect(found_second);
    try std.testing.expect(found_third);
}

test "sqlite get returns entry with all fields" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("test_key", "test_content", .daily, null);

    const entry = (try m.get(std.testing.allocator, "test_key")).?;
    defer entry.deinit(std.testing.allocator);

    try std.testing.expectEqualStrings("test_key", entry.key);
    try std.testing.expectEqualStrings("test_content", entry.content);
    try std.testing.expect(entry.category.eql(.daily));
    try std.testing.expect(entry.id.len > 0);
    try std.testing.expect(entry.timestamp.len > 0);
}

test "sqlite recall with quotes in query" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("quotes", "He said \"hello\" to the world", .core, null);

    const results = try m.recall(std.testing.allocator, "hello", 10, null);
    defer root.freeEntries(std.testing.allocator, results);

    try std.testing.expect(results.len > 0);
}

test "sqlite health check after operations" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("k", "v", .core, null);
    _ = try m.forget("k");

    try std.testing.expect(m.healthCheck());
}

test "sqlite kv table exists" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();

    const sql = "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='kv'";
    var stmt: ?*c.sqlite3_stmt = null;
    var rc = c.sqlite3_prepare_v2(mem.db, sql, -1, &stmt, null);
    try std.testing.expectEqual(c.SQLITE_OK, rc);
    defer _ = c.sqlite3_finalize(stmt);

    rc = c.sqlite3_step(stmt);
    try std.testing.expectEqual(c.SQLITE_ROW, rc);
    try std.testing.expectEqual(@as(i64, 1), c.sqlite3_column_int64(stmt, 0));
}

// ── Session ID tests ──────────────────────────────────────────────

test "sqlite store with session_id persists in scoped namespace" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("k1", "session data", .core, "sess-abc");

    const entry = (try m.getScoped(std.testing.allocator, "k1", "sess-abc")).?;
    defer entry.deinit(std.testing.allocator);

    try std.testing.expectEqualStrings("session data", entry.content);
    try std.testing.expect(entry.session_id != null);
    try std.testing.expectEqualStrings("sess-abc", entry.session_id.?);
    try std.testing.expect(try m.get(std.testing.allocator, "k1") == null);
}

test "sqlite store without session_id gives null" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("k1", "no session", .core, null);

    const entry = (try m.get(std.testing.allocator, "k1")).?;
    defer entry.deinit(std.testing.allocator);

    try std.testing.expect(entry.session_id == null);
}

test "sqlite recall with session_id filters correctly" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("k1", "session A fact", .core, "sess-a");
    try m.store("k2", "session B fact", .core, "sess-b");
    try m.store("k3", "no session fact", .core, null);

    // Recall with session-a filter returns only session-a entry
    const results = try m.recall(std.testing.allocator, "fact", 10, "sess-a");
    defer root.freeEntries(std.testing.allocator, results);

    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqualStrings("k1", results[0].key);
    try std.testing.expect(results[0].session_id != null);
    try std.testing.expectEqualStrings("sess-a", results[0].session_id.?);
}

test "sqlite recall with null session_id returns all" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("k1", "alpha fact", .core, "sess-a");
    try m.store("k2", "beta fact", .core, "sess-b");
    try m.store("k3", "gamma fact", .core, null);

    const results = try m.recall(std.testing.allocator, "fact", 10, null);
    defer root.freeEntries(std.testing.allocator, results);

    try std.testing.expectEqual(@as(usize, 3), results.len);
}

test "sqlite list with session_id filter" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("k1", "a1", .core, "sess-a");
    try m.store("k2", "a2", .conversation, "sess-a");
    try m.store("k3", "b1", .core, "sess-b");
    try m.store("k4", "none1", .core, null);

    // List with session-a filter
    const results = try m.list(std.testing.allocator, null, "sess-a");
    defer root.freeEntries(std.testing.allocator, results);

    try std.testing.expectEqual(@as(usize, 2), results.len);
    for (results) |entry| {
        try std.testing.expect(entry.session_id != null);
        try std.testing.expectEqualStrings("sess-a", entry.session_id.?);
    }
}

test "sqlite list with session_id and category filter" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("k1", "a1", .core, "sess-a");
    try m.store("k2", "a2", .conversation, "sess-a");
    try m.store("k3", "b1", .core, "sess-b");

    const results = try m.list(std.testing.allocator, .core, "sess-a");
    defer root.freeEntries(std.testing.allocator, results);

    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqualStrings("k1", results[0].key);
}

test "sqlite cross-session recall isolation" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("secret", "session A secret data", .core, "sess-a");

    // Session B cannot see session A data
    const results_b = try m.recall(std.testing.allocator, "secret", 10, "sess-b");
    defer root.freeEntries(std.testing.allocator, results_b);
    try std.testing.expectEqual(@as(usize, 0), results_b.len);

    // Session A can see its own data
    const results_a = try m.recall(std.testing.allocator, "secret", 10, "sess-a");
    defer root.freeEntries(std.testing.allocator, results_a);
    try std.testing.expectEqual(@as(usize, 1), results_a.len);
}

test "sqlite schema has session_id column" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();

    // Verify session_id column exists by querying it
    const sql = "SELECT session_id FROM memories LIMIT 0";
    var stmt: ?*c.sqlite3_stmt = null;
    const rc = c.sqlite3_prepare_v2(mem.db, sql, -1, &stmt, null);
    try std.testing.expectEqual(c.SQLITE_OK, rc);
    _ = c.sqlite3_finalize(stmt);
}

test "sqlite schema migration is idempotent" {
    // Calling migrateSessionId twice should not fail
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();

    // migrateSessionId already ran during init; call it again
    try mem.migrateSessionId();

    // Store with session_id should still work
    const m = mem.memory();
    try m.store("k1", "data", .core, "sess-x");
    const entry = (try m.getScoped(std.testing.allocator, "k1", "sess-x")).?;
    defer entry.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("sess-x", entry.session_id.?);
}

// ── clearAutoSaved tests ──────────────────────────────────────────

test "sqlite clearAutoSaved removes autosave entries" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("autosave_user_1000", "user msg", .conversation, null);
    try m.store("autosave_assistant_1001", "assistant reply", .daily, null);
    try m.store("normal_key", "keep this", .core, null);

    try std.testing.expectEqual(@as(usize, 3), try m.count());

    try mem.clearAutoSaved(null);

    try std.testing.expectEqual(@as(usize, 1), try m.count());
    const entry = (try m.get(std.testing.allocator, "normal_key")).?;
    defer entry.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("keep this", entry.content);
}

test "sqlite clearAutoSaved scoped by session_id" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("autosave_user_a", "a", .conversation, "sess-a");
    try m.store("autosave_user_b", "b", .conversation, "sess-b");
    try m.store("normal_key", "keep this", .core, "sess-b");

    try mem.clearAutoSaved("sess-a");

    const a_entry = try m.getScoped(std.testing.allocator, "autosave_user_a", "sess-a");
    defer if (a_entry) |entry| entry.deinit(std.testing.allocator);
    try std.testing.expect(a_entry == null);

    const b_entry = try m.getScoped(std.testing.allocator, "autosave_user_b", "sess-b");
    defer if (b_entry) |entry| entry.deinit(std.testing.allocator);
    try std.testing.expect(b_entry != null);
    try std.testing.expectEqualStrings("b", b_entry.?.content);

    const normal = try m.getScoped(std.testing.allocator, "normal_key", "sess-b");
    defer if (normal) |entry| entry.deinit(std.testing.allocator);
    try std.testing.expect(normal != null);
    try std.testing.expectEqualStrings("keep this", normal.?.content);
}

test "sqlite clearAutoSaved preserves non-autosave entries" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("user_pref", "likes Zig", .core, null);
    try m.store("daily_note", "some note", .daily, null);
    try m.store("autosave_like_prefix", "not autosave", .core, null);

    try mem.clearAutoSaved(null);

    // "autosave_like_prefix" starts with "autosave_" so it IS removed
    try std.testing.expectEqual(@as(usize, 2), try m.count());
}

test "sqlite clearAutoSaved no-op on empty" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();

    try mem.clearAutoSaved(null);
    const m = mem.memory();
    try std.testing.expectEqual(@as(usize, 0), try m.count());
}

// ── SessionStore vtable tests ─────────────────────────────────────

test "sqlite sessionStore returns valid vtable" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();

    const store = mem.sessionStore();
    try std.testing.expect(store.vtable == &SqliteMemory.session_vtable);
}

test "sqlite sessionStore saveMessage + loadMessages roundtrip" {
    const allocator = std.testing.allocator;
    var mem = try SqliteMemory.init(allocator, ":memory:");
    defer mem.deinit();

    const store = mem.sessionStore();
    try store.saveMessage("s1", "user", "hello");
    try store.saveMessage("s1", "assistant", "hi there");

    const msgs = try store.loadMessages(allocator, "s1");
    defer root.freeMessages(allocator, msgs);

    try std.testing.expectEqual(@as(usize, 2), msgs.len);
    try std.testing.expectEqualStrings("user", msgs[0].role);
    try std.testing.expectEqualStrings("hello", msgs[0].content);
    try std.testing.expectEqualStrings("assistant", msgs[1].role);
    try std.testing.expectEqualStrings("hi there", msgs[1].content);
}

test "sqlite sessionStore history views hide runtime command rows" {
    const allocator = std.testing.allocator;
    var mem = try SqliteMemory.init(allocator, ":memory:");
    defer mem.deinit();

    const store = mem.sessionStore();
    try store.saveMessage("s1", root.RUNTIME_COMMAND_ROLE, "/usage full");
    try store.saveMessage("s1", "user", "hello");
    try store.saveMessage("s1", "assistant", "hi there");
    try store.saveMessage("s2", root.RUNTIME_COMMAND_ROLE, "/think high");

    const raw = try store.loadMessages(allocator, "s1");
    defer root.freeMessages(allocator, raw);
    try std.testing.expectEqual(@as(usize, 3), raw.len);
    try std.testing.expectEqualStrings(root.RUNTIME_COMMAND_ROLE, raw[0].role);

    try std.testing.expectEqual(@as(u64, 1), try store.countSessions());

    const sessions = try store.listSessions(allocator, 10, 0);
    defer root.freeSessionInfos(allocator, sessions);
    try std.testing.expectEqual(@as(usize, 1), sessions.len);
    try std.testing.expectEqualStrings("s1", sessions[0].session_id);
    try std.testing.expectEqual(@as(u64, 2), sessions[0].message_count);

    try std.testing.expectEqual(@as(u64, 2), try store.countDetailedMessages("s1"));
    try std.testing.expectEqual(@as(u64, 0), try store.countDetailedMessages("s2"));

    const detailed = try store.loadMessagesDetailed(allocator, "s1", 10, 0);
    defer root.freeDetailedMessages(allocator, detailed);
    try std.testing.expectEqual(@as(usize, 2), detailed.len);
    try std.testing.expectEqualStrings("user", detailed[0].role);
    try std.testing.expectEqualStrings("assistant", detailed[1].role);
}

test "sqlite sessionStore clearMessages" {
    const allocator = std.testing.allocator;
    var mem = try SqliteMemory.init(allocator, ":memory:");
    defer mem.deinit();

    const store = mem.sessionStore();
    try store.saveMessage("s1", "user", "hello");
    try store.saveUsage("s1", 99);
    try store.clearMessages("s1");

    const msgs = try store.loadMessages(allocator, "s1");
    defer allocator.free(msgs);
    try std.testing.expectEqual(@as(usize, 0), msgs.len);
    try std.testing.expectEqual(@as(?u64, null), try store.loadUsage("s1"));
}

test "sqlite sessionStore saveUsage + loadUsage roundtrip" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();

    const store = mem.sessionStore();
    try store.saveUsage("s1", 123);
    try std.testing.expectEqual(@as(?u64, 123), try store.loadUsage("s1"));
}

test "sqlite sessionStore clearAutoSaved" {
    const allocator = std.testing.allocator;
    var mem = try SqliteMemory.init(allocator, ":memory:");
    defer mem.deinit();

    const m = mem.memory();
    try m.store("autosave_user_1", "auto data", .core, "s1");
    try m.store("normal_key", "normal data", .core, null);

    const store = mem.sessionStore();
    try store.clearAutoSaved("s1");

    // autosave entry should be gone
    const entry = try m.getScoped(allocator, "autosave_user_1", "s1");
    try std.testing.expect(entry == null);

    // normal entry should remain
    const normal = try m.get(allocator, "normal_key");
    try std.testing.expect(normal != null);
    var e = normal.?;
    defer e.deinit(allocator);
}

// ── R3 additional tests ───────────────────────────────────────────

test "sqlite recall with SQL LIKE wildcard percent in content" {
    // Verify that % in search query does not match everything
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("k1", "100% safe data", .core, null);
    try m.store("k2", "completely unrelated", .core, null);

    // Searching for "%" should NOT match "completely unrelated"
    // because % is escaped in LIKE patterns
    const results = try m.recall(std.testing.allocator, "%", 10, null);
    defer root.freeEntries(std.testing.allocator, results);

    // FTS5 may or may not match "%" — but LIKE fallback must not wildcard-match everything.
    // If FTS5 returns 0 results (likely for single %), the LIKE search must be precise.
    for (results) |entry| {
        // Every returned result must actually contain "%" in key or content
        const has_pct = std.mem.indexOf(u8, entry.content, "%") != null or
            std.mem.indexOf(u8, entry.key, "%") != null;
        try std.testing.expect(has_pct);
    }
}

test "sqlite recall with SQL LIKE wildcard underscore in content" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("k1", "test_value", .core, null);
    try m.store("k2", "testXvalue", .core, null);

    // Searching for "_" should not match "testXvalue" via LIKE _
    // (underscore matches single char in unescaped LIKE)
    const results = try m.recall(std.testing.allocator, "_", 10, null);
    defer root.freeEntries(std.testing.allocator, results);

    for (results) |entry| {
        const has_underscore = std.mem.indexOf(u8, entry.content, "_") != null or
            std.mem.indexOf(u8, entry.key, "_") != null;
        try std.testing.expect(has_underscore);
    }
}

test "sqlite escapeLikePattern escapes wildcards" {
    const alloc = std.testing.allocator;

    // Normal word — just wrapped with %
    {
        const result = try SqliteMemory.escapeLikePattern(alloc, "hello");
        defer alloc.free(result);
        try std.testing.expectEqualStrings("%hello%", result);
    }

    // Percent sign — escaped
    {
        const result = try SqliteMemory.escapeLikePattern(alloc, "100%");
        defer alloc.free(result);
        try std.testing.expectEqualStrings("%100\\%%", result);
    }

    // Underscore — escaped
    {
        const result = try SqliteMemory.escapeLikePattern(alloc, "test_value");
        defer alloc.free(result);
        try std.testing.expectEqualStrings("%test\\_value%", result);
    }

    // Backslash — escaped
    {
        const result = try SqliteMemory.escapeLikePattern(alloc, "path\\to");
        defer alloc.free(result);
        try std.testing.expectEqualStrings("%path\\\\to%", result);
    }

    // Empty string
    {
        const result = try SqliteMemory.escapeLikePattern(alloc, "");
        defer alloc.free(result);
        try std.testing.expectEqualStrings("%%", result);
    }
}

test "sqlite store and get with special chars in key" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    const key = "key with \"quotes\" and 'apostrophes' and %wildcards%";
    try m.store(key, "content", .core, null);

    const entry = (try m.get(std.testing.allocator, key)).?;
    defer entry.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings(key, entry.key);
}

test "sqlite store newlines in content roundtrip" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    const content = "line1\nline2\ttab\r\nwindows\n\ndouble newline";
    try m.store("nl", content, .core, null);

    const entry = (try m.get(std.testing.allocator, "nl")).?;
    defer entry.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings(content, entry.content);
}

test "sqlite same key can exist in global and scoped namespaces" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("k", "v", .core, null);
    try m.store("k", "v2", .core, "sess-new");

    const global_entry = (try m.getScoped(std.testing.allocator, "k", null)).?;
    defer global_entry.deinit(std.testing.allocator);
    try std.testing.expect(global_entry.session_id == null);
    try std.testing.expectEqualStrings("v", global_entry.content);
    const unscoped_get = (try m.get(std.testing.allocator, "k")).?;
    defer unscoped_get.deinit(std.testing.allocator);
    try std.testing.expect(unscoped_get.session_id == null);
    try std.testing.expectEqualStrings("v", unscoped_get.content);

    const scoped_entry = (try m.getScoped(std.testing.allocator, "k", "sess-new")).?;
    defer scoped_entry.deinit(std.testing.allocator);
    try std.testing.expect(scoped_entry.session_id != null);
    try std.testing.expectEqualStrings("sess-new", scoped_entry.session_id.?);
    try std.testing.expectEqualStrings("v2", scoped_entry.content);
}

test "sqlite get does not fall back to scoped namespace" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("scoped-only", "scoped value", .core, "sess-a");

    try std.testing.expect(try m.get(std.testing.allocator, "scoped-only") == null);

    const scoped_entry = (try m.getScoped(std.testing.allocator, "scoped-only", "sess-a")).?;
    defer scoped_entry.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("scoped value", scoped_entry.content);
}

test "sqlite scoped forget removes only matching namespace" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("k", "v", .core, "sess-old");
    try m.store("k", "v2", .core, null);

    try std.testing.expect(try m.forgetScoped(std.testing.allocator, "k", "sess-old"));

    const scoped_entry = try m.getScoped(std.testing.allocator, "k", "sess-old");
    defer if (scoped_entry) |entry| entry.deinit(std.testing.allocator);
    try std.testing.expect(scoped_entry == null);

    const global_entry = (try m.getScoped(std.testing.allocator, "k", null)).?;
    defer global_entry.deinit(std.testing.allocator);
    try std.testing.expect(global_entry.session_id == null);
    try std.testing.expectEqualStrings("v2", global_entry.content);
}

test "sqlite loadMessages empty session" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();

    const msgs = try mem.loadMessages(std.testing.allocator, "nonexistent");
    defer std.testing.allocator.free(msgs);
    try std.testing.expectEqual(@as(usize, 0), msgs.len);
}

test "sqlite loadMessages preserves order" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();

    try mem.saveMessage("s1", "user", "first");
    try mem.saveMessage("s1", "assistant", "second");
    try mem.saveMessage("s1", "user", "third");

    const msgs = try mem.loadMessages(std.testing.allocator, "s1");
    defer root.freeMessages(std.testing.allocator, msgs);

    try std.testing.expectEqual(@as(usize, 3), msgs.len);
    try std.testing.expectEqualStrings("first", msgs[0].content);
    try std.testing.expectEqualStrings("second", msgs[1].content);
    try std.testing.expectEqualStrings("third", msgs[2].content);
}

test "sqlite clearMessages does not affect other sessions" {
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();

    try mem.saveMessage("s1", "user", "s1 msg");
    try mem.saveMessage("s2", "user", "s2 msg");

    try mem.clearMessages("s1");

    const s1_msgs = try mem.loadMessages(std.testing.allocator, "s1");
    defer std.testing.allocator.free(s1_msgs);
    try std.testing.expectEqual(@as(usize, 0), s1_msgs.len);

    const s2_msgs = try mem.loadMessages(std.testing.allocator, "s2");
    defer root.freeMessages(std.testing.allocator, s2_msgs);
    try std.testing.expectEqual(@as(usize, 1), s2_msgs.len);
}

test "sqlite event feed converges across replicas and is idempotent" {
    var source = try SqliteMemory.initWithInstanceId(std.testing.allocator, ":memory:", "agent-a");
    defer source.deinit();
    var replica = try SqliteMemory.initWithInstanceId(std.testing.allocator, ":memory:", "agent-b");
    defer replica.deinit();

    const source_mem = source.memory();
    const replica_mem = replica.memory();

    try source_mem.store("preferences.theme", "dark", .core, null);
    try source_mem.store("preferences.style", "concise", .core, "sess-1");
    try std.testing.expect(try source_mem.forgetScoped(std.testing.allocator, "preferences.style", "sess-1"));

    const events = try source_mem.listEvents(std.testing.allocator, 0, 32);
    defer root.freeEvents(std.testing.allocator, events);
    try std.testing.expectEqual(@as(usize, 3), events.len);

    for (events) |event| {
        try replica_mem.applyEvent(.{
            .origin_instance_id = event.origin_instance_id,
            .origin_sequence = event.origin_sequence,
            .timestamp_ms = event.timestamp_ms,
            .operation = event.operation,
            .key = event.key,
            .session_id = event.session_id,
            .category = event.category,
            .content = event.content,
        });
    }

    const theme = (try replica_mem.getScoped(std.testing.allocator, "preferences.theme", null)).?;
    defer theme.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("dark", theme.content);

    const style = try replica_mem.getScoped(std.testing.allocator, "preferences.style", "sess-1");
    defer if (style) |entry| entry.deinit(std.testing.allocator);
    try std.testing.expect(style == null);

    for (events) |event| {
        try replica_mem.applyEvent(.{
            .origin_instance_id = event.origin_instance_id,
            .origin_sequence = event.origin_sequence,
            .timestamp_ms = event.timestamp_ms,
            .operation = event.operation,
            .key = event.key,
            .session_id = event.session_id,
            .category = event.category,
            .content = event.content,
        });
    }
    try std.testing.expectEqual(@as(usize, 1), try replica_mem.count());
    try std.testing.expectEqual(@as(u64, 3), try replica_mem.lastEventSequence());
}

test "sqlite event feed supports deterministic behavioral merge ops" {
    var mem = try SqliteMemory.initWithInstanceId(std.testing.allocator, ":memory:", "agent-a");
    defer mem.deinit();
    const memory = mem.memory();

    try memory.applyEvent(.{
        .origin_instance_id = "agent-a",
        .origin_sequence = 1,
        .timestamp_ms = 10,
        .operation = .merge_string_set,
        .key = "traits.tags",
        .category = .core,
        .value_kind = .string_set,
        .content = "friendly",
    });
    try memory.applyEvent(.{
        .origin_instance_id = "agent-b",
        .origin_sequence = 1,
        .timestamp_ms = 11,
        .operation = .merge_string_set,
        .key = "traits.tags",
        .value_kind = .string_set,
        .content = "[\"concise\",\"friendly\"]",
    });

    const tags = (try memory.getScoped(std.testing.allocator, "traits.tags", null)).?;
    defer tags.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("[\"concise\",\"friendly\"]", tags.content);

    try memory.applyEvent(.{
        .origin_instance_id = "agent-a",
        .origin_sequence = 2,
        .timestamp_ms = 20,
        .operation = .merge_object,
        .key = "profile.behavior",
        .category = .core,
        .value_kind = .json_object,
        .content = "{\"tone\":\"formal\",\"persona\":{\"warm\":true}}",
    });
    try memory.applyEvent(.{
        .origin_instance_id = "agent-b",
        .origin_sequence = 2,
        .timestamp_ms = 21,
        .operation = .merge_object,
        .key = "profile.behavior",
        .value_kind = .json_object,
        .content = "{\"persona\":{\"direct\":true},\"verbosity\":\"low\"}",
    });

    const behavior = (try memory.getScoped(std.testing.allocator, "profile.behavior", null)).?;
    defer behavior.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings(
        "{\"persona\":{\"direct\":true,\"warm\":true},\"tone\":\"formal\",\"verbosity\":\"low\"}",
        behavior.content,
    );
}

test "sqlite migration backfills existing memories into event feed" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const ws_path = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(ws_path);

    const db_path = try std.fs.path.joinZ(std.testing.allocator, &.{ ws_path, "memory.db" });
    defer std.testing.allocator.free(db_path);

    {
        var mem = try SqliteMemory.initWithInstanceId(std.testing.allocator, db_path, "agent-a");
        defer mem.deinit();
        const memory = mem.memory();

        try memory.store("preferences.theme", "dark", .core, null);
        try memory.store("preferences.locale", "en", .core, "sess-1");

        try mem.execSql("test reset events", "DELETE FROM memory_events;");
        try mem.execSql("test reset frontiers", "DELETE FROM memory_event_frontiers;");
    }

    var reopened = try SqliteMemory.initWithInstanceId(std.testing.allocator, db_path, "agent-a");
    defer reopened.deinit();
    const reopened_mem = reopened.memory();

    const events = try reopened_mem.listEvents(std.testing.allocator, 0, 32);
    defer root.freeEvents(std.testing.allocator, events);
    try std.testing.expectEqual(@as(usize, 2), events.len);

    try std.testing.expectEqualStrings("preferences.theme", events[0].key);
    try std.testing.expectEqual(@as(u64, 1), events[0].origin_sequence);
    try std.testing.expect(events[0].session_id == null);
    try std.testing.expectEqualStrings("dark", events[0].content.?);
    try std.testing.expectEqualStrings("preferences.locale", events[1].key);
    try std.testing.expectEqual(@as(u64, 2), events[1].origin_sequence);
    try std.testing.expectEqualStrings("sess-1", events[1].session_id.?);
    try std.testing.expectEqualStrings("en", events[1].content.?);
    try std.testing.expect(events[0].sequence < events[1].sequence);
    try std.testing.expectEqual(events[1].sequence, try reopened_mem.lastEventSequence());
}

test "sqlite event feed compaction enforces cursor floor and preserves state across reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const ws_path = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(ws_path);

    const db_path = try std.fs.path.joinZ(std.testing.allocator, &.{ ws_path, "compact.db" });
    defer std.testing.allocator.free(db_path);

    var compacted_through: u64 = 0;
    {
        var mem = try SqliteMemory.initWithInstanceId(std.testing.allocator, db_path, "agent-a");
        defer mem.deinit();
        const memory = mem.memory();

        try memory.store("preferences.theme", "dark", .core, null);
        compacted_through = try memory.compactEvents();
        try std.testing.expect(compacted_through > 0);
        try std.testing.expectError(error.CursorExpired, memory.listEvents(std.testing.allocator, 0, 8));

        const info = try memory.eventFeedInfo(std.testing.allocator);
        defer info.deinit(std.testing.allocator);
        try std.testing.expect(info.supports_compaction);
        try std.testing.expectEqual(compacted_through, info.compacted_through_sequence);
        try std.testing.expectEqual(compacted_through + 1, info.oldest_available_sequence);
    }

    {
        var reopened = try SqliteMemory.initWithInstanceId(std.testing.allocator, db_path, "agent-a");
        defer reopened.deinit();
        const memory = reopened.memory();

        const entry = (try memory.getScoped(std.testing.allocator, "preferences.theme", null)).?;
        defer entry.deinit(std.testing.allocator);
        try std.testing.expectEqualStrings("dark", entry.content);
        try std.testing.expectEqual(compacted_through, try memory.lastEventSequence());

        const no_tail = try memory.listEvents(std.testing.allocator, compacted_through, 8);
        defer root.freeEvents(std.testing.allocator, no_tail);
        try std.testing.expectEqual(@as(usize, 0), no_tail.len);

        try memory.store("preferences.locale", "en", .core, null);
        const tail = try memory.listEvents(std.testing.allocator, compacted_through, 8);
        defer root.freeEvents(std.testing.allocator, tail);
        try std.testing.expectEqual(@as(usize, 1), tail.len);
        try std.testing.expectEqual(compacted_through + 1, tail[0].sequence);
    }
}

test "sqlite checkpoint restores replica and preserves local origin frontier" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const ws_path = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(ws_path);

    const source_path = try std.fs.path.joinZ(std.testing.allocator, &.{ ws_path, "checkpoint-source.db" });
    defer std.testing.allocator.free(source_path);
    const replica_path = try std.fs.path.joinZ(std.testing.allocator, &.{ ws_path, "checkpoint-replica.db" });
    defer std.testing.allocator.free(replica_path);

    var source = try SqliteMemory.initWithInstanceId(std.testing.allocator, source_path, "agent-a");
    defer source.deinit();
    const source_mem = source.memory();
    try source_mem.store("preferences.theme", "dark", .core, null);
    _ = try source_mem.compactEvents();

    const checkpoint = try source_mem.exportCheckpoint(std.testing.allocator);
    defer std.testing.allocator.free(checkpoint);

    var replica = try SqliteMemory.initWithInstanceId(std.testing.allocator, replica_path, "agent-a");
    defer replica.deinit();
    const replica_mem = replica.memory();
    try replica_mem.applyCheckpoint(checkpoint);

    const entry = (try replica_mem.getScoped(std.testing.allocator, "preferences.theme", null)).?;
    defer entry.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("dark", entry.content);

    const info = try replica_mem.eventFeedInfo(std.testing.allocator);
    defer info.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 2), info.next_local_origin_sequence);

    try replica_mem.store("preferences.locale", "en", .core, null);
    const tail = try replica_mem.listEvents(std.testing.allocator, info.compacted_through_sequence, 8);
    defer root.freeEvents(std.testing.allocator, tail);
    try std.testing.expectEqual(@as(usize, 1), tail.len);
    try std.testing.expectEqual(@as(u64, 2), tail[0].origin_sequence);
}

test "sqlite checkpoint rejects unsupported schema without clearing state" {
    var mem = try SqliteMemory.initWithInstanceId(std.testing.allocator, ":memory:", "agent-a");
    defer mem.deinit();
    const memory = mem.memory();

    try memory.store("preferences.theme", "dark", .core, null);

    const bad_checkpoint =
        \\{"kind":"meta","schema_version":2,"last_sequence":1,"last_timestamp_ms":1,"compacted_through_sequence":1}
        \\{"kind":"state","key":"preferences.theme","session_id":null,"category":"core","value_kind":null,"content":"light","timestamp_ms":1,"origin_instance_id":"agent-a","origin_sequence":1}
        \\
    ;

    try std.testing.expectError(error.InvalidEvent, memory.applyCheckpoint(bad_checkpoint));

    const entry = (try memory.getScoped(std.testing.allocator, "preferences.theme", null)).?;
    defer entry.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("dark", entry.content);
}

test "sqlite tombstones block older cross-origin put replay" {
    var mem = try SqliteMemory.initWithInstanceId(std.testing.allocator, ":memory:", "replica");
    defer mem.deinit();
    const memory = mem.memory();

    try memory.applyEvent(.{
        .origin_instance_id = "agent-delete",
        .origin_sequence = 1,
        .timestamp_ms = 2000,
        .operation = .delete_all,
        .key = "preferences.locale",
    });
    try memory.applyEvent(.{
        .origin_instance_id = "agent-put",
        .origin_sequence = 1,
        .timestamp_ms = 1000,
        .operation = .put,
        .key = "preferences.locale",
        .category = .core,
        .content = "ru",
    });

    const entry = try memory.getScoped(std.testing.allocator, "preferences.locale", null);
    defer if (entry) |value| value.deinit(std.testing.allocator);
    try std.testing.expect(entry == null);
}
