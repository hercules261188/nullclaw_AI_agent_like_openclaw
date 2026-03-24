//! LanceDB-style vector memory backend with native deterministic feed.
//!
//! Stores exact memory state plus embedding projection in SQLite and exposes
//! the same native event/apply/checkpoint lifecycle as the first-class
//! persistent backends.

const std = @import("std");
const build_options = @import("build_options");
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
const vector = @import("../vector/math.zig");
const embeddings_mod = @import("../vector/embeddings.zig");
const EmbeddingProvider = embeddings_mod.EmbeddingProvider;
const sqlite_mod = if (build_options.enable_sqlite) @import("sqlite.zig") else @import("sqlite_disabled.zig");
const c = sqlite_mod.c;
const SQLITE_STATIC = sqlite_mod.SQLITE_STATIC;
const log = std.log.scoped(.lancedb_memory);

pub const LanceDbConfig = struct {
    min_search_score: f32 = 0.3,
    default_importance: f32 = 0.5,
};

pub const LanceDbMemory = struct {
    db: ?*c.sqlite3,
    allocator: std.mem.Allocator,
    embedder: ?EmbeddingProvider,
    config: LanceDbConfig,
    instance_id: []const u8,
    owns_instance_id: bool = false,
    owns_self: bool = false,

    const Self = @This();
    const BUSY_TIMEOUT_MS: c_int = 5000;

    const CheckpointMeta = struct {
        last_sequence: u64,
        compacted_through_sequence: u64,
        last_timestamp_ms: i64,
    };

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

    pub fn init(
        allocator: std.mem.Allocator,
        db_path: [*:0]const u8,
        embedder: ?EmbeddingProvider,
        config: LanceDbConfig,
    ) !Self {
        return initWithInstanceId(allocator, db_path, "default", embedder, config);
    }

    pub fn initWithInstanceId(
        allocator: std.mem.Allocator,
        db_path: [*:0]const u8,
        instance_id: []const u8,
        embedder: ?EmbeddingProvider,
        config: LanceDbConfig,
    ) !Self {
        const use_wal = sqlite_mod.shouldUseWal(db_path);

        var db: ?*c.sqlite3 = null;
        const rc = c.sqlite3_open(db_path, &db);
        if (rc != c.SQLITE_OK) {
            if (db) |d| _ = c.sqlite3_close(d);
            return error.SqliteOpenFailed;
        }
        if (db) |d| _ = c.sqlite3_busy_timeout(d, BUSY_TIMEOUT_MS);

        const effective_instance_id = if (instance_id.len > 0) instance_id else "default";
        var self_ = Self{
            .db = db,
            .allocator = allocator,
            .embedder = embedder,
            .config = config,
            .instance_id = try allocator.dupe(u8, effective_instance_id),
            .owns_instance_id = true,
        };
        errdefer if (self_.owns_instance_id) allocator.free(self_.instance_id);
        self_.configurePragmas(use_wal);
        try self_.migrate();
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

    fn configurePragmas(self: *Self, use_wal: bool) void {
        const journal_pragma: [:0]const u8 = if (use_wal)
            "PRAGMA journal_mode = WAL;"
        else
            "PRAGMA journal_mode = DELETE;";
        const pragmas = [_][:0]const u8{
            journal_pragma,
            "PRAGMA synchronous  = NORMAL;",
            "PRAGMA temp_store   = MEMORY;",
        };
        for (pragmas) |pragma| {
            var err_msg: [*c]u8 = null;
            const rc = c.sqlite3_exec(self.db, pragma, null, null, &err_msg);
            if (rc != c.SQLITE_OK) {
                if (err_msg) |msg| {
                    log.err("pragma failed: {s}", .{std.mem.span(msg)});
                    c.sqlite3_free(msg);
                }
            }
        }
    }

    fn migrate(self: *Self) !void {
        const base_sql =
            \\CREATE TABLE IF NOT EXISTS lancedb_memories (
            \\  id                       TEXT PRIMARY KEY,
            \\  key                      TEXT NOT NULL,
            \\  text                     TEXT NOT NULL,
            \\  embedding                BLOB,
            \\  importance               REAL DEFAULT 0.5,
            \\  category                 TEXT DEFAULT 'conversation',
            \\  value_kind               TEXT,
            \\  created_at               TEXT NOT NULL,
            \\  updated_at               TEXT NOT NULL,
            \\  session_id               TEXT,
            \\  event_timestamp_ms       INTEGER NOT NULL DEFAULT 0,
            \\  event_origin_instance_id TEXT NOT NULL DEFAULT 'default',
            \\  event_origin_sequence    INTEGER NOT NULL DEFAULT 0
            \\);
            \\CREATE INDEX IF NOT EXISTS idx_lance_category ON lancedb_memories(category);
            \\CREATE INDEX IF NOT EXISTS idx_lance_session ON lancedb_memories(session_id);
            \\CREATE INDEX IF NOT EXISTS idx_lance_key ON lancedb_memories(key);
            \\CREATE INDEX IF NOT EXISTS idx_lance_event_order ON lancedb_memories(event_timestamp_ms, event_origin_instance_id, event_origin_sequence);
            \\CREATE UNIQUE INDEX IF NOT EXISTS idx_lance_key_session ON lancedb_memories(key, COALESCE(session_id, '__global__'));
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
            \\CREATE INDEX IF NOT EXISTS idx_memory_events_local_sequence ON memory_events(local_sequence);
            \\CREATE INDEX IF NOT EXISTS idx_memory_events_origin ON memory_events(origin_instance_id, origin_sequence);
            \\CREATE TABLE IF NOT EXISTS memory_event_frontiers (
            \\  origin_instance_id TEXT PRIMARY KEY,
            \\  last_origin_sequence INTEGER NOT NULL
            \\);
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
            \\CREATE INDEX IF NOT EXISTS idx_memory_tombstones_key ON memory_tombstones(key);
            \\CREATE TABLE IF NOT EXISTS memory_feed_meta (
            \\  key TEXT PRIMARY KEY,
            \\  value TEXT NOT NULL
            \\);
        ;
        try self.execSql("lancedb migration", base_sql);
        try self.execSqlAllowDuplicateColumn("lancedb migration", "ALTER TABLE lancedb_memories ADD COLUMN value_kind TEXT;");
        try self.execSqlAllowDuplicateColumn("lancedb migration", "ALTER TABLE lancedb_memories ADD COLUMN event_timestamp_ms INTEGER NOT NULL DEFAULT 0;");
        try self.execSqlAllowDuplicateColumn("lancedb migration", "ALTER TABLE lancedb_memories ADD COLUMN event_origin_instance_id TEXT NOT NULL DEFAULT 'default';");
        try self.execSqlAllowDuplicateColumn("lancedb migration", "ALTER TABLE lancedb_memories ADD COLUMN event_origin_sequence INTEGER NOT NULL DEFAULT 0;");
        try self.execSql("lancedb migration", "CREATE INDEX IF NOT EXISTS idx_lance_event_order ON lancedb_memories(event_timestamp_ms, event_origin_instance_id, event_origin_sequence);");
        try self.bootstrapEventFeedFromExistingMemories();
    }

    fn localInstanceId(self: *Self) []const u8 {
        return self.instance_id;
    }

    fn execSql(self: *Self, context: []const u8, sql: [:0]const u8) !void {
        var err_msg: [*c]u8 = null;
        const rc = c.sqlite3_exec(self.db, sql, null, null, &err_msg);
        if (rc != c.SQLITE_OK) {
            if (err_msg) |msg| {
                log.err("{s} failed: {s}", .{ context, std.mem.span(msg) });
                c.sqlite3_free(msg);
            }
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
            if (err_msg) |msg| {
                log.err("{s} failed: {s}", .{ context, std.mem.span(msg) });
                c.sqlite3_free(msg);
            }
            return error.MigrationFailed;
        }
        if (err_msg) |msg| c.sqlite3_free(msg);
    }

    fn execTxnSql(self: *Self, sql: [:0]const u8) !void {
        var err_msg: [*c]u8 = null;
        const rc = c.sqlite3_exec(self.db, sql, null, null, &err_msg);
        if (rc != c.SQLITE_OK) {
            if (err_msg) |msg| {
                log.err("transaction failed: {s}", .{std.mem.span(msg)});
                c.sqlite3_free(msg);
            }
            return error.StepFailed;
        }
    }

    fn beginImmediate(self: *Self) !bool {
        var err_msg: [*c]u8 = null;
        const rc = c.sqlite3_exec(self.db, "BEGIN IMMEDIATE;", null, null, &err_msg);
        if (rc == c.SQLITE_OK) return true;

        if (err_msg) |msg| {
            const text = std.mem.span(msg);
            if (std.mem.indexOf(u8, text, "cannot start a transaction within a transaction") != null) {
                c.sqlite3_free(msg);
                return false;
            }
            log.err("begin immediate failed: {s}", .{text});
            c.sqlite3_free(msg);
        }
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

    fn categoryToString(cat: MemoryCategory) []const u8 {
        return cat.toString();
    }

    fn parseCategoryOwned(allocator: std.mem.Allocator, value: []const u8) !MemoryCategory {
        if (std.mem.eql(u8, value, "core")) return .core;
        if (std.mem.eql(u8, value, "daily")) return .daily;
        if (std.mem.eql(u8, value, "conversation")) return .conversation;
        return .{ .custom = try allocator.dupe(u8, value) };
    }

    fn ownedCategoryFromString(allocator: std.mem.Allocator, value: []const u8) !MemoryCategory {
        return try parseCategoryOwned(allocator, value);
    }

    fn dupeColumnText(stmt: *c.sqlite3_stmt, col: c_int, allocator: std.mem.Allocator) ![]u8 {
        const raw = c.sqlite3_column_text(stmt, col);
        const len: usize = @intCast(c.sqlite3_column_bytes(stmt, col));
        if (raw == null or len == 0) return allocator.dupe(u8, "");
        const slice: []const u8 = @as([*]const u8, @ptrCast(raw))[0..len];
        return allocator.dupe(u8, slice);
    }

    fn dupeColumnTextNullable(stmt: *c.sqlite3_stmt, col: c_int, allocator: std.mem.Allocator) !?[]u8 {
        if (c.sqlite3_column_type(stmt, col) == c.SQLITE_NULL) return null;
        const raw = c.sqlite3_column_text(stmt, col);
        if (raw == null) return null;
        const len: usize = @intCast(c.sqlite3_column_bytes(stmt, col));
        const slice: []const u8 = @as([*]const u8, @ptrCast(raw))[0..len];
        return try allocator.dupe(u8, slice);
    }

    fn readEntryFromRow(stmt: *c.sqlite3_stmt, allocator: std.mem.Allocator) !MemoryEntry {
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
        const sid = try dupeColumnTextNullable(stmt, 5, allocator);
        errdefer if (sid) |s| allocator.free(s);

        const category = blk: {
            const parsed = try parseCategoryOwned(allocator, cat_str);
            allocator.free(cat_str);
            break :blk parsed;
        };

        return .{
            .id = id,
            .key = key,
            .content = content,
            .category = category,
            .timestamp = timestamp,
            .session_id = sid,
            .score = null,
        };
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

    fn queryCount(self: *Self, sql: [:0]const u8) !u64 {
        var stmt: ?*c.sqlite3_stmt = null;
        const rc = c.sqlite3_prepare_v2(self.db, sql.ptr, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);
        if (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            const value = c.sqlite3_column_int64(stmt, 0);
            return if (value < 0) 0 else @intCast(value);
        }
        return 0;
    }

    fn queryMaxI64(self: *Self, sql: [:0]const u8) !i64 {
        var stmt: ?*c.sqlite3_stmt = null;
        const rc = c.sqlite3_prepare_v2(self.db, sql.ptr, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);
        if (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            return c.sqlite3_column_int64(stmt, 0);
        }
        return 0;
    }

    fn getCompactedThroughSequence(self: *Self) !u64 {
        const sql = "SELECT value FROM memory_feed_meta WHERE key = 'compacted_through_sequence' LIMIT 1";
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);
        if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return 0;
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
        if (c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);
        var buf: [32]u8 = undefined;
        const text = try std.fmt.bufPrint(&buf, "{d}", .{sequence});
        _ = c.sqlite3_bind_text(stmt, 1, text.ptr, @intCast(text.len), SQLITE_STATIC);
        if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.StepFailed;
    }

    fn getFrontierTx(self: *Self, origin_instance_id: []const u8) !u64 {
        const sql = "SELECT last_origin_sequence FROM memory_event_frontiers WHERE origin_instance_id = ?1";
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);
        _ = c.sqlite3_bind_text(stmt, 1, origin_instance_id.ptr, @intCast(origin_instance_id.len), SQLITE_STATIC);
        if (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            const value = c.sqlite3_column_int64(stmt, 0);
            return if (value < 0) 0 else @intCast(value);
        }
        return 0;
    }

    fn setFrontierTx(self: *Self, origin_instance_id: []const u8, origin_sequence: u64) !void {
        const sql =
            "INSERT INTO memory_event_frontiers (origin_instance_id, last_origin_sequence) VALUES (?1, ?2) " ++
            "ON CONFLICT(origin_instance_id) DO UPDATE SET last_origin_sequence = MAX(last_origin_sequence, excluded.last_origin_sequence)";
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);
        _ = c.sqlite3_bind_text(stmt, 1, origin_instance_id.ptr, @intCast(origin_instance_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_int64(stmt, 2, @intCast(origin_sequence));
        if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.StepFailed;
    }

    fn nextLocalOriginSequenceTx(self: *Self) !u64 {
        return (try self.getFrontierTx(self.localInstanceId())) + 1;
    }

    fn nextEventSequenceTx(self: *Self) !u64 {
        const sql = "SELECT COALESCE(MAX(local_sequence), 0) FROM memory_events";
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);
        const compacted_through = try self.getCompactedThroughSequence();
        if (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            const value = c.sqlite3_column_int64(stmt, 0);
            const last_in_events = if (value < 0) 0 else @as(u64, @intCast(value));
            return @max(last_in_events, compacted_through) + 1;
        }
        return compacted_through + 1;
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

    fn sessionKeyFor(session_id: ?[]const u8) []const u8 {
        return if (session_id) |sid| sid else "__global__";
    }

    fn computeEmbeddingBytes(self: *Self, content: []const u8) !?[]u8 {
        if (self.embedder == null) return null;
        const embedding = self.embedder.?.embed(self.allocator, content) catch return null;
        defer self.allocator.free(embedding);
        if (embedding.len == 0) return null;
        return vector.vecToBytes(self.allocator, embedding) catch null;
    }

    fn bootstrapEventFeedFromExistingMemories(self: *Self) !void {
        if (try self.getCompactedThroughSequence() > 0) return;
        if (try self.queryCount("SELECT COUNT(*) FROM memory_events") > 0) return;
        if (try self.queryCount("SELECT COUNT(*) FROM lancedb_memories") == 0) return;

        var owns_tx = c.sqlite3_get_autocommit(self.db) != 0;
        if (owns_tx) owns_tx = try self.beginImmediate();
        var committed = false;
        errdefer if (owns_tx and !committed) self.rollbackTxn();

        const select_sql =
            "SELECT rowid, key, text, category, session_id, value_kind, " ++
            "COALESCE(CAST(updated_at AS INTEGER), 0) " ++
            "FROM lancedb_memories ORDER BY updated_at ASC, rowid ASC";
        var select_stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self.db, select_sql, -1, &select_stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(select_stmt);

        const update_sql =
            "UPDATE lancedb_memories SET event_timestamp_ms = ?1, event_origin_instance_id = ?2, event_origin_sequence = ?3 WHERE rowid = ?4";
        var update_stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self.db, update_sql, -1, &update_stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(update_stmt);

        var next_origin_sequence: u64 = 1;
        while (c.sqlite3_step(select_stmt) == c.SQLITE_ROW) {
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
            defer if (value_kind_text) |text| self.allocator.free(text);
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
                .value_kind = if (value_kind_text) |text| MemoryValueKind.fromString(text) else null,
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
            if (c.sqlite3_step(update_stmt) != c.SQLITE_DONE) return error.StepFailed;

            next_origin_sequence += 1;
        }

        if (next_origin_sequence > 1) {
            try self.setFrontierTx(self.localInstanceId(), next_origin_sequence - 1);
        }

        if (owns_tx) try self.commitTxn();
        committed = true;
    }

    fn insertEventTx(self: *Self, input: MemoryEventInput) !bool {
        const sql =
            "INSERT INTO memory_events (local_sequence, schema_version, origin_instance_id, origin_sequence, timestamp_ms, operation, key, session_id, category, value_kind, content) " ++
            "VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)";
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        const next_event_sequence = try self.nextEventSequenceTx();
        const category_str = if (input.category) |category| category.toString() else null;
        const value_kind_str = if (input.value_kind) |kind| kind.toString() else null;
        const op_str = input.operation.toString();

        _ = c.sqlite3_bind_int64(stmt, 1, @intCast(next_event_sequence));
        _ = c.sqlite3_bind_int64(stmt, 2, 1);
        _ = c.sqlite3_bind_text(stmt, 3, input.origin_instance_id.ptr, @intCast(input.origin_instance_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_int64(stmt, 4, @intCast(input.origin_sequence));
        _ = c.sqlite3_bind_int64(stmt, 5, input.timestamp_ms);
        _ = c.sqlite3_bind_text(stmt, 6, op_str.ptr, @intCast(op_str.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 7, input.key.ptr, @intCast(input.key.len), SQLITE_STATIC);
        bindNullableText(stmt, 8, input.session_id);
        bindNullableText(stmt, 9, category_str);
        bindNullableText(stmt, 10, value_kind_str);
        bindNullableText(stmt, 11, input.content);

        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) return true;
        if (rc == c.SQLITE_CONSTRAINT) return false;
        return error.StepFailed;
    }

    fn tombstoneBlocksPutTx(self: *Self, input: MemoryEventInput) !bool {
        const scoped_session_key = sessionKeyFor(input.session_id);
        const sql =
            "SELECT timestamp_ms, origin_instance_id, origin_sequence FROM memory_tombstones " ++
            "WHERE key = ?1 AND ((scope = 'scoped' AND session_key = ?2) OR (scope = 'all' AND session_key = '*'))";
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);
        _ = c.sqlite3_bind_text(stmt, 1, input.key.ptr, @intCast(input.key.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 2, scoped_session_key.ptr, @intCast(scoped_session_key.len), SQLITE_STATIC);

        while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
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
            "SELECT text, category, value_kind, event_timestamp_ms, event_origin_instance_id, event_origin_sequence FROM lancedb_memories WHERE key = ?1 AND session_id = ?2 LIMIT 1"
        else
            "SELECT text, category, value_kind, event_timestamp_ms, event_origin_instance_id, event_origin_sequence FROM lancedb_memories WHERE key = ?1 AND session_id IS NULL LIMIT 1";
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self.db, select_sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);
        _ = c.sqlite3_bind_text(stmt, 1, input.key.ptr, @intCast(input.key.len), SQLITE_STATIC);
        if (input.session_id) |sid| _ = c.sqlite3_bind_text(stmt, 2, sid.ptr, @intCast(sid.len), SQLITE_STATIC);

        var existing_content: ?[]u8 = null;
        defer if (existing_content) |value| self.allocator.free(value);
        var existing_category: ?MemoryCategory = null;
        defer if (existing_category) |category| switch (category) {
            .custom => |name| self.allocator.free(name),
            else => {},
        };
        var existing_value_kind: ?MemoryValueKind = null;

        if (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
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

        const id = try key_codec.encode(self.allocator, input.key, input.session_id);
        defer self.allocator.free(id);
        const emb_bytes = try self.computeEmbeddingBytes(resolved_state.content);
        defer if (emb_bytes) |bytes| self.allocator.free(bytes);
        const now = try std.fmt.allocPrint(self.allocator, "{d}", .{@divTrunc(input.timestamp_ms, 1000)});
        defer self.allocator.free(now);
        const cat_str = resolved_state.category.toString();
        const value_kind_str = if (resolved_state.value_kind) |kind| kind.toString() else null;

        const sql =
            "INSERT INTO lancedb_memories (id, key, text, embedding, importance, category, value_kind, session_id, event_timestamp_ms, event_origin_instance_id, event_origin_sequence, created_at, updated_at) " ++
            "VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13) " ++
            "ON CONFLICT(key, COALESCE(session_id, '__global__')) DO UPDATE SET " ++
            "id = excluded.id, text = excluded.text, embedding = excluded.embedding, importance = excluded.importance, " ++
            "category = excluded.category, value_kind = excluded.value_kind, event_timestamp_ms = excluded.event_timestamp_ms, " ++
            "event_origin_instance_id = excluded.event_origin_instance_id, event_origin_sequence = excluded.event_origin_sequence, updated_at = excluded.updated_at";
        var upsert_stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self.db, sql, -1, &upsert_stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(upsert_stmt);

        _ = c.sqlite3_bind_text(upsert_stmt, 1, id.ptr, @intCast(id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(upsert_stmt, 2, input.key.ptr, @intCast(input.key.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(upsert_stmt, 3, resolved_state.content.ptr, @intCast(resolved_state.content.len), SQLITE_STATIC);
        if (emb_bytes) |bytes| {
            _ = c.sqlite3_bind_blob(upsert_stmt, 4, bytes.ptr, @intCast(bytes.len), SQLITE_STATIC);
        } else {
            _ = c.sqlite3_bind_null(upsert_stmt, 4);
        }
        _ = c.sqlite3_bind_double(upsert_stmt, 5, self.config.default_importance);
        _ = c.sqlite3_bind_text(upsert_stmt, 6, cat_str.ptr, @intCast(cat_str.len), SQLITE_STATIC);
        bindNullableText(upsert_stmt, 7, value_kind_str);
        bindNullableText(upsert_stmt, 8, input.session_id);
        _ = c.sqlite3_bind_int64(upsert_stmt, 9, input.timestamp_ms);
        _ = c.sqlite3_bind_text(upsert_stmt, 10, input.origin_instance_id.ptr, @intCast(input.origin_instance_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_int64(upsert_stmt, 11, @intCast(input.origin_sequence));
        _ = c.sqlite3_bind_text(upsert_stmt, 12, now.ptr, @intCast(now.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(upsert_stmt, 13, now.ptr, @intCast(now.len), SQLITE_STATIC);
        if (c.sqlite3_step(upsert_stmt) != c.SQLITE_DONE) return error.StepFailed;
    }

    fn deleteScopedStateTx(self: *Self, input: MemoryEventInput) !void {
        const select_sql = if (input.session_id != null)
            "SELECT event_timestamp_ms, event_origin_instance_id, event_origin_sequence FROM lancedb_memories WHERE key = ?1 AND session_id = ?2 LIMIT 1"
        else
            "SELECT event_timestamp_ms, event_origin_instance_id, event_origin_sequence FROM lancedb_memories WHERE key = ?1 AND session_id IS NULL LIMIT 1";
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self.db, select_sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);
        _ = c.sqlite3_bind_text(stmt, 1, input.key.ptr, @intCast(input.key.len), SQLITE_STATIC);
        if (input.session_id) |sid| _ = c.sqlite3_bind_text(stmt, 2, sid.ptr, @intCast(sid.len), SQLITE_STATIC);

        if (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            const timestamp_ms = c.sqlite3_column_int64(stmt, 0);
            const origin_ptr = c.sqlite3_column_text(stmt, 1);
            const origin_len: usize = @intCast(c.sqlite3_column_bytes(stmt, 1));
            const origin = if (origin_ptr == null) "" else @as([*]const u8, @ptrCast(origin_ptr))[0..origin_len];
            const origin_sequence: u64 = @intCast(@max(c.sqlite3_column_int64(stmt, 2), 0));
            if (compareInputToMetadata(input, timestamp_ms, origin, origin_sequence) >= 0) {
                const delete_sql = if (input.session_id != null)
                    "DELETE FROM lancedb_memories WHERE key = ?1 AND session_id = ?2"
                else
                    "DELETE FROM lancedb_memories WHERE key = ?1 AND session_id IS NULL";
                var delete_stmt: ?*c.sqlite3_stmt = null;
                if (c.sqlite3_prepare_v2(self.db, delete_sql, -1, &delete_stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
                defer _ = c.sqlite3_finalize(delete_stmt);
                _ = c.sqlite3_bind_text(delete_stmt, 1, input.key.ptr, @intCast(input.key.len), SQLITE_STATIC);
                if (input.session_id) |sid| _ = c.sqlite3_bind_text(delete_stmt, 2, sid.ptr, @intCast(sid.len), SQLITE_STATIC);
                if (c.sqlite3_step(delete_stmt) != c.SQLITE_DONE) return error.StepFailed;
            }
        }
    }

    fn deleteAllStateTx(self: *Self, input: MemoryEventInput) !void {
        const select_sql =
            "SELECT session_id, event_timestamp_ms, event_origin_instance_id, event_origin_sequence FROM lancedb_memories WHERE key = ?1";
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self.db, select_sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);
        _ = c.sqlite3_bind_text(stmt, 1, input.key.ptr, @intCast(input.key.len), SQLITE_STATIC);

        var sessions_to_delete: std.ArrayListUnmanaged(?[]u8) = .empty;
        defer {
            for (sessions_to_delete.items) |sid_opt| if (sid_opt) |sid| self.allocator.free(sid);
            sessions_to_delete.deinit(self.allocator);
        }

        while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
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
                "DELETE FROM lancedb_memories WHERE key = ?1 AND session_id = ?2"
            else
                "DELETE FROM lancedb_memories WHERE key = ?1 AND session_id IS NULL";
            var delete_stmt: ?*c.sqlite3_stmt = null;
            if (c.sqlite3_prepare_v2(self.db, delete_sql, -1, &delete_stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
            defer _ = c.sqlite3_finalize(delete_stmt);
            _ = c.sqlite3_bind_text(delete_stmt, 1, input.key.ptr, @intCast(input.key.len), SQLITE_STATIC);
            if (sid_opt) |sid| _ = c.sqlite3_bind_text(delete_stmt, 2, sid.ptr, @intCast(sid.len), SQLITE_STATIC);
            if (c.sqlite3_step(delete_stmt) != c.SQLITE_DONE) return error.StepFailed;
        }
    }

    fn upsertTombstoneTx(self: *Self, input: MemoryEventInput, scope: []const u8, session_key: []const u8, session_id: ?[]const u8) !void {
        const select_sql =
            "SELECT timestamp_ms, origin_instance_id, origin_sequence FROM memory_tombstones WHERE key = ?1 AND scope = ?2 AND session_key = ?3 LIMIT 1";
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self.db, select_sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);
        _ = c.sqlite3_bind_text(stmt, 1, input.key.ptr, @intCast(input.key.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 2, scope.ptr, @intCast(scope.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 3, session_key.ptr, @intCast(session_key.len), SQLITE_STATIC);

        if (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
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
            "session_id = excluded.session_id, timestamp_ms = excluded.timestamp_ms, " ++
            "origin_instance_id = excluded.origin_instance_id, origin_sequence = excluded.origin_sequence";
        var upsert_stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self.db, upsert_sql, -1, &upsert_stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(upsert_stmt);
        _ = c.sqlite3_bind_text(upsert_stmt, 1, input.key.ptr, @intCast(input.key.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(upsert_stmt, 2, scope.ptr, @intCast(scope.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(upsert_stmt, 3, session_key.ptr, @intCast(session_key.len), SQLITE_STATIC);
        bindNullableText(upsert_stmt, 4, session_id);
        _ = c.sqlite3_bind_int64(upsert_stmt, 5, input.timestamp_ms);
        _ = c.sqlite3_bind_text(upsert_stmt, 6, input.origin_instance_id.ptr, @intCast(input.origin_instance_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_int64(upsert_stmt, 7, @intCast(input.origin_sequence));
        if (c.sqlite3_step(upsert_stmt) != c.SQLITE_DONE) return error.StepFailed;
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

    fn emitLocalEvent(self: *Self, operation: MemoryEventOp, key: []const u8, session_id: ?[]const u8, category: ?MemoryCategory, value_kind: ?MemoryValueKind, content: ?[]const u8) !void {
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
            .value_kind = value_kind,
            .content = content,
        };
        try self.applyEventTx(input);
        if (owns_tx) try self.commitTxn();
        committed = true;
    }

    fn appendCheckpointMetaLine(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), last_sequence: u64, last_timestamp_ms: i64) !void {
        try out.append(allocator, '{');
        try json_util.appendJsonKeyValue(out, allocator, "kind", "meta");
        try out.append(allocator, ',');
        try json_util.appendJsonKey(out, allocator, "schema_version");
        try out.writer(allocator).print("1", .{});
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

    fn exportCheckpointPayload(self: *Self, allocator: std.mem.Allocator) ![]u8 {
        var out: std.ArrayListUnmanaged(u8) = .empty;
        errdefer out.deinit(allocator);

        const last_sequence = try self.memory().lastEventSequence();
        const last_timestamp_ms = @max(
            try self.queryMaxI64("SELECT COALESCE(MAX(event_timestamp_ms), 0) FROM lancedb_memories"),
            try self.queryMaxI64("SELECT COALESCE(MAX(timestamp_ms), 0) FROM memory_tombstones"),
        );
        try appendCheckpointMetaLine(allocator, &out, last_sequence, last_timestamp_ms);

        {
            const sql = "SELECT origin_instance_id, last_origin_sequence FROM memory_event_frontiers ORDER BY origin_instance_id ASC";
            var stmt: ?*c.sqlite3_stmt = null;
            if (c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
            defer _ = c.sqlite3_finalize(stmt);
            while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
                const origin_instance_id = try dupeColumnText(stmt.?, 0, allocator);
                defer allocator.free(origin_instance_id);
                const origin_sequence: u64 = @intCast(@max(c.sqlite3_column_int64(stmt, 1), 0));
                try appendCheckpointFrontierLine(allocator, &out, origin_instance_id, origin_sequence);
            }
        }

        {
            const sql =
                "SELECT key, session_id, category, value_kind, text, event_timestamp_ms, event_origin_instance_id, event_origin_sequence " ++
                "FROM lancedb_memories ORDER BY key ASC, COALESCE(session_id, '') ASC";
            var stmt: ?*c.sqlite3_stmt = null;
            if (c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
            defer _ = c.sqlite3_finalize(stmt);
            while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
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
            if (c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
            defer _ = c.sqlite3_finalize(stmt);
            while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
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

    fn insertCheckpointFrontier(self: *Self, origin_instance_id: []const u8, origin_sequence: u64) !void {
        const sql = "INSERT INTO memory_event_frontiers (origin_instance_id, last_origin_sequence) VALUES (?1, ?2)";
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);
        _ = c.sqlite3_bind_text(stmt, 1, origin_instance_id.ptr, @intCast(origin_instance_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_int64(stmt, 2, @intCast(origin_sequence));
        if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.StepFailed;
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
        const id = try key_codec.encode(self.allocator, key, session_id);
        defer self.allocator.free(id);
        const emb_bytes = try self.computeEmbeddingBytes(content);
        defer if (emb_bytes) |bytes| self.allocator.free(bytes);
        const now = try std.fmt.allocPrint(self.allocator, "{d}", .{@divTrunc(timestamp_ms, 1000)});
        defer self.allocator.free(now);

        const sql =
            "INSERT INTO lancedb_memories (id, key, text, embedding, importance, category, value_kind, session_id, event_timestamp_ms, event_origin_instance_id, event_origin_sequence, created_at, updated_at) " ++
            "VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)";
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);
        _ = c.sqlite3_bind_text(stmt, 1, id.ptr, @intCast(id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 2, key.ptr, @intCast(key.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 3, content.ptr, @intCast(content.len), SQLITE_STATIC);
        if (emb_bytes) |bytes| {
            _ = c.sqlite3_bind_blob(stmt, 4, bytes.ptr, @intCast(bytes.len), SQLITE_STATIC);
        } else {
            _ = c.sqlite3_bind_null(stmt, 4);
        }
        _ = c.sqlite3_bind_double(stmt, 5, self.config.default_importance);
        _ = c.sqlite3_bind_text(stmt, 6, category.ptr, @intCast(category.len), SQLITE_STATIC);
        bindNullableText(stmt, 7, value_kind);
        bindNullableText(stmt, 8, session_id);
        _ = c.sqlite3_bind_int64(stmt, 9, timestamp_ms);
        _ = c.sqlite3_bind_text(stmt, 10, origin_instance_id.ptr, @intCast(origin_instance_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_int64(stmt, 11, @intCast(origin_sequence));
        _ = c.sqlite3_bind_text(stmt, 12, now.ptr, @intCast(now.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 13, now.ptr, @intCast(now.len), SQLITE_STATIC);
        if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.StepFailed;
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
        if (c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);
        _ = c.sqlite3_bind_text(stmt, 1, decoded.logical_key.ptr, @intCast(decoded.logical_key.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 2, scope.ptr, @intCast(scope.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 3, session_key.ptr, @intCast(session_key.len), SQLITE_STATIC);
        bindNullableText(stmt, 4, session_id);
        _ = c.sqlite3_bind_int64(stmt, 5, timestamp_ms);
        _ = c.sqlite3_bind_text(stmt, 6, origin_instance_id.ptr, @intCast(origin_instance_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_int64(stmt, 7, @intCast(origin_sequence));
        if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.StepFailed;
    }

    fn applyCheckpointPayload(self: *Self, payload: []const u8) !void {
        var meta: ?CheckpointMeta = null;
        var frontiers: std.ArrayListUnmanaged(struct { origin_instance_id: []u8, origin_sequence: u64 }) = .empty;
        defer {
            for (frontiers.items) |row| self.allocator.free(row.origin_instance_id);
            frontiers.deinit(self.allocator);
        }
        var states: std.ArrayListUnmanaged(CheckpointStateRow) = .empty;
        defer {
            for (states.items) |row| row.deinit(self.allocator);
            states.deinit(self.allocator);
        }
        var tombstones: std.ArrayListUnmanaged(CheckpointTombstoneRow) = .empty;
        defer {
            for (tombstones.items) |row| row.deinit(self.allocator);
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
                const schema_version = checkpointJsonUnsignedField(parsed.value, "schema_version") orelse return error.InvalidEvent;
                if (schema_version != 1) return error.InvalidEvent;
                meta = .{
                    .last_sequence = checkpointJsonUnsignedField(parsed.value, "last_sequence") orelse 0,
                    .compacted_through_sequence = checkpointJsonUnsignedField(parsed.value, "compacted_through_sequence") orelse 0,
                    .last_timestamp_ms = checkpointJsonIntegerField(parsed.value, "last_timestamp_ms") orelse 0,
                };
                continue;
            }
            if (std.mem.eql(u8, kind, "frontier")) {
                try frontiers.append(self.allocator, .{
                    .origin_instance_id = try self.allocator.dupe(u8, checkpointJsonStringField(parsed.value, "origin_instance_id") orelse return error.InvalidEvent),
                    .origin_sequence = checkpointJsonUnsignedField(parsed.value, "origin_sequence") orelse return error.InvalidEvent,
                });
                continue;
            }
            if (std.mem.eql(u8, kind, "state")) {
                try states.append(self.allocator, .{
                    .key = try self.allocator.dupe(u8, checkpointJsonStringField(parsed.value, "key") orelse return error.InvalidEvent),
                    .session_id = if (checkpointJsonNullableStringField(parsed.value, "session_id")) |sid| try self.allocator.dupe(u8, sid) else null,
                    .category = try self.allocator.dupe(u8, checkpointJsonStringField(parsed.value, "category") orelse return error.InvalidEvent),
                    .value_kind = if (checkpointJsonNullableStringField(parsed.value, "value_kind")) |kind_text| try self.allocator.dupe(u8, kind_text) else null,
                    .content = try self.allocator.dupe(u8, checkpointJsonStringField(parsed.value, "content") orelse return error.InvalidEvent),
                    .timestamp_ms = checkpointJsonIntegerField(parsed.value, "timestamp_ms") orelse return error.InvalidEvent,
                    .origin_instance_id = try self.allocator.dupe(u8, checkpointJsonStringField(parsed.value, "origin_instance_id") orelse return error.InvalidEvent),
                    .origin_sequence = checkpointJsonUnsignedField(parsed.value, "origin_sequence") orelse return error.InvalidEvent,
                });
                continue;
            }
            if (std.mem.eql(u8, kind, "scoped_tombstone") or std.mem.eql(u8, kind, "key_tombstone")) {
                try tombstones.append(self.allocator, .{
                    .kind = if (std.mem.eql(u8, kind, "key_tombstone")) "key_tombstone" else "scoped_tombstone",
                    .key = try self.allocator.dupe(u8, checkpointJsonStringField(parsed.value, "key") orelse return error.InvalidEvent),
                    .timestamp_ms = checkpointJsonIntegerField(parsed.value, "timestamp_ms") orelse return error.InvalidEvent,
                    .origin_instance_id = try self.allocator.dupe(u8, checkpointJsonStringField(parsed.value, "origin_instance_id") orelse return error.InvalidEvent),
                    .origin_sequence = checkpointJsonUnsignedField(parsed.value, "origin_sequence") orelse return error.InvalidEvent,
                });
                continue;
            }
            return error.InvalidEvent;
        }

        const checkpoint_meta = meta orelse return error.InvalidEvent;

        var owns_tx = c.sqlite3_get_autocommit(self.db) != 0;
        if (owns_tx) owns_tx = try self.beginImmediate();
        var committed = false;
        errdefer if (owns_tx and !committed) self.rollbackTxn();

        try self.execTxnSql("DELETE FROM memory_events;");
        try self.execTxnSql("DELETE FROM memory_tombstones;");
        try self.execTxnSql("DELETE FROM memory_event_frontiers;");
        try self.execTxnSql("DELETE FROM memory_feed_meta;");
        try self.execTxnSql("DELETE FROM lancedb_memories;");

        for (frontiers.items) |row| try self.insertCheckpointFrontier(row.origin_instance_id, row.origin_sequence);
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
            try self.insertCheckpointTombstone(row.kind, row.key, row.timestamp_ms, row.origin_instance_id, row.origin_sequence);
        }
        try self.setCompactedThroughSequenceTx(checkpoint_meta.compacted_through_sequence);

        if (owns_tx) try self.commitTxn();
        committed = true;
    }

    fn implName(_: *anyopaque) []const u8 {
        return "lancedb";
    }

    fn implStore(ptr: *anyopaque, key: []const u8, content: []const u8, category: MemoryCategory, session_id: ?[]const u8) anyerror!void {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        try self_.emitLocalEvent(.put, key, session_id, category, null, content);
    }

    fn vectorRecall(self_: *Self, allocator: std.mem.Allocator, query_emb: []const f32, limit: usize, session_id: ?[]const u8) ![]MemoryEntry {
        const sql = if (session_id != null)
            "SELECT id, key, text, category, updated_at, embedding, session_id FROM lancedb_memories WHERE embedding IS NOT NULL AND session_id = ?1 ORDER BY updated_at DESC LIMIT 1000"
        else
            "SELECT id, key, text, category, updated_at, embedding, session_id FROM lancedb_memories WHERE embedding IS NOT NULL ORDER BY updated_at DESC LIMIT 1000";
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self_.db, sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        var sid_z: ?[:0]u8 = null;
        defer if (sid_z) |sid| allocator.free(sid);
        if (session_id) |sid| {
            sid_z = try allocator.dupeZ(u8, sid);
            _ = c.sqlite3_bind_text(stmt, 1, sid_z.?.ptr, @intCast(sid_z.?.len), SQLITE_STATIC);
        }

        const Scored = struct {
            entry: MemoryEntry,
            score: f32,
        };
        var scored: std.ArrayListUnmanaged(Scored) = .empty;
        errdefer {
            for (scored.items) |*item| item.entry.deinit(allocator);
            scored.deinit(allocator);
        }

        while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            const blob_ptr = c.sqlite3_column_blob(stmt, 5);
            const blob_len = c.sqlite3_column_bytes(stmt, 5);
            if (blob_ptr == null or blob_len <= 0) continue;

            const bytes: [*]const u8 = @ptrCast(blob_ptr);
            const slice = bytes[0..@intCast(blob_len)];
            const entry_emb = vector.bytesToVec(self_.allocator, slice) catch continue;
            defer self_.allocator.free(entry_emb);

            const sim = vector.cosineSimilarity(query_emb, entry_emb);
            if (sim < self_.config.min_search_score) continue;

            const id_ptr = c.sqlite3_column_text(stmt, 0);
            const key_ptr = c.sqlite3_column_text(stmt, 1);
            const text_ptr = c.sqlite3_column_text(stmt, 2);
            const cat_ptr = c.sqlite3_column_text(stmt, 3);
            const ts_ptr = c.sqlite3_column_text(stmt, 4);
            const sid_ptr = c.sqlite3_column_text(stmt, 6);
            if (id_ptr == null or key_ptr == null or text_ptr == null) continue;

            const id = try allocator.dupe(u8, std.mem.span(id_ptr));
            errdefer allocator.free(id);
            const key = try allocator.dupe(u8, std.mem.span(key_ptr));
            errdefer allocator.free(key);
            const content = try allocator.dupe(u8, std.mem.span(text_ptr));
            errdefer allocator.free(content);
            const timestamp = if (ts_ptr != null) try allocator.dupe(u8, std.mem.span(ts_ptr)) else try allocator.dupe(u8, "0");
            errdefer allocator.free(timestamp);
            const cat_str = if (cat_ptr != null) std.mem.span(cat_ptr) else "conversation";
            const stored_sid = if (sid_ptr != null and c.sqlite3_column_bytes(stmt, 6) > 0)
                try allocator.dupe(u8, std.mem.span(sid_ptr))
            else
                null;
            errdefer if (stored_sid) |sid| allocator.free(sid);

            try scored.append(allocator, .{
                .entry = .{
                    .id = id,
                    .key = key,
                    .content = content,
                    .category = try ownedCategoryFromString(allocator, cat_str),
                    .timestamp = timestamp,
                    .session_id = stored_sid,
                },
                .score = sim,
            });
        }

        std.mem.sort(Scored, scored.items, {}, struct {
            fn lessThan(_: void, a: Scored, b: Scored) bool {
                return a.score > b.score;
            }
        }.lessThan);

        const take = @min(scored.items.len, limit);
        const result = try allocator.alloc(MemoryEntry, take);
        for (0..take) |i| result[i] = scored.items[i].entry;
        for (take..scored.items.len) |i| scored.items[i].entry.deinit(allocator);
        scored.clearAndFree(allocator);
        return result;
    }

    fn textRecall(_: *Self, db: *c.sqlite3, allocator: std.mem.Allocator, query: []const u8, limit: usize, session_id: ?[]const u8) ![]MemoryEntry {
        const like_pattern = try std.fmt.allocPrint(allocator, "%{s}%", .{query});
        defer allocator.free(like_pattern);

        const sql = if (session_id != null)
            "SELECT id, key, text, category, updated_at, session_id FROM lancedb_memories WHERE (text LIKE ?1 OR key LIKE ?1) AND session_id = ?3 ORDER BY updated_at DESC LIMIT ?2"
        else
            "SELECT id, key, text, category, updated_at, session_id FROM lancedb_memories WHERE text LIKE ?1 OR key LIKE ?1 ORDER BY updated_at DESC LIMIT ?2";
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(db, sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, like_pattern.ptr, @intCast(like_pattern.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_int(stmt, 2, @intCast(limit));

        var sid_z: ?[:0]u8 = null;
        defer if (sid_z) |sid| allocator.free(sid);
        if (session_id) |sid| {
            sid_z = try allocator.dupeZ(u8, sid);
            _ = c.sqlite3_bind_text(stmt, 3, sid_z.?.ptr, @intCast(sid_z.?.len), SQLITE_STATIC);
        }

        var results: std.ArrayListUnmanaged(MemoryEntry) = .empty;
        errdefer {
            for (results.items) |*entry| entry.deinit(allocator);
            results.deinit(allocator);
        }

        while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            try results.append(allocator, try readEntryFromRow(stmt.?, allocator));
        }
        return results.toOwnedSlice(allocator);
    }

    fn implRecall(ptr: *anyopaque, allocator: std.mem.Allocator, query: []const u8, limit: usize, session_id: ?[]const u8) anyerror![]MemoryEntry {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        if (self_.embedder) |ep| {
            const query_emb = ep.embed(allocator, query) catch null;
            if (query_emb) |embedding| {
                defer allocator.free(embedding);
                if (embedding.len > 0) {
                    return self_.vectorRecall(allocator, embedding, limit, session_id);
                }
            }
        }
        return self_.textRecall(self_.db orelse return error.NotConnected, allocator, query, limit, session_id);
    }

    fn implGet(ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8) anyerror!?MemoryEntry {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const sql = "SELECT id, key, text, category, updated_at, session_id FROM lancedb_memories WHERE key = ?1 AND session_id IS NULL LIMIT 1";
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self_.db, sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);
        _ = c.sqlite3_bind_text(stmt, 1, key.ptr, @intCast(key.len), SQLITE_STATIC);
        if (c.sqlite3_step(stmt) == c.SQLITE_ROW) return try readEntryFromRow(stmt.?, allocator);
        return null;
    }

    fn implGetScoped(ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8, session_id: ?[]const u8) anyerror!?MemoryEntry {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const sql = if (session_id != null)
            "SELECT id, key, text, category, updated_at, session_id FROM lancedb_memories WHERE key = ?1 AND session_id = ?2 LIMIT 1"
        else
            "SELECT id, key, text, category, updated_at, session_id FROM lancedb_memories WHERE key = ?1 AND session_id IS NULL LIMIT 1";
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self_.db, sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);
        _ = c.sqlite3_bind_text(stmt, 1, key.ptr, @intCast(key.len), SQLITE_STATIC);
        if (session_id) |sid| _ = c.sqlite3_bind_text(stmt, 2, sid.ptr, @intCast(sid.len), SQLITE_STATIC);
        if (c.sqlite3_step(stmt) == c.SQLITE_ROW) return try readEntryFromRow(stmt.?, allocator);
        return null;
    }

    fn implList(ptr: *anyopaque, allocator: std.mem.Allocator, category: ?MemoryCategory, session_id: ?[]const u8) anyerror![]MemoryEntry {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const sql = if (category != null and session_id != null)
            "SELECT id, key, text, category, updated_at, session_id FROM lancedb_memories WHERE category = ?1 AND session_id = ?2 ORDER BY updated_at DESC"
        else if (category != null)
            "SELECT id, key, text, category, updated_at, session_id FROM lancedb_memories WHERE category = ?1 ORDER BY updated_at DESC"
        else if (session_id != null)
            "SELECT id, key, text, category, updated_at, session_id FROM lancedb_memories WHERE session_id = ?1 ORDER BY updated_at DESC"
        else
            "SELECT id, key, text, category, updated_at, session_id FROM lancedb_memories ORDER BY updated_at DESC";
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self_.db, sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        var cat_z: ?[:0]u8 = null;
        defer if (cat_z) |value| allocator.free(value);
        var sid_z: ?[:0]u8 = null;
        defer if (sid_z) |value| allocator.free(value);
        if (category != null and session_id != null) {
            cat_z = try allocator.dupeZ(u8, category.?.toString());
            sid_z = try allocator.dupeZ(u8, session_id.?);
            _ = c.sqlite3_bind_text(stmt, 1, cat_z.?.ptr, @intCast(cat_z.?.len), SQLITE_STATIC);
            _ = c.sqlite3_bind_text(stmt, 2, sid_z.?.ptr, @intCast(sid_z.?.len), SQLITE_STATIC);
        } else if (category) |cat| {
            cat_z = try allocator.dupeZ(u8, cat.toString());
            _ = c.sqlite3_bind_text(stmt, 1, cat_z.?.ptr, @intCast(cat_z.?.len), SQLITE_STATIC);
        } else if (session_id) |sid| {
            sid_z = try allocator.dupeZ(u8, sid);
            _ = c.sqlite3_bind_text(stmt, 1, sid_z.?.ptr, @intCast(sid_z.?.len), SQLITE_STATIC);
        }

        var results: std.ArrayListUnmanaged(MemoryEntry) = .empty;
        errdefer {
            for (results.items) |*entry| entry.deinit(allocator);
            results.deinit(allocator);
        }
        while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            try results.append(allocator, try readEntryFromRow(stmt.?, allocator));
        }
        return results.toOwnedSlice(allocator);
    }

    fn implForget(ptr: *anyopaque, key: []const u8) anyerror!bool {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const sql = "SELECT 1 FROM lancedb_memories WHERE key = ?1 LIMIT 1";
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self_.db, sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);
        _ = c.sqlite3_bind_text(stmt, 1, key.ptr, @intCast(key.len), SQLITE_STATIC);
        if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return false;
        try self_.emitLocalEvent(.delete_all, key, null, null, null, null);
        return true;
    }

    fn implForgetScoped(ptr: *anyopaque, key: []const u8, session_id: ?[]const u8) anyerror!bool {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const existing = try implGetScoped(ptr, self_.allocator, key, session_id);
        if (existing == null) return false;
        existing.?.deinit(self_.allocator);
        try self_.emitLocalEvent(.delete_scoped, key, session_id, null, null, null);
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
        if (c.sqlite3_prepare_v2(self_.db, sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);
        _ = c.sqlite3_bind_int64(stmt, 1, @intCast(after_sequence));
        _ = c.sqlite3_bind_int64(stmt, 2, @intCast(limit));

        var events: std.ArrayListUnmanaged(MemoryEvent) = .empty;
        errdefer {
            for (events.items) |*event| event.deinit(allocator);
            events.deinit(allocator);
        }
        while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
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
        const compacted_through = try self_.getCompactedThroughSequence();
        const max_in_events = try self_.queryMaxI64("SELECT COALESCE(MAX(local_sequence), 0) FROM memory_events");
        const tail_sequence: u64 = if (max_in_events < 0) 0 else @intCast(max_in_events);
        return @max(compacted_through, tail_sequence);
    }

    fn implEventFeedInfo(ptr: *anyopaque, allocator: std.mem.Allocator) anyerror!MemoryEventFeedInfo {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const compacted_through = try self_.getCompactedThroughSequence();
        return .{
            .instance_id = try allocator.dupe(u8, self_.localInstanceId()),
            .last_sequence = try self_.memory().lastEventSequence(),
            .next_local_origin_sequence = try self_.nextLocalOriginSequenceTx(),
            .supports_compaction = true,
            .storage_kind = .native,
            .journal_path = null,
            .checkpoint_path = null,
            .compacted_through_sequence = compacted_through,
            .oldest_available_sequence = if (compacted_through > 0) compacted_through + 1 else 1,
        };
    }

    fn implCompactEvents(ptr: *anyopaque) anyerror!u64 {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const through = try self_.memory().lastEventSequence();
        if (through == 0) return 0;

        var owns_tx = c.sqlite3_get_autocommit(self_.db) != 0;
        if (owns_tx) owns_tx = try self_.beginImmediate();
        var committed = false;
        errdefer if (owns_tx and !committed) self_.rollbackTxn();

        const delete_sql = "DELETE FROM memory_events WHERE local_sequence <= ?1";
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self_.db, delete_sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);
        _ = c.sqlite3_bind_int64(stmt, 1, @intCast(through));
        if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.StepFailed;
        try self_.setCompactedThroughSequenceTx(through);

        if (owns_tx) try self_.commitTxn();
        committed = true;
        return through;
    }

    fn implExportCheckpoint(ptr: *anyopaque, allocator: std.mem.Allocator) anyerror![]u8 {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        return self_.exportCheckpointPayload(allocator);
    }

    fn implApplyCheckpoint(ptr: *anyopaque, payload: []const u8) anyerror!void {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        try self_.applyCheckpointPayload(payload);
    }

    fn implCount(ptr: *anyopaque) anyerror!usize {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const sql = "SELECT COUNT(*) FROM lancedb_memories";
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self_.db, sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);
        if (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            return @intCast(c.sqlite3_column_int64(stmt, 0));
        }
        return 0;
    }

    fn implHealthCheck(ptr: *anyopaque) bool {
        _ = implCount(ptr) catch return false;
        return true;
    }

    fn implDeinit(ptr: *anyopaque) void {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        self_.deinit();
        if (self_.owns_self) self_.allocator.destroy(self_);
    }

    const vtable = Memory.VTable{
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
};

const testing = std.testing;

const ConstantEmbedding = struct {
    values: []const f32,

    const Self = @This();

    fn implName(_: *anyopaque) []const u8 {
        return "test-constant";
    }

    fn implDimensions(ptr: *anyopaque) u32 {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        return @intCast(self_.values.len);
    }

    fn implEmbed(ptr: *anyopaque, allocator: std.mem.Allocator, _: []const u8) anyerror![]f32 {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        return allocator.dupe(f32, self_.values);
    }

    fn implDeinit(_: *anyopaque) void {}

    const vtable = EmbeddingProvider.VTable{
        .name = &implName,
        .dimensions = &implDimensions,
        .embed = &implEmbed,
        .deinit = &implDeinit,
    };

    fn provider(self: *Self) EmbeddingProvider {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }
};

test "lancedb store and recall" {
    if (!build_options.enable_sqlite) return;
    var impl_ = try LanceDbMemory.init(testing.allocator, ":memory:", null, .{});
    defer impl_.deinit();

    var mem = impl_.memory();
    try mem.store("greeting", "Hello, world!", .core, null);
    try testing.expectEqual(@as(usize, 1), try mem.count());

    const results = try mem.recall(testing.allocator, "Hello", 10, null);
    defer root.freeEntries(testing.allocator, results);
    try testing.expect(results.len >= 1);
    try testing.expectEqualStrings("greeting", results[0].key);
}

test "lancedb get by key" {
    if (!build_options.enable_sqlite) return;
    var impl_ = try LanceDbMemory.init(testing.allocator, ":memory:", null, .{});
    defer impl_.deinit();
    var mem = impl_.memory();
    try mem.store("test_key", "test content", .daily, null);
    const entry = try mem.get(testing.allocator, "test_key");
    try testing.expect(entry != null);
    if (entry) |e| {
        var owned = e;
        defer owned.deinit(testing.allocator);
        try testing.expectEqualStrings("test content", owned.content);
    }
}

test "lancedb forget" {
    if (!build_options.enable_sqlite) return;
    var impl_ = try LanceDbMemory.init(testing.allocator, ":memory:", null, .{});
    defer impl_.deinit();
    var mem = impl_.memory();
    try mem.store("to_forget", "bye", .conversation, null);
    try testing.expectEqual(@as(usize, 1), try mem.count());
    try testing.expect(try mem.forget("to_forget"));
    try testing.expectEqual(@as(usize, 0), try mem.count());
}

test "lancedb list by category" {
    if (!build_options.enable_sqlite) return;
    var impl_ = try LanceDbMemory.init(testing.allocator, ":memory:", null, .{});
    defer impl_.deinit();
    var mem = impl_.memory();
    try mem.store("fact1", "Earth orbits Sun", .core, null);
    try mem.store("pref1", "User likes Zig", .daily, null);
    try mem.store("talk1", "Discussed memory", .conversation, null);
    const facts = try mem.list(testing.allocator, .core, null);
    defer root.freeEntries(testing.allocator, facts);
    try testing.expectEqual(@as(usize, 1), facts.len);
}

test "lancedb empty recall" {
    if (!build_options.enable_sqlite) return;
    var impl_ = try LanceDbMemory.init(testing.allocator, ":memory:", null, .{});
    defer impl_.deinit();
    var mem = impl_.memory();
    const results = try mem.recall(testing.allocator, "anything", 10, null);
    defer root.freeEntries(testing.allocator, results);
    try testing.expectEqual(@as(usize, 0), results.len);
}

test "lancedb health check" {
    if (!build_options.enable_sqlite) return;
    var impl_ = try LanceDbMemory.init(testing.allocator, ":memory:", null, .{});
    defer impl_.deinit();
    try testing.expect(impl_.memory().healthCheck());
}

test "lancedb upsert updates existing" {
    if (!build_options.enable_sqlite) return;
    var impl_ = try LanceDbMemory.init(testing.allocator, ":memory:", null, .{});
    defer impl_.deinit();
    var mem = impl_.memory();
    try mem.store("key1", "version 1", .core, null);
    try mem.store("key1", "version 2", .core, null);
    try testing.expectEqual(@as(usize, 1), try mem.count());
    const entry = try mem.get(testing.allocator, "key1");
    try testing.expect(entry != null);
    if (entry) |e| {
        var owned = e;
        defer owned.deinit(testing.allocator);
        try testing.expectEqualStrings("version 2", owned.content);
    }
}

test "lancedb forget nonexistent returns false" {
    if (!build_options.enable_sqlite) return;
    var impl_ = try LanceDbMemory.init(testing.allocator, ":memory:", null, .{});
    defer impl_.deinit();
    try testing.expect(!(try impl_.memory().forget("no_such_key")));
}

test "lancedb does not drop stores with identical embeddings" {
    if (!build_options.enable_sqlite) return;
    var embedder_impl = ConstantEmbedding{ .values = &.{ 1.0, 0.0, 0.0 } };
    var impl_ = try LanceDbMemory.init(testing.allocator, ":memory:", embedder_impl.provider(), .{});
    defer impl_.deinit();
    var mem = impl_.memory();
    try mem.store("alpha", "first", .core, null);
    try mem.store("beta", "second", .core, null);
    try testing.expectEqual(@as(usize, 2), try mem.count());
}

test "lancedb preserves custom categories" {
    if (!build_options.enable_sqlite) return;
    var impl_ = try LanceDbMemory.init(testing.allocator, ":memory:", null, .{});
    defer impl_.deinit();
    var mem = impl_.memory();
    try mem.store("behavior", "precise", .{ .custom = "behavior" }, null);
    const entry = try mem.get(testing.allocator, "behavior");
    try testing.expect(entry != null);
    if (entry) |e| {
        var owned = e;
        defer owned.deinit(testing.allocator);
        try testing.expect(owned.category.eql(.{ .custom = "behavior" }));
    }
}

test "lancedb native feed roundtrip applies events" {
    if (!build_options.enable_sqlite) return;
    var source = try LanceDbMemory.initWithInstanceId(testing.allocator, ":memory:", "agent-a", null, .{});
    defer source.deinit();
    var replica = try LanceDbMemory.initWithInstanceId(testing.allocator, ":memory:", "agent-b", null, .{});
    defer replica.deinit();

    var source_mem = source.memory();
    var replica_mem = replica.memory();
    try source_mem.store("pref.theme", "{\"theme\":\"dark\"}", .{ .custom = "preferences" }, null);
    try source_mem.store("shared", "agent memory", .conversation, "sess-a");

    const events = try source_mem.listEvents(testing.allocator, 0, 32);
    defer root.freeEvents(testing.allocator, events);
    try testing.expect(events.len >= 2);
    for (events) |event| {
        try replica_mem.applyEvent(.{
            .origin_instance_id = event.origin_instance_id,
            .origin_sequence = event.origin_sequence,
            .timestamp_ms = event.timestamp_ms,
            .operation = event.operation,
            .key = event.key,
            .session_id = event.session_id,
            .category = event.category,
            .value_kind = event.value_kind,
            .content = event.content,
        });
    }

    const global_entry = try replica_mem.get(testing.allocator, "pref.theme");
    try testing.expect(global_entry != null);
    if (global_entry) |entry| {
        var owned = entry;
        defer owned.deinit(testing.allocator);
        try testing.expectEqualStrings("{\"theme\":\"dark\"}", owned.content);
    }
    const scoped_entry = try replica_mem.getScoped(testing.allocator, "shared", "sess-a");
    try testing.expect(scoped_entry != null);
    if (scoped_entry) |entry| {
        var owned = entry;
        defer owned.deinit(testing.allocator);
        try testing.expectEqualStrings("agent memory", owned.content);
    }
}

test "lancedb compact and checkpoint restore preserve sequence continuity" {
    if (!build_options.enable_sqlite) return;
    var source = try LanceDbMemory.initWithInstanceId(testing.allocator, ":memory:", "agent-a", null, .{});
    defer source.deinit();
    var source_mem = source.memory();

    try source_mem.store("alpha", "one", .core, null);
    try source_mem.store("beta", "two", .conversation, "sess-a");
    const compacted = try source_mem.compactEvents();
    try testing.expect(compacted > 0);
    try testing.expectError(error.CursorExpired, source_mem.listEvents(testing.allocator, 0, 8));
    const checkpoint = try source_mem.exportCheckpoint(testing.allocator);
    defer testing.allocator.free(checkpoint);

    var replica = try LanceDbMemory.initWithInstanceId(testing.allocator, ":memory:", "agent-a", null, .{});
    defer replica.deinit();
    var replica_mem = replica.memory();
    try replica_mem.applyCheckpoint(checkpoint);
    const info = try replica_mem.eventFeedInfo(testing.allocator);
    defer info.deinit(testing.allocator);
    try testing.expectEqual(compacted, info.compacted_through_sequence);
    try testing.expectEqual(compacted + 1, info.next_local_origin_sequence);

    try replica_mem.store("gamma", "three", .daily, null);
    const tail = try replica_mem.listEvents(testing.allocator, compacted, 8);
    defer root.freeEvents(testing.allocator, tail);
    try testing.expectEqual(@as(usize, 1), tail.len);
    try testing.expectEqualStrings("gamma", tail[0].key);
    try testing.expectEqual(compacted + 1, tail[0].origin_sequence);
}
