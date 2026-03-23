const std = @import("std");
const build_options = @import("build_options");
const sqlite_backend = if (build_options.enable_sqlite) @import("engines/sqlite.zig") else @import("engines/sqlite_disabled.zig");
const root = @import("root.zig");

const log = std.log.scoped(.memory_context_core);

const c = sqlite_backend.c;
const SQLITE_STATIC = sqlite_backend.SQLITE_STATIC;
const BUSY_TIMEOUT_MS: c_int = 5000;
const CORE_DB_FILENAME = ".nullclaw-context.db";
const LEGACY_EVENT_JOURNAL_FILENAME_PREFIX = ".nullclaw-memory.";
const LEGACY_EVENT_JOURNAL_FILENAME_SUFFIX = ".events.jsonl";
const LEGACY_EVENT_CHECKPOINT_FILENAME_SUFFIX = ".checkpoint.json";
const LEGACY_MAX_EVENT_LINE_BYTES = 16 * 1024 * 1024;
const LEGACY_CHECKPOINT_ORIGIN = "_legacy_checkpoint";

pub const ContextApplyResult = struct {
    accepted: bool,
    materialized: bool,
    sequence: u64 = 0,
};

const VersionStamp = struct {
    timestamp_ms: i64,
    origin_instance_id: []const u8,
    origin_sequence: u64,
    event_sequence: u64,
};

const JoinedStateRow = struct {
    entry: ?root.MemoryEntry,
    deleted: bool,
    version: VersionStamp,
    tombstone: ?VersionStamp,

    fn takeEntry(self: *JoinedStateRow) root.MemoryEntry {
        const entry = self.entry.?;
        self.entry = null;
        return entry;
    }

    fn deinit(self: *JoinedStateRow, allocator: std.mem.Allocator) void {
        if (self.entry) |*entry| entry.deinit(allocator);
        if (self.tombstone) |ts| allocator.free(ts.origin_instance_id);
        allocator.free(self.version.origin_instance_id);
    }
};

pub const ContextCore = struct {
    allocator: std.mem.Allocator,
    db: ?*c.sqlite3,
    db_path: [:0]u8,
    local_instance_id: []const u8,
    owns_self: bool = false,

    const Self = @This();

    pub fn init(
        allocator: std.mem.Allocator,
        workspace_dir: []const u8,
        instance_id: []const u8,
    ) !Self {
        if (!build_options.enable_sqlite) return error.SqliteNotEnabled;

        const stable_instance_id = if (instance_id.len > 0) instance_id else "default";
        try ensureWorkspaceDir(workspace_dir);
        const db_path = try std.fs.path.joinZ(allocator, &.{ workspace_dir, CORE_DB_FILENAME });
        errdefer allocator.free(db_path);

        var db: ?*c.sqlite3 = null;
        const rc = c.sqlite3_open(db_path, &db);
        if (rc != c.SQLITE_OK or db == null) {
            if (db) |opened| _ = c.sqlite3_close(opened);
            return error.OpenFailed;
        }
        errdefer _ = c.sqlite3_close(db);

        _ = c.sqlite3_busy_timeout(db, BUSY_TIMEOUT_MS);

        const use_wal = sqlite_backend.shouldUseWal(db_path);
        try execSql(db.?, if (use_wal) "PRAGMA journal_mode=WAL;" else "PRAGMA journal_mode=DELETE;");
        try execSql(db.?, "PRAGMA synchronous=NORMAL;");
        try execSql(db.?, "PRAGMA temp_store=MEMORY;");
        try execSql(db.?, "PRAGMA foreign_keys=OFF;");

        const local_instance_id = try allocator.dupe(u8, stable_instance_id);
        errdefer allocator.free(local_instance_id);

        var self = Self{
            .allocator = allocator,
            .db = db,
            .db_path = db_path,
            .local_instance_id = local_instance_id,
        };
        errdefer self.release();

        try self.initSchema();
        try self.ensureMetaDefaults();
        _ = try self.maybeImportLegacyArtifacts(workspace_dir, stable_instance_id);
        return self;
    }

    pub fn deinit(self: *Self) void {
        self.release();
        if (self.owns_self) self.allocator.destroy(self);
    }

    fn release(self: *Self) void {
        if (self.db) |db| {
            _ = c.sqlite3_close(db);
            self.db = null;
        }
        self.allocator.free(self.db_path);
        self.allocator.free(self.local_instance_id);
    }

    pub fn databasePath(self: *const Self) [*:0]const u8 {
        return self.db_path;
    }

    pub fn lastEventSequence(self: *Self) !u64 {
        return (try self.getMetaU64("latest_sequence")) orelse 0;
    }

    pub fn compactedThroughSequence(self: *Self) !u64 {
        return (try self.getMetaU64("compacted_through_sequence")) orelse 0;
    }

    pub fn isEmpty(self: *Self) !bool {
        const latest = try self.lastEventSequence();
        if (latest != 0) return false;
        return (try self.countStateRows()) == 0;
    }

    pub fn markMigrationComplete(self: *Self, source: []const u8) !void {
        try self.setMetaString("migration_complete_source", source);
    }

    pub fn migrationComplete(self: *Self) !bool {
        const source = try self.getMetaStringAlloc(self.allocator, "migration_complete_source");
        defer if (source) |value| self.allocator.free(value);
        return source != null;
    }

    pub fn getProjectionSequence(self: *Self, backend_name: []const u8) !u64 {
        const stored_backend = try self.getMetaStringAlloc(self.allocator, "projection_backend");
        defer if (stored_backend) |value| self.allocator.free(value);
        if (stored_backend == null or !std.mem.eql(u8, stored_backend.?, backend_name)) return 0;
        return (try self.getMetaU64("projection_last_applied_sequence")) orelse 0;
    }

    pub fn setProjectionSequence(self: *Self, backend_name: []const u8, sequence: u64) !void {
        try self.setMetaString("projection_backend", backend_name);
        try self.setMetaU64("projection_last_applied_sequence", sequence);
    }

    pub fn store(
        self: *Self,
        key: []const u8,
        content: []const u8,
        category: root.MemoryCategory,
        session_id: ?[]const u8,
    ) !ContextApplyResult {
        return self.applyInput(.{
            .operation = .put,
            .key = key,
            .content = content,
            .category = category,
            .session_id = session_id,
        });
    }

    pub fn forget(self: *Self, key: []const u8) !ContextApplyResult {
        return self.applyInput(.{
            .operation = .delete_all,
            .key = key,
        });
    }

    pub fn forgetScoped(self: *Self, key: []const u8, session_id: ?[]const u8) !ContextApplyResult {
        return self.applyInput(.{
            .operation = .delete_scoped,
            .key = key,
            .session_id = session_id,
        });
    }

    pub fn applyInput(self: *Self, input: root.MemoryEventInput) !ContextApplyResult {
        if (input.schema_version) |schema_version| {
            if (schema_version != root.MEMORY_EVENT_SCHEMA_VERSION) return error.UnsupportedEventSchema;
        }
        const db = self.db orelse return error.Unavailable;

        try execSql(db, "BEGIN IMMEDIATE;");
        errdefer execSql(db, "ROLLBACK;") catch {};

        const owned_origin_id = if (input.origin_instance_id) |value|
            try self.allocator.dupe(u8, value)
        else
            try self.allocator.dupe(u8, self.local_instance_id);
        defer self.allocator.free(owned_origin_id);

        const current_frontier = try self.getOriginFrontierTx(owned_origin_id);
        const origin_sequence = input.origin_sequence orelse current_frontier + 1;
        if (current_frontier >= origin_sequence) {
            try execSql(db, "ROLLBACK;");
            return .{ .accepted = false, .materialized = false };
        }

        const timestamp_ms = input.timestamp_ms orelse std.time.milliTimestamp();
        const next_sequence = ((try self.getMetaU64Tx("latest_sequence")) orelse 0) + 1;
        const event_version = VersionStamp{
            .timestamp_ms = timestamp_ms,
            .origin_instance_id = owned_origin_id,
            .origin_sequence = origin_sequence,
            .event_sequence = next_sequence,
        };

        try self.insertEventTx(next_sequence, input, event_version);
        try self.setOriginFrontierTx(owned_origin_id, origin_sequence);
        const materialized = try self.applyMaterializationTx(input, event_version);
        try self.setMetaU64Tx("latest_sequence", next_sequence);
        try execSql(db, "COMMIT;");

        return .{
            .accepted = true,
            .materialized = materialized,
            .sequence = next_sequence,
        };
    }

    pub fn importLegacyEvent(self: *Self, event: root.MemoryEvent) !void {
        const db = self.db orelse return error.Unavailable;

        try execSql(db, "BEGIN IMMEDIATE;");
        errdefer execSql(db, "ROLLBACK;") catch {};

        const current_frontier = try self.getOriginFrontierTx(event.origin_instance_id);
        if (current_frontier >= event.origin_sequence) {
            try execSql(db, "ROLLBACK;");
            return;
        }

        try self.insertExplicitEventTx(event);
        try self.setOriginFrontierTx(event.origin_instance_id, event.origin_sequence);
        _ = try self.applyMaterializationTx(.{
            .schema_version = event.schema_version,
            .operation = event.operation,
            .key = event.key,
            .content = event.content,
            .category = event.category,
            .session_id = event.session_id,
            .origin_instance_id = event.origin_instance_id,
            .origin_sequence = event.origin_sequence,
            .timestamp_ms = event.timestamp_ms,
        }, .{
            .timestamp_ms = event.timestamp_ms,
            .origin_instance_id = event.origin_instance_id,
            .origin_sequence = event.origin_sequence,
            .event_sequence = event.sequence,
        });

        const current_latest = (try self.getMetaU64Tx("latest_sequence")) orelse 0;
        if (event.sequence > current_latest) {
            try self.setMetaU64Tx("latest_sequence", event.sequence);
        }
        try execSql(db, "COMMIT;");
    }

    pub fn bootstrapFromMemory(self: *Self, memory: root.Memory) !void {
        const entries = try memory.list(self.allocator, null, null);
        defer root.freeEntries(self.allocator, entries);

        for (entries) |entry| {
            _ = try self.store(entry.key, entry.content, entry.category, entry.session_id);
        }
    }

    pub fn get(self: *Self, allocator: std.mem.Allocator, key: []const u8) !?root.MemoryEntry {
        const db = self.db orelse return error.Unavailable;
        const sql =
            "SELECT s.key, s.session_id, s.deleted, s.content, s.category, s.timestamp_ms, s.origin_instance_id, s.origin_sequence, s.event_sequence, " ++
            "kt.timestamp_ms, kt.origin_instance_id, kt.origin_sequence, kt.event_sequence " ++
            "FROM state s LEFT JOIN key_tombstones kt ON kt.key = s.key " ++
            "WHERE s.key = ?1 ORDER BY s.event_sequence DESC";

        var stmt: ?*c.sqlite3_stmt = null;
        try prepare(db, sql, &stmt);
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, key.ptr, @intCast(key.len), SQLITE_STATIC);

        var best: ?JoinedStateRow = null;
        errdefer if (best) |*value| value.deinit(allocator);

        while (true) {
            const rc = c.sqlite3_step(stmt);
            if (rc == c.SQLITE_DONE) break;
            if (rc != c.SQLITE_ROW) return error.StepFailed;

            var row = try readJoinedStateRow(stmt.?, allocator);
            if (!rowVisible(row)) {
                row.deinit(allocator);
                continue;
            }

            const is_global = row.entry.?.session_id == null;
            const best_global = if (best) |value| value.entry.?.session_id == null else false;
            const best_sequence = if (best) |value| value.version.event_sequence else 0;
            const row_sequence = row.version.event_sequence;
            if (best == null or
                (is_global and !best_global) or
                (is_global == best_global and row_sequence > best_sequence))
            {
                if (best) |*previous| previous.deinit(allocator);
                best = row;
            } else {
                row.deinit(allocator);
            }
        }

        if (best) |*value| {
            const entry = value.takeEntry();
            value.deinit(allocator);
            return entry;
        }
        return null;
    }

    pub fn getScoped(
        self: *Self,
        allocator: std.mem.Allocator,
        key: []const u8,
        session_id: ?[]const u8,
    ) !?root.MemoryEntry {
        const db = self.db orelse return error.Unavailable;
        const sql =
            "SELECT s.key, s.session_id, s.deleted, s.content, s.category, s.timestamp_ms, s.origin_instance_id, s.origin_sequence, s.event_sequence, " ++
            "kt.timestamp_ms, kt.origin_instance_id, kt.origin_sequence, kt.event_sequence " ++
            "FROM state s LEFT JOIN key_tombstones kt ON kt.key = s.key " ++
            "WHERE s.key = ?1 AND s.session_id = ?2 LIMIT 1";

        var stmt: ?*c.sqlite3_stmt = null;
        try prepare(db, sql, &stmt);
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, key.ptr, @intCast(key.len), SQLITE_STATIC);
        const sid = normalizeSessionId(session_id);
        _ = c.sqlite3_bind_text(stmt, 2, sid.ptr, @intCast(sid.len), SQLITE_STATIC);

        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) return null;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        var row = try readJoinedStateRow(stmt.?, allocator);
        defer row.deinit(allocator);
        if (!rowVisible(row)) return null;
        return row.takeEntry();
    }

    pub fn list(
        self: *Self,
        allocator: std.mem.Allocator,
        category: ?root.MemoryCategory,
        session_id: ?[]const u8,
    ) ![]root.MemoryEntry {
        const db = self.db orelse return error.Unavailable;
        const sql_with_category =
            "SELECT s.key, s.session_id, s.deleted, s.content, s.category, s.timestamp_ms, s.origin_instance_id, s.origin_sequence, s.event_sequence, " ++
            "kt.timestamp_ms, kt.origin_instance_id, kt.origin_sequence, kt.event_sequence " ++
            "FROM state s LEFT JOIN key_tombstones kt ON kt.key = s.key " ++
            "WHERE s.category = ?1 ORDER BY s.event_sequence DESC";
        const sql_all =
            "SELECT s.key, s.session_id, s.deleted, s.content, s.category, s.timestamp_ms, s.origin_instance_id, s.origin_sequence, s.event_sequence, " ++
            "kt.timestamp_ms, kt.origin_instance_id, kt.origin_sequence, kt.event_sequence " ++
            "FROM state s LEFT JOIN key_tombstones kt ON kt.key = s.key " ++
            "ORDER BY s.event_sequence DESC";

        var stmt: ?*c.sqlite3_stmt = null;
        try prepare(db, if (category != null) sql_with_category else sql_all, &stmt);
        defer _ = c.sqlite3_finalize(stmt);

        if (category) |value| {
            const cat = value.toString();
            _ = c.sqlite3_bind_text(stmt, 1, cat.ptr, @intCast(cat.len), SQLITE_STATIC);
        }

        var results: std.ArrayListUnmanaged(root.MemoryEntry) = .empty;
        errdefer {
            for (results.items) |*entry| entry.deinit(allocator);
            results.deinit(allocator);
        }

        while (true) {
            const rc = c.sqlite3_step(stmt);
            if (rc == c.SQLITE_DONE) break;
            if (rc != c.SQLITE_ROW) return error.StepFailed;

            var row = try readJoinedStateRow(stmt.?, allocator);
            defer row.deinit(allocator);

            if (!rowVisible(row)) continue;
            if (!sessionMatches(row.entry.?.session_id, session_id)) continue;
            try results.append(allocator, row.takeEntry());
        }

        return results.toOwnedSlice(allocator);
    }

    pub fn recall(
        self: *Self,
        allocator: std.mem.Allocator,
        query: []const u8,
        limit: usize,
        session_id: ?[]const u8,
    ) ![]root.MemoryEntry {
        const trimmed = std.mem.trim(u8, query, " \t\r\n");
        if (trimmed.len == 0 or limit == 0) return allocator.alloc(root.MemoryEntry, 0);

        const db = self.db orelse return error.Unavailable;
        const like_pattern = try buildLikePattern(allocator, trimmed);
        defer allocator.free(like_pattern);

        const sql =
            "SELECT s.key, s.session_id, s.deleted, s.content, s.category, s.timestamp_ms, s.origin_instance_id, s.origin_sequence, s.event_sequence, " ++
            "kt.timestamp_ms, kt.origin_instance_id, kt.origin_sequence, kt.event_sequence " ++
            "FROM state s LEFT JOIN key_tombstones kt ON kt.key = s.key " ++
            "WHERE (s.key LIKE ?1 ESCAPE '\\' OR s.content LIKE ?1 ESCAPE '\\') ORDER BY s.event_sequence DESC";

        var stmt: ?*c.sqlite3_stmt = null;
        try prepare(db, sql, &stmt);
        defer _ = c.sqlite3_finalize(stmt);
        _ = c.sqlite3_bind_text(stmt, 1, like_pattern.ptr, @intCast(like_pattern.len), SQLITE_STATIC);

        var results: std.ArrayListUnmanaged(root.MemoryEntry) = .empty;
        errdefer {
            for (results.items) |*entry| entry.deinit(allocator);
            results.deinit(allocator);
        }

        while (results.items.len < limit) {
            const rc = c.sqlite3_step(stmt);
            if (rc == c.SQLITE_DONE) break;
            if (rc != c.SQLITE_ROW) return error.StepFailed;

            var row = try readJoinedStateRow(stmt.?, allocator);
            defer row.deinit(allocator);

            if (!rowVisible(row)) continue;
            if (!sessionMatches(row.entry.?.session_id, session_id)) continue;

            try results.append(allocator, row.takeEntry());
        }

        return results.toOwnedSlice(allocator);
    }

    pub fn count(self: *Self) !usize {
        const entries = try self.list(self.allocator, null, null);
        defer root.freeEntries(self.allocator, entries);
        return entries.len;
    }

    pub fn listEvents(
        self: *Self,
        allocator: std.mem.Allocator,
        after_sequence: ?u64,
        limit: usize,
    ) ![]root.MemoryEvent {
        const compacted_through = try self.compactedThroughSequence();
        if (after_sequence) |after| {
            if (after < compacted_through) return error.CursorExpired;
        }

        const db = self.db orelse return error.Unavailable;
        const sql =
            "SELECT sequence, schema_version, timestamp_ms, origin_instance_id, origin_sequence, operation, key, content, category, session_id " ++
            "FROM events WHERE sequence > ?1 ORDER BY sequence ASC LIMIT ?2";
        var stmt: ?*c.sqlite3_stmt = null;
        try prepare(db, sql, &stmt);
        defer _ = c.sqlite3_finalize(stmt);

        const after = after_sequence orelse compacted_through;
        _ = c.sqlite3_bind_int64(stmt, 1, @intCast(after));
        _ = c.sqlite3_bind_int64(stmt, 2, @intCast(limit));

        var events: std.ArrayListUnmanaged(root.MemoryEvent) = .empty;
        errdefer {
            for (events.items) |*event| event.deinit(allocator);
            events.deinit(allocator);
        }

        while (true) {
            const rc = c.sqlite3_step(stmt);
            if (rc == c.SQLITE_DONE) break;
            if (rc != c.SQLITE_ROW) return error.StepFailed;
            try events.append(allocator, try readEventRow(stmt.?, allocator));
        }

        return events.toOwnedSlice(allocator);
    }

    pub fn compactEvents(self: *Self) !usize {
        const db = self.db orelse return error.Unavailable;
        const latest = try self.lastEventSequence();
        if (latest == 0) return 0;

        try execSql(db, "BEGIN IMMEDIATE;");
        errdefer execSql(db, "ROLLBACK;") catch {};
        try self.setMetaU64Tx("compacted_through_sequence", latest);
        const sql = "DELETE FROM events WHERE sequence <= ?1";
        var stmt: ?*c.sqlite3_stmt = null;
        try prepare(db, sql, &stmt);
        defer _ = c.sqlite3_finalize(stmt);
        _ = c.sqlite3_bind_int64(stmt, 1, @intCast(latest));
        if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.StepFailed;
        const removed: usize = @intCast(c.sqlite3_changes(db));
        try execSql(db, "COMMIT;");
        return removed;
    }

    pub fn rebuildStateFromEvents(self: *Self) !void {
        const db = self.db orelse return error.Unavailable;
        const compacted = try self.compactedThroughSequence();
        if (compacted != 0) return error.NotSupported;

        try execSql(db, "BEGIN IMMEDIATE;");
        errdefer execSql(db, "ROLLBACK;") catch {};
        try execSql(db, "DELETE FROM state;");
        try execSql(db, "DELETE FROM key_tombstones;");
        try execSql(db, "DELETE FROM origin_frontiers;");

        const sql =
            "SELECT sequence, schema_version, timestamp_ms, origin_instance_id, origin_sequence, operation, key, content, category, session_id " ++
            "FROM events ORDER BY sequence ASC";
        var stmt: ?*c.sqlite3_stmt = null;
        try prepare(db, sql, &stmt);
        defer _ = c.sqlite3_finalize(stmt);

        while (true) {
            const rc = c.sqlite3_step(stmt);
            if (rc == c.SQLITE_DONE) break;
            if (rc != c.SQLITE_ROW) return error.StepFailed;

            const sequence: u64 = @intCast(c.sqlite3_column_int64(stmt, 0));
            const timestamp_ms: i64 = c.sqlite3_column_int64(stmt, 2);
            const origin_instance_id = columnText(stmt.?, 3) orelse return error.InvalidEventJournal;
            const origin_sequence: u64 = @intCast(c.sqlite3_column_int64(stmt, 4));
            const operation = root.MemoryEventOp.fromString(columnText(stmt.?, 5) orelse return error.InvalidEventJournal) orelse
                return error.InvalidEventJournal;
            const key = columnText(stmt.?, 6) orelse return error.InvalidEventJournal;
            const content = columnText(stmt.?, 7);
            const category_text = columnText(stmt.?, 8);
            const session = nullIfEmpty(columnText(stmt.?, 9) orelse "");

            _ = try self.applyMaterializationTx(.{
                .schema_version = root.MEMORY_EVENT_SCHEMA_VERSION,
                .operation = operation,
                .key = key,
                .content = content,
                .category = if (category_text) |cat| root.MemoryCategory.fromString(cat) else null,
                .session_id = session,
                .origin_instance_id = origin_instance_id,
                .origin_sequence = origin_sequence,
                .timestamp_ms = timestamp_ms,
            }, .{
                .timestamp_ms = timestamp_ms,
                .origin_instance_id = origin_instance_id,
                .origin_sequence = origin_sequence,
                .event_sequence = sequence,
            });
            try self.setOriginFrontierTx(origin_instance_id, origin_sequence);
        }

        try execSql(db, "COMMIT;");
    }

    fn initSchema(self: *Self) !void {
        const db = self.db orelse return error.Unavailable;
        try execSql(db, "CREATE TABLE IF NOT EXISTS meta (" ++
            "key TEXT PRIMARY KEY, " ++
            "value TEXT NOT NULL" ++
            ");");
        try execSql(db, "CREATE TABLE IF NOT EXISTS events (" ++
            "sequence INTEGER PRIMARY KEY, " ++
            "schema_version INTEGER NOT NULL, " ++
            "timestamp_ms INTEGER NOT NULL, " ++
            "origin_instance_id TEXT NOT NULL, " ++
            "origin_sequence INTEGER NOT NULL, " ++
            "operation TEXT NOT NULL, " ++
            "key TEXT NOT NULL, " ++
            "content TEXT, " ++
            "category TEXT, " ++
            "session_id TEXT, " ++
            "UNIQUE(origin_instance_id, origin_sequence)" ++
            ");");
        try execSql(db, "CREATE INDEX IF NOT EXISTS idx_context_events_sequence ON events(sequence);");
        try execSql(db, "CREATE TABLE IF NOT EXISTS state (" ++
            "key TEXT NOT NULL, " ++
            "session_id TEXT NOT NULL DEFAULT '', " ++
            "deleted INTEGER NOT NULL DEFAULT 0, " ++
            "content TEXT, " ++
            "category TEXT, " ++
            "timestamp_ms INTEGER NOT NULL, " ++
            "origin_instance_id TEXT NOT NULL, " ++
            "origin_sequence INTEGER NOT NULL, " ++
            "event_sequence INTEGER NOT NULL, " ++
            "PRIMARY KEY(key, session_id)" ++
            ");");
        try execSql(db, "CREATE INDEX IF NOT EXISTS idx_context_state_event_sequence ON state(event_sequence DESC);");
        try execSql(db, "CREATE TABLE IF NOT EXISTS key_tombstones (" ++
            "key TEXT PRIMARY KEY, " ++
            "timestamp_ms INTEGER NOT NULL, " ++
            "origin_instance_id TEXT NOT NULL, " ++
            "origin_sequence INTEGER NOT NULL, " ++
            "event_sequence INTEGER NOT NULL" ++
            ");");
        try execSql(db, "CREATE TABLE IF NOT EXISTS origin_frontiers (" ++
            "origin_instance_id TEXT PRIMARY KEY, " ++
            "origin_sequence INTEGER NOT NULL" ++
            ");");
    }

    fn ensureMetaDefaults(self: *Self) !void {
        try self.setMetaDefault("schema_version", "1");
        try self.setMetaDefault("latest_sequence", "0");
        try self.setMetaDefault("compacted_through_sequence", "0");
    }

    fn maybeImportLegacyArtifacts(self: *Self, workspace_dir: []const u8, stable_instance_id: []const u8) !bool {
        if (!(try self.isEmpty())) return false;
        if (try self.migrationComplete()) return false;

        const journal_path = try buildLegacyArtifactPath(self.allocator, workspace_dir, stable_instance_id, LEGACY_EVENT_JOURNAL_FILENAME_SUFFIX);
        defer self.allocator.free(journal_path);
        const checkpoint_path = try buildLegacyArtifactPath(self.allocator, workspace_dir, stable_instance_id, LEGACY_EVENT_CHECKPOINT_FILENAME_SUFFIX);
        defer self.allocator.free(checkpoint_path);

        var imported = false;
        if (fileExistsAbsolute(checkpoint_path)) {
            try self.importLegacyCheckpoint(checkpoint_path);
            imported = true;
        }
        if (fileExistsAbsolute(journal_path)) {
            try self.importLegacyJournal(journal_path);
            imported = true;
        }
        if (imported) {
            try self.markMigrationComplete("legacy_event_store");
        }
        return imported;
    }

    fn importLegacyCheckpoint(self: *Self, checkpoint_path: []const u8) !void {
        const file = try std.fs.openFileAbsolute(checkpoint_path, .{});
        defer file.close();
        const content = try file.readToEndAlloc(self.allocator, 16 * 1024 * 1024);
        defer self.allocator.free(content);
        if (content.len == 0) return;

        var parsed = try std.json.parseFromSlice(std.json.Value, self.allocator, content, .{});
        defer parsed.deinit();
        if (parsed.value != .object) return error.InvalidEventCheckpoint;

        const db = self.db orelse return error.Unavailable;
        const obj = parsed.value.object;
        const included_sequence = try parseRequiredU64(obj, "included_sequence");

        try execSql(db, "BEGIN IMMEDIATE;");
        errdefer execSql(db, "ROLLBACK;") catch {};
        try execSql(db, "DELETE FROM state;");
        try execSql(db, "DELETE FROM key_tombstones;");
        try execSql(db, "DELETE FROM origin_frontiers;");

        const entries_value = obj.get("entries") orelse return error.InvalidEventCheckpoint;
        const entries = switch (entries_value) {
            .array => |value| value,
            else => return error.InvalidEventCheckpoint,
        };

        for (entries.items) |item| {
            const entry_obj = switch (item) {
                .object => |value| value,
                else => return error.InvalidEventCheckpoint,
            };
            const key = try parseRequiredString(entry_obj, "key");
            const session_id = try parseOptionalString(entry_obj, "session_id");
            const timestamp = try parseRequiredString(entry_obj, "timestamp");
            const timestamp_ms = std.fmt.parseInt(i64, timestamp, 10) catch 0;
            try self.upsertStateTx(
                key,
                normalizeSessionId(session_id),
                false,
                try parseRequiredString(entry_obj, "content"),
                try parseRequiredString(entry_obj, "category"),
                .{
                    .timestamp_ms = timestamp_ms,
                    .origin_instance_id = LEGACY_CHECKPOINT_ORIGIN,
                    .origin_sequence = try parseRequiredU64(entry_obj, "sequence"),
                    .event_sequence = try parseRequiredU64(entry_obj, "sequence"),
                },
            );
        }

        if (obj.get("origin_frontiers")) |frontiers_value| {
            const frontiers_obj = switch (frontiers_value) {
                .object => |value| value,
                else => return error.InvalidEventCheckpoint,
            };
            var it = frontiers_obj.iterator();
            while (it.next()) |entry| {
                const frontier = switch (entry.value_ptr.*) {
                    .integer => |n| blk: {
                        if (n < 0) return error.InvalidEventCheckpoint;
                        break :blk @as(u64, @intCast(n));
                    },
                    else => return error.InvalidEventCheckpoint,
                };
                try self.setOriginFrontierTx(entry.key_ptr.*, frontier);
            }
        }

        try self.setMetaU64Tx("latest_sequence", included_sequence);
        try self.setMetaU64Tx("compacted_through_sequence", included_sequence);
        try execSql(db, "COMMIT;");
    }

    fn importLegacyJournal(self: *Self, journal_path: []const u8) !void {
        const file = try std.fs.openFileAbsolute(journal_path, .{});
        defer file.close();

        var reader = file.deprecatedReader();
        while (try reader.readUntilDelimiterOrEofAlloc(self.allocator, '\n', LEGACY_MAX_EVENT_LINE_BYTES)) |raw_line| {
            defer self.allocator.free(raw_line);
            const line = std.mem.trim(u8, raw_line, " \t\r");
            if (line.len == 0) continue;
            var event = try parseLegacyEventLine(self.allocator, line);
            defer event.deinit(self.allocator);
            if (event.sequence <= (try self.compactedThroughSequence())) continue;
            try self.importLegacyEvent(event);
        }
    }

    fn applyMaterializationTx(self: *Self, input: root.MemoryEventInput, version: VersionStamp) !bool {
        const key_tombstone = try self.readKeyTombstoneTx(input.key);
        defer if (key_tombstone) |ts| self.allocator.free(ts.origin_instance_id);

        if (key_tombstone) |ts| {
            if (compareVersion(version, ts) != .gt) return false;
        }

        switch (input.operation) {
            .put => {
                const content = input.content orelse return error.InvalidEvent;
                const category = input.category orelse return error.InvalidEvent;
                const current = try self.readStateVersionTx(input.key, normalizeSessionId(input.session_id));
                defer if (current) |value| self.allocator.free(value.origin_instance_id);
                if (current) |value| {
                    if (compareVersion(version, value) != .gt) return false;
                }
                try self.upsertStateTx(input.key, normalizeSessionId(input.session_id), false, content, category.toString(), version);
                return true;
            },
            .delete_scoped => {
                const current = try self.readStateVersionTx(input.key, normalizeSessionId(input.session_id));
                defer if (current) |value| self.allocator.free(value.origin_instance_id);
                if (current) |value| {
                    if (compareVersion(version, value) != .gt) return false;
                }
                try self.upsertStateTx(input.key, normalizeSessionId(input.session_id), true, null, null, version);
                return true;
            },
            .delete_all => {
                if (key_tombstone) |ts| {
                    if (compareVersion(version, ts) != .gt) return false;
                }
                try self.upsertKeyTombstoneTx(input.key, version);
                return true;
            },
        }
    }

    fn insertEventTx(self: *Self, sequence: u64, input: root.MemoryEventInput, version: VersionStamp) !void {
        const sql =
            "INSERT INTO events (" ++
            "sequence, schema_version, timestamp_ms, origin_instance_id, origin_sequence, operation, key, content, category, session_id" ++
            ") VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)";
        var stmt: ?*c.sqlite3_stmt = null;
        try prepare(self.db.?, sql, &stmt);
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_int64(stmt, 1, @intCast(sequence));
        _ = c.sqlite3_bind_int64(stmt, 2, root.MEMORY_EVENT_SCHEMA_VERSION);
        _ = c.sqlite3_bind_int64(stmt, 3, version.timestamp_ms);
        _ = c.sqlite3_bind_text(stmt, 4, version.origin_instance_id.ptr, @intCast(version.origin_instance_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_int64(stmt, 5, @intCast(version.origin_sequence));
        _ = c.sqlite3_bind_text(stmt, 6, input.operation.toString().ptr, @intCast(input.operation.toString().len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 7, input.key.ptr, @intCast(input.key.len), SQLITE_STATIC);

        if (input.content) |content| {
            _ = c.sqlite3_bind_text(stmt, 8, content.ptr, @intCast(content.len), SQLITE_STATIC);
        } else {
            _ = c.sqlite3_bind_null(stmt, 8);
        }

        if (input.category) |category| {
            const name = category.toString();
            _ = c.sqlite3_bind_text(stmt, 9, name.ptr, @intCast(name.len), SQLITE_STATIC);
        } else {
            _ = c.sqlite3_bind_null(stmt, 9);
        }

        if (input.session_id) |sid| {
            _ = c.sqlite3_bind_text(stmt, 10, sid.ptr, @intCast(sid.len), SQLITE_STATIC);
        } else {
            _ = c.sqlite3_bind_null(stmt, 10);
        }

        if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.StepFailed;
    }

    fn insertExplicitEventTx(self: *Self, event: root.MemoryEvent) !void {
        try self.insertEventTx(event.sequence, .{
            .schema_version = event.schema_version,
            .operation = event.operation,
            .key = event.key,
            .content = event.content,
            .category = event.category,
            .session_id = event.session_id,
            .origin_instance_id = event.origin_instance_id,
            .origin_sequence = event.origin_sequence,
            .timestamp_ms = event.timestamp_ms,
        }, .{
            .timestamp_ms = event.timestamp_ms,
            .origin_instance_id = event.origin_instance_id,
            .origin_sequence = event.origin_sequence,
            .event_sequence = event.sequence,
        });
    }

    fn upsertStateTx(
        self: *Self,
        key: []const u8,
        session_id: []const u8,
        deleted: bool,
        content: ?[]const u8,
        category_name: ?[]const u8,
        version: VersionStamp,
    ) !void {
        const sql =
            "INSERT INTO state (" ++
            "key, session_id, deleted, content, category, timestamp_ms, origin_instance_id, origin_sequence, event_sequence" ++
            ") VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9) " ++
            "ON CONFLICT(key, session_id) DO UPDATE SET " ++
            "deleted = excluded.deleted, " ++
            "content = excluded.content, " ++
            "category = excluded.category, " ++
            "timestamp_ms = excluded.timestamp_ms, " ++
            "origin_instance_id = excluded.origin_instance_id, " ++
            "origin_sequence = excluded.origin_sequence, " ++
            "event_sequence = excluded.event_sequence";

        var stmt: ?*c.sqlite3_stmt = null;
        try prepare(self.db.?, sql, &stmt);
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, key.ptr, @intCast(key.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 2, session_id.ptr, @intCast(session_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_int(stmt, 3, if (deleted) 1 else 0);

        if (content) |value| {
            _ = c.sqlite3_bind_text(stmt, 4, value.ptr, @intCast(value.len), SQLITE_STATIC);
        } else {
            _ = c.sqlite3_bind_null(stmt, 4);
        }

        if (category_name) |value| {
            _ = c.sqlite3_bind_text(stmt, 5, value.ptr, @intCast(value.len), SQLITE_STATIC);
        } else {
            _ = c.sqlite3_bind_null(stmt, 5);
        }

        _ = c.sqlite3_bind_int64(stmt, 6, version.timestamp_ms);
        _ = c.sqlite3_bind_text(stmt, 7, version.origin_instance_id.ptr, @intCast(version.origin_instance_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_int64(stmt, 8, @intCast(version.origin_sequence));
        _ = c.sqlite3_bind_int64(stmt, 9, @intCast(version.event_sequence));

        if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.StepFailed;
    }

    fn upsertKeyTombstoneTx(self: *Self, key: []const u8, version: VersionStamp) !void {
        const sql =
            "INSERT INTO key_tombstones (" ++
            "key, timestamp_ms, origin_instance_id, origin_sequence, event_sequence" ++
            ") VALUES (?1, ?2, ?3, ?4, ?5) " ++
            "ON CONFLICT(key) DO UPDATE SET " ++
            "timestamp_ms = excluded.timestamp_ms, " ++
            "origin_instance_id = excluded.origin_instance_id, " ++
            "origin_sequence = excluded.origin_sequence, " ++
            "event_sequence = excluded.event_sequence";
        var stmt: ?*c.sqlite3_stmt = null;
        try prepare(self.db.?, sql, &stmt);
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, key.ptr, @intCast(key.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_int64(stmt, 2, version.timestamp_ms);
        _ = c.sqlite3_bind_text(stmt, 3, version.origin_instance_id.ptr, @intCast(version.origin_instance_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_int64(stmt, 4, @intCast(version.origin_sequence));
        _ = c.sqlite3_bind_int64(stmt, 5, @intCast(version.event_sequence));

        if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.StepFailed;
    }

    fn readStateVersionTx(self: *Self, key: []const u8, session_id: []const u8) !?VersionStamp {
        const sql =
            "SELECT timestamp_ms, origin_instance_id, origin_sequence, event_sequence " ++
            "FROM state WHERE key = ?1 AND session_id = ?2 LIMIT 1";
        var stmt: ?*c.sqlite3_stmt = null;
        try prepare(self.db.?, sql, &stmt);
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, key.ptr, @intCast(key.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 2, session_id.ptr, @intCast(session_id.len), SQLITE_STATIC);

        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) return null;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        return .{
            .timestamp_ms = c.sqlite3_column_int64(stmt, 0),
            .origin_instance_id = try self.allocator.dupe(u8, columnText(stmt.?, 1) orelse return error.InvalidState),
            .origin_sequence = @intCast(c.sqlite3_column_int64(stmt, 2)),
            .event_sequence = @intCast(c.sqlite3_column_int64(stmt, 3)),
        };
    }

    fn readKeyTombstoneTx(self: *Self, key: []const u8) !?VersionStamp {
        const sql =
            "SELECT timestamp_ms, origin_instance_id, origin_sequence, event_sequence " ++
            "FROM key_tombstones WHERE key = ?1 LIMIT 1";
        var stmt: ?*c.sqlite3_stmt = null;
        try prepare(self.db.?, sql, &stmt);
        defer _ = c.sqlite3_finalize(stmt);
        _ = c.sqlite3_bind_text(stmt, 1, key.ptr, @intCast(key.len), SQLITE_STATIC);

        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) return null;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        return .{
            .timestamp_ms = c.sqlite3_column_int64(stmt, 0),
            .origin_instance_id = try self.allocator.dupe(u8, columnText(stmt.?, 1) orelse return error.InvalidState),
            .origin_sequence = @intCast(c.sqlite3_column_int64(stmt, 2)),
            .event_sequence = @intCast(c.sqlite3_column_int64(stmt, 3)),
        };
    }

    fn getOriginFrontierTx(self: *Self, origin_instance_id: []const u8) !u64 {
        const sql = "SELECT origin_sequence FROM origin_frontiers WHERE origin_instance_id = ?1 LIMIT 1";
        var stmt: ?*c.sqlite3_stmt = null;
        try prepare(self.db.?, sql, &stmt);
        defer _ = c.sqlite3_finalize(stmt);
        _ = c.sqlite3_bind_text(stmt, 1, origin_instance_id.ptr, @intCast(origin_instance_id.len), SQLITE_STATIC);

        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) return 0;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        return @intCast(c.sqlite3_column_int64(stmt, 0));
    }

    fn setOriginFrontierTx(self: *Self, origin_instance_id: []const u8, origin_sequence: u64) !void {
        const sql =
            "INSERT INTO origin_frontiers (origin_instance_id, origin_sequence) VALUES (?1, ?2) " ++
            "ON CONFLICT(origin_instance_id) DO UPDATE SET origin_sequence = excluded.origin_sequence";
        var stmt: ?*c.sqlite3_stmt = null;
        try prepare(self.db.?, sql, &stmt);
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, origin_instance_id.ptr, @intCast(origin_instance_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_int64(stmt, 2, @intCast(origin_sequence));
        if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.StepFailed;
    }

    fn countStateRows(self: *Self) !usize {
        const sql = "SELECT COUNT(*) FROM state";
        var stmt: ?*c.sqlite3_stmt = null;
        try prepare(self.db.?, sql, &stmt);
        defer _ = c.sqlite3_finalize(stmt);
        if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.StepFailed;
        return @intCast(c.sqlite3_column_int64(stmt, 0));
    }

    fn getMetaStringAlloc(self: *Self, allocator: std.mem.Allocator, key: []const u8) !?[]u8 {
        const sql = "SELECT value FROM meta WHERE key = ?1 LIMIT 1";
        var stmt: ?*c.sqlite3_stmt = null;
        try prepare(self.db.?, sql, &stmt);
        defer _ = c.sqlite3_finalize(stmt);
        _ = c.sqlite3_bind_text(stmt, 1, key.ptr, @intCast(key.len), SQLITE_STATIC);

        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) return null;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        return try allocator.dupe(u8, columnText(stmt.?, 0) orelse "");
    }

    fn getMetaU64(self: *Self, key: []const u8) !?u64 {
        const value = try self.getMetaStringAlloc(self.allocator, key);
        defer if (value) |text| self.allocator.free(text);
        if (value) |text| return try std.fmt.parseInt(u64, text, 10);
        return null;
    }

    fn getMetaU64Tx(self: *Self, key: []const u8) !?u64 {
        return self.getMetaU64(key);
    }

    fn setMetaDefault(self: *Self, key: []const u8, value: []const u8) !void {
        const sql = "INSERT OR IGNORE INTO meta (key, value) VALUES (?1, ?2)";
        var stmt: ?*c.sqlite3_stmt = null;
        try prepare(self.db.?, sql, &stmt);
        defer _ = c.sqlite3_finalize(stmt);
        _ = c.sqlite3_bind_text(stmt, 1, key.ptr, @intCast(key.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 2, value.ptr, @intCast(value.len), SQLITE_STATIC);
        if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.StepFailed;
    }

    fn setMetaString(self: *Self, key: []const u8, value: []const u8) !void {
        try self.setMetaStringTx(key, value);
    }

    fn setMetaStringTx(self: *Self, key: []const u8, value: []const u8) !void {
        const sql =
            "INSERT INTO meta (key, value) VALUES (?1, ?2) " ++
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value";
        var stmt: ?*c.sqlite3_stmt = null;
        try prepare(self.db.?, sql, &stmt);
        defer _ = c.sqlite3_finalize(stmt);
        _ = c.sqlite3_bind_text(stmt, 1, key.ptr, @intCast(key.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 2, value.ptr, @intCast(value.len), SQLITE_STATIC);
        if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.StepFailed;
    }

    fn setMetaU64(self: *Self, key: []const u8, value: u64) !void {
        try self.setMetaU64Tx(key, value);
    }

    fn setMetaU64Tx(self: *Self, key: []const u8, value: u64) !void {
        var buf: [32]u8 = undefined;
        const value_text = try std.fmt.bufPrint(&buf, "{d}", .{value});
        try self.setMetaStringTx(key, value_text);
    }
};

fn execSql(db: *c.sqlite3, sql: []const u8) !void {
    var err_msg: [*c]u8 = null;
    const rc = c.sqlite3_exec(db, sql.ptr, null, null, &err_msg);
    defer if (err_msg != null) c.sqlite3_free(err_msg);
    if (rc != c.SQLITE_OK) {
        return error.ExecFailed;
    }
}

fn prepare(db: *c.sqlite3, sql: []const u8, stmt: *?*c.sqlite3_stmt) !void {
    const rc = c.sqlite3_prepare_v2(db, sql.ptr, @intCast(sql.len), stmt, null);
    if (rc != c.SQLITE_OK) return error.PrepareFailed;
}

fn columnText(stmt: *c.sqlite3_stmt, index: c_int) ?[]const u8 {
    if (c.sqlite3_column_type(stmt, index) == c.SQLITE_NULL) return null;
    const text_ptr = c.sqlite3_column_text(stmt, index) orelse return null;
    const text_len = c.sqlite3_column_bytes(stmt, index);
    const slice: [*]const u8 = @ptrCast(text_ptr);
    return slice[0..@intCast(text_len)];
}

fn normalizeSessionId(session_id: ?[]const u8) []const u8 {
    return session_id orelse "";
}

fn nullIfEmpty(value: []const u8) ?[]const u8 {
    if (value.len == 0) return null;
    return value;
}

fn compareVersion(a: VersionStamp, b: VersionStamp) std.math.Order {
    if (a.timestamp_ms < b.timestamp_ms) return .lt;
    if (a.timestamp_ms > b.timestamp_ms) return .gt;
    const origin_order = std.mem.order(u8, a.origin_instance_id, b.origin_instance_id);
    if (origin_order != .eq) return origin_order;
    if (a.origin_sequence < b.origin_sequence) return .lt;
    if (a.origin_sequence > b.origin_sequence) return .gt;
    if (a.event_sequence < b.event_sequence) return .lt;
    if (a.event_sequence > b.event_sequence) return .gt;
    return .eq;
}

fn sessionMatches(entry_session_id: ?[]const u8, query_session_id: ?[]const u8) bool {
    if (query_session_id) |sid| {
        return entry_session_id != null and std.mem.eql(u8, entry_session_id.?, sid);
    }
    return true;
}

fn rowVisible(row: JoinedStateRow) bool {
    if (row.deleted) return false;
    if (row.tombstone) |ts| {
        if (compareVersion(row.version, ts) != .gt) return false;
    }
    return true;
}

fn buildLikePattern(allocator: std.mem.Allocator, query: []const u8) ![]u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.append(allocator, '%');
    for (query) |ch| {
        if (ch == '%' or ch == '_' or ch == '\\') try out.append(allocator, '\\');
        try out.append(allocator, ch);
    }
    try out.append(allocator, '%');
    return out.toOwnedSlice(allocator);
}

fn readJoinedStateRow(stmt: *c.sqlite3_stmt, allocator: std.mem.Allocator) !JoinedStateRow {
    const key = try allocator.dupe(u8, columnText(stmt, 0) orelse return error.InvalidState);
    errdefer allocator.free(key);
    const session_text = columnText(stmt, 1) orelse "";
    const session_id = if (session_text.len > 0) try allocator.dupe(u8, session_text) else null;
    errdefer if (session_id) |value| allocator.free(value);

    const deleted = c.sqlite3_column_int(stmt, 2) != 0;
    const content = if (!deleted)
        try allocator.dupe(u8, columnText(stmt, 3) orelse return error.InvalidState)
    else
        try allocator.dupe(u8, "");
    errdefer allocator.free(content);

    const category_name = if (!deleted) columnText(stmt, 4) orelse "core" else "core";
    const category = try dupCategory(allocator, root.MemoryCategory.fromString(category_name));
    errdefer switch (category) {
        .custom => |name| allocator.free(name),
        else => {},
    };

    const timestamp = try std.fmt.allocPrint(allocator, "{d}", .{c.sqlite3_column_int64(stmt, 5)});
    errdefer allocator.free(timestamp);
    const row_origin_instance_id = try allocator.dupe(u8, columnText(stmt, 6) orelse return error.InvalidState);
    errdefer allocator.free(row_origin_instance_id);
    const row_origin_sequence: u64 = @intCast(c.sqlite3_column_int64(stmt, 7));
    const event_sequence: u64 = @intCast(c.sqlite3_column_int64(stmt, 8));
    const id = try std.fmt.allocPrint(allocator, "{d}", .{event_sequence});
    errdefer allocator.free(id);

    var tombstone: ?VersionStamp = null;
    if (c.sqlite3_column_type(stmt, 9) != c.SQLITE_NULL) {
        tombstone = .{
            .timestamp_ms = c.sqlite3_column_int64(stmt, 9),
            .origin_instance_id = try allocator.dupe(u8, columnText(stmt, 10) orelse return error.InvalidState),
            .origin_sequence = @intCast(c.sqlite3_column_int64(stmt, 11)),
            .event_sequence = @intCast(c.sqlite3_column_int64(stmt, 12)),
        };
    }
    errdefer if (tombstone) |value| allocator.free(value.origin_instance_id);

    return .{
        .entry = .{
            .id = id,
            .key = key,
            .content = content,
            .category = category,
            .timestamp = timestamp,
            .session_id = session_id,
        },
        .deleted = deleted,
        .version = .{
            .timestamp_ms = c.sqlite3_column_int64(stmt, 5),
            .origin_instance_id = row_origin_instance_id,
            .origin_sequence = row_origin_sequence,
            .event_sequence = event_sequence,
        },
        .tombstone = tombstone,
    };
}

fn dupCategory(allocator: std.mem.Allocator, category: root.MemoryCategory) !root.MemoryCategory {
    return switch (category) {
        .custom => |name| .{ .custom = try allocator.dupe(u8, name) },
        else => category,
    };
}

fn readEventRow(stmt: *c.sqlite3_stmt, allocator: std.mem.Allocator) !root.MemoryEvent {
    const category_text = columnText(stmt, 8);
    return .{
        .schema_version = @intCast(c.sqlite3_column_int(stmt, 1)),
        .sequence = @intCast(c.sqlite3_column_int64(stmt, 0)),
        .timestamp_ms = c.sqlite3_column_int64(stmt, 2),
        .origin_instance_id = try allocator.dupe(u8, columnText(stmt, 3) orelse return error.InvalidEventJournal),
        .origin_sequence = @intCast(c.sqlite3_column_int64(stmt, 4)),
        .operation = root.MemoryEventOp.fromString(columnText(stmt, 5) orelse return error.InvalidEventJournal) orelse
            return error.InvalidEventJournal,
        .key = try allocator.dupe(u8, columnText(stmt, 6) orelse return error.InvalidEventJournal),
        .content = if (columnText(stmt, 7)) |content| try allocator.dupe(u8, content) else null,
        .category = if (category_text) |category_name| try dupCategory(allocator, root.MemoryCategory.fromString(category_name)) else null,
        .session_id = if (columnText(stmt, 9)) |sid| try allocator.dupe(u8, sid) else null,
    };
}

fn fileExistsAbsolute(path: []const u8) bool {
    std.fs.accessAbsolute(path, .{}) catch return false;
    return true;
}

fn ensureWorkspaceDir(workspace_dir: []const u8) !void {
    if (std.fs.path.isAbsolute(workspace_dir)) {
        std.fs.makeDirAbsolute(workspace_dir) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => try std.fs.cwd().makePath(workspace_dir),
        };
        return;
    }
    try std.fs.cwd().makePath(workspace_dir);
}

fn buildLegacyArtifactPath(
    allocator: std.mem.Allocator,
    workspace_dir: []const u8,
    stable_instance_id: []const u8,
    suffix: []const u8,
) ![]u8 {
    var hash_buf: [32]u8 = undefined;
    const hash = std.hash.Wyhash.hash(0, stable_instance_id);
    const hash_str = try std.fmt.bufPrint(&hash_buf, "{x}", .{hash});
    const filename = try std.fmt.allocPrint(
        allocator,
        "{s}{s}{s}",
        .{ LEGACY_EVENT_JOURNAL_FILENAME_PREFIX, hash_str, suffix },
    );
    defer allocator.free(filename);
    return std.fs.path.join(allocator, &.{ workspace_dir, filename });
}

fn parseRequiredString(obj: std.json.ObjectMap, key: []const u8) ![]const u8 {
    const value = obj.get(key) orelse return error.InvalidEventJournal;
    return switch (value) {
        .string => |s| s,
        else => error.InvalidEventJournal,
    };
}

fn parseOptionalString(obj: std.json.ObjectMap, key: []const u8) !?[]const u8 {
    const value = obj.get(key) orelse return null;
    return switch (value) {
        .null => null,
        .string => |s| s,
        else => error.InvalidEventJournal,
    };
}

fn parseRequiredU64(obj: std.json.ObjectMap, key: []const u8) !u64 {
    const value = obj.get(key) orelse return error.InvalidEventJournal;
    return switch (value) {
        .integer => |n| {
            if (n < 0) return error.InvalidEventJournal;
            return @intCast(n);
        },
        else => error.InvalidEventJournal,
    };
}

fn parseOptionalU64(obj: std.json.ObjectMap, key: []const u8) !?u64 {
    const value = obj.get(key) orelse return null;
    return switch (value) {
        .null => null,
        .integer => |n| {
            if (n < 0) return error.InvalidEventJournal;
            return @intCast(n);
        },
        else => error.InvalidEventJournal,
    };
}

fn parseRequiredI64(obj: std.json.ObjectMap, key: []const u8) !i64 {
    const value = obj.get(key) orelse return error.InvalidEventJournal;
    return switch (value) {
        .integer => |n| n,
        else => error.InvalidEventJournal,
    };
}

fn parseLegacyEventLine(allocator: std.mem.Allocator, line: []const u8) !root.MemoryEvent {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, line, .{});
    defer parsed.deinit();

    if (parsed.value != .object) return error.InvalidEventJournal;
    const obj = parsed.value.object;

    const op_name = try parseRequiredString(obj, "operation");
    const operation = root.MemoryEventOp.fromString(op_name) orelse return error.InvalidEventJournal;
    const category_name = try parseOptionalString(obj, "category");
    const schema_version = (try parseOptionalU64(obj, "schema_version")) orelse root.MEMORY_EVENT_SCHEMA_VERSION;
    if (schema_version != root.MEMORY_EVENT_SCHEMA_VERSION) return error.UnsupportedEventSchema;

    return .{
        .schema_version = @intCast(schema_version),
        .sequence = try parseRequiredU64(obj, "sequence"),
        .timestamp_ms = try parseRequiredI64(obj, "timestamp_ms"),
        .origin_instance_id = try allocator.dupe(u8, try parseRequiredString(obj, "origin_instance_id")),
        .origin_sequence = try parseRequiredU64(obj, "origin_sequence"),
        .operation = operation,
        .key = try allocator.dupe(u8, try parseRequiredString(obj, "key")),
        .content = if (try parseOptionalString(obj, "content")) |content| try allocator.dupe(u8, content) else null,
        .category = if (category_name) |name| try dupCategory(allocator, root.MemoryCategory.fromString(name)) else null,
        .session_id = if (try parseOptionalString(obj, "session_id")) |sid| try allocator.dupe(u8, sid) else null,
    };
}

test "context core applies deterministic LWW ordering" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const workspace = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(workspace);

    var core = try ContextCore.init(std.testing.allocator, workspace, "agent-a");
    defer core.deinit();

    _ = try core.applyInput(.{
        .operation = .put,
        .key = "pref",
        .content = "old",
        .category = .core,
        .origin_instance_id = "agent-a",
        .origin_sequence = 1,
        .timestamp_ms = 100,
    });
    _ = try core.applyInput(.{
        .operation = .put,
        .key = "pref",
        .content = "new",
        .category = .core,
        .origin_instance_id = "agent-b",
        .origin_sequence = 1,
        .timestamp_ms = 101,
    });
    _ = try core.applyInput(.{
        .operation = .put,
        .key = "pref",
        .content = "older-late",
        .category = .core,
        .origin_instance_id = "agent-c",
        .origin_sequence = 1,
        .timestamp_ms = 99,
    });

    const entry = (try core.get(std.testing.allocator, "pref")).?;
    defer entry.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("new", entry.content);
}

test "context core compaction expires stale cursors" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const workspace = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(workspace);

    var core = try ContextCore.init(std.testing.allocator, workspace, "agent-a");
    defer core.deinit();

    _ = try core.store("a", "1", .core, null);
    _ = try core.store("b", "2", .core, null);

    _ = try core.compactEvents();
    try std.testing.expectError(error.CursorExpired, core.listEvents(std.testing.allocator, 0, 8));
}

test "context core compaction returns removed event count" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const workspace = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(workspace);

    var core = try ContextCore.init(std.testing.allocator, workspace, "agent-a");
    defer core.deinit();

    _ = try core.store("pref", "vim", .core, null);
    _ = try core.store("pref", "helix", .core, null);
    try std.testing.expectEqual(@as(usize, 1), try core.count());

    const compacted = try core.compactEvents();
    try std.testing.expectEqual(@as(usize, 2), compacted);
}

test "context core imports legacy journal on first init" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const workspace = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(workspace);

    const journal_path = try buildLegacyArtifactPath(
        std.testing.allocator,
        workspace,
        "agent-legacy",
        LEGACY_EVENT_JOURNAL_FILENAME_SUFFIX,
    );
    defer std.testing.allocator.free(journal_path);

    const legacy_line =
        "{\"sequence\":1,\"timestamp_ms\":1234,\"origin_instance_id\":\"agent-legacy\",\"origin_sequence\":1,\"operation\":\"put\",\"key\":\"legacy\",\"content\":\"value\",\"category\":\"core\",\"session_id\":null}\n";

    var file = try std.fs.createFileAbsolute(journal_path, .{ .read = true, .truncate = true });
    defer file.close();
    try file.writeAll(legacy_line);

    var core = try ContextCore.init(std.testing.allocator, workspace, "agent-legacy");
    defer core.deinit();

    try std.testing.expect(try core.migrationComplete());
    try std.testing.expectEqual(@as(u64, 1), try core.lastEventSequence());

    const entry = (try core.get(std.testing.allocator, "legacy")).?;
    defer entry.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("value", entry.content);
}
