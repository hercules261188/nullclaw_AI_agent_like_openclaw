//! PostgreSQL-backed persistent memory via libpq.
//!
//! Compile-time gated behind `build_options.enable_postgres`.
//! When disabled, this file provides only pure-logic helpers and their tests.

const std = @import("std");
const build_options = @import("build_options");
const json_util = @import("../../json_util.zig");
const root = @import("../root.zig");
const key_codec = @import("../vector/key_codec.zig");
const Memory = root.Memory;
const MemoryEvent = root.MemoryEvent;
const MemoryEventFeedInfo = root.MemoryEventFeedInfo;
const MemoryEventOp = root.MemoryEventOp;
const MemoryEventInput = root.MemoryEventInput;
const MemoryValueKind = root.MemoryValueKind;
const MemoryCategory = root.MemoryCategory;
const MemoryEntry = root.MemoryEntry;
const ResolvedMemoryState = root.ResolvedMemoryState;
const SessionStore = root.SessionStore;

const c = if (build_options.enable_postgres) @cImport({
    @cInclude("libpq-fe.h");
}) else struct {};

// ── SQL injection protection ──────────────────────────────────────

pub const IdentifierError = error{
    EmptyIdentifier,
    IdentifierTooLong,
    InvalidCharacter,
};

/// Validate a SQL identifier (schema/table name).
/// Must be 1-63 chars, alphanumeric or underscore only.
pub fn validateIdentifier(name: []const u8) IdentifierError!void {
    if (name.len == 0) return error.EmptyIdentifier;
    if (name.len > 63) return error.IdentifierTooLong;
    for (name) |ch| {
        if (!std.ascii.isAlphanumeric(ch) and ch != '_') {
            return error.InvalidCharacter;
        }
    }
}

/// Quote a SQL identifier by wrapping in double-quotes.
/// The identifier must have been validated first.
pub fn quoteIdentifier(allocator: std.mem.Allocator, name: []const u8) ![]u8 {
    return std.fmt.allocPrint(allocator, "\"{s}\"", .{name});
}

/// Build a query by substituting {schema} and {table} placeholders.
/// Uses pre-validated, pre-quoted identifiers.
/// Returns a null-terminated slice suitable for passing to libpq C functions.
pub fn buildQuery(allocator: std.mem.Allocator, template: []const u8, schema_q: []const u8, table_q: []const u8) ![:0]u8 {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(allocator);

    var i: usize = 0;
    while (i < template.len) {
        if (i + 8 <= template.len and std.mem.eql(u8, template[i .. i + 8], "{schema}")) {
            try buf.appendSlice(allocator, schema_q);
            i += 8;
        } else if (i + 7 <= template.len and std.mem.eql(u8, template[i .. i + 7], "{table}")) {
            try buf.appendSlice(allocator, table_q);
            i += 7;
        } else {
            try buf.append(allocator, template[i]);
            i += 1;
        }
    }

    return buf.toOwnedSliceSentinel(allocator, 0);
}

fn allocPrintZCompat(allocator: std.mem.Allocator, comptime fmt: []const u8, args: anytype) ![:0]u8 {
    const rendered = try std.fmt.allocPrint(allocator, fmt, args);
    defer allocator.free(rendered);
    return allocator.dupeZ(u8, rendered);
}

fn bytesToHexLower(bytes: []const u8, out: []u8) []const u8 {
    std.debug.assert(out.len >= bytes.len * 2);
    const alphabet = "0123456789abcdef";
    for (bytes, 0..) |byte, idx| {
        out[idx * 2] = alphabet[byte >> 4];
        out[idx * 2 + 1] = alphabet[byte & 0x0f];
    }
    return out[0 .. bytes.len * 2];
}

fn getNowTimestamp(allocator: std.mem.Allocator) ![]u8 {
    const ts = std.time.timestamp();
    return std.fmt.allocPrint(allocator, "{d}", .{ts});
}

fn normalizeInstanceId(instance_id: []const u8) []const u8 {
    return if (instance_id.len > 0) instance_id else "default";
}

fn generateId(allocator: std.mem.Allocator) ![]u8 {
    const ts = std.time.nanoTimestamp();
    var buf: [16]u8 = undefined;
    std.crypto.random.bytes(&buf);
    const rand_hi = std.mem.readInt(u64, buf[0..8], .little);
    const rand_lo = std.mem.readInt(u64, buf[8..16], .little);
    return std.fmt.allocPrint(allocator, "{d}-{x}-{x}", .{ ts, rand_hi, rand_lo });
}

// ── PostgresMemory (only available when enable_postgres is true) ──

pub const PostgresMemory = if (build_options.enable_postgres) PostgresMemoryImpl else struct {};

const PostgresMemoryImpl = struct {
    conn: *c.PGconn,
    allocator: std.mem.Allocator,
    owns_self: bool = false,
    schema_q: []const u8, // validated+quoted schema name
    table_q: []const u8, // validated+quoted table name
    instance_id: []const u8 = "default",

    // Pre-built query templates
    q_store: []const u8,
    q_get: []const u8,
    q_get_scoped: []const u8,
    q_list_cat: []const u8,
    q_list_all: []const u8,
    q_recall: []const u8,
    q_forget: []const u8,
    q_forget_scoped: []const u8,
    q_count: []const u8,
    q_save_msg: []const u8,
    q_load_msgs: []const u8,
    q_clear_msgs: []const u8,
    q_save_usage: []const u8,
    q_load_usage: []const u8,
    q_clear_usage: []const u8,
    q_count_sessions: []const u8,
    q_list_sessions: []const u8,
    q_count_detailed_msgs: []const u8,
    q_load_msgs_detailed: []const u8,
    q_clear_auto: []const u8,
    q_clear_auto_sid: []const u8,
    q_recall_sid: []const u8,
    q_list_cat_sid: []const u8,
    q_list_sid: []const u8,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, url: [*:0]const u8, schema: []const u8, table: []const u8, instance_id: []const u8) !Self {
        try validateIdentifier(schema);
        try validateIdentifier(table);

        const schema_q = try quoteIdentifier(allocator, schema);
        errdefer allocator.free(schema_q);
        const table_q = try quoteIdentifier(allocator, table);
        errdefer allocator.free(table_q);

        const conn = c.PQconnectdb(url) orelse return error.ConnectionFailed;
        errdefer c.PQfinish(conn);

        if (c.PQstatus(conn) != c.CONNECTION_OK) {
            return error.ConnectionFailed;
        }

        var self_ = Self{
            .conn = conn,
            .allocator = allocator,
            .schema_q = schema_q,
            .table_q = table_q,
            .instance_id = normalizeInstanceId(instance_id),
            .q_store = undefined,
            .q_get = undefined,
            .q_get_scoped = undefined,
            .q_list_cat = undefined,
            .q_list_all = undefined,
            .q_recall = undefined,
            .q_forget = undefined,
            .q_forget_scoped = undefined,
            .q_count = undefined,
            .q_save_msg = undefined,
            .q_load_msgs = undefined,
            .q_clear_msgs = undefined,
            .q_save_usage = undefined,
            .q_load_usage = undefined,
            .q_clear_usage = undefined,
            .q_count_sessions = undefined,
            .q_list_sessions = undefined,
            .q_count_detailed_msgs = undefined,
            .q_load_msgs_detailed = undefined,
            .q_clear_auto = undefined,
            .q_clear_auto_sid = undefined,
            .q_recall_sid = undefined,
            .q_list_cat_sid = undefined,
            .q_list_sid = undefined,
        };

        // Build query templates.
        // instance_id is normalized eagerly, so every plane uses the same namespace.
        self_.q_store = try buildQuery(allocator,
            "WITH deleted AS (" ++
                "DELETE FROM {schema}.{table} WHERE key = $2 AND instance_id = $6 AND ((session_id IS NULL AND $5 IS NULL) OR session_id = $5)" ++
            ") " ++
            "INSERT INTO {schema}.{table} (id, key, content, category, session_id, instance_id, created_at, updated_at) " ++
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            schema_q,
            table_q,
        );
        errdefer allocator.free(self_.q_store);

        self_.q_get = try buildQuery(allocator, "SELECT id, key, content, category, updated_at, session_id FROM {schema}.{table} WHERE key = $1 AND session_id IS NULL AND instance_id = $2 LIMIT 1", schema_q, table_q);
        errdefer allocator.free(self_.q_get);

        self_.q_get_scoped = try buildQuery(allocator, "SELECT id, key, content, category, updated_at, session_id FROM {schema}.{table} WHERE key = $1 AND ((session_id IS NULL AND $2 IS NULL) OR session_id = $2) AND instance_id = $3 LIMIT 1", schema_q, table_q);
        errdefer allocator.free(self_.q_get_scoped);

        self_.q_list_cat = try buildQuery(allocator, "SELECT id, key, content, category, updated_at, session_id FROM {schema}.{table} WHERE category = $1 AND instance_id = $2 ORDER BY updated_at DESC", schema_q, table_q);
        errdefer allocator.free(self_.q_list_cat);

        self_.q_list_all = try buildQuery(allocator, "SELECT id, key, content, category, updated_at, session_id FROM {schema}.{table} WHERE instance_id = $1 ORDER BY updated_at DESC", schema_q, table_q);
        errdefer allocator.free(self_.q_list_all);

        self_.q_recall = try buildQuery(allocator, "SELECT id, key, content, category, updated_at, session_id, " ++
            "CASE WHEN key ILIKE $1 THEN 2.0 ELSE 0.0 END + " ++
            "CASE WHEN content ILIKE $1 THEN 1.0 ELSE 0.0 END AS score " ++
            "FROM {schema}.{table} WHERE (key ILIKE $1 OR content ILIKE $1) AND instance_id = $3 " ++
            "ORDER BY score DESC LIMIT $2", schema_q, table_q);
        errdefer allocator.free(self_.q_recall);

        self_.q_forget = try buildQuery(allocator, "DELETE FROM {schema}.{table} WHERE key = $1 AND instance_id = $2", schema_q, table_q);
        errdefer allocator.free(self_.q_forget);

        self_.q_forget_scoped = try buildQuery(allocator, "DELETE FROM {schema}.{table} WHERE key = $1 AND ((session_id IS NULL AND $2 IS NULL) OR session_id = $2) AND instance_id = $3", schema_q, table_q);
        errdefer allocator.free(self_.q_forget_scoped);

        self_.q_count = try buildQuery(allocator, "SELECT COUNT(*) FROM {schema}.{table} WHERE instance_id = $1", schema_q, table_q);
        errdefer allocator.free(self_.q_count);

        self_.q_save_msg = try buildQuery(allocator, "INSERT INTO {schema}.messages (session_id, instance_id, role, content) VALUES ($1, $2, $3, $4)", schema_q, table_q);
        errdefer allocator.free(self_.q_save_msg);

        self_.q_load_msgs = try buildQuery(allocator, "SELECT role, content FROM {schema}.messages WHERE session_id = $1 AND instance_id = $2 ORDER BY id ASC", schema_q, table_q);
        errdefer allocator.free(self_.q_load_msgs);

        self_.q_clear_msgs = try buildQuery(allocator, "DELETE FROM {schema}.messages WHERE session_id = $1 AND instance_id = $2", schema_q, table_q);
        errdefer allocator.free(self_.q_clear_msgs);

        self_.q_save_usage = try buildQuery(allocator, "INSERT INTO {schema}.session_usage (session_id, instance_id, total_tokens, updated_at) VALUES ($1, $2, $3, NOW()) " ++
            "ON CONFLICT (session_id, instance_id) DO UPDATE SET total_tokens = EXCLUDED.total_tokens, updated_at = NOW()", schema_q, table_q);
        errdefer allocator.free(self_.q_save_usage);

        self_.q_load_usage = try buildQuery(allocator, "SELECT total_tokens FROM {schema}.session_usage WHERE session_id = $1 AND instance_id = $2", schema_q, table_q);
        errdefer allocator.free(self_.q_load_usage);

        self_.q_clear_usage = try buildQuery(allocator, "DELETE FROM {schema}.session_usage WHERE session_id = $1 AND instance_id = $2", schema_q, table_q);
        errdefer allocator.free(self_.q_clear_usage);

        self_.q_count_sessions = try buildQuery(
            allocator,
            "SELECT COUNT(*) FROM (SELECT 1 FROM {schema}.messages WHERE instance_id = $1 AND role <> '" ++ root.RUNTIME_COMMAND_ROLE ++ "' GROUP BY session_id) AS sessions",
            schema_q,
            table_q,
        );
        errdefer allocator.free(self_.q_count_sessions);

        self_.q_list_sessions = try buildQuery(
            allocator,
            "SELECT session_id, COUNT(*), MIN(created_at)::text, MAX(created_at)::text FROM {schema}.messages WHERE instance_id = $1 AND role <> '" ++ root.RUNTIME_COMMAND_ROLE ++ "' GROUP BY session_id ORDER BY MAX(created_at) DESC LIMIT $2 OFFSET $3",
            schema_q,
            table_q,
        );
        errdefer allocator.free(self_.q_list_sessions);

        self_.q_count_detailed_msgs = try buildQuery(
            allocator,
            "SELECT COUNT(*) FROM {schema}.messages WHERE session_id = $1 AND instance_id = $2 AND role <> '" ++ root.RUNTIME_COMMAND_ROLE ++ "'",
            schema_q,
            table_q,
        );
        errdefer allocator.free(self_.q_count_detailed_msgs);

        self_.q_load_msgs_detailed = try buildQuery(
            allocator,
            "SELECT role, content, created_at::text FROM {schema}.messages WHERE session_id = $1 AND instance_id = $2 AND role <> '" ++ root.RUNTIME_COMMAND_ROLE ++ "' ORDER BY id ASC LIMIT $3 OFFSET $4",
            schema_q,
            table_q,
        );
        errdefer allocator.free(self_.q_load_msgs_detailed);

        self_.q_clear_auto = try buildQuery(allocator, "DELETE FROM {schema}.{table} WHERE key LIKE 'autosave_%' AND instance_id = $1", schema_q, table_q);
        errdefer allocator.free(self_.q_clear_auto);

        self_.q_clear_auto_sid = try buildQuery(allocator, "DELETE FROM {schema}.{table} WHERE key LIKE 'autosave_%' AND session_id = $1 AND instance_id = $2", schema_q, table_q);
        errdefer allocator.free(self_.q_clear_auto_sid);

        self_.q_recall_sid = try buildQuery(allocator, "SELECT id, key, content, category, updated_at, session_id, " ++
            "CASE WHEN key ILIKE $1 THEN 2.0 ELSE 0.0 END + " ++
            "CASE WHEN content ILIKE $1 THEN 1.0 ELSE 0.0 END AS score " ++
            "FROM {schema}.{table} WHERE (key ILIKE $1 OR content ILIKE $1) AND session_id = $3 AND instance_id = $4 " ++
            "ORDER BY score DESC LIMIT $2", schema_q, table_q);
        errdefer allocator.free(self_.q_recall_sid);

        self_.q_list_cat_sid = try buildQuery(allocator, "SELECT id, key, content, category, updated_at, session_id FROM {schema}.{table} WHERE category = $1 AND session_id = $2 AND instance_id = $3 ORDER BY updated_at DESC", schema_q, table_q);
        errdefer allocator.free(self_.q_list_cat_sid);

        self_.q_list_sid = try buildQuery(allocator, "SELECT id, key, content, category, updated_at, session_id FROM {schema}.{table} WHERE session_id = $1 AND instance_id = $2 ORDER BY updated_at DESC", schema_q, table_q);
        errdefer allocator.free(self_.q_list_sid);

        // Run migrations
        try self_.migrate(schema, table);

        return self_;
    }

    pub fn deinit(self: *Self) void {
        c.PQfinish(self.conn);
        self.allocator.free(self.q_store);
        self.allocator.free(self.q_get);
        self.allocator.free(self.q_get_scoped);
        self.allocator.free(self.q_list_cat);
        self.allocator.free(self.q_list_all);
        self.allocator.free(self.q_recall);
        self.allocator.free(self.q_forget);
        self.allocator.free(self.q_forget_scoped);
        self.allocator.free(self.q_count);
        self.allocator.free(self.q_save_msg);
        self.allocator.free(self.q_load_msgs);
        self.allocator.free(self.q_clear_msgs);
        self.allocator.free(self.q_save_usage);
        self.allocator.free(self.q_load_usage);
        self.allocator.free(self.q_clear_usage);
        self.allocator.free(self.q_count_sessions);
        self.allocator.free(self.q_list_sessions);
        self.allocator.free(self.q_count_detailed_msgs);
        self.allocator.free(self.q_load_msgs_detailed);
        self.allocator.free(self.q_clear_auto);
        self.allocator.free(self.q_clear_auto_sid);
        self.allocator.free(self.q_recall_sid);
        self.allocator.free(self.q_list_cat_sid);
        self.allocator.free(self.q_list_sid);
        self.allocator.free(self.schema_q);
        self.allocator.free(self.table_q);
        if (self.owns_self) {
            self.allocator.destroy(self);
        }
    }

    fn migrate(self: *Self, raw_schema: []const u8, raw_table: []const u8) !void {
        // raw_schema/raw_table are pre-validated (alphanumeric + underscore only) so safe where used below.
        // Index names must NOT use quoted identifiers, so we use raw_table directly.
        var ddl_buf: std.ArrayListUnmanaged(u8) = .empty;
        defer ddl_buf.deinit(self.allocator);
        const w = ddl_buf.writer(self.allocator);

        try w.print(
            \\CREATE TABLE IF NOT EXISTS {s}.{s} (
            \\    id TEXT PRIMARY KEY,
            \\    key TEXT NOT NULL,
            \\    content TEXT NOT NULL,
            \\    category TEXT NOT NULL DEFAULT 'core',
            \\    session_id TEXT,
            \\    instance_id TEXT NOT NULL DEFAULT 'default',
            \\    value_kind TEXT,
            \\    event_timestamp_ms BIGINT NOT NULL DEFAULT 0,
            \\    event_origin_instance_id TEXT NOT NULL DEFAULT 'default',
            \\    event_origin_sequence BIGINT NOT NULL DEFAULT 0,
            \\    created_at TEXT NOT NULL,
            \\    updated_at TEXT NOT NULL
            \\);
        , .{ self.schema_q, self.table_q });
        try w.print("ALTER TABLE {s}.{s} ADD COLUMN IF NOT EXISTS instance_id TEXT NOT NULL DEFAULT 'default';\n", .{ self.schema_q, self.table_q });
        try w.print("ALTER TABLE {s}.{s} ADD COLUMN IF NOT EXISTS value_kind TEXT;\n", .{ self.schema_q, self.table_q });
        try w.print("ALTER TABLE {s}.{s} ADD COLUMN IF NOT EXISTS event_timestamp_ms BIGINT NOT NULL DEFAULT 0;\n", .{ self.schema_q, self.table_q });
        try w.print("ALTER TABLE {s}.{s} ADD COLUMN IF NOT EXISTS event_origin_instance_id TEXT NOT NULL DEFAULT 'default';\n", .{ self.schema_q, self.table_q });
        try w.print("ALTER TABLE {s}.{s} ADD COLUMN IF NOT EXISTS event_origin_sequence BIGINT NOT NULL DEFAULT 0;\n", .{ self.schema_q, self.table_q });
        try w.print("DROP INDEX IF EXISTS {s}.idx_{s}_key;\n", .{ self.schema_q, raw_table });
        try w.print("DROP INDEX IF EXISTS {s}.idx_{s}_key_instance;\n", .{ self.schema_q, raw_table });
        try w.print("CREATE UNIQUE INDEX IF NOT EXISTS idx_{s}_key_instance_session ON {s}.{s}(key, instance_id, COALESCE(session_id, '__global__'));\n", .{ raw_table, self.schema_q, self.table_q });
        try w.print("CREATE INDEX IF NOT EXISTS idx_{s}_category ON {s}.{s}(category);\n", .{ raw_table, self.schema_q, self.table_q });
        try w.print("CREATE INDEX IF NOT EXISTS idx_{s}_session ON {s}.{s}(session_id);\n", .{ raw_table, self.schema_q, self.table_q });
        try w.print("CREATE INDEX IF NOT EXISTS idx_{s}_instance ON {s}.{s}(instance_id);\n", .{ raw_table, self.schema_q, self.table_q });
        try w.print("CREATE INDEX IF NOT EXISTS idx_{s}_event_order ON {s}.{s}(instance_id, event_timestamp_ms, event_origin_instance_id, event_origin_sequence);\n", .{ raw_table, self.schema_q, self.table_q });

        try w.print(
            \\CREATE TABLE IF NOT EXISTS {s}.memory_events (
            \\    instance_id TEXT NOT NULL DEFAULT 'default',
            \\    local_sequence BIGINT NOT NULL,
            \\    schema_version INTEGER NOT NULL DEFAULT 1,
            \\    origin_instance_id TEXT NOT NULL,
            \\    origin_sequence BIGINT NOT NULL,
            \\    timestamp_ms BIGINT NOT NULL,
            \\    operation TEXT NOT NULL,
            \\    key TEXT NOT NULL,
            \\    session_id TEXT,
            \\    category TEXT,
            \\    value_kind TEXT,
            \\    content TEXT,
            \\    PRIMARY KEY(instance_id, local_sequence)
            \\);
            \\CREATE UNIQUE INDEX IF NOT EXISTS idx_memory_events_origin ON {s}.memory_events(instance_id, origin_instance_id, origin_sequence);
            \\CREATE TABLE IF NOT EXISTS {s}.memory_event_frontiers (
            \\    instance_id TEXT NOT NULL DEFAULT 'default',
            \\    origin_instance_id TEXT NOT NULL,
            \\    last_origin_sequence BIGINT NOT NULL,
            \\    PRIMARY KEY(instance_id, origin_instance_id)
            \\);
            \\CREATE TABLE IF NOT EXISTS {s}.memory_tombstones (
            \\    instance_id TEXT NOT NULL DEFAULT 'default',
            \\    key TEXT NOT NULL,
            \\    scope TEXT NOT NULL,
            \\    session_key TEXT NOT NULL,
            \\    session_id TEXT,
            \\    timestamp_ms BIGINT NOT NULL,
            \\    origin_instance_id TEXT NOT NULL,
            \\    origin_sequence BIGINT NOT NULL,
            \\    PRIMARY KEY(instance_id, key, scope, session_key)
            \\);
            \\CREATE INDEX IF NOT EXISTS idx_memory_tombstones_key ON {s}.memory_tombstones(instance_id, key);
            \\CREATE TABLE IF NOT EXISTS {s}.memory_feed_meta (
            \\    instance_id TEXT NOT NULL DEFAULT 'default',
            \\    key TEXT NOT NULL,
            \\    value TEXT NOT NULL,
            \\    PRIMARY KEY(instance_id, key)
            \\);
        , .{
            self.schema_q,
            self.schema_q,
            self.schema_q,
            self.schema_q,
            self.schema_q,
            self.schema_q,
        });

        try w.print(
            \\CREATE TABLE IF NOT EXISTS {s}.messages (
            \\    id SERIAL PRIMARY KEY,
            \\    session_id TEXT NOT NULL,
            \\    instance_id TEXT NOT NULL DEFAULT 'default',
            \\    role TEXT NOT NULL,
            \\    content TEXT NOT NULL,
            \\    created_at TIMESTAMP DEFAULT NOW()
            \\);
            \\ALTER TABLE {s}.messages ADD COLUMN IF NOT EXISTS instance_id TEXT NOT NULL DEFAULT 'default';
            \\CREATE INDEX IF NOT EXISTS idx_messages_instance_session ON {s}.messages(instance_id, session_id);
            \\CREATE TABLE IF NOT EXISTS {s}.session_usage (
            \\    session_id TEXT NOT NULL,
            \\    instance_id TEXT NOT NULL DEFAULT 'default',
            \\    total_tokens BIGINT NOT NULL DEFAULT 0,
            \\    updated_at TIMESTAMP DEFAULT NOW()
            \\);
            \\ALTER TABLE {s}.session_usage ADD COLUMN IF NOT EXISTS instance_id TEXT NOT NULL DEFAULT 'default';
            \\DO $$
            \\DECLARE
            \\    old_pk_name text;
            \\BEGIN
            \\    SELECT conname INTO old_pk_name
            \\    FROM pg_constraint
            \\    WHERE conrelid = '{s}.session_usage'::regclass
            \\      AND contype = 'p'
            \\      AND array_length(conkey, 1) = 1
            \\    LIMIT 1;
            \\    IF old_pk_name IS NOT NULL THEN
            \\        EXECUTE format('ALTER TABLE %I.session_usage DROP CONSTRAINT %I', '{s}', old_pk_name);
            \\    END IF;
            \\END $$;
            \\CREATE UNIQUE INDEX IF NOT EXISTS idx_session_usage_session_instance ON {s}.session_usage(session_id, instance_id);
            \\CREATE INDEX IF NOT EXISTS idx_session_usage_instance ON {s}.session_usage(instance_id);
        , .{
            self.schema_q,
            self.schema_q,
            self.schema_q,
            self.schema_q,
            self.schema_q,
            raw_schema,
            raw_schema,
            self.schema_q,
            self.schema_q,
        });

        const ddl = try self.allocator.dupeZ(u8, ddl_buf.items);
        defer self.allocator.free(ddl);

        const result = c.PQexec(self.conn, ddl.ptr);
        defer c.PQclear(result);

        const status = c.PQresultStatus(result);
        if (status != c.PGRES_COMMAND_OK and status != c.PGRES_TUPLES_OK) {
            return error.MigrationFailed;
        }

        if (std.mem.eql(u8, self.localInstanceId(), "default")) {
            try self.promoteLegacyDefaultInstanceIds();
        }
        try self.backfillFeedFromExistingState();
    }

    fn promoteLegacyDefaultInstanceIds(self: *Self) !void {
        const updates = [_][]const u8{
            "UPDATE {schema}.{table} SET instance_id = 'default' WHERE instance_id = ''",
            "UPDATE {schema}.memory_events SET instance_id = 'default' WHERE instance_id = ''",
            "UPDATE {schema}.memory_event_frontiers SET instance_id = 'default' WHERE instance_id = ''",
            "UPDATE {schema}.memory_tombstones SET instance_id = 'default' WHERE instance_id = ''",
            "UPDATE {schema}.memory_feed_meta SET instance_id = 'default' WHERE instance_id = ''",
            "UPDATE {schema}.messages SET instance_id = 'default' WHERE instance_id = ''",
            "UPDATE {schema}.session_usage SET instance_id = 'default' WHERE instance_id = ''",
        };
        inline for (updates) |template| {
            const sql = try buildQuery(self.allocator, template, self.schema_q, self.table_q);
            defer self.allocator.free(sql);
            try self.execSql(sql);
        }
    }

    fn querySingleU64(self: *Self, query: []const u8, params: []const ?[*:0]const u8, lengths: []const c_int) !u64 {
        const result = try self.execParams(query, params, lengths);
        defer c.PQclear(result);
        if (c.PQntuples(result) == 0 or c.PQgetisnull(result, 0, 0) != 0) return 0;
        const value = c.PQgetvalue(result, 0, 0);
        const len: usize = @intCast(c.PQgetlength(result, 0, 0));
        return std.fmt.parseInt(u64, value[0..len], 10) catch 0;
    }

    fn backfillFeedFromExistingState(self: *Self) !void {
        const local_instance_id = self.localInstanceId();
        const iid_z = try self.allocator.dupeZ(u8, local_instance_id);
        defer self.allocator.free(iid_z);
        const params = [_]?[*:0]const u8{iid_z};
        const lengths = [_]c_int{@intCast(local_instance_id.len)};

        const state_count_sql = try buildQuery(
            self.allocator,
            "SELECT COUNT(*) FROM {schema}.{table} WHERE instance_id = $1",
            self.schema_q,
            self.table_q,
        );
        defer self.allocator.free(state_count_sql);
        const state_count = try self.querySingleU64(state_count_sql, &params, &lengths);
        if (state_count == 0) return;

        const event_count_sql = try buildQuery(
            self.allocator,
            "SELECT COUNT(*) FROM {schema}.memory_events WHERE instance_id = $1",
            self.schema_q,
            self.table_q,
        );
        defer self.allocator.free(event_count_sql);
        const event_count = try self.querySingleU64(event_count_sql, &params, &lengths);
        if (event_count != 0) return;

        try self.begin();
        var committed = false;
        errdefer if (!committed) self.rollback();

        const insert_events_sql = try buildQuery(
            self.allocator,
            "WITH ordered AS (" ++
                "SELECT key, session_id, category, value_kind, content, event_timestamp_ms, event_origin_instance_id, event_origin_sequence, " ++
                "ROW_NUMBER() OVER (ORDER BY event_timestamp_ms ASC, event_origin_instance_id ASC, event_origin_sequence ASC, key ASC, COALESCE(session_id, '')) AS seq " ++
                "FROM {schema}.{table} WHERE instance_id = $1" ++
            ") " ++
            "INSERT INTO {schema}.memory_events (instance_id, local_sequence, schema_version, origin_instance_id, origin_sequence, timestamp_ms, operation, key, session_id, category, value_kind, content) " ++
            "SELECT $1, seq, 1, event_origin_instance_id, event_origin_sequence, event_timestamp_ms, 'put', key, session_id, category, value_kind, content FROM ordered",
            self.schema_q,
            self.table_q,
        );
        defer self.allocator.free(insert_events_sql);
        {
            const result_insert_events = try self.execParams(insert_events_sql, &params, &lengths);
            c.PQclear(result_insert_events);
        }

        const insert_frontiers_sql = try buildQuery(
            self.allocator,
            "INSERT INTO {schema}.memory_event_frontiers (instance_id, origin_instance_id, last_origin_sequence) " ++
                "SELECT $1, event_origin_instance_id, MAX(event_origin_sequence) " ++
                "FROM {schema}.{table} WHERE instance_id = $1 GROUP BY event_origin_instance_id " ++
                "ON CONFLICT (instance_id, origin_instance_id) DO UPDATE SET last_origin_sequence = EXCLUDED.last_origin_sequence",
            self.schema_q,
            self.table_q,
        );
        defer self.allocator.free(insert_frontiers_sql);
        {
            const result_insert_frontiers = try self.execParams(insert_frontiers_sql, &params, &lengths);
            c.PQclear(result_insert_frontiers);
        }

        try self.setLastSequenceTx(self.allocator, state_count);
        try self.commit();
        committed = true;
    }

    fn execParams(self: *Self, query: []const u8, params: []const ?[*:0]const u8, lengths: []const c_int) !*c.PGresult {
        const n: c_int = @intCast(params.len);
        const result = c.PQexecParams(
            self.conn,
            query.ptr,
            n,
            null, // paramTypes — let PG infer
            @ptrCast(params.ptr),
            lengths.ptr,
            null, // paramFormats — text
            0, // resultFormat — text
        ) orelse return error.ExecFailed;

        const status = c.PQresultStatus(result);
        if (status != c.PGRES_COMMAND_OK and status != c.PGRES_TUPLES_OK) {
            c.PQclear(result);
            return error.ExecFailed;
        }
        return result;
    }

    fn execSql(self: *Self, sql: []const u8) !void {
        const result = c.PQexec(self.conn, sql.ptr) orelse return error.ExecFailed;
        defer c.PQclear(result);
        const status = c.PQresultStatus(result);
        if (status != c.PGRES_COMMAND_OK and status != c.PGRES_TUPLES_OK) return error.ExecFailed;
    }

    fn begin(self: *Self) !void {
        try self.execSql("BEGIN");
    }

    fn commit(self: *Self) !void {
        try self.execSql("COMMIT");
    }

    fn rollback(self: *Self) void {
        const result = c.PQexec(self.conn, "ROLLBACK");
        if (result) |r| c.PQclear(r);
    }

    fn localInstanceId(self: *Self) []const u8 {
        return normalizeInstanceId(self.instance_id);
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

    fn schemaQuery(self: *Self, allocator: std.mem.Allocator, comptime fmt: []const u8) ![:0]u8 {
        return allocPrintZCompat(allocator, fmt, .{self.schema_q});
    }

    fn schemaTableQuery(self: *Self, allocator: std.mem.Allocator, comptime fmt: []const u8) ![:0]u8 {
        return allocPrintZCompat(allocator, fmt, .{ self.schema_q, self.table_q });
    }

    fn getMetaValueTx(self: *Self, allocator: std.mem.Allocator, key: []const u8) !?[]u8 {
        const sql = try self.schemaQuery(allocator,
            "SELECT value FROM {s}.memory_feed_meta WHERE instance_id = $1 AND key = $2 LIMIT 1",
        );
        defer allocator.free(sql);

        const iid_z = try allocator.dupeZ(u8, self.localInstanceId());
        defer allocator.free(iid_z);
        const key_z = try allocator.dupeZ(u8, key);
        defer allocator.free(key_z);

        const params = [_]?[*:0]const u8{ iid_z, key_z };
        const lengths = [_]c_int{ @intCast(self.localInstanceId().len), @intCast(key.len) };
        const result = try self.execParams(sql, &params, &lengths);
        defer c.PQclear(result);

        if (c.PQntuples(result) == 0) return null;
        return try dupeResultValueOpt(allocator, result, 0, 0);
    }

    fn setMetaValueTx(self: *Self, allocator: std.mem.Allocator, key: []const u8, value: u64) !void {
        const sql = try self.schemaQuery(allocator,
            "INSERT INTO {s}.memory_feed_meta (instance_id, key, value) VALUES ($1, $2, $3) " ++
                "ON CONFLICT (instance_id, key) DO UPDATE SET value = EXCLUDED.value",
        );
        defer allocator.free(sql);

        const iid_z = try allocator.dupeZ(u8, self.localInstanceId());
        defer allocator.free(iid_z);
        const key_z = try allocator.dupeZ(u8, key);
        defer allocator.free(key_z);
        const value_str = try allocPrintZCompat(allocator, "{d}", .{value});
        defer allocator.free(value_str);

        const params = [_]?[*:0]const u8{ iid_z, key_z, value_str };
        const lengths = [_]c_int{ @intCast(self.localInstanceId().len), @intCast(key.len), @intCast(value_str.len - 1) };
        const result = try self.execParams(sql, &params, &lengths);
        c.PQclear(result);
    }

    fn getCompactedThroughSequence(self: *Self, allocator: std.mem.Allocator) !u64 {
        const value = try self.getMetaValueTx(allocator, "compacted_through_sequence") orelse return 0;
        defer allocator.free(value);
        return std.fmt.parseInt(u64, value, 10) catch 0;
    }

    fn setCompactedThroughSequenceTx(self: *Self, allocator: std.mem.Allocator, sequence: u64) !void {
        try self.setMetaValueTx(allocator, "compacted_through_sequence", sequence);
    }

    fn getLastSequenceTx(self: *Self, allocator: std.mem.Allocator) !u64 {
        const value = try self.getMetaValueTx(allocator, "last_sequence") orelse return try self.getCompactedThroughSequence(allocator);
        defer allocator.free(value);
        return std.fmt.parseInt(u64, value, 10) catch 0;
    }

    fn setLastSequenceTx(self: *Self, allocator: std.mem.Allocator, sequence: u64) !void {
        try self.setMetaValueTx(allocator, "last_sequence", sequence);
    }

    fn getFrontierTx(self: *Self, allocator: std.mem.Allocator, origin_instance_id: []const u8) !u64 {
        const sql = try self.schemaQuery(allocator,
            "SELECT last_origin_sequence FROM {s}.memory_event_frontiers WHERE instance_id = $1 AND origin_instance_id = $2 LIMIT 1",
        );
        defer allocator.free(sql);

        const iid_z = try allocator.dupeZ(u8, self.localInstanceId());
        defer allocator.free(iid_z);
        const origin_z = try allocator.dupeZ(u8, origin_instance_id);
        defer allocator.free(origin_z);

        const params = [_]?[*:0]const u8{ iid_z, origin_z };
        const lengths = [_]c_int{ @intCast(self.localInstanceId().len), @intCast(origin_instance_id.len) };
        const result = try self.execParams(sql, &params, &lengths);
        defer c.PQclear(result);

        if (c.PQntuples(result) == 0) return 0;
        const raw = c.PQgetvalue(result, 0, 0);
        const len: usize = @intCast(c.PQgetlength(result, 0, 0));
        return std.fmt.parseInt(u64, raw[0..len], 10) catch 0;
    }

    fn setFrontierTx(self: *Self, allocator: std.mem.Allocator, origin_instance_id: []const u8, origin_sequence: u64) !void {
        const sql = try self.schemaQuery(allocator,
            "INSERT INTO {s}.memory_event_frontiers (instance_id, origin_instance_id, last_origin_sequence) VALUES ($1, $2, $3) " ++
                "ON CONFLICT (instance_id, origin_instance_id) DO UPDATE SET last_origin_sequence = GREATEST(memory_event_frontiers.last_origin_sequence, EXCLUDED.last_origin_sequence)",
        );
        defer allocator.free(sql);

        const iid_z = try allocator.dupeZ(u8, self.localInstanceId());
        defer allocator.free(iid_z);
        const origin_z = try allocator.dupeZ(u8, origin_instance_id);
        defer allocator.free(origin_z);
        const seq_z = try allocPrintZCompat(allocator, "{d}", .{origin_sequence});
        defer allocator.free(seq_z);

        const params = [_]?[*:0]const u8{ iid_z, origin_z, seq_z };
        const lengths = [_]c_int{ @intCast(self.localInstanceId().len), @intCast(origin_instance_id.len), @intCast(seq_z.len - 1) };
        const result = try self.execParams(sql, &params, &lengths);
        c.PQclear(result);
    }

    fn nextLocalOriginSequenceTx(self: *Self, allocator: std.mem.Allocator) !u64 {
        return (try self.getFrontierTx(allocator, self.localInstanceId())) + 1;
    }

    fn nextEventSequenceTx(self: *Self, allocator: std.mem.Allocator) !u64 {
        const compacted_through = try self.getCompactedThroughSequence(allocator);
        const last_sequence = try self.getLastSequenceTx(allocator);
        return @max(last_sequence, compacted_through) + 1;
    }

    fn dupeResultValue(allocator: std.mem.Allocator, result: *c.PGresult, row: c_int, col: c_int) ![]u8 {
        if (c.PQgetisnull(result, row, col) != 0) {
            return allocator.dupe(u8, "");
        }
        const val = c.PQgetvalue(result, row, col);
        const len: usize = @intCast(c.PQgetlength(result, row, col));
        return allocator.dupe(u8, val[0..len]);
    }

    fn dupeResultValueOpt(allocator: std.mem.Allocator, result: *c.PGresult, row: c_int, col: c_int) !?[]u8 {
        if (c.PQgetisnull(result, row, col) != 0) {
            return null;
        }
        const val = c.PQgetvalue(result, row, col);
        const len: usize = @intCast(c.PQgetlength(result, row, col));
        return try allocator.dupe(u8, val[0..len]);
    }

    fn readEntryFromResult(allocator: std.mem.Allocator, result: *c.PGresult, row: c_int) !MemoryEntry {
        // Columns: id(0), key(1), content(2), category(3), updated_at(4), session_id(5)
        const id = try dupeResultValue(allocator, result, row, 0);
        errdefer allocator.free(id);
        const key = try dupeResultValue(allocator, result, row, 1);
        errdefer allocator.free(key);
        const content = try dupeResultValue(allocator, result, row, 2);
        errdefer allocator.free(content);
        const cat_str = try dupeResultValue(allocator, result, row, 3);
        const category = MemoryCategory.fromString(cat_str);
        // Free cat_str only if it wasn't captured by .custom
        switch (category) {
            .custom => {}, // cat_str is now owned by category.custom
            else => allocator.free(cat_str),
        }
        errdefer switch (category) {
            .custom => |name| allocator.free(name),
            else => {},
        };
        const timestamp = try dupeResultValue(allocator, result, row, 4);
        errdefer allocator.free(timestamp);
        const session_id = try dupeResultValueOpt(allocator, result, row, 5);

        return .{
            .id = id,
            .key = key,
            .content = content,
            .category = category,
            .timestamp = timestamp,
            .session_id = session_id,
        };
    }

    fn readEventFromResult(allocator: std.mem.Allocator, result: *c.PGresult, row: c_int) !MemoryEvent {
        const sequence_raw = c.PQgetvalue(result, row, 0);
        const sequence_len: usize = @intCast(c.PQgetlength(result, row, 0));
        const sequence = std.fmt.parseInt(u64, sequence_raw[0..sequence_len], 10) catch return error.InvalidEvent;

        const schema_raw = c.PQgetvalue(result, row, 1);
        const schema_len: usize = @intCast(c.PQgetlength(result, row, 1));
        const schema_version = std.fmt.parseInt(u32, schema_raw[0..schema_len], 10) catch 1;

        const origin_instance_id = try dupeResultValue(allocator, result, row, 2);
        errdefer allocator.free(origin_instance_id);

        const origin_seq_raw = c.PQgetvalue(result, row, 3);
        const origin_seq_len: usize = @intCast(c.PQgetlength(result, row, 3));
        const origin_sequence = std.fmt.parseInt(u64, origin_seq_raw[0..origin_seq_len], 10) catch return error.InvalidEvent;

        const timestamp_raw = c.PQgetvalue(result, row, 4);
        const timestamp_len: usize = @intCast(c.PQgetlength(result, row, 4));
        const timestamp_ms = std.fmt.parseInt(i64, timestamp_raw[0..timestamp_len], 10) catch return error.InvalidEvent;

        const op_text = try dupeResultValue(allocator, result, row, 5);
        defer allocator.free(op_text);
        const operation = MemoryEventOp.fromString(op_text) orelse return error.InvalidEvent;

        const key = try dupeResultValue(allocator, result, row, 6);
        errdefer allocator.free(key);
        const session_id = try dupeResultValueOpt(allocator, result, row, 7);
        errdefer if (session_id) |sid| allocator.free(sid);

        const category_text = try dupeResultValueOpt(allocator, result, row, 8);
        defer if (category_text) |text| allocator.free(text);
        const category = if (category_text) |text| try parseCategoryOwned(allocator, text) else null;
        errdefer if (category) |cat| switch (cat) {
            .custom => |name| allocator.free(name),
            else => {},
        };

        const value_kind_text = try dupeResultValueOpt(allocator, result, row, 9);
        defer if (value_kind_text) |text| allocator.free(text);
        const value_kind = if (value_kind_text) |text|
            MemoryValueKind.fromString(text) orelse return error.InvalidEvent
        else
            null;

        const content = try dupeResultValueOpt(allocator, result, row, 10);

        return .{
            .schema_version = schema_version,
            .sequence = sequence,
            .origin_instance_id = origin_instance_id,
            .origin_sequence = origin_sequence,
            .timestamp_ms = timestamp_ms,
            .operation = operation,
            .key = key,
            .session_id = session_id,
            .category = category,
            .value_kind = value_kind,
            .content = content,
        };
    }

    fn parseCategoryOwned(allocator: std.mem.Allocator, text: []const u8) !MemoryCategory {
        const category = MemoryCategory.fromString(text);
        return switch (category) {
            .custom => .{ .custom = try allocator.dupe(u8, text) },
            else => category,
        };
    }

    fn insertEventTx(self: *Self, input: MemoryEventInput) !bool {
        const sql = try self.schemaQuery(self.allocator,
            "INSERT INTO {s}.memory_events (instance_id, local_sequence, schema_version, origin_instance_id, origin_sequence, timestamp_ms, operation, key, session_id, category, value_kind, content) " ++
                "VALUES ($1, $2, 1, $3, $4, $5, $6, $7, $8, $9, $10, $11) " ++
                "ON CONFLICT (instance_id, origin_instance_id, origin_sequence) DO NOTHING " ++
                "RETURNING local_sequence",
        );
        defer self.allocator.free(sql);

        const next_event_sequence = try self.nextEventSequenceTx(self.allocator);
        const category_str = if (input.category) |category| category.toString() else null;
        const value_kind_str = if (input.value_kind) |kind| kind.toString() else null;

        const iid_z = try self.allocator.dupeZ(u8, self.localInstanceId());
        defer self.allocator.free(iid_z);
        const seq_z = try allocPrintZCompat(self.allocator, "{d}", .{next_event_sequence});
        defer self.allocator.free(seq_z);
        const origin_z = try self.allocator.dupeZ(u8, input.origin_instance_id);
        defer self.allocator.free(origin_z);
        const origin_seq_z = try allocPrintZCompat(self.allocator, "{d}", .{input.origin_sequence});
        defer self.allocator.free(origin_seq_z);
        const timestamp_z = try allocPrintZCompat(self.allocator, "{d}", .{input.timestamp_ms});
        defer self.allocator.free(timestamp_z);
        const op_z = try self.allocator.dupeZ(u8, input.operation.toString());
        defer self.allocator.free(op_z);
        const key_z = try self.allocator.dupeZ(u8, input.key);
        defer self.allocator.free(key_z);
        const sid_z: ?[:0]u8 = if (input.session_id) |sid| try self.allocator.dupeZ(u8, sid) else null;
        defer if (sid_z) |sid| self.allocator.free(sid);
        const cat_z: ?[:0]u8 = if (category_str) |cat| try self.allocator.dupeZ(u8, cat) else null;
        defer if (cat_z) |cat| self.allocator.free(cat);
        const kind_z: ?[:0]u8 = if (value_kind_str) |kind| try self.allocator.dupeZ(u8, kind) else null;
        defer if (kind_z) |kind| self.allocator.free(kind);
        const content_z: ?[:0]u8 = if (input.content) |content| try self.allocator.dupeZ(u8, content) else null;
        defer if (content_z) |content| self.allocator.free(content);

        const params = [_]?[*:0]const u8{
            iid_z,
            seq_z,
            origin_z,
            origin_seq_z,
            timestamp_z,
            op_z,
            key_z,
            if (sid_z) |sid| sid.ptr else null,
            if (cat_z) |cat| cat.ptr else null,
            if (kind_z) |kind| kind.ptr else null,
            if (content_z) |content| content.ptr else null,
        };
        const lengths = [_]c_int{
            @intCast(self.localInstanceId().len),
            @intCast(seq_z.len - 1),
            @intCast(input.origin_instance_id.len),
            @intCast(origin_seq_z.len - 1),
            @intCast(timestamp_z.len - 1),
            @intCast(input.operation.toString().len),
            @intCast(input.key.len),
            if (input.session_id) |sid| @intCast(sid.len) else 0,
            if (category_str) |cat| @intCast(cat.len) else 0,
            if (value_kind_str) |kind| @intCast(kind.len) else 0,
            if (input.content) |content| @intCast(content.len) else 0,
        };
        const result = try self.execParams(sql, &params, &lengths);
        defer c.PQclear(result);

        if (c.PQntuples(result) == 0) return false;
        try self.setLastSequenceTx(self.allocator, next_event_sequence);
        return true;
    }

    fn tombstoneBlocksPutTx(self: *Self, input: MemoryEventInput) !bool {
        const sql = try self.schemaQuery(self.allocator,
            "SELECT timestamp_ms, origin_instance_id, origin_sequence FROM {s}.memory_tombstones " ++
                "WHERE instance_id = $1 AND key = $2 AND ((scope = 'scoped' AND session_key = $3) OR (scope = 'all' AND session_key = '*'))",
        );
        defer self.allocator.free(sql);

        const iid_z = try self.allocator.dupeZ(u8, self.localInstanceId());
        defer self.allocator.free(iid_z);
        const key_z = try self.allocator.dupeZ(u8, input.key);
        defer self.allocator.free(key_z);
        const session_key = sessionKeyFor(input.session_id);
        const session_key_z = try self.allocator.dupeZ(u8, session_key);
        defer self.allocator.free(session_key_z);

        const params = [_]?[*:0]const u8{ iid_z, key_z, session_key_z };
        const lengths = [_]c_int{ @intCast(self.localInstanceId().len), @intCast(input.key.len), @intCast(session_key.len) };
        const result = try self.execParams(sql, &params, &lengths);
        defer c.PQclear(result);

        var row: c_int = 0;
        while (row < c.PQntuples(result)) : (row += 1) {
            const ts_raw = c.PQgetvalue(result, row, 0);
            const ts_len: usize = @intCast(c.PQgetlength(result, row, 0));
            const timestamp_ms = std.fmt.parseInt(i64, ts_raw[0..ts_len], 10) catch 0;
            const origin = try dupeResultValue(self.allocator, result, row, 1);
            defer self.allocator.free(origin);
            const seq_raw = c.PQgetvalue(result, row, 2);
            const seq_len: usize = @intCast(c.PQgetlength(result, row, 2));
            const origin_sequence = std.fmt.parseInt(u64, seq_raw[0..seq_len], 10) catch 0;
            if (compareInputToMetadata(input, timestamp_ms, origin, origin_sequence) <= 0) return true;
        }

        return false;
    }

    fn putStateTx(self: *Self, input: MemoryEventInput) !void {
        if (try self.tombstoneBlocksPutTx(input)) return;

        const select_sql = try self.schemaTableQuery(self.allocator,
            "SELECT content, category, value_kind, event_timestamp_ms, event_origin_instance_id, event_origin_sequence " ++
                "FROM {s}.{s} WHERE instance_id = $1 AND key = $2 AND ((session_id IS NULL AND $3 IS NULL) OR session_id = $3) LIMIT 1",
        );
        defer self.allocator.free(select_sql);

        const iid_z = try self.allocator.dupeZ(u8, self.localInstanceId());
        defer self.allocator.free(iid_z);
        const key_z = try self.allocator.dupeZ(u8, input.key);
        defer self.allocator.free(key_z);
        const sid_z: ?[:0]u8 = if (input.session_id) |sid| try self.allocator.dupeZ(u8, sid) else null;
        defer if (sid_z) |sid| self.allocator.free(sid);

        const select_params = [_]?[*:0]const u8{ iid_z, key_z, if (sid_z) |sid| sid.ptr else null };
        const select_lengths = [_]c_int{
            @intCast(self.localInstanceId().len),
            @intCast(input.key.len),
            if (input.session_id) |sid| @intCast(sid.len) else 0,
        };
        const result = try self.execParams(select_sql, &select_params, &select_lengths);
        defer c.PQclear(result);

        var existing_content: ?[]u8 = null;
        defer if (existing_content) |value| self.allocator.free(value);
        var existing_category: ?MemoryCategory = null;
        defer if (existing_category) |category| switch (category) {
            .custom => |name| self.allocator.free(name),
            else => {},
        };
        var existing_value_kind: ?MemoryValueKind = null;

        if (c.PQntuples(result) > 0) {
            existing_content = try dupeResultValue(self.allocator, result, 0, 0);
            const category_text = try dupeResultValue(self.allocator, result, 0, 1);
            defer self.allocator.free(category_text);
            existing_category = try parseCategoryOwned(self.allocator, category_text);
            const value_kind_text = try dupeResultValueOpt(self.allocator, result, 0, 2);
            defer if (value_kind_text) |value| self.allocator.free(value);
            if (value_kind_text) |value| {
                existing_value_kind = MemoryValueKind.fromString(value) orelse return error.InvalidEvent;
            }
            const ts_raw = c.PQgetvalue(result, 0, 3);
            const ts_len: usize = @intCast(c.PQgetlength(result, 0, 3));
            const timestamp_ms = std.fmt.parseInt(i64, ts_raw[0..ts_len], 10) catch 0;
            const origin = try dupeResultValue(self.allocator, result, 0, 4);
            defer self.allocator.free(origin);
            const seq_raw = c.PQgetvalue(result, 0, 5);
            const seq_len: usize = @intCast(c.PQgetlength(result, 0, 5));
            const origin_sequence = std.fmt.parseInt(u64, seq_raw[0..seq_len], 10) catch 0;
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

        const delete_sql = try self.schemaTableQuery(self.allocator,
            "DELETE FROM {s}.{s} WHERE instance_id = $1 AND key = $2 AND ((session_id IS NULL AND $3 IS NULL) OR session_id = $3)",
        );
        defer self.allocator.free(delete_sql);
        {
            const delete_result = try self.execParams(delete_sql, &select_params, &select_lengths);
            c.PQclear(delete_result);
        }

        const insert_sql = try self.schemaTableQuery(self.allocator,
            "INSERT INTO {s}.{s} (id, key, content, category, session_id, instance_id, value_kind, event_timestamp_ms, event_origin_instance_id, event_origin_sequence, created_at, updated_at) " ++
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
        );
        defer self.allocator.free(insert_sql);

        const id = try generateId(self.allocator);
        defer self.allocator.free(id);
        const id_z = try self.allocator.dupeZ(u8, id);
        defer self.allocator.free(id_z);
        const content_z = try self.allocator.dupeZ(u8, resolved_state.content);
        defer self.allocator.free(content_z);
        const cat_str = resolved_state.category.toString();
        const cat_z = try self.allocator.dupeZ(u8, cat_str);
        defer self.allocator.free(cat_z);
        const value_kind_str = if (resolved_state.value_kind) |kind| kind.toString() else null;
        const kind_z: ?[:0]u8 = if (value_kind_str) |kind| try self.allocator.dupeZ(u8, kind) else null;
        defer if (kind_z) |kind| self.allocator.free(kind);
        const timestamp_z = try allocPrintZCompat(self.allocator, "{d}", .{input.timestamp_ms});
        defer self.allocator.free(timestamp_z);
        const origin_z = try self.allocator.dupeZ(u8, input.origin_instance_id);
        defer self.allocator.free(origin_z);
        const origin_seq_z = try allocPrintZCompat(self.allocator, "{d}", .{input.origin_sequence});
        defer self.allocator.free(origin_seq_z);
        const now = try allocPrintZCompat(self.allocator, "{d}", .{@divTrunc(input.timestamp_ms, 1000)});
        defer self.allocator.free(now);

        const insert_params = [_]?[*:0]const u8{
            id_z,
            key_z,
            content_z,
            cat_z,
            if (sid_z) |sid| sid.ptr else null,
            iid_z,
            if (kind_z) |kind| kind.ptr else null,
            timestamp_z,
            origin_z,
            origin_seq_z,
            now,
            now,
        };
        const insert_lengths = [_]c_int{
            @intCast(id.len),
            @intCast(input.key.len),
            @intCast(resolved_state.content.len),
            @intCast(cat_str.len),
            if (input.session_id) |sid| @intCast(sid.len) else 0,
            @intCast(self.localInstanceId().len),
            if (value_kind_str) |kind| @intCast(kind.len) else 0,
            @intCast(timestamp_z.len - 1),
            @intCast(input.origin_instance_id.len),
            @intCast(origin_seq_z.len - 1),
            @intCast(now.len - 1),
            @intCast(now.len - 1),
        };
        const insert_result = try self.execParams(insert_sql, &insert_params, &insert_lengths);
        c.PQclear(insert_result);
    }

    fn deleteScopedStateTx(self: *Self, input: MemoryEventInput) !void {
        const select_sql = try self.schemaTableQuery(self.allocator,
            "SELECT event_timestamp_ms, event_origin_instance_id, event_origin_sequence FROM {s}.{s} " ++
                "WHERE instance_id = $1 AND key = $2 AND ((session_id IS NULL AND $3 IS NULL) OR session_id = $3) LIMIT 1",
        );
        defer self.allocator.free(select_sql);

        const iid_z = try self.allocator.dupeZ(u8, self.localInstanceId());
        defer self.allocator.free(iid_z);
        const key_z = try self.allocator.dupeZ(u8, input.key);
        defer self.allocator.free(key_z);
        const sid_z: ?[:0]u8 = if (input.session_id) |sid| try self.allocator.dupeZ(u8, sid) else null;
        defer if (sid_z) |sid| self.allocator.free(sid);

        const params = [_]?[*:0]const u8{ iid_z, key_z, if (sid_z) |sid| sid.ptr else null };
        const lengths = [_]c_int{ @intCast(self.localInstanceId().len), @intCast(input.key.len), if (input.session_id) |sid| @intCast(sid.len) else 0 };
        const result = try self.execParams(select_sql, &params, &lengths);
        defer c.PQclear(result);
        if (c.PQntuples(result) == 0) return;

        const ts_raw = c.PQgetvalue(result, 0, 0);
        const ts_len: usize = @intCast(c.PQgetlength(result, 0, 0));
        const timestamp_ms = std.fmt.parseInt(i64, ts_raw[0..ts_len], 10) catch 0;
        const origin = try dupeResultValue(self.allocator, result, 0, 1);
        defer self.allocator.free(origin);
        const seq_raw = c.PQgetvalue(result, 0, 2);
        const seq_len: usize = @intCast(c.PQgetlength(result, 0, 2));
        const origin_sequence = std.fmt.parseInt(u64, seq_raw[0..seq_len], 10) catch 0;
        if (compareInputToMetadata(input, timestamp_ms, origin, origin_sequence) < 0) return;

        const delete_sql = try self.schemaTableQuery(self.allocator,
            "DELETE FROM {s}.{s} WHERE instance_id = $1 AND key = $2 AND ((session_id IS NULL AND $3 IS NULL) OR session_id = $3)",
        );
        defer self.allocator.free(delete_sql);
        const delete_result = try self.execParams(delete_sql, &params, &lengths);
        c.PQclear(delete_result);
    }

    fn deleteAllStateTx(self: *Self, input: MemoryEventInput) !void {
        const select_sql = try self.schemaTableQuery(self.allocator,
            "SELECT session_id, event_timestamp_ms, event_origin_instance_id, event_origin_sequence FROM {s}.{s} WHERE instance_id = $1 AND key = $2",
        );
        defer self.allocator.free(select_sql);

        const iid_z = try self.allocator.dupeZ(u8, self.localInstanceId());
        defer self.allocator.free(iid_z);
        const key_z = try self.allocator.dupeZ(u8, input.key);
        defer self.allocator.free(key_z);

        const params = [_]?[*:0]const u8{ iid_z, key_z };
        const lengths = [_]c_int{ @intCast(self.localInstanceId().len), @intCast(input.key.len) };
        const result = try self.execParams(select_sql, &params, &lengths);
        defer c.PQclear(result);

        var sessions_to_delete: std.ArrayListUnmanaged(?[]u8) = .empty;
        defer {
            for (sessions_to_delete.items) |sid_opt| if (sid_opt) |sid| self.allocator.free(sid);
            sessions_to_delete.deinit(self.allocator);
        }

        var row: c_int = 0;
        while (row < c.PQntuples(result)) : (row += 1) {
            const sid = try dupeResultValueOpt(self.allocator, result, row, 0);
            errdefer if (sid) |value| self.allocator.free(value);
            const ts_raw = c.PQgetvalue(result, row, 1);
            const ts_len: usize = @intCast(c.PQgetlength(result, row, 1));
            const timestamp_ms = std.fmt.parseInt(i64, ts_raw[0..ts_len], 10) catch 0;
            const origin = try dupeResultValue(self.allocator, result, row, 2);
            defer self.allocator.free(origin);
            const seq_raw = c.PQgetvalue(result, row, 3);
            const seq_len: usize = @intCast(c.PQgetlength(result, row, 3));
            const origin_sequence = std.fmt.parseInt(u64, seq_raw[0..seq_len], 10) catch 0;
            if (compareInputToMetadata(input, timestamp_ms, origin, origin_sequence) >= 0) {
                try sessions_to_delete.append(self.allocator, sid);
            } else if (sid) |value| {
                self.allocator.free(value);
            }
        }

        const delete_sql = try self.schemaTableQuery(self.allocator,
            "DELETE FROM {s}.{s} WHERE instance_id = $1 AND key = $2 AND ((session_id IS NULL AND $3 IS NULL) OR session_id = $3)",
        );
        defer self.allocator.free(delete_sql);

        for (sessions_to_delete.items) |sid_opt| {
            const sid_z: ?[:0]u8 = if (sid_opt) |sid| try self.allocator.dupeZ(u8, sid) else null;
            defer if (sid_z) |sid| self.allocator.free(sid);
            const delete_params = [_]?[*:0]const u8{ iid_z, key_z, if (sid_z) |sid| sid.ptr else null };
            const delete_lengths = [_]c_int{ @intCast(self.localInstanceId().len), @intCast(input.key.len), if (sid_opt) |sid| @intCast(sid.len) else 0 };
            const delete_result = try self.execParams(delete_sql, &delete_params, &delete_lengths);
            c.PQclear(delete_result);
        }
    }

    fn upsertTombstoneTx(self: *Self, input: MemoryEventInput, scope: []const u8, session_key: []const u8, session_id: ?[]const u8) !void {
        const select_sql = try self.schemaQuery(self.allocator,
            "SELECT timestamp_ms, origin_instance_id, origin_sequence FROM {s}.memory_tombstones " ++
                "WHERE instance_id = $1 AND key = $2 AND scope = $3 AND session_key = $4 LIMIT 1",
        );
        defer self.allocator.free(select_sql);

        const iid_z = try self.allocator.dupeZ(u8, self.localInstanceId());
        defer self.allocator.free(iid_z);
        const key_z = try self.allocator.dupeZ(u8, input.key);
        defer self.allocator.free(key_z);
        const scope_z = try self.allocator.dupeZ(u8, scope);
        defer self.allocator.free(scope_z);
        const session_key_z = try self.allocator.dupeZ(u8, session_key);
        defer self.allocator.free(session_key_z);

        const select_params = [_]?[*:0]const u8{ iid_z, key_z, scope_z, session_key_z };
        const select_lengths = [_]c_int{ @intCast(self.localInstanceId().len), @intCast(input.key.len), @intCast(scope.len), @intCast(session_key.len) };
        const existing = try self.execParams(select_sql, &select_params, &select_lengths);
        defer c.PQclear(existing);

        if (c.PQntuples(existing) > 0) {
            const ts_raw = c.PQgetvalue(existing, 0, 0);
            const ts_len: usize = @intCast(c.PQgetlength(existing, 0, 0));
            const timestamp_ms = std.fmt.parseInt(i64, ts_raw[0..ts_len], 10) catch 0;
            const origin = try dupeResultValue(self.allocator, existing, 0, 1);
            defer self.allocator.free(origin);
            const seq_raw = c.PQgetvalue(existing, 0, 2);
            const seq_len: usize = @intCast(c.PQgetlength(existing, 0, 2));
            const origin_sequence = std.fmt.parseInt(u64, seq_raw[0..seq_len], 10) catch 0;
            if (compareInputToMetadata(input, timestamp_ms, origin, origin_sequence) <= 0) return;
        }

        const upsert_sql = try self.schemaQuery(self.allocator,
            "INSERT INTO {s}.memory_tombstones (instance_id, key, scope, session_key, session_id, timestamp_ms, origin_instance_id, origin_sequence) " ++
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8) " ++
                "ON CONFLICT (instance_id, key, scope, session_key) DO UPDATE SET " ++
                "session_id = EXCLUDED.session_id, timestamp_ms = EXCLUDED.timestamp_ms, origin_instance_id = EXCLUDED.origin_instance_id, origin_sequence = EXCLUDED.origin_sequence",
        );
        defer self.allocator.free(upsert_sql);

        const sid_z: ?[:0]u8 = if (session_id) |sid| try self.allocator.dupeZ(u8, sid) else null;
        defer if (sid_z) |sid| self.allocator.free(sid);
        const timestamp_z = try allocPrintZCompat(self.allocator, "{d}", .{input.timestamp_ms});
        defer self.allocator.free(timestamp_z);
        const origin_z = try self.allocator.dupeZ(u8, input.origin_instance_id);
        defer self.allocator.free(origin_z);
        const origin_seq_z = try allocPrintZCompat(self.allocator, "{d}", .{input.origin_sequence});
        defer self.allocator.free(origin_seq_z);

        const upsert_params = [_]?[*:0]const u8{
            iid_z,
            key_z,
            scope_z,
            session_key_z,
            if (sid_z) |sid| sid.ptr else null,
            timestamp_z,
            origin_z,
            origin_seq_z,
        };
        const upsert_lengths = [_]c_int{
            @intCast(self.localInstanceId().len),
            @intCast(input.key.len),
            @intCast(scope.len),
            @intCast(session_key.len),
            if (session_id) |sid| @intCast(sid.len) else 0,
            @intCast(timestamp_z.len - 1),
            @intCast(input.origin_instance_id.len),
            @intCast(origin_seq_z.len - 1),
        };
        const upsert_result = try self.execParams(upsert_sql, &upsert_params, &upsert_lengths);
        c.PQclear(upsert_result);
    }

    fn applyEventTx(self: *Self, input: MemoryEventInput) !void {
        const frontier = try self.getFrontierTx(self.allocator, input.origin_instance_id);
        if (input.origin_sequence <= frontier) return;

        const inserted = try self.insertEventTx(input);
        if (!inserted) {
            try self.setFrontierTx(self.allocator, input.origin_instance_id, input.origin_sequence);
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

        try self.setFrontierTx(self.allocator, input.origin_instance_id, input.origin_sequence);
    }

    fn applyEventInternal(self: *Self, input: MemoryEventInput) !void {
        try self.begin();
        var committed = false;
        errdefer if (!committed) self.rollback();
        try self.applyEventTx(input);
        try self.commit();
        committed = true;
    }

    fn emitLocalEvent(self: *Self, operation: MemoryEventOp, key: []const u8, session_id: ?[]const u8, category: ?MemoryCategory, value_kind: ?MemoryValueKind, content: ?[]const u8) !void {
        const now_ms = std.time.milliTimestamp();
        try self.begin();
        var committed = false;
        errdefer if (!committed) self.rollback();
        try self.applyEventTx(.{
            .origin_instance_id = self.localInstanceId(),
            .origin_sequence = try self.nextLocalOriginSequenceTx(self.allocator),
            .timestamp_ms = now_ms,
            .operation = operation,
            .key = key,
            .session_id = session_id,
            .category = category,
            .value_kind = value_kind,
            .content = content,
        });
        try self.commit();
        committed = true;
    }

    // ── Memory vtable implementation ──────────────────────────────

    fn implName(_: *anyopaque) []const u8 {
        return "postgres";
    }

    fn implStore(ptr: *anyopaque, key: []const u8, content: []const u8, category: MemoryCategory, session_id: ?[]const u8) anyerror!void {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        try self_.emitLocalEvent(.put, key, session_id, category, null, content);
    }

    fn implRecall(ptr: *anyopaque, allocator: std.mem.Allocator, query: []const u8, limit: usize, session_id: ?[]const u8) anyerror![]MemoryEntry {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const local_instance_id = self_.localInstanceId();

        const trimmed = std.mem.trim(u8, query, " \t\n\r");
        if (trimmed.len == 0) return allocator.alloc(MemoryEntry, 0);

        // Build ILIKE pattern: %query%
        const pattern = try allocPrintZCompat(allocator, "%{s}%", .{trimmed});
        defer allocator.free(pattern);

        var limit_buf: [20]u8 = undefined;
        const limit_str = try std.fmt.bufPrintZ(&limit_buf, "{d}", .{limit});

        const iid_z = try allocator.dupeZ(u8, local_instance_id);
        defer allocator.free(iid_z);

        var result: *c.PGresult = undefined;
        if (session_id) |sid| {
            const sid_z = try allocator.dupeZ(u8, sid);
            defer allocator.free(sid_z);
            const params = [_]?[*:0]const u8{ pattern.ptr, limit_str.ptr, sid_z, iid_z };
            const lengths = [_]c_int{ @intCast(pattern.len - 1), @intCast(limit_str.len), @intCast(sid.len), @intCast(local_instance_id.len) };
            result = try self_.execParams(self_.q_recall_sid, &params, &lengths);
        } else {
            const params = [_]?[*:0]const u8{ pattern.ptr, limit_str.ptr, iid_z };
            const lengths = [_]c_int{ @intCast(pattern.len - 1), @intCast(limit_str.len), @intCast(local_instance_id.len) };
            result = try self_.execParams(self_.q_recall, &params, &lengths);
        }
        defer c.PQclear(result);

        const nrows = c.PQntuples(result);
        var entries: std.ArrayList(MemoryEntry) = .empty;
        errdefer {
            for (entries.items) |*entry| entry.deinit(allocator);
            entries.deinit(allocator);
        }

        var row: c_int = 0;
        while (row < nrows) : (row += 1) {
            var entry = try readEntryFromResult(allocator, result, row);
            // Read score from column 6
            if (c.PQgetisnull(result, row, 6) == 0) {
                const score_str = c.PQgetvalue(result, row, 6);
                const score_slice: []const u8 = score_str[0..@intCast(c.PQgetlength(result, row, 6))];
                entry.score = std.fmt.parseFloat(f64, score_slice) catch null;
            }
            try entries.append(allocator, entry);
        }

        return entries.toOwnedSlice(allocator);
    }

    fn implGet(ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8) anyerror!?MemoryEntry {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const local_instance_id = self_.localInstanceId();

        const key_z = try allocator.dupeZ(u8, key);
        defer allocator.free(key_z);
        const iid_z = try allocator.dupeZ(u8, local_instance_id);
        defer allocator.free(iid_z);

        const params = [_]?[*:0]const u8{ key_z, iid_z };
        const lengths = [_]c_int{ @intCast(key.len), @intCast(local_instance_id.len) };

        const result = try self_.execParams(self_.q_get, &params, &lengths);
        defer c.PQclear(result);

        if (c.PQntuples(result) == 0) return null;
        return try readEntryFromResult(allocator, result, 0);
    }

    fn implGetScoped(ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8, session_id: ?[]const u8) anyerror!?MemoryEntry {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const local_instance_id = self_.localInstanceId();

        const key_z = try allocator.dupeZ(u8, key);
        defer allocator.free(key_z);
        const sid_z: ?[:0]u8 = if (session_id) |sid| try allocator.dupeZ(u8, sid) else null;
        defer if (sid_z) |sid| allocator.free(sid);
        const iid_z = try allocator.dupeZ(u8, local_instance_id);
        defer allocator.free(iid_z);

        const params = [_]?[*:0]const u8{ key_z, if (sid_z) |sid| sid.ptr else null, iid_z };
        const lengths = [_]c_int{
            @intCast(key.len),
            if (session_id) |sid| @intCast(sid.len) else 0,
            @intCast(local_instance_id.len),
        };

        const result = try self_.execParams(self_.q_get_scoped, &params, &lengths);
        defer c.PQclear(result);

        if (c.PQntuples(result) == 0) return null;
        return try readEntryFromResult(allocator, result, 0);
    }

    fn implList(ptr: *anyopaque, allocator: std.mem.Allocator, category: ?MemoryCategory, session_id: ?[]const u8) anyerror![]MemoryEntry {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const local_instance_id = self_.localInstanceId();

        const iid_z = try allocator.dupeZ(u8, local_instance_id);
        defer allocator.free(iid_z);

        var result: *c.PGresult = undefined;
        if (category) |cat| {
            const cat_str = cat.toString();
            const cat_z = try allocator.dupeZ(u8, cat_str);
            defer allocator.free(cat_z);
            if (session_id) |sid| {
                const sid_z = try allocator.dupeZ(u8, sid);
                defer allocator.free(sid_z);
                const params = [_]?[*:0]const u8{ cat_z, sid_z, iid_z };
                const lengths = [_]c_int{ @intCast(cat_str.len), @intCast(sid.len), @intCast(local_instance_id.len) };
                result = try self_.execParams(self_.q_list_cat_sid, &params, &lengths);
            } else {
                const params = [_]?[*:0]const u8{ cat_z, iid_z };
                const lengths = [_]c_int{ @intCast(cat_str.len), @intCast(local_instance_id.len) };
                result = try self_.execParams(self_.q_list_cat, &params, &lengths);
            }
        } else if (session_id) |sid| {
            const sid_z = try allocator.dupeZ(u8, sid);
            defer allocator.free(sid_z);
            const params = [_]?[*:0]const u8{ sid_z, iid_z };
            const lengths = [_]c_int{ @intCast(sid.len), @intCast(local_instance_id.len) };
            result = try self_.execParams(self_.q_list_sid, &params, &lengths);
        } else {
            const params = [_]?[*:0]const u8{iid_z};
            const lengths = [_]c_int{@intCast(local_instance_id.len)};
            result = try self_.execParams(self_.q_list_all, &params, &lengths);
        }
        defer c.PQclear(result);

        const nrows = c.PQntuples(result);
        var entries: std.ArrayList(MemoryEntry) = .empty;
        errdefer {
            for (entries.items) |*entry| entry.deinit(allocator);
            entries.deinit(allocator);
        }

        var row: c_int = 0;
        while (row < nrows) : (row += 1) {
            const entry = try readEntryFromResult(allocator, result, row);
            try entries.append(allocator, entry);
        }

        return entries.toOwnedSlice(allocator);
    }

    fn implForget(ptr: *anyopaque, key: []const u8) anyerror!bool {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const existing = try implGet(ptr, self_.allocator, key);
        if (existing == null) return false;
        existing.?.deinit(self_.allocator);
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
        const compacted_through = try self_.getCompactedThroughSequence(allocator);
        if (after_sequence < compacted_through) return error.CursorExpired;

        const sql = try self_.schemaQuery(allocator,
            "SELECT local_sequence, schema_version, origin_instance_id, origin_sequence, timestamp_ms, operation, key, session_id, category, value_kind, content " ++
                "FROM {s}.memory_events WHERE instance_id = $1 AND local_sequence > $2 ORDER BY local_sequence ASC LIMIT $3",
        );
        defer allocator.free(sql);

        const iid_z = try allocator.dupeZ(u8, self_.localInstanceId());
        defer allocator.free(iid_z);
        const after_z = try allocPrintZCompat(allocator, "{d}", .{after_sequence});
        defer allocator.free(after_z);
        const limit_z = try allocPrintZCompat(allocator, "{d}", .{limit});
        defer allocator.free(limit_z);

        const params = [_]?[*:0]const u8{ iid_z, after_z, limit_z };
        const lengths = [_]c_int{ @intCast(self_.localInstanceId().len), @intCast(after_z.len - 1), @intCast(limit_z.len - 1) };
        const result = try self_.execParams(sql, &params, &lengths);
        defer c.PQclear(result);

        var events: std.ArrayListUnmanaged(MemoryEvent) = .empty;
        errdefer {
            for (events.items) |*event| event.deinit(allocator);
            events.deinit(allocator);
        }

        var row: c_int = 0;
        while (row < c.PQntuples(result)) : (row += 1) {
            try events.append(allocator, try readEventFromResult(allocator, result, row));
        }
        return events.toOwnedSlice(allocator);
    }

    fn implApplyEvent(ptr: *anyopaque, input: MemoryEventInput) anyerror!void {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        try self_.applyEventInternal(input);
    }

    fn implLastEventSequence(ptr: *anyopaque) anyerror!u64 {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        return try self_.getLastSequenceTx(self_.allocator);
    }

    fn implEventFeedInfo(ptr: *anyopaque, allocator: std.mem.Allocator) anyerror!MemoryEventFeedInfo {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const compacted_through = try self_.getCompactedThroughSequence(allocator);
        return .{
            .instance_id = try allocator.dupe(u8, self_.localInstanceId()),
            .last_sequence = try self_.getLastSequenceTx(allocator),
            .next_local_origin_sequence = try self_.nextLocalOriginSequenceTx(allocator),
            .supports_compaction = true,
            .storage_kind = .native,
            .compacted_through_sequence = compacted_through,
            .oldest_available_sequence = compacted_through + 1,
        };
    }

    fn implCompactEvents(ptr: *anyopaque) anyerror!u64 {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const compacted_through = try self_.getLastSequenceTx(self_.allocator);
        try self_.begin();
        var committed = false;
        errdefer if (!committed) self_.rollback();

        const sql = try self_.schemaQuery(self_.allocator,
            "DELETE FROM {s}.memory_events WHERE instance_id = $1 AND local_sequence <= $2",
        );
        defer self_.allocator.free(sql);
        const iid_z = try self_.allocator.dupeZ(u8, self_.localInstanceId());
        defer self_.allocator.free(iid_z);
        const seq_z = try allocPrintZCompat(self_.allocator, "{d}", .{compacted_through});
        defer self_.allocator.free(seq_z);
        const params = [_]?[*:0]const u8{ iid_z, seq_z };
        const lengths = [_]c_int{ @intCast(self_.localInstanceId().len), @intCast(seq_z.len - 1) };
        const result = try self_.execParams(sql, &params, &lengths);
        c.PQclear(result);

        try self_.setCompactedThroughSequenceTx(self_.allocator, compacted_through);
        try self_.commit();
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

    fn appendCheckpointMetaLine(
        allocator: std.mem.Allocator,
        out: *std.ArrayListUnmanaged(u8),
        last_sequence: u64,
        last_timestamp_ms: i64,
        compacted_through: u64,
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
        try out.writer(allocator).print("{d}", .{compacted_through});
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

    fn appendCheckpointStateLine(
        allocator: std.mem.Allocator,
        out: *std.ArrayListUnmanaged(u8),
        key: []const u8,
        session_id: ?[]const u8,
        category: []const u8,
        value_kind: ?[]const u8,
        content: []const u8,
        timestamp_ms: i64,
        origin_instance_id: []const u8,
        origin_sequence: u64,
    ) !void {
        try out.append(allocator, '{');
        try json_util.appendJsonKeyValue(out, allocator, "kind", "state");
        try out.append(allocator, ',');
        try json_util.appendJsonKeyValue(out, allocator, "key", key);
        try out.append(allocator, ',');
        try json_util.appendJsonKey(out, allocator, "session_id");
        if (session_id) |sid| {
            try json_util.appendJsonString(out, allocator, sid);
        } else {
            try out.appendSlice(allocator, "null");
        }
        try out.append(allocator, ',');
        try json_util.appendJsonKeyValue(out, allocator, "category", category);
        try out.append(allocator, ',');
        try json_util.appendJsonKey(out, allocator, "value_kind");
        if (value_kind) |kind| {
            try json_util.appendJsonString(out, allocator, kind);
        } else {
            try out.appendSlice(allocator, "null");
        }
        try out.append(allocator, ',');
        try json_util.appendJsonKeyValue(out, allocator, "content", content);
        try out.append(allocator, ',');
        try json_util.appendJsonKey(out, allocator, "timestamp_ms");
        try out.writer(allocator).print("{d}", .{timestamp_ms});
        try out.append(allocator, ',');
        try json_util.appendJsonKeyValue(out, allocator, "origin_instance_id", origin_instance_id);
        try out.append(allocator, ',');
        try json_util.appendJsonKey(out, allocator, "origin_sequence");
        try out.writer(allocator).print("{d}", .{origin_sequence});
        try out.appendSlice(allocator, "}\n");
    }

    fn appendCheckpointTombstoneLine(
        allocator: std.mem.Allocator,
        out: *std.ArrayListUnmanaged(u8),
        kind: []const u8,
        key: []const u8,
        timestamp_ms: i64,
        origin_instance_id: []const u8,
        origin_sequence: u64,
    ) !void {
        try out.append(allocator, '{');
        try json_util.appendJsonKeyValue(out, allocator, "kind", kind);
        try out.append(allocator, ',');
        try json_util.appendJsonKeyValue(out, allocator, "key", key);
        try out.append(allocator, ',');
        try json_util.appendJsonKey(out, allocator, "timestamp_ms");
        try out.writer(allocator).print("{d}", .{timestamp_ms});
        try out.append(allocator, ',');
        try json_util.appendJsonKeyValue(out, allocator, "origin_instance_id", origin_instance_id);
        try out.append(allocator, ',');
        try json_util.appendJsonKey(out, allocator, "origin_sequence");
        try out.writer(allocator).print("{d}", .{origin_sequence});
        try out.appendSlice(allocator, "}\n");
    }

    fn querySingleI64(self: *Self, _: std.mem.Allocator, query: []const u8, params: []const ?[*:0]const u8, lengths: []const c_int) !i64 {
        const result = try self.execParams(query, params, lengths);
        defer c.PQclear(result);
        if (c.PQntuples(result) == 0 or c.PQgetisnull(result, 0, 0) != 0) return 0;
        const value = c.PQgetvalue(result, 0, 0);
        const len: usize = @intCast(c.PQgetlength(result, 0, 0));
        return std.fmt.parseInt(i64, value[0..len], 10) catch 0;
    }

    fn exportCheckpointPayload(self: *Self, allocator: std.mem.Allocator) ![]u8 {
        var out: std.ArrayListUnmanaged(u8) = .empty;
        errdefer out.deinit(allocator);

        const local_instance_id = self.localInstanceId();
        const iid_z = try allocator.dupeZ(u8, local_instance_id);
        defer allocator.free(iid_z);
        const params = [_]?[*:0]const u8{iid_z};
        const lengths = [_]c_int{@intCast(local_instance_id.len)};

        const last_sequence = try self.getLastSequenceTx(allocator);

        const state_ts_sql = try buildQuery(
            allocator,
            "SELECT COALESCE(MAX(event_timestamp_ms), 0) FROM {schema}.{table} WHERE instance_id = $1",
            self.schema_q,
            self.table_q,
        );
        defer allocator.free(state_ts_sql);
        const state_max_ts = try self.querySingleI64(allocator, state_ts_sql, &params, &lengths);

        const tomb_ts_sql = try buildQuery(
            allocator,
            "SELECT COALESCE(MAX(timestamp_ms), 0) FROM {schema}.memory_tombstones WHERE instance_id = $1",
            self.schema_q,
            self.table_q,
        );
        defer allocator.free(tomb_ts_sql);
        const tombstone_max_ts = try self.querySingleI64(allocator, tomb_ts_sql, &params, &lengths);

        try appendCheckpointMetaLine(allocator, &out, last_sequence, @max(state_max_ts, tombstone_max_ts), last_sequence);

        const frontiers_sql = try self.schemaQuery(allocator,
            "SELECT origin_instance_id, last_origin_sequence FROM {s}.memory_event_frontiers WHERE instance_id = $1 ORDER BY origin_instance_id ASC",
        );
        defer allocator.free(frontiers_sql);
        {
            const result = try self.execParams(frontiers_sql, &params, &lengths);
            defer c.PQclear(result);
            var row: c_int = 0;
            while (row < c.PQntuples(result)) : (row += 1) {
                const origin_instance_id = try dupeResultValue(allocator, result, row, 0);
                defer allocator.free(origin_instance_id);
                const seq_raw = c.PQgetvalue(result, row, 1);
                const seq_len: usize = @intCast(c.PQgetlength(result, row, 1));
                const origin_sequence = std.fmt.parseInt(u64, seq_raw[0..seq_len], 10) catch 0;
                try appendCheckpointFrontierLine(allocator, &out, origin_instance_id, origin_sequence);
            }
        }

        const state_sql = try buildQuery(
            allocator,
            "SELECT key, session_id, category, value_kind, content, event_timestamp_ms, event_origin_instance_id, event_origin_sequence " ++
                "FROM {schema}.{table} WHERE instance_id = $1 ORDER BY key ASC, COALESCE(session_id, '') ASC",
            self.schema_q,
            self.table_q,
        );
        defer allocator.free(state_sql);
        {
            const result = try self.execParams(state_sql, &params, &lengths);
            defer c.PQclear(result);
            var row: c_int = 0;
            while (row < c.PQntuples(result)) : (row += 1) {
                const key = try dupeResultValue(allocator, result, row, 0);
                defer allocator.free(key);
                const session_id = try dupeResultValueOpt(allocator, result, row, 1);
                defer if (session_id) |sid| allocator.free(sid);
                const category = try dupeResultValue(allocator, result, row, 2);
                defer allocator.free(category);
                const value_kind = try dupeResultValueOpt(allocator, result, row, 3);
                defer if (value_kind) |kind| allocator.free(kind);
                const content = try dupeResultValue(allocator, result, row, 4);
                defer allocator.free(content);
                const ts_raw = c.PQgetvalue(result, row, 5);
                const ts_len: usize = @intCast(c.PQgetlength(result, row, 5));
                const timestamp_ms = std.fmt.parseInt(i64, ts_raw[0..ts_len], 10) catch 0;
                const origin_instance_id = try dupeResultValue(allocator, result, row, 6);
                defer allocator.free(origin_instance_id);
                const seq_raw = c.PQgetvalue(result, row, 7);
                const seq_len: usize = @intCast(c.PQgetlength(result, row, 7));
                const origin_sequence = std.fmt.parseInt(u64, seq_raw[0..seq_len], 10) catch 0;
                try appendCheckpointStateLine(allocator, &out, key, session_id, category, value_kind, content, timestamp_ms, origin_instance_id, origin_sequence);
            }
        }

        const tombstones_sql = try self.schemaQuery(allocator,
            "SELECT key, scope, session_id, timestamp_ms, origin_instance_id, origin_sequence FROM {s}.memory_tombstones " ++
                "WHERE instance_id = $1 ORDER BY key ASC, scope ASC, session_key ASC",
        );
        defer allocator.free(tombstones_sql);
        {
            const result = try self.execParams(tombstones_sql, &params, &lengths);
            defer c.PQclear(result);
            var row: c_int = 0;
            while (row < c.PQntuples(result)) : (row += 1) {
                const logical_key = try dupeResultValue(allocator, result, row, 0);
                defer allocator.free(logical_key);
                const scope = try dupeResultValue(allocator, result, row, 1);
                defer allocator.free(scope);
                const session_id = try dupeResultValueOpt(allocator, result, row, 2);
                defer if (session_id) |sid| allocator.free(sid);
                const ts_raw = c.PQgetvalue(result, row, 3);
                const ts_len: usize = @intCast(c.PQgetlength(result, row, 3));
                const timestamp_ms = std.fmt.parseInt(i64, ts_raw[0..ts_len], 10) catch 0;
                const origin_instance_id = try dupeResultValue(allocator, result, row, 4);
                defer allocator.free(origin_instance_id);
                const seq_raw = c.PQgetvalue(result, row, 5);
                const seq_len: usize = @intCast(c.PQgetlength(result, row, 5));
                const origin_sequence = std.fmt.parseInt(u64, seq_raw[0..seq_len], 10) catch 0;

                const encoded_key = if (std.mem.eql(u8, scope, "all"))
                    try allocator.dupe(u8, logical_key)
                else
                    try key_codec.encode(allocator, logical_key, session_id);
                defer allocator.free(encoded_key);

                try appendCheckpointTombstoneLine(
                    allocator,
                    &out,
                    if (std.mem.eql(u8, scope, "all")) "key_tombstone" else "scoped_tombstone",
                    encoded_key,
                    timestamp_ms,
                    origin_instance_id,
                    origin_sequence,
                );
            }
        }

        return out.toOwnedSlice(allocator);
    }

    fn applyCheckpointPayload(self: *Self, payload: []const u8) !void {
        const local_instance_id = self.localInstanceId();
        const iid_z = try self.allocator.dupeZ(u8, local_instance_id);
        defer self.allocator.free(iid_z);
        const instance_params = [_]?[*:0]const u8{iid_z};
        const instance_lengths = [_]c_int{@intCast(local_instance_id.len)};

        try self.begin();
        var committed = false;
        errdefer if (!committed) self.rollback();

        const clear_queries = [_][]const u8{
            "DELETE FROM {schema}.memory_events WHERE instance_id = $1",
            "DELETE FROM {schema}.memory_tombstones WHERE instance_id = $1",
            "DELETE FROM {schema}.memory_event_frontiers WHERE instance_id = $1",
            "DELETE FROM {schema}.{table} WHERE instance_id = $1",
            "DELETE FROM {schema}.memory_feed_meta WHERE instance_id = $1",
        };
        inline for (clear_queries) |template| {
            const sql = try buildQuery(self.allocator, template, self.schema_q, self.table_q);
            defer self.allocator.free(sql);
            const result = try self.execParams(sql, &instance_params, &instance_lengths);
            c.PQclear(result);
        }

        var last_sequence: u64 = 0;
        var compacted_through: u64 = 0;
        var saw_meta = false;

        var lines = std.mem.splitScalar(u8, payload, '\n');
        while (lines.next()) |raw_line| {
            const line = std.mem.trim(u8, raw_line, " \t\r\n");
            if (line.len == 0) continue;

            var parsed = try std.json.parseFromSlice(std.json.Value, self.allocator, line, .{});
            defer parsed.deinit();

            const kind = checkpointJsonStringField(parsed.value, "kind") orelse return error.InvalidEvent;
            if (std.mem.eql(u8, kind, "meta")) {
                saw_meta = true;
                last_sequence = checkpointJsonUnsignedField(parsed.value, "last_sequence") orelse 0;
                compacted_through = checkpointJsonUnsignedField(parsed.value, "compacted_through_sequence") orelse last_sequence;
                continue;
            }
            if (std.mem.eql(u8, kind, "frontier")) {
                const origin_instance_id = checkpointJsonStringField(parsed.value, "origin_instance_id") orelse return error.InvalidEvent;
                const origin_sequence = checkpointJsonUnsignedField(parsed.value, "origin_sequence") orelse return error.InvalidEvent;
                try self.insertCheckpointFrontier(origin_instance_id, origin_sequence);
                continue;
            }
            if (std.mem.eql(u8, kind, "state")) {
                try self.insertCheckpointState(
                    checkpointJsonStringField(parsed.value, "key") orelse return error.InvalidEvent,
                    checkpointJsonNullableStringField(parsed.value, "session_id"),
                    checkpointJsonStringField(parsed.value, "category") orelse return error.InvalidEvent,
                    checkpointJsonNullableStringField(parsed.value, "value_kind"),
                    checkpointJsonStringField(parsed.value, "content") orelse return error.InvalidEvent,
                    checkpointJsonIntegerField(parsed.value, "timestamp_ms") orelse return error.InvalidEvent,
                    checkpointJsonStringField(parsed.value, "origin_instance_id") orelse return error.InvalidEvent,
                    checkpointJsonUnsignedField(parsed.value, "origin_sequence") orelse return error.InvalidEvent,
                );
                continue;
            }
            if (std.mem.eql(u8, kind, "scoped_tombstone") or std.mem.eql(u8, kind, "key_tombstone")) {
                try self.insertCheckpointTombstone(
                    kind,
                    checkpointJsonStringField(parsed.value, "key") orelse return error.InvalidEvent,
                    checkpointJsonIntegerField(parsed.value, "timestamp_ms") orelse return error.InvalidEvent,
                    checkpointJsonStringField(parsed.value, "origin_instance_id") orelse return error.InvalidEvent,
                    checkpointJsonUnsignedField(parsed.value, "origin_sequence") orelse return error.InvalidEvent,
                );
                continue;
            }
            return error.InvalidEvent;
        }

        if (!saw_meta) return error.InvalidEvent;
        try self.setLastSequenceTx(self.allocator, last_sequence);
        try self.setCompactedThroughSequenceTx(self.allocator, compacted_through);
        try self.commit();
        committed = true;
    }

    fn insertCheckpointFrontier(self: *Self, origin_instance_id: []const u8, origin_sequence: u64) !void {
        try self.setFrontierTx(self.allocator, origin_instance_id, origin_sequence);
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
        const sql = try buildQuery(
            self.allocator,
            "INSERT INTO {schema}.{table} (id, key, content, category, session_id, instance_id, value_kind, event_timestamp_ms, event_origin_instance_id, event_origin_sequence, created_at, updated_at) " ++
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
            self.schema_q,
            self.table_q,
        );
        defer self.allocator.free(sql);

        const id = try generateId(self.allocator);
        defer self.allocator.free(id);
        const id_z = try self.allocator.dupeZ(u8, id);
        defer self.allocator.free(id_z);
        const key_z = try self.allocator.dupeZ(u8, key);
        defer self.allocator.free(key_z);
        const content_z = try self.allocator.dupeZ(u8, content);
        defer self.allocator.free(content_z);
        const category_z = try self.allocator.dupeZ(u8, category);
        defer self.allocator.free(category_z);
        const sid_z: ?[:0]u8 = if (session_id) |sid| try self.allocator.dupeZ(u8, sid) else null;
        defer if (sid_z) |sid| self.allocator.free(sid);
        const iid_z = try self.allocator.dupeZ(u8, self.localInstanceId());
        defer self.allocator.free(iid_z);
        const kind_z: ?[:0]u8 = if (value_kind) |kind| try self.allocator.dupeZ(u8, kind) else null;
        defer if (kind_z) |kind| self.allocator.free(kind);
        const timestamp_z = try allocPrintZCompat(self.allocator, "{d}", .{timestamp_ms});
        defer self.allocator.free(timestamp_z);
        const origin_z = try self.allocator.dupeZ(u8, origin_instance_id);
        defer self.allocator.free(origin_z);
        const origin_seq_z = try allocPrintZCompat(self.allocator, "{d}", .{origin_sequence});
        defer self.allocator.free(origin_seq_z);
        const now = try allocPrintZCompat(self.allocator, "{d}", .{@divTrunc(timestamp_ms, 1000)});
        defer self.allocator.free(now);

        const params = [_]?[*:0]const u8{
            id_z,
            key_z,
            content_z,
            category_z,
            if (sid_z) |sid| sid.ptr else null,
            iid_z,
            if (kind_z) |kind| kind.ptr else null,
            timestamp_z,
            origin_z,
            origin_seq_z,
            now,
            now,
        };
        const lengths = [_]c_int{
            @intCast(id.len),
            @intCast(key.len),
            @intCast(content.len),
            @intCast(category.len),
            if (session_id) |sid| @intCast(sid.len) else 0,
            @intCast(self.localInstanceId().len),
            if (value_kind) |kind| @intCast(kind.len) else 0,
            @intCast(timestamp_z.len - 1),
            @intCast(origin_instance_id.len),
            @intCast(origin_seq_z.len - 1),
            @intCast(now.len - 1),
            @intCast(now.len - 1),
        };
        const result = try self.execParams(sql, &params, &lengths);
        c.PQclear(result);
    }

    fn insertCheckpointTombstone(
        self: *Self,
        kind: []const u8,
        encoded_key: []const u8,
        timestamp_ms: i64,
        origin_instance_id: []const u8,
        origin_sequence: u64,
    ) !void {
        const scope = if (std.mem.eql(u8, kind, "key_tombstone")) "all" else "scoped";
        const logical_key: []const u8, const session_id: ?[]const u8, const session_key: []const u8 = blk: {
            if (std.mem.eql(u8, scope, "all")) break :blk .{ encoded_key, null, "*" };
            const decoded = key_codec.decode(encoded_key);
            if (decoded.is_legacy) return error.InvalidEvent;
            break :blk .{ decoded.logical_key, decoded.session_id, sessionKeyFor(decoded.session_id) };
        };

        const sql = try self.schemaQuery(self.allocator,
            "INSERT INTO {s}.memory_tombstones (instance_id, key, scope, session_key, session_id, timestamp_ms, origin_instance_id, origin_sequence) " ++
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        );
        defer self.allocator.free(sql);

        const iid_z = try self.allocator.dupeZ(u8, self.localInstanceId());
        defer self.allocator.free(iid_z);
        const key_z = try self.allocator.dupeZ(u8, logical_key);
        defer self.allocator.free(key_z);
        const scope_z = try self.allocator.dupeZ(u8, scope);
        defer self.allocator.free(scope_z);
        const session_key_z = try self.allocator.dupeZ(u8, session_key);
        defer self.allocator.free(session_key_z);
        const sid_z: ?[:0]u8 = if (session_id) |sid| try self.allocator.dupeZ(u8, sid) else null;
        defer if (sid_z) |sid| self.allocator.free(sid);
        const timestamp_z = try allocPrintZCompat(self.allocator, "{d}", .{timestamp_ms});
        defer self.allocator.free(timestamp_z);
        const origin_z = try self.allocator.dupeZ(u8, origin_instance_id);
        defer self.allocator.free(origin_z);
        const origin_seq_z = try allocPrintZCompat(self.allocator, "{d}", .{origin_sequence});
        defer self.allocator.free(origin_seq_z);

        const params = [_]?[*:0]const u8{
            iid_z,
            key_z,
            scope_z,
            session_key_z,
            if (sid_z) |sid| sid.ptr else null,
            timestamp_z,
            origin_z,
            origin_seq_z,
        };
        const lengths = [_]c_int{
            @intCast(self.localInstanceId().len),
            @intCast(logical_key.len),
            @intCast(scope.len),
            @intCast(session_key.len),
            if (session_id) |sid| @intCast(sid.len) else 0,
            @intCast(timestamp_z.len - 1),
            @intCast(origin_instance_id.len),
            @intCast(origin_seq_z.len - 1),
        };
        const result = try self.execParams(sql, &params, &lengths);
        c.PQclear(result);
    }

    fn checkpointJsonStringField(val: std.json.Value, key: []const u8) ?[]const u8 {
        if (val != .object) return null;
        const field = val.object.get(key) orelse return null;
        return switch (field) {
            .string => |text| text,
            else => null,
        };
    }

    fn checkpointJsonNullableStringField(val: std.json.Value, key: []const u8) ?[]const u8 {
        if (val != .object) return null;
        const field = val.object.get(key) orelse return null;
        return switch (field) {
            .null => null,
            .string => |text| text,
            else => null,
        };
    }

    fn checkpointJsonIntegerField(val: std.json.Value, key: []const u8) ?i64 {
        if (val != .object) return null;
        const field = val.object.get(key) orelse return null;
        return switch (field) {
            .integer => |num| num,
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
        const local_instance_id = self_.localInstanceId();

        const iid_z = try self_.allocator.dupeZ(u8, local_instance_id);
        defer self_.allocator.free(iid_z);
        const params = [_]?[*:0]const u8{iid_z};
        const lengths = [_]c_int{@intCast(local_instance_id.len)};

        const result = try self_.execParams(self_.q_count, &params, &lengths);
        defer c.PQclear(result);

        if (c.PQntuples(result) == 0) return 0;
        const val = c.PQgetvalue(result, 0, 0);
        const len: usize = @intCast(c.PQgetlength(result, 0, 0));
        const count_str: []const u8 = val[0..len];
        return std.fmt.parseInt(usize, count_str, 10) catch 0;
    }

    fn implHealthCheck(ptr: *anyopaque) bool {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const result = c.PQexec(self_.conn, "SELECT 1");
        if (result) |r| {
            defer c.PQclear(r);
            return c.PQresultStatus(r) == c.PGRES_TUPLES_OK;
        }
        return false;
    }

    fn implDeinit(ptr: *anyopaque) void {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        self_.deinit();
    }

    pub const mem_vtable = Memory.VTable{
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
        return .{ .ptr = @ptrCast(self), .vtable = &mem_vtable };
    }

    // ── SessionStore vtable implementation ────────────────────────

    fn implSessionSaveMessage(ptr: *anyopaque, session_id: []const u8, role: []const u8, content: []const u8) anyerror!void {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const local_instance_id = self_.localInstanceId();

        const sid_z = try self_.allocator.dupeZ(u8, session_id);
        defer self_.allocator.free(sid_z);
        const iid_z = try self_.allocator.dupeZ(u8, local_instance_id);
        defer self_.allocator.free(iid_z);
        const role_z = try self_.allocator.dupeZ(u8, role);
        defer self_.allocator.free(role_z);
        const content_z = try self_.allocator.dupeZ(u8, content);
        defer self_.allocator.free(content_z);

        const params = [_]?[*:0]const u8{ sid_z, iid_z, role_z, content_z };
        const lengths = [_]c_int{
            @intCast(session_id.len),
            @intCast(local_instance_id.len),
            @intCast(role.len),
            @intCast(content.len),
        };

        const result = try self_.execParams(self_.q_save_msg, &params, &lengths);
        c.PQclear(result);
    }

    fn implSessionLoadMessages(ptr: *anyopaque, allocator: std.mem.Allocator, session_id: []const u8) anyerror![]root.MessageEntry {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const local_instance_id = self_.localInstanceId();

        const sid_z = try allocator.dupeZ(u8, session_id);
        defer allocator.free(sid_z);
        const iid_z = try allocator.dupeZ(u8, local_instance_id);
        defer allocator.free(iid_z);

        const params = [_]?[*:0]const u8{ sid_z, iid_z };
        const lengths = [_]c_int{ @intCast(session_id.len), @intCast(local_instance_id.len) };

        const result = try self_.execParams(self_.q_load_msgs, &params, &lengths);
        defer c.PQclear(result);

        const nrows = c.PQntuples(result);
        var messages = try allocator.alloc(root.MessageEntry, @intCast(nrows));
        var filled: usize = 0;
        errdefer {
            for (messages[0..filled]) |entry| {
                allocator.free(entry.role);
                allocator.free(entry.content);
            }
            allocator.free(messages);
        }

        var row: c_int = 0;
        while (row < nrows) : (row += 1) {
            const idx: usize = @intCast(row);
            messages[idx] = .{
                .role = try dupeResultValue(allocator, result, row, 0),
                .content = try dupeResultValue(allocator, result, row, 1),
            };
            filled += 1;
        }

        return messages;
    }

    fn implSessionClearMessages(ptr: *anyopaque, session_id: []const u8) anyerror!void {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const local_instance_id = self_.localInstanceId();

        const sid_z = try self_.allocator.dupeZ(u8, session_id);
        defer self_.allocator.free(sid_z);
        const iid_z = try self_.allocator.dupeZ(u8, local_instance_id);
        defer self_.allocator.free(iid_z);

        const params = [_]?[*:0]const u8{ sid_z, iid_z };
        const lengths = [_]c_int{ @intCast(session_id.len), @intCast(local_instance_id.len) };

        const result = try self_.execParams(self_.q_clear_msgs, &params, &lengths);
        c.PQclear(result);

        const usage_result = try self_.execParams(self_.q_clear_usage, &params, &lengths);
        c.PQclear(usage_result);
    }

    fn implSessionClearAutoSaved(ptr: *anyopaque, session_id: ?[]const u8) anyerror!void {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const local_instance_id = self_.localInstanceId();

        const iid_z = try self_.allocator.dupeZ(u8, local_instance_id);
        defer self_.allocator.free(iid_z);

        if (session_id) |sid| {
            const sid_z = try self_.allocator.dupeZ(u8, sid);
            defer self_.allocator.free(sid_z);
            const params = [_]?[*:0]const u8{ sid_z, iid_z };
            const lengths = [_]c_int{ @intCast(sid.len), @intCast(local_instance_id.len) };
            const result = try self_.execParams(self_.q_clear_auto_sid, &params, &lengths);
            c.PQclear(result);
        } else {
            const params = [_]?[*:0]const u8{iid_z};
            const lengths = [_]c_int{@intCast(local_instance_id.len)};
            const result = try self_.execParams(self_.q_clear_auto, &params, &lengths);
            c.PQclear(result);
        }
    }

    fn implSessionSaveUsage(ptr: *anyopaque, session_id: []const u8, total_tokens: u64) anyerror!void {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const local_instance_id = self_.localInstanceId();

        const sid_z = try self_.allocator.dupeZ(u8, session_id);
        defer self_.allocator.free(sid_z);
        const iid_z = try self_.allocator.dupeZ(u8, local_instance_id);
        defer self_.allocator.free(iid_z);

        const total_z = try allocPrintZCompat(self_.allocator, "{d}", .{total_tokens});
        defer self_.allocator.free(total_z);
        const params = [_]?[*:0]const u8{ sid_z, iid_z, total_z };
        const lengths = [_]c_int{ @intCast(session_id.len), @intCast(local_instance_id.len), @intCast(total_z.len) };

        const result = try self_.execParams(self_.q_save_usage, &params, &lengths);
        c.PQclear(result);
    }

    fn implSessionLoadUsage(ptr: *anyopaque, session_id: []const u8) anyerror!?u64 {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const local_instance_id = self_.localInstanceId();

        const sid_z = try self_.allocator.dupeZ(u8, session_id);
        defer self_.allocator.free(sid_z);
        const iid_z = try self_.allocator.dupeZ(u8, local_instance_id);
        defer self_.allocator.free(iid_z);

        const params = [_]?[*:0]const u8{ sid_z, iid_z };
        const lengths = [_]c_int{ @intCast(session_id.len), @intCast(local_instance_id.len) };

        const result = try self_.execParams(self_.q_load_usage, &params, &lengths);
        defer c.PQclear(result);

        if (c.PQntuples(result) == 0) return null;
        const raw = c.PQgetvalue(result, 0, 0);
        const len: usize = @intCast(c.PQgetlength(result, 0, 0));
        return try std.fmt.parseInt(u64, raw[0..len], 10);
    }

    fn implSessionCountSessions(ptr: *anyopaque) anyerror!u64 {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const local_instance_id = self_.localInstanceId();

        const iid_z = try self_.allocator.dupeZ(u8, local_instance_id);
        defer self_.allocator.free(iid_z);
        const params = [_]?[*:0]const u8{iid_z};
        const lengths = [_]c_int{@intCast(local_instance_id.len)};

        const result = try self_.execParams(self_.q_count_sessions, &params, &lengths);
        defer c.PQclear(result);

        if (c.PQntuples(result) == 0) return 0;
        const raw = c.PQgetvalue(result, 0, 0);
        const len: usize = @intCast(c.PQgetlength(result, 0, 0));
        return std.fmt.parseInt(u64, raw[0..len], 10) catch 0;
    }

    fn implSessionListSessions(ptr: *anyopaque, allocator: std.mem.Allocator, limit: usize, offset: usize) anyerror![]root.SessionInfo {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const local_instance_id = self_.localInstanceId();

        const iid_z = try allocator.dupeZ(u8, local_instance_id);
        defer allocator.free(iid_z);
        var limit_buf: [20]u8 = undefined;
        const limit_str = try std.fmt.bufPrintZ(&limit_buf, "{d}", .{limit});
        var offset_buf: [20]u8 = undefined;
        const offset_str = try std.fmt.bufPrintZ(&offset_buf, "{d}", .{offset});

        const params = [_]?[*:0]const u8{ iid_z, limit_str.ptr, offset_str.ptr };
        const lengths = [_]c_int{ @intCast(local_instance_id.len), @intCast(limit_str.len), @intCast(offset_str.len) };

        const result = try self_.execParams(self_.q_list_sessions, &params, &lengths);
        defer c.PQclear(result);

        const nrows = c.PQntuples(result);
        var sessions = try allocator.alloc(root.SessionInfo, @intCast(nrows));
        var filled: usize = 0;
        errdefer {
            for (sessions[0..filled]) |info| info.deinit(allocator);
            allocator.free(sessions);
        }

        var row: c_int = 0;
        while (row < nrows) : (row += 1) {
            if (c.PQgetisnull(result, row, 0) != 0) continue;

            const raw_count = c.PQgetvalue(result, row, 1);
            const raw_count_len: usize = @intCast(c.PQgetlength(result, row, 1));

            sessions[filled] = .{
                .session_id = try dupeResultValue(allocator, result, row, 0),
                .message_count = std.fmt.parseInt(u64, raw_count[0..raw_count_len], 10) catch 0,
                .first_message_at = try dupeResultValue(allocator, result, row, 2),
                .last_message_at = try dupeResultValue(allocator, result, row, 3),
            };
            filled += 1;
        }

        if (filled < sessions.len) {
            return allocator.realloc(sessions, filled);
        }
        return sessions;
    }

    fn implSessionCountDetailedMessages(ptr: *anyopaque, session_id: []const u8) anyerror!u64 {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const local_instance_id = self_.localInstanceId();

        const sid_z = try self_.allocator.dupeZ(u8, session_id);
        defer self_.allocator.free(sid_z);
        const iid_z = try self_.allocator.dupeZ(u8, local_instance_id);
        defer self_.allocator.free(iid_z);

        const params = [_]?[*:0]const u8{ sid_z, iid_z };
        const lengths = [_]c_int{ @intCast(session_id.len), @intCast(local_instance_id.len) };

        const result = try self_.execParams(self_.q_count_detailed_msgs, &params, &lengths);
        defer c.PQclear(result);

        if (c.PQntuples(result) == 0) return 0;
        const raw = c.PQgetvalue(result, 0, 0);
        const len: usize = @intCast(c.PQgetlength(result, 0, 0));
        return std.fmt.parseInt(u64, raw[0..len], 10) catch 0;
    }

    fn implSessionLoadMessagesDetailed(ptr: *anyopaque, allocator: std.mem.Allocator, session_id: []const u8, limit: usize, offset: usize) anyerror![]root.DetailedMessageEntry {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const local_instance_id = self_.localInstanceId();

        const sid_z = try allocator.dupeZ(u8, session_id);
        defer allocator.free(sid_z);
        const iid_z = try allocator.dupeZ(u8, local_instance_id);
        defer allocator.free(iid_z);
        var limit_buf: [20]u8 = undefined;
        const limit_str = try std.fmt.bufPrintZ(&limit_buf, "{d}", .{limit});
        var offset_buf: [20]u8 = undefined;
        const offset_str = try std.fmt.bufPrintZ(&offset_buf, "{d}", .{offset});

        const params = [_]?[*:0]const u8{ sid_z, iid_z, limit_str.ptr, offset_str.ptr };
        const lengths = [_]c_int{
            @intCast(session_id.len),
            @intCast(local_instance_id.len),
            @intCast(limit_str.len),
            @intCast(offset_str.len),
        };

        const result = try self_.execParams(self_.q_load_msgs_detailed, &params, &lengths);
        defer c.PQclear(result);

        const nrows = c.PQntuples(result);
        var messages = try allocator.alloc(root.DetailedMessageEntry, @intCast(nrows));
        var filled: usize = 0;
        errdefer {
            for (messages[0..filled]) |entry| {
                allocator.free(entry.role);
                allocator.free(entry.content);
                allocator.free(entry.created_at);
            }
            allocator.free(messages);
        }

        var row: c_int = 0;
        while (row < nrows) : (row += 1) {
            messages[filled] = .{
                .role = try dupeResultValue(allocator, result, row, 0),
                .content = try dupeResultValue(allocator, result, row, 1),
                .created_at = try dupeResultValue(allocator, result, row, 2),
            };
            filled += 1;
        }

        if (filled < messages.len) {
            return allocator.realloc(messages, filled);
        }
        return messages;
    }

    const session_vtable = SessionStore.VTable{
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

    pub fn sessionStore(self: *Self) SessionStore {
        return .{ .ptr = @ptrCast(self), .vtable = &session_vtable };
    }
};

// ── Tests ──────────────────────────────────────────────────────────

// Pure logic tests (no PG server needed)

const PostgresIntegrationTest = if (build_options.enable_postgres) struct {
    allocator: std.mem.Allocator,
    url_z: [:0]u8,
    schema: []u8,

    const Self = @This();

    fn init(allocator: std.mem.Allocator) !Self {
        const raw_url = std.process.getEnvVarOwned(allocator, "NULLCLAW_TEST_POSTGRES_URL") catch return error.SkipZigTest;
        defer allocator.free(raw_url);

        const url_z = try allocator.dupeZ(u8, raw_url);
        errdefer allocator.free(url_z);
        const schema = try makeSchemaName(allocator);
        errdefer allocator.free(schema);

        var self = Self{
            .allocator = allocator,
            .url_z = url_z,
            .schema = schema,
        };
        try self.execAdminSqlFmt("CREATE SCHEMA IF NOT EXISTS \"{s}\"", .{schema});
        return self;
    }

    fn deinit(self: *Self) void {
        self.execAdminSqlFmt("DROP SCHEMA IF EXISTS \"{s}\" CASCADE", .{self.schema}) catch {};
        self.allocator.free(self.schema);
        self.allocator.free(self.url_z);
    }

    fn initMemory(self: *Self, instance_id: []const u8) !PostgresMemory {
        return try PostgresMemory.init(self.allocator, self.url_z.ptr, self.schema, "memories", instance_id);
    }

    fn execAdminSqlFmt(self: *Self, comptime fmt: []const u8, args: anytype) !void {
        const sql = try std.fmt.allocPrint(self.allocator, fmt, args);
        defer self.allocator.free(sql);
        try self.execAdminSql(sql);
    }

    fn execAdminSql(self: *Self, sql: []const u8) !void {
        const conn = c.PQconnectdb(self.url_z.ptr) orelse return error.ConnectionFailed;
        defer c.PQfinish(conn);
        if (c.PQstatus(conn) != c.CONNECTION_OK) return error.ConnectionFailed;

        const sql_z = try self.allocator.dupeZ(u8, sql);
        defer self.allocator.free(sql_z);

        const result = c.PQexec(conn, sql_z.ptr) orelse return error.ExecFailed;
        defer c.PQclear(result);
        const status = c.PQresultStatus(result);
        if (status != c.PGRES_COMMAND_OK and status != c.PGRES_TUPLES_OK) return error.ExecFailed;
    }

    fn makeSchemaName(allocator: std.mem.Allocator) ![]u8 {
        var rand: [4]u8 = undefined;
        std.crypto.random.bytes(&rand);
        var suffix_buf: [8]u8 = undefined;
        const suffix = bytesToHexLower(&rand, &suffix_buf);
        return std.fmt.allocPrint(allocator, "nullclaw_feed_{d}_{s}", .{ std.time.nanoTimestamp(), suffix });
    }
} else struct {};

test "validateIdentifier accepts valid names" {
    try validateIdentifier("public");
    try validateIdentifier("my_schema");
    try validateIdentifier("table123");
    try validateIdentifier("a");
    try validateIdentifier("A_B_C");
}

test "validateIdentifier rejects empty" {
    try std.testing.expectError(error.EmptyIdentifier, validateIdentifier(""));
}

test "validateIdentifier rejects too long" {
    const long = "a" ** 64;
    try std.testing.expectError(error.IdentifierTooLong, validateIdentifier(long));
}

test "validateIdentifier rejects special chars" {
    try std.testing.expectError(error.InvalidCharacter, validateIdentifier("my-schema"));
    try std.testing.expectError(error.InvalidCharacter, validateIdentifier("my.schema"));
    try std.testing.expectError(error.InvalidCharacter, validateIdentifier("my schema"));
    try std.testing.expectError(error.InvalidCharacter, validateIdentifier("table;drop"));
    try std.testing.expectError(error.InvalidCharacter, validateIdentifier("tab\"le"));
}

test "validateIdentifier accepts max length 63" {
    const ok = "a" ** 63;
    try validateIdentifier(ok);
}

test "quoteIdentifier wraps correctly" {
    const result = try quoteIdentifier(std.testing.allocator, "public");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("\"public\"", result);
}

test "quoteIdentifier with underscore" {
    const result = try quoteIdentifier(std.testing.allocator, "my_table");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("\"my_table\"", result);
}

test "buildQuery replaces schema and table" {
    const result = try buildQuery(
        std.testing.allocator,
        "SELECT * FROM {schema}.{table} WHERE {table}.id = 1",
        "\"public\"",
        "\"memories\"",
    );
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings(
        "SELECT * FROM \"public\".\"memories\" WHERE \"memories\".id = 1",
        result,
    );
}

test "buildQuery no placeholders" {
    const result = try buildQuery(std.testing.allocator, "SELECT 1", "\"s\"", "\"t\"");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("SELECT 1", result);
}

test "normalizeInstanceId maps empty id to default" {
    try std.testing.expectEqualStrings("default", normalizeInstanceId(""));
    try std.testing.expectEqualStrings("agent-a", normalizeInstanceId("agent-a"));
}

test "getNowTimestamp returns numeric string" {
    const ts = try getNowTimestamp(std.testing.allocator);
    defer std.testing.allocator.free(ts);
    try std.testing.expect(ts.len > 0);
    for (ts) |ch| {
        try std.testing.expect(ch == '-' or std.ascii.isDigit(ch));
    }
}

test "generateId produces unique values" {
    const id1 = try generateId(std.testing.allocator);
    defer std.testing.allocator.free(id1);
    const id2 = try generateId(std.testing.allocator);
    defer std.testing.allocator.free(id2);
    try std.testing.expect(!std.mem.eql(u8, id1, id2));
}

test "generateId format has dashes" {
    const id = try generateId(std.testing.allocator);
    defer std.testing.allocator.free(id);
    try std.testing.expect(std.mem.indexOf(u8, id, "-") != null);
}

test "buildQuery returns null-terminated string" {
    const result = try buildQuery(std.testing.allocator, "SELECT * FROM {schema}.{table}", "\"public\"", "\"memories\"");
    defer std.testing.allocator.free(result);
    // Verify null sentinel at position result.len
    try std.testing.expectEqual(@as(u8, 0), result[result.len]);
    try std.testing.expectEqualStrings("SELECT * FROM \"public\".\"memories\"", result);
}

test "integration: postgres native feed roundtrip" {
    if (!build_options.enable_postgres) return error.SkipZigTest;

    var fixture = try PostgresIntegrationTest.init(std.testing.allocator);
    defer fixture.deinit();

    var first = try fixture.initMemory("agent-a");
    defer first.deinit();
    var second = try fixture.initMemory("agent-b");
    defer second.deinit();

    const first_mem = first.memory();
    const second_mem = second.memory();

    try first_mem.store("prefs/theme", "solarized", .core, "sess-a");

    var info = try first_mem.eventFeedInfo(std.testing.allocator);
    defer info.deinit(std.testing.allocator);
    try std.testing.expectEqual(root.MemoryEventFeedStorage.native, info.storage_kind);
    try std.testing.expectEqual(@as(u64, 1), info.last_sequence);

    const events = try first_mem.listEvents(std.testing.allocator, 0, 10);
    defer root.freeEvents(std.testing.allocator, events);
    try std.testing.expectEqual(@as(usize, 1), events.len);
    try std.testing.expectEqual(root.MemoryEventOp.put, events[0].operation);

    try second_mem.applyEvent(.{
        .origin_instance_id = events[0].origin_instance_id,
        .origin_sequence = events[0].origin_sequence,
        .timestamp_ms = events[0].timestamp_ms,
        .operation = events[0].operation,
        .key = events[0].key,
        .session_id = events[0].session_id,
        .category = events[0].category,
        .value_kind = events[0].value_kind,
        .content = events[0].content,
    });

    const restored = try second_mem.getScoped(std.testing.allocator, "prefs/theme", "sess-a") orelse
        return error.TestUnexpectedResult;
    defer restored.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("solarized", restored.content);
}

test "integration: postgres feed compact and checkpoint restore" {
    if (!build_options.enable_postgres) return error.SkipZigTest;

    var fixture = try PostgresIntegrationTest.init(std.testing.allocator);
    defer fixture.deinit();

    var source = try fixture.initMemory("agent-a");
    defer source.deinit();

    const source_mem = source.memory();
    try source_mem.store("prefs/language", "zig", .core, null);
    try source_mem.store("prefs/editor", "zed", .core, "sess-b");

    const checkpoint = try source_mem.exportCheckpoint(std.testing.allocator);
    defer std.testing.allocator.free(checkpoint);

    const compacted = try source_mem.compactEvents();
    try std.testing.expect(compacted >= 2);
    try std.testing.expectError(error.CursorExpired, source_mem.listEvents(std.testing.allocator, 0, 10));

    var restored = try fixture.initMemory("agent-b");
    defer restored.deinit();

    const restored_mem = restored.memory();
    try restored_mem.applyCheckpoint(checkpoint);

    const global_entry = try restored_mem.get(std.testing.allocator, "prefs/language") orelse
        return error.TestUnexpectedResult;
    defer global_entry.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("zig", global_entry.content);

    const scoped_entry = try restored_mem.getScoped(std.testing.allocator, "prefs/editor", "sess-b") orelse
        return error.TestUnexpectedResult;
    defer scoped_entry.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("zed", scoped_entry.content);

    var restored_info = try restored_mem.eventFeedInfo(std.testing.allocator);
    defer restored_info.deinit(std.testing.allocator);
    try std.testing.expect(restored_info.next_local_origin_sequence >= 2);
    try std.testing.expect(restored_info.last_sequence >= 2);
}

test "integration: postgres migration backfills existing state and promotes default instance ids" {
    if (!build_options.enable_postgres) return error.SkipZigTest;

    var fixture = try PostgresIntegrationTest.init(std.testing.allocator);
    defer fixture.deinit();

    try fixture.execAdminSqlFmt(
        \\CREATE TABLE IF NOT EXISTS "{s}"."memories" (
        \\    id TEXT PRIMARY KEY,
        \\    key TEXT NOT NULL,
        \\    content TEXT NOT NULL,
        \\    category TEXT NOT NULL DEFAULT 'core',
        \\    session_id TEXT,
        \\    instance_id TEXT NOT NULL DEFAULT '',
        \\    created_at TEXT NOT NULL,
        \\    updated_at TEXT NOT NULL
        \\)
    , .{fixture.schema});
    try fixture.execAdminSqlFmt(
        \\INSERT INTO "{s}"."memories" (id, key, content, category, session_id, instance_id, created_at, updated_at)
        \\VALUES ('legacy-1', 'prefs/theme', 'amber', 'core', NULL, '', '0', '0')
    , .{fixture.schema});

    var mem = try fixture.initMemory("");
    defer mem.deinit();

    const memory = mem.memory();
    const entry = try memory.get(std.testing.allocator, "prefs/theme") orelse return error.TestUnexpectedResult;
    defer entry.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("amber", entry.content);

    var info = try memory.eventFeedInfo(std.testing.allocator);
    defer info.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 1), info.last_sequence);

    const events = try memory.listEvents(std.testing.allocator, 0, 10);
    defer root.freeEvents(std.testing.allocator, events);
    try std.testing.expectEqual(@as(usize, 1), events.len);
    try std.testing.expectEqualStrings("prefs/theme", events[0].key);

    const default_sql = try buildQuery(
        std.testing.allocator,
        "SELECT COUNT(*) FROM {schema}.{table} WHERE instance_id = 'default' AND key = 'prefs/theme'",
        mem.schema_q,
        mem.table_q,
    );
    defer std.testing.allocator.free(default_sql);
    const legacy_sql = try buildQuery(
        std.testing.allocator,
        "SELECT COUNT(*) FROM {schema}.{table} WHERE instance_id = '' AND key = 'prefs/theme'",
        mem.schema_q,
        mem.table_q,
    );
    defer std.testing.allocator.free(legacy_sql);

    const no_params = [_]?[*:0]const u8{};
    const no_lengths = [_]c_int{};
    try std.testing.expectEqual(@as(u64, 1), try mem.querySingleU64(default_sql, &no_params, &no_lengths));
    try std.testing.expectEqual(@as(u64, 0), try mem.querySingleU64(legacy_sql, &no_params, &no_lengths));
}
