//! Cross-backend contract tests for the Memory vtable.
//!
//! Every backend that implements Memory must satisfy these invariants.
//! Each test creates its own backend instance, runs the contract, and deinits.

const std = @import("std");
const build_options = @import("build_options");
const root = @import("../root.zig");
const Memory = root.Memory;
const MemoryCategory = root.MemoryCategory;

const SqliteMemory = if (build_options.enable_sqlite) @import("sqlite.zig").SqliteMemory else @import("sqlite_disabled.zig").SqliteMemory;
const NoneMemory = @import("none.zig").NoneMemory;
const MarkdownMemory = @import("markdown.zig").MarkdownMemory;
const InMemoryLruMemory = @import("memory_lru.zig").InMemoryLruMemory;
const LanceDbMemory = if (build_options.enable_memory_lancedb and build_options.enable_sqlite) @import("lancedb.zig").LanceDbMemory else struct {};
const registry = @import("registry.zig");

// ── Contract: common invariants ─────────────────────────────────────

/// Validates that a Memory backend satisfies the basic vtable contract:
/// name() returns a non-empty string, healthCheck() is true, and the
/// vtable methods do not crash on an empty store.
fn contractBasics(m: Memory) !void {
    const allocator = std.testing.allocator;

    // name() returns non-empty
    const n = m.name();
    try std.testing.expect(n.len > 0);

    // healthCheck is true after init
    try std.testing.expect(m.healthCheck());

    // Empty store: count is 0
    try std.testing.expectEqual(@as(usize, 0), try m.count());

    // Empty store: get returns null
    const got = try m.get(allocator, "nonexistent");
    try std.testing.expect(got == null);

    // Empty store: recall returns empty
    const recalled = try m.recall(allocator, "query", 10, null);
    defer root.freeEntries(allocator, recalled);
    try std.testing.expectEqual(@as(usize, 0), recalled.len);

    // Empty store: list returns empty
    const listed = try m.list(allocator, null, null);
    defer root.freeEntries(allocator, listed);
    try std.testing.expectEqual(@as(usize, 0), listed.len);
}

/// Exact CRUD contract for backends that store concrete memory state.
/// After store(), the entry is retrievable via get(), recall(), list(), count().
/// After forget(), the entry is gone.
fn contractCrud(m: Memory) !void {
    const allocator = std.testing.allocator;

    // 1. store a memory entry
    try m.store("test_key", "test content", .core, null);

    // 2. get the entry back, verify content matches
    {
        const entry = try m.get(allocator, "test_key");
        try std.testing.expect(entry != null);
        defer entry.?.deinit(allocator);
        try std.testing.expectEqualStrings("test_key", entry.?.key);
        try std.testing.expectEqualStrings("test content", entry.?.content);
        try std.testing.expect(entry.?.category.eql(.core));
    }

    // 3. recall with a query, verify the entry appears
    {
        const results = try m.recall(allocator, "test", 10, null);
        defer root.freeEntries(allocator, results);
        try std.testing.expect(results.len >= 1);
        // At least one result should contain our content
        var found = false;
        for (results) |e| {
            if (std.mem.indexOf(u8, e.content, "test content") != null) {
                found = true;
                break;
            }
        }
        try std.testing.expect(found);
    }

    // 4. list all core entries, verify count
    {
        const core_list = try m.list(allocator, .core, null);
        defer root.freeEntries(allocator, core_list);
        try std.testing.expect(core_list.len >= 1);
    }

    // 5. count total entries
    try std.testing.expectEqual(@as(usize, 1), try m.count());

    // 6. store a second entry, verify count=2
    try m.store("second_key", "second content", .core, null);
    try std.testing.expectEqual(@as(usize, 2), try m.count());

    // 7. forget the first entry, verify count=1
    const forgotten = try m.forget("test_key");
    try std.testing.expect(forgotten);
    try std.testing.expectEqual(@as(usize, 1), try m.count());

    // 8. get the forgotten entry returns null
    {
        const entry = try m.get(allocator, "test_key");
        try std.testing.expect(entry == null);
    }
}

/// Contract for NoneMemory: every write is a no-op, every read returns empty.
fn contractNone(m: Memory) !void {
    const allocator = std.testing.allocator;

    try std.testing.expectEqualStrings("none", m.name());

    // store does not crash
    try m.store("test_key", "test content", .core, null);

    // get returns null
    const got = try m.get(allocator, "test_key");
    try std.testing.expect(got == null);

    // recall returns empty
    const recalled = try m.recall(allocator, "test", 10, null);
    defer root.freeEntries(allocator, recalled);
    try std.testing.expectEqual(@as(usize, 0), recalled.len);

    // list returns empty
    const listed = try m.list(allocator, .core, null);
    defer root.freeEntries(allocator, listed);
    try std.testing.expectEqual(@as(usize, 0), listed.len);

    // count is always 0
    try std.testing.expectEqual(@as(usize, 0), try m.count());

    // forget returns false
    try std.testing.expect(!(try m.forget("test_key")));

    // Store a second entry, count is still 0
    try m.store("second_key", "second content", .core, null);
    try std.testing.expectEqual(@as(usize, 0), try m.count());
}

/// Contract for MarkdownMemory: exact file-backed CRUD with markdown formatting.
fn contractMarkdown(m: Memory) !void {
    const allocator = std.testing.allocator;

    try std.testing.expectEqualStrings("markdown", m.name());
    try std.testing.expect(m.healthCheck());

    // Empty at start
    try std.testing.expectEqual(@as(usize, 0), try m.count());

    // Store an entry
    try m.store("test_key", "test content", .core, null);

    // count should be 1
    try std.testing.expectEqual(@as(usize, 1), try m.count());

    // get by key — markdown stores as "**key**: content" with metadata comments
    {
        const entry = try m.get(allocator, "test_key");
        try std.testing.expect(entry != null);
        defer entry.?.deinit(allocator);
        try std.testing.expectEqualStrings("test_key", entry.?.key);
        try std.testing.expectEqualStrings("test content", entry.?.content);
    }

    // recall with query
    {
        const results = try m.recall(allocator, "test", 10, null);
        defer root.freeEntries(allocator, results);
        try std.testing.expect(results.len >= 1);
    }

    // list core entries
    {
        const core_list = try m.list(allocator, .core, null);
        defer root.freeEntries(allocator, core_list);
        try std.testing.expect(core_list.len >= 1);
    }

    // Store a second entry
    try m.store("second_key", "second content", .core, null);
    try std.testing.expectEqual(@as(usize, 2), try m.count());

    // forget removes all scopes for the logical key
    try std.testing.expect(try m.forget("test_key"));
    try std.testing.expectEqual(@as(usize, 1), try m.count());
}

fn expectScopedEntry(m: Memory, key: []const u8, session_id: ?[]const u8, expected_content: []const u8) !void {
    const allocator = std.testing.allocator;
    const entry = try m.getScoped(allocator, key, session_id);
    try std.testing.expect(entry != null);
    defer entry.?.deinit(allocator);

    try std.testing.expectEqualStrings(key, entry.?.key);
    try std.testing.expectEqualStrings(expected_content, entry.?.content);
    if (session_id) |sid| {
        try std.testing.expect(entry.?.session_id != null);
        try std.testing.expectEqualStrings(sid, entry.?.session_id.?);
    } else {
        try std.testing.expect(entry.?.session_id == null);
    }
}

/// Exact scoped memory contract:
/// the same logical key may coexist in global and session-specific namespaces,
/// and forgetScoped() must only remove the targeted namespace.
fn contractScopedNamespaces(m: Memory) !void {
    const allocator = std.testing.allocator;

    try m.store("shared", "global content", .core, null);
    try m.store("shared", "session A content", .conversation, "sess-a");
    try m.store("shared", "session B content", .daily, "sess-b");
    try m.store("scoped-only", "session only", .core, "sess-a");

    try std.testing.expectEqual(@as(usize, 4), try m.count());

    try expectScopedEntry(m, "shared", null, "global content");
    try expectScopedEntry(m, "shared", "sess-a", "session A content");
    try expectScopedEntry(m, "shared", "sess-b", "session B content");

    {
        const global_get = try m.get(allocator, "shared");
        try std.testing.expect(global_get != null);
        defer global_get.?.deinit(allocator);
        try std.testing.expect(global_get.?.session_id == null);
        try std.testing.expectEqualStrings("global content", global_get.?.content);
    }

    try std.testing.expect(try m.get(allocator, "scoped-only") == null);

    {
        const sess_a_entries = try m.list(allocator, null, "sess-a");
        defer root.freeEntries(allocator, sess_a_entries);
        try std.testing.expectEqual(@as(usize, 2), sess_a_entries.len);
        var found_shared = false;
        var found_scoped_only = false;
        for (sess_a_entries) |entry| {
            try std.testing.expect(entry.session_id != null);
            try std.testing.expectEqualStrings("sess-a", entry.session_id.?);
            if (std.mem.eql(u8, entry.key, "shared")) {
                try std.testing.expectEqualStrings("session A content", entry.content);
                found_shared = true;
            } else if (std.mem.eql(u8, entry.key, "scoped-only")) {
                try std.testing.expectEqualStrings("session only", entry.content);
                found_scoped_only = true;
            }
        }
        try std.testing.expect(found_shared);
        try std.testing.expect(found_scoped_only);
    }

    {
        const sess_b_entries = try m.recall(allocator, "content", 10, "sess-b");
        defer root.freeEntries(allocator, sess_b_entries);
        try std.testing.expectEqual(@as(usize, 1), sess_b_entries.len);
        try std.testing.expect(sess_b_entries[0].session_id != null);
        try std.testing.expectEqualStrings("sess-b", sess_b_entries[0].session_id.?);
        try std.testing.expectEqualStrings("session B content", sess_b_entries[0].content);
    }

    try std.testing.expect(try m.forgetScoped(allocator, "shared", "sess-a"));
    try std.testing.expect(try m.getScoped(allocator, "shared", "sess-a") == null);
    try expectScopedEntry(m, "shared", null, "global content");
    try expectScopedEntry(m, "shared", "sess-b", "session B content");
    try expectScopedEntry(m, "scoped-only", "sess-a", "session only");
    try std.testing.expectEqual(@as(usize, 3), try m.count());

    try std.testing.expect(try m.forget("shared"));
    try std.testing.expect(try m.getScoped(allocator, "shared", null) == null);
    try std.testing.expect(try m.getScoped(allocator, "shared", "sess-b") == null);
    try expectScopedEntry(m, "scoped-only", "sess-a", "session only");
    try std.testing.expectEqual(@as(usize, 1), try m.count());
}

fn createHybridInstance(allocator: std.mem.Allocator, workspace_dir: []const u8) !registry.BackendInstance {
    const desc = registry.findBackend("hybrid") orelse return error.TestUnexpectedResult;
    const cfg = try registry.resolvePaths(allocator, desc, workspace_dir, "default", null, null, null, null);
    errdefer {
        if (cfg.postgres_url) |pu| allocator.free(std.mem.span(pu));
        if (cfg.db_path) |p| allocator.free(std.mem.span(p));
    }
    const instance = try desc.create(allocator, cfg);
    if (cfg.postgres_url) |pu| allocator.free(std.mem.span(pu));
    if (cfg.db_path) |p| allocator.free(std.mem.span(p));
    return instance;
}

// ── SQLite tests ─────────────────────────────────────────────────────

test "contract: sqlite basics" {
    if (!build_options.enable_sqlite) return;
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    try contractBasics(mem.memory());
}

test "contract: sqlite crud" {
    if (!build_options.enable_sqlite) return;
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    try contractCrud(mem.memory());
}

test "contract: sqlite scoped namespaces" {
    if (!build_options.enable_sqlite) return;
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    try contractScopedNamespaces(mem.memory());
}

test "contract: hybrid basics" {
    if (!build_options.enable_sqlite) return;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    var instance = try createHybridInstance(std.testing.allocator, base);
    defer instance.memory.deinit();
    try contractBasics(instance.memory);
}

test "contract: hybrid crud" {
    if (!build_options.enable_sqlite) return;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    var instance = try createHybridInstance(std.testing.allocator, base);
    defer instance.memory.deinit();
    try contractCrud(instance.memory);
}

test "contract: hybrid scoped namespaces" {
    if (!build_options.enable_sqlite) return;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    var instance = try createHybridInstance(std.testing.allocator, base);
    defer instance.memory.deinit();
    try contractScopedNamespaces(instance.memory);
}

// ── NoneMemory tests ─────────────────────────────────────────────────

test "contract: none basics" {
    var mem = NoneMemory.init();
    defer mem.deinit();
    try contractBasics(mem.memory());
}

test "contract: none noop" {
    var mem = NoneMemory.init();
    defer mem.deinit();
    try contractNone(mem.memory());
}

// ── MarkdownMemory tests ─────────────────────────────────────────────

test "contract: markdown basics" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    var mem = try MarkdownMemory.init(std.testing.allocator, base);
    defer mem.deinit();
    try contractBasics(mem.memory());
}

test "contract: markdown crud" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    var mem = try MarkdownMemory.init(std.testing.allocator, base);
    defer mem.deinit();
    try contractMarkdown(mem.memory());
}

test "contract: markdown scoped namespaces" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    var mem = try MarkdownMemory.init(std.testing.allocator, base);
    defer mem.deinit();
    try contractScopedNamespaces(mem.memory());
}

// ── InMemoryLruMemory tests ──────────────────────────────────────────

test "contract: memory_lru basics" {
    var mem = InMemoryLruMemory.init(std.testing.allocator, 100);
    defer mem.deinit();
    try contractBasics(mem.memory());
}

test "contract: memory_lru crud" {
    var mem = InMemoryLruMemory.init(std.testing.allocator, 100);
    defer mem.deinit();
    try contractCrud(mem.memory());
}

test "contract: memory_lru scoped namespaces" {
    var mem = InMemoryLruMemory.init(std.testing.allocator, 100);
    defer mem.deinit();
    try contractScopedNamespaces(mem.memory());
}

test "contract: lancedb scoped namespaces" {
    if (!build_options.enable_memory_lancedb or !build_options.enable_sqlite) return;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);
    const db_path = try std.fs.path.join(std.testing.allocator, &.{ base, "test.lancedb.sqlite" });
    defer std.testing.allocator.free(db_path);

    const db_path_z = try std.testing.allocator.dupeZ(u8, db_path);
    defer std.testing.allocator.free(db_path_z);

    var mem = try LanceDbMemory.init(std.testing.allocator, db_path_z, null, .{});
    defer mem.deinit();
    try contractScopedNamespaces(mem.memory());
}
