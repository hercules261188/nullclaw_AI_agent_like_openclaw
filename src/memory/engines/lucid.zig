//! Lucid memory backend — local SQLite memory with optional best-effort export
//! to the external `lucid` CLI.
//!
//! Architecture:
//!   - Local SqliteMemory is authoritative for all CRUD operations
//!   - `lucid store` mirrors global writes to the external lucid-memory service
//!   - recall stays fully local so feed/apply semantics remain deterministic
//!   - On CLI failure, export enters cooldown and local memory continues working
//!
//! Mirrors ZeroClaw's `LucidMemory` (src/memory/lucid.rs).

const std = @import("std");
const root = @import("../root.zig");
const Memory = root.Memory;
const MemoryCategory = root.MemoryCategory;
const MemoryEntry = root.MemoryEntry;
const MemoryEvent = root.MemoryEvent;
const MemoryEventFeedInfo = root.MemoryEventFeedInfo;
const MemoryEventInput = root.MemoryEventInput;

pub const LucidMemory = struct {
    local: root.SqliteMemory,
    allocator: std.mem.Allocator,
    owns_self: bool = false,
    lucid_cmd: []const u8,
    workspace_dir: []const u8,
    failure_cooldown_ms: u64,
    /// Timestamp (ms since epoch) after which we retry lucid.
    /// 0 means no cooldown active.
    cooldown_until_ms: i64,

    const Self = @This();

    const DEFAULT_LUCID_CMD = "lucid";
    const DEFAULT_FAILURE_COOLDOWN_MS: u64 = 15_000;

    pub fn init(allocator: std.mem.Allocator, db_path: [*:0]const u8, workspace_dir: []const u8) !Self {
        return initWithInstanceId(allocator, db_path, "default", workspace_dir);
    }

    pub fn initWithInstanceId(
        allocator: std.mem.Allocator,
        db_path: [*:0]const u8,
        instance_id: []const u8,
        workspace_dir: []const u8,
    ) !Self {
        return Self{
            .local = try root.SqliteMemory.initWithInstanceId(allocator, db_path, instance_id),
            .allocator = allocator,
            .lucid_cmd = DEFAULT_LUCID_CMD,
            .workspace_dir = workspace_dir,
            .failure_cooldown_ms = DEFAULT_FAILURE_COOLDOWN_MS,
            .cooldown_until_ms = 0,
        };
    }

    /// Test-only constructor with all knobs exposed.
    pub fn initWithOptions(
        allocator: std.mem.Allocator,
        db_path: [*:0]const u8,
        lucid_cmd: []const u8,
        workspace_dir: []const u8,
        failure_cooldown_ms: u64,
    ) !Self {
        return Self{
            .local = try root.SqliteMemory.init(allocator, db_path),
            .allocator = allocator,
            .lucid_cmd = lucid_cmd,
            .workspace_dir = workspace_dir,
            .failure_cooldown_ms = failure_cooldown_ms,
            .cooldown_until_ms = 0,
        };
    }

    pub fn deinit(self: *Self) void {
        self.local.deinit();
    }

    // ── Cooldown ─────────────────────────────────────────────────

    fn nowMs() i64 {
        return std.time.milliTimestamp();
    }

    fn inFailureCooldown(self: *const Self) bool {
        if (self.cooldown_until_ms == 0) return false;
        return nowMs() < self.cooldown_until_ms;
    }

    fn markFailure(self: *Self) void {
        self.cooldown_until_ms = nowMs() + @as(i64, @intCast(self.failure_cooldown_ms));
    }

    fn clearFailure(self: *Self) void {
        self.cooldown_until_ms = 0;
    }

    // ── Category mapping ─────────────────────────────────────────

    fn toLucidType(category: MemoryCategory) []const u8 {
        return switch (category) {
            .core => "decision",
            .daily => "context",
            .conversation => "conversation",
            .custom => "learning",
        };
    }

    // ── Lucid CLI interaction ────────────────────────────────────

    fn runLucidCommand(self: *Self, args: []const []const u8) ?[]u8 {
        const argv_buf = self.allocator.alloc([]const u8, args.len + 1) catch return null;
        defer self.allocator.free(argv_buf);
        argv_buf[0] = self.lucid_cmd;
        for (args, 0..) |arg, i| {
            argv_buf[i + 1] = arg;
        }

        var child = std.process.Child.init(argv_buf, self.allocator);
        child.stdout_behavior = .Pipe;
        child.stderr_behavior = .Ignore;

        child.spawn() catch return null;

        const stdout_raw = child.stdout.?.readToEndAlloc(self.allocator, 1024 * 1024) catch {
            _ = child.wait() catch {};
            return null;
        };

        const term = child.wait() catch {
            self.allocator.free(stdout_raw);
            return null;
        };

        switch (term) {
            .Exited => |code| if (code != 0) {
                self.allocator.free(stdout_raw);
                return null;
            },
            else => {
                self.allocator.free(stdout_raw);
                return null;
            },
        }

        return stdout_raw;
    }

    fn syncToLucid(self: *Self, key: []const u8, content: []const u8, category: MemoryCategory) void {
        if (self.inFailureCooldown()) return;

        const payload = std.fmt.allocPrint(self.allocator, "{s}: {s}", .{ key, content }) catch return;
        defer self.allocator.free(payload);

        const type_flag = std.fmt.allocPrint(self.allocator, "--type={s}", .{toLucidType(category)}) catch return;
        defer self.allocator.free(type_flag);

        const project_flag = std.fmt.allocPrint(self.allocator, "--project={s}", .{self.workspace_dir}) catch return;
        defer self.allocator.free(project_flag);

        const args = [_][]const u8{ "store", payload, type_flag, project_flag };
        if (self.runLucidCommand(&args)) |out| {
            self.allocator.free(out);
            self.clearFailure();
        } else {
            self.markFailure();
        }
    }

    // ── Memory vtable implementation ─────────────────────────────

    /// Get the local SQLite memory interface. The pointer into self.local
    /// is stable because LucidMemory is not moved after init.
    fn localMemory(self: *Self) Memory {
        return self.local.memory();
    }

    fn implName(_: *anyopaque) []const u8 {
        return "lucid";
    }

    fn implStore(ptr: *anyopaque, key: []const u8, content: []const u8, category: MemoryCategory, session_id: ?[]const u8) anyerror!void {
        const self = castSelf(ptr);
        // Store locally first (authoritative)
        const local = self.localMemory();
        try local.store(key, content, category, session_id);
        // Session-scoped memories stay local to preserve deterministic isolation.
        if (session_id == null) {
            self.syncToLucid(key, content, category);
        }
    }

    fn implRecall(ptr: *anyopaque, allocator: std.mem.Allocator, query: []const u8, limit: usize, session_id: ?[]const u8) anyerror![]MemoryEntry {
        const self = castSelf(ptr);
        const local = self.localMemory();
        return local.recall(allocator, query, limit, session_id);
    }

    fn implGet(ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8) anyerror!?MemoryEntry {
        const self = castSelf(ptr);
        return self.localMemory().get(allocator, key);
    }

    fn implGetScoped(ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8, session_id: ?[]const u8) anyerror!?MemoryEntry {
        const self = castSelf(ptr);
        return self.localMemory().getScoped(allocator, key, session_id);
    }

    fn implList(ptr: *anyopaque, allocator: std.mem.Allocator, category: ?MemoryCategory, session_id: ?[]const u8) anyerror![]MemoryEntry {
        const self = castSelf(ptr);
        return self.localMemory().list(allocator, category, session_id);
    }

    fn implForget(ptr: *anyopaque, key: []const u8) anyerror!bool {
        const self = castSelf(ptr);
        return self.localMemory().forget(key);
    }

    fn implForgetScoped(ptr: *anyopaque, key: []const u8, session_id: ?[]const u8) anyerror!bool {
        const self = castSelf(ptr);
        return self.localMemory().forgetScoped(self.allocator, key, session_id);
    }

    fn implListEvents(ptr: *anyopaque, allocator: std.mem.Allocator, after_sequence: u64, limit: usize) anyerror![]MemoryEvent {
        const self = castSelf(ptr);
        return self.localMemory().listEvents(allocator, after_sequence, limit);
    }

    fn implApplyEvent(ptr: *anyopaque, input: MemoryEventInput) anyerror!void {
        const self = castSelf(ptr);
        return self.localMemory().applyEvent(input);
    }

    fn implLastEventSequence(ptr: *anyopaque) anyerror!u64 {
        const self = castSelf(ptr);
        return self.localMemory().lastEventSequence();
    }

    fn implEventFeedInfo(ptr: *anyopaque, allocator: std.mem.Allocator) anyerror!MemoryEventFeedInfo {
        const self = castSelf(ptr);
        return self.localMemory().eventFeedInfo(allocator);
    }

    fn implCompactEvents(ptr: *anyopaque) anyerror!u64 {
        const self = castSelf(ptr);
        return self.localMemory().compactEvents();
    }

    fn implExportCheckpoint(ptr: *anyopaque, allocator: std.mem.Allocator) anyerror![]u8 {
        const self = castSelf(ptr);
        return self.localMemory().exportCheckpoint(allocator);
    }

    fn implApplyCheckpoint(ptr: *anyopaque, payload: []const u8) anyerror!void {
        const self = castSelf(ptr);
        return self.localMemory().applyCheckpoint(payload);
    }

    fn implCount(ptr: *anyopaque) anyerror!usize {
        const self = castSelf(ptr);
        return self.localMemory().count();
    }

    fn implHealthCheck(ptr: *anyopaque) bool {
        const self = castSelf(ptr);
        return self.localMemory().healthCheck();
    }

    fn implDeinit(ptr: *anyopaque) void {
        const self = castSelf(ptr);
        self.deinit();
        if (self.owns_self) {
            self.allocator.destroy(self);
        }
    }

    fn castSelf(ptr: *anyopaque) *Self {
        return @ptrCast(@alignCast(ptr));
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

    // ── SessionStore vtable ────────────────────────────────────────

    fn implSessionSaveMessage(ptr: *anyopaque, session_id: []const u8, role: []const u8, content: []const u8) anyerror!void {
        const self = castSelf(ptr);
        return self.local.saveMessage(session_id, role, content);
    }

    fn implSessionLoadMessages(ptr: *anyopaque, allocator: std.mem.Allocator, session_id: []const u8) anyerror![]root.MessageEntry {
        const self = castSelf(ptr);
        return self.local.loadMessages(allocator, session_id);
    }

    fn implSessionClearMessages(ptr: *anyopaque, session_id: []const u8) anyerror!void {
        const self = castSelf(ptr);
        return self.local.clearMessages(session_id);
    }

    fn implSessionClearAutoSaved(ptr: *anyopaque, session_id: ?[]const u8) anyerror!void {
        const self = castSelf(ptr);
        return self.local.clearAutoSaved(session_id);
    }

    fn implSessionSaveUsage(ptr: *anyopaque, session_id: []const u8, total_tokens: u64) anyerror!void {
        const self = castSelf(ptr);
        return self.local.saveUsage(session_id, total_tokens);
    }

    fn implSessionLoadUsage(ptr: *anyopaque, session_id: []const u8) anyerror!?u64 {
        const self = castSelf(ptr);
        return self.local.loadUsage(session_id);
    }

    fn implSessionCountSessions(ptr: *anyopaque) anyerror!u64 {
        const self = castSelf(ptr);
        return self.local.countSessions();
    }

    fn implSessionListSessions(ptr: *anyopaque, allocator: std.mem.Allocator, limit: usize, offset: usize) anyerror![]root.SessionInfo {
        const self = castSelf(ptr);
        return self.local.listSessions(allocator, limit, offset);
    }

    fn implSessionCountDetailedMessages(ptr: *anyopaque, session_id: []const u8) anyerror!u64 {
        const self = castSelf(ptr);
        return self.local.countDetailedMessages(session_id);
    }

    fn implSessionLoadMessagesDetailed(ptr: *anyopaque, allocator: std.mem.Allocator, session_id: []const u8, limit: usize, offset: usize) anyerror![]root.DetailedMessageEntry {
        const self = castSelf(ptr);
        return self.local.loadMessagesDetailed(allocator, session_id, limit, offset);
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
};

// ── Tests ──────────────────────────────────────────────────────────

test "lucid memory name" {
    const allocator = std.testing.allocator;
    var mem = try LucidMemory.initWithOptions(
        allocator,
        ":memory:",
        "nonexistent-lucid-binary",
        "/tmp/test",
        2000,
    );
    defer mem.deinit();
    const m = mem.memory();
    try std.testing.expectEqualStrings("lucid", m.name());
}

test "lucid store succeeds when lucid binary missing" {
    const allocator = std.testing.allocator;
    var mem = try LucidMemory.initWithOptions(
        allocator,
        ":memory:",
        "nonexistent-lucid-binary",
        "/tmp/test",
        2000,
    );
    defer mem.deinit();
    const m = mem.memory();

    try m.store("lang", "User prefers Zig", .core, null);

    const entry = try m.get(allocator, "lang");
    try std.testing.expect(entry != null);
    var e = entry.?;
    defer e.deinit(allocator);
    try std.testing.expectEqualStrings("User prefers Zig", e.content);
}

test "lucid recall returns local results when lucid unavailable" {
    const allocator = std.testing.allocator;
    var mem = try LucidMemory.initWithOptions(
        allocator,
        ":memory:",
        "nonexistent-lucid-binary",
        "/tmp/test",
        2000,
    );
    defer mem.deinit();
    const m = mem.memory();

    try m.store("pref", "Zig is fast", .core, null);

    const results = try m.recall(allocator, "zig", 5, null);
    defer root.freeEntries(allocator, results);
    try std.testing.expect(results.len >= 1);
    try std.testing.expect(std.mem.indexOf(u8, results[0].content, "Zig is fast") != null);
}

test "lucid list delegates to local" {
    const allocator = std.testing.allocator;
    var mem = try LucidMemory.initWithOptions(
        allocator,
        ":memory:",
        "nonexistent-lucid-binary",
        "/tmp/test",
        2000,
    );
    defer mem.deinit();
    const m = mem.memory();

    try m.store("a", "alpha", .core, null);
    try m.store("b", "beta", .daily, null);

    const all = try m.list(allocator, null, null);
    defer root.freeEntries(allocator, all);
    try std.testing.expectEqual(@as(usize, 2), all.len);

    const core_only = try m.list(allocator, .core, null);
    defer root.freeEntries(allocator, core_only);
    try std.testing.expectEqual(@as(usize, 1), core_only.len);
}

test "lucid forget delegates to local" {
    const allocator = std.testing.allocator;
    var mem = try LucidMemory.initWithOptions(
        allocator,
        ":memory:",
        "nonexistent-lucid-binary",
        "/tmp/test",
        2000,
    );
    defer mem.deinit();
    const m = mem.memory();

    try m.store("temp", "temporary data", .core, null);
    const forgotten = try m.forget("temp");
    try std.testing.expect(forgotten);

    const entry = try m.get(allocator, "temp");
    try std.testing.expect(entry == null);
}

test "lucid exposes native feed via local sqlite with configured instance id" {
    const allocator = std.testing.allocator;
    var mem = try LucidMemory.initWithInstanceId(
        allocator,
        ":memory:",
        "agent-b",
        "/tmp/test",
    );
    defer mem.deinit();
    const m = mem.memory();

    try m.store("pref.theme", "dark", .core, null);

    var info = try m.eventFeedInfo(allocator);
    defer info.deinit(allocator);
    try std.testing.expectEqualStrings("agent-b", info.instance_id);
    try std.testing.expectEqual(root.MemoryEventFeedStorage.native, info.storage_kind);
    try std.testing.expect(info.last_sequence > 0);

    const events = try m.listEvents(allocator, 0, 8);
    defer root.freeEvents(allocator, events);
    try std.testing.expectEqual(@as(usize, 1), events.len);
    try std.testing.expectEqualStrings("agent-b", events[0].origin_instance_id);
    try std.testing.expectEqualStrings("pref.theme", events[0].key);
}

test "lucid count delegates to local" {
    const allocator = std.testing.allocator;
    var mem = try LucidMemory.initWithOptions(
        allocator,
        ":memory:",
        "nonexistent-lucid-binary",
        "/tmp/test",
        2000,
    );
    defer mem.deinit();
    const m = mem.memory();

    try std.testing.expectEqual(@as(usize, 0), try m.count());
    try m.store("x", "data", .core, null);
    try std.testing.expectEqual(@as(usize, 1), try m.count());
}

test "lucid health check delegates to local" {
    const allocator = std.testing.allocator;
    var mem = try LucidMemory.initWithOptions(
        allocator,
        ":memory:",
        "nonexistent-lucid-binary",
        "/tmp/test",
        2000,
    );
    defer mem.deinit();
    const m = mem.memory();
    try std.testing.expect(m.healthCheck());
}

test "lucid failure cooldown is set on export failure" {
    const allocator = std.testing.allocator;
    var mem = try LucidMemory.initWithOptions(
        allocator,
        ":memory:",
        "nonexistent-lucid-binary",
        "/tmp/test",
        5000,
    );
    defer mem.deinit();

    // Initial state: no cooldown
    try std.testing.expect(!mem.inFailureCooldown());

    const m = mem.memory();
    try m.store("pref", "export me", .core, null);

    try std.testing.expect(mem.cooldown_until_ms > 0);
    try std.testing.expect(mem.inFailureCooldown());
}

test "lucid clearFailure resets cooldown" {
    const allocator = std.testing.allocator;
    var mem = try LucidMemory.initWithOptions(
        allocator,
        ":memory:",
        "nonexistent-lucid-binary",
        "/tmp/test",
        5000,
    );
    defer mem.deinit();

    mem.markFailure();
    try std.testing.expect(mem.inFailureCooldown());
    mem.clearFailure();
    try std.testing.expect(!mem.inFailureCooldown());
}

test "toLucidType maps categories correctly" {
    try std.testing.expectEqualStrings("decision", LucidMemory.toLucidType(.core));
    try std.testing.expectEqualStrings("context", LucidMemory.toLucidType(.daily));
    try std.testing.expectEqualStrings("conversation", LucidMemory.toLucidType(.conversation));
    try std.testing.expectEqualStrings("learning", LucidMemory.toLucidType(.{ .custom = "anything" }));
}

test "lucid recall does not enter cooldown on missing lucid binary" {
    const allocator = std.testing.allocator;
    var mem = try LucidMemory.initWithOptions(
        allocator,
        ":memory:",
        "nonexistent-lucid-binary",
        "/tmp/test",
        5000,
    );
    defer mem.deinit();
    const m = mem.memory();

    try m.store("pref", "Zig stays local-first", .core, null);
    mem.clearFailure(); // isolate recall from export side-effects
    try std.testing.expect(!mem.inFailureCooldown());

    const results = try m.recall(allocator, "zig", 5, null);
    defer root.freeEntries(allocator, results);
    try std.testing.expect(results.len >= 1);
    try std.testing.expect(!mem.inFailureCooldown());
}

test "lucid session-scoped store does not trigger lucid export" {
    const allocator = std.testing.allocator;
    var mem = try LucidMemory.initWithOptions(
        allocator,
        ":memory:",
        "nonexistent-lucid-binary",
        "/tmp/test",
        5000,
    );
    defer mem.deinit();
    const m = mem.memory();

    try std.testing.expect(!mem.inFailureCooldown());
    try m.store("sess_pref", "private", .core, "session-abc");
    try std.testing.expect(!mem.inFailureCooldown());
}

test "lucid store accepts session_id" {
    const allocator = std.testing.allocator;
    var mem = try LucidMemory.initWithOptions(
        allocator,
        ":memory:",
        "nonexistent-lucid-binary",
        "/tmp/test",
        2000,
    );
    defer mem.deinit();
    const m = mem.memory();

    // Store with explicit session_id
    try m.store("sess_key", "session data", .core, "session-abc");

    const entry = try m.getScoped(allocator, "sess_key", "session-abc");
    try std.testing.expect(entry != null);
    var e = entry.?;
    defer e.deinit(allocator);
    try std.testing.expectEqualStrings("session data", e.content);
}

test "lucid recall accepts session_id" {
    const allocator = std.testing.allocator;
    var mem = try LucidMemory.initWithOptions(
        allocator,
        ":memory:",
        "nonexistent-lucid-binary",
        "/tmp/test",
        2000,
    );
    defer mem.deinit();
    const m = mem.memory();

    // Store with same session_id so it's retrievable by that session
    try m.store("data", "searchable content", .core, "session-abc");

    const results = try m.recall(allocator, "searchable", 5, "session-abc");
    defer root.freeEntries(allocator, results);
    try std.testing.expect(results.len >= 1);
}

test "lucid list accepts session_id" {
    const allocator = std.testing.allocator;
    var mem = try LucidMemory.initWithOptions(
        allocator,
        ":memory:",
        "nonexistent-lucid-binary",
        "/tmp/test",
        2000,
    );
    defer mem.deinit();
    const m = mem.memory();

    // Store with same session_id so it's listable by that session
    try m.store("a", "alpha", .core, "session-abc");

    const results = try m.list(allocator, null, "session-abc");
    defer root.freeEntries(allocator, results);
    try std.testing.expect(results.len >= 1);
}

// ── SessionStore vtable tests ─────────────────────────────────────

test "lucid sessionStore returns valid vtable" {
    const allocator = std.testing.allocator;
    var mem = try LucidMemory.initWithOptions(
        allocator,
        ":memory:",
        "nonexistent-lucid-binary",
        "/tmp/test",
        2000,
    );
    defer mem.deinit();

    const store = mem.sessionStore();
    try std.testing.expect(store.vtable == &LucidMemory.session_vtable);
}

test "lucid sessionStore saveMessage + loadMessages roundtrip" {
    const allocator = std.testing.allocator;
    var mem = try LucidMemory.initWithOptions(
        allocator,
        ":memory:",
        "nonexistent-lucid-binary",
        "/tmp/test",
        2000,
    );
    defer mem.deinit();

    const store = mem.sessionStore();
    try store.saveMessage("s1", "user", "hello from lucid");
    try store.saveMessage("s1", "assistant", "hi back");

    const msgs = try store.loadMessages(allocator, "s1");
    defer root.freeMessages(allocator, msgs);

    try std.testing.expectEqual(@as(usize, 2), msgs.len);
    try std.testing.expectEqualStrings("user", msgs[0].role);
    try std.testing.expectEqualStrings("hello from lucid", msgs[0].content);
    try std.testing.expectEqualStrings("assistant", msgs[1].role);
    try std.testing.expectEqualStrings("hi back", msgs[1].content);
}
