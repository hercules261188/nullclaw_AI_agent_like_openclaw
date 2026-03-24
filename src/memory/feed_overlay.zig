const std = @import("std");
const fs_compat = @import("../fs_compat.zig");
const json_util = @import("../json_util.zig");
const root = @import("root.zig");
const key_codec = @import("vector/key_codec.zig");

const Memory = root.Memory;
const MemoryCategory = root.MemoryCategory;
const MemoryEntry = root.MemoryEntry;
const MemoryEvent = root.MemoryEvent;
const MemoryEventFeedInfo = root.MemoryEventFeedInfo;
const MemoryEventInput = root.MemoryEventInput;
const MemoryValueKind = root.MemoryValueKind;
const ResolvedMemoryState = root.ResolvedMemoryState;
const log = std.log.scoped(.memory_feed_overlay);

const MAX_EVENT_LINE_BYTES: usize = 1024 * 1024;

pub const EventFeedOverlay = struct {
    allocator: std.mem.Allocator,
    backend: Memory,
    journal_path: []u8,
    checkpoint_path: []u8,
    instance_id: []u8,
    last_sequence: u64 = 0,
    last_timestamp_ms: i64 = 0,
    compacted_through_sequence: u64 = 0,
    loaded_size_bytes: u64 = 0,
    projection_offset_bytes: u64 = 0,
    origin_frontiers: std.StringHashMapUnmanaged(u64) = .{},
    state_entries: std.StringHashMapUnmanaged(StoredState) = .{},
    scoped_tombstones: std.StringHashMapUnmanaged(EventMeta) = .{},
    key_tombstones: std.StringHashMapUnmanaged(EventMeta) = .{},
    owns_self: bool = false,

    const Self = @This();

    const EventMeta = struct {
        timestamp_ms: i64,
        origin_instance_id: []const u8,
        origin_sequence: u64,
    };

    const StoredState = struct {
        key: []const u8,
        content: []const u8,
        category: MemoryCategory,
        value_kind: ?MemoryValueKind,
        session_id: ?[]const u8,
        timestamp_ms: i64,
        origin_instance_id: []const u8,
        origin_sequence: u64,
    };

    const Effect = enum {
        none,
        put,
        delete_scoped,
        delete_all,

        fn toString(self: Effect) []const u8 {
            return switch (self) {
                .none => "none",
                .put => "put",
                .delete_scoped => "delete_scoped",
                .delete_all => "delete_all",
            };
        }

        fn fromString(value: []const u8) ?Effect {
            if (std.mem.eql(u8, value, "none")) return .none;
            if (std.mem.eql(u8, value, "put")) return .put;
            if (std.mem.eql(u8, value, "delete_scoped")) return .delete_scoped;
            if (std.mem.eql(u8, value, "delete_all")) return .delete_all;
            return null;
        }
    };

    const EventDecision = struct {
        effect: Effect,
        resolved_state: ?ResolvedMemoryState = null,

        fn deinit(self: *EventDecision, allocator: std.mem.Allocator) void {
            if (self.resolved_state) |*state| state.deinit(allocator);
        }
    };

    const RecordedEvent = struct {
        event: MemoryEvent,
        effect: Effect,
        resolved_state: ?ResolvedMemoryState = null,

        fn deinit(self: *RecordedEvent, allocator: std.mem.Allocator) void {
            self.event.deinit(allocator);
            if (self.resolved_state) |*state| state.deinit(allocator);
        }
    };

    pub fn init(
        allocator: std.mem.Allocator,
        backend: Memory,
        journal_root_dir: []const u8,
        journal_identity: []const u8,
        instance_id: []const u8,
    ) !Self {
        const effective_instance_id = if (instance_id.len > 0) instance_id else "default";
        const journal_path = try buildJournalPath(allocator, journal_root_dir, journal_identity);
        const checkpoint_path = try buildCheckpointPath(allocator, journal_root_dir, journal_identity);
        const owned_instance_id = allocator.dupe(u8, effective_instance_id) catch |err| {
            allocator.free(journal_path);
            allocator.free(checkpoint_path);
            return err;
        };

        var self = Self{
            .allocator = allocator,
            .backend = backend,
            .journal_path = journal_path,
            .checkpoint_path = checkpoint_path,
            .instance_id = owned_instance_id,
        };
        errdefer self.deinitMembers();

        try ensureJournalParent(journal_path);
        try ensureJournalParent(checkpoint_path);

        var file = try self.openJournalExclusive();
        defer file.close();

        try self.loadCheckpoint();
        try self.refreshJournalLocked(&file);
        try self.rebuildProjectionFromCanonical();
        self.projection_offset_bytes = self.loaded_size_bytes;

        return self;
    }

    pub fn deinit(self: *Self) void {
        self.deinitMembers();
        self.backend.deinit();
        if (self.owns_self) self.allocator.destroy(self);
    }

    fn deinitMembers(self: *Self) void {
        self.allocator.free(self.journal_path);
        self.allocator.free(self.checkpoint_path);
        self.allocator.free(self.instance_id);
        self.clearJournalState();
    }

    fn clearJournalState(self: *Self) void {
        self.last_sequence = 0;
        self.last_timestamp_ms = 0;
        self.compacted_through_sequence = 0;
        self.loaded_size_bytes = 0;
        self.projection_offset_bytes = 0;

        var frontier_it = self.origin_frontiers.iterator();
        while (frontier_it.next()) |kv| self.allocator.free(kv.key_ptr.*);
        self.origin_frontiers.deinit(self.allocator);
        self.origin_frontiers = .{};

        var state_it = self.state_entries.iterator();
        while (state_it.next()) |kv| {
            self.allocator.free(kv.key_ptr.*);
            self.freeStoredState(kv.value_ptr.*);
        }
        self.state_entries.deinit(self.allocator);
        self.state_entries = .{};

        var scoped_it = self.scoped_tombstones.iterator();
        while (scoped_it.next()) |kv| {
            self.allocator.free(kv.key_ptr.*);
            self.freeEventMeta(kv.value_ptr.*);
        }
        self.scoped_tombstones.deinit(self.allocator);
        self.scoped_tombstones = .{};

        var key_it = self.key_tombstones.iterator();
        while (key_it.next()) |kv| {
            self.allocator.free(kv.key_ptr.*);
            self.freeEventMeta(kv.value_ptr.*);
        }
        self.key_tombstones.deinit(self.allocator);
        self.key_tombstones = .{};
    }

    pub fn memory(self: *Self) Memory {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    fn implName(ptr: *anyopaque) []const u8 {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        return self_.backend.name();
    }

    fn implStore(ptr: *anyopaque, key: []const u8, content: []const u8, category: MemoryCategory, session_id: ?[]const u8) anyerror!void {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        try self_.storeLocal(key, content, category, session_id);
    }

    fn implRecall(ptr: *anyopaque, allocator: std.mem.Allocator, query: []const u8, limit: usize, session_id: ?[]const u8) anyerror![]MemoryEntry {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        try self_.ensureProjectionUpToDate();
        return self_.backend.recall(allocator, query, limit, session_id) catch |err| {
            if (err != error.NotSupported) {
                log.warn("projection recall failed for overlay journal {s}: {}; falling back to canonical recall", .{
                    self_.journal_path,
                    err,
                });
            }
            return self_.recallCanonical(allocator, query, limit, session_id);
        };
    }

    fn implGet(ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8) anyerror!?MemoryEntry {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        try self_.ensureProjectionUpToDate();
        if (self_.findDefaultStatePtr(key)) |state| return try self_.cloneStateEntry(allocator, state.*);
        return null;
    }

    fn implGetScoped(ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8, session_id: ?[]const u8) anyerror!?MemoryEntry {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        try self_.ensureProjectionUpToDate();
        const storage_key = try key_codec.encode(allocator, key, session_id);
        defer allocator.free(storage_key);
        const state = self_.state_entries.getPtr(storage_key) orelse return null;
        return try self_.cloneStateEntry(allocator, state.*);
    }

    fn implList(ptr: *anyopaque, allocator: std.mem.Allocator, category: ?MemoryCategory, session_id: ?[]const u8) anyerror![]MemoryEntry {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        try self_.ensureProjectionUpToDate();
        return self_.listCanonical(allocator, category, session_id);
    }

    fn implForget(ptr: *anyopaque, key: []const u8) anyerror!bool {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        return self_.deleteLocalKey(key);
    }

    fn implForgetScoped(ptr: *anyopaque, key: []const u8, session_id: ?[]const u8) anyerror!bool {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        return self_.deleteLocalScoped(key, session_id);
    }

    fn implListEvents(ptr: *anyopaque, allocator: std.mem.Allocator, after_sequence: u64, limit: usize) anyerror![]MemoryEvent {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        var file = try self_.openJournalShared();
        defer file.close();
        try self_.refreshJournalLocked(&file);
        if (after_sequence < self_.compacted_through_sequence) return error.CursorExpired;
        return self_.readEvents(allocator, after_sequence, limit);
    }

    fn implApplyEvent(ptr: *anyopaque, input: MemoryEventInput) anyerror!void {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        try self_.applyRemoteEvent(input);
    }

    fn implLastEventSequence(ptr: *anyopaque) anyerror!u64 {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        var file = try self_.openJournalShared();
        defer file.close();
        try self_.refreshJournalLocked(&file);
        return self_.last_sequence;
    }

    fn implEventFeedInfo(ptr: *anyopaque, allocator: std.mem.Allocator) anyerror!MemoryEventFeedInfo {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        var file = try self_.openJournalShared();
        defer file.close();
        try self_.refreshJournalLocked(&file);
        return .{
            .instance_id = try allocator.dupe(u8, self_.instance_id),
            .last_sequence = self_.last_sequence,
            .next_local_origin_sequence = (self_.origin_frontiers.get(self_.instance_id) orelse 0) + 1,
            .supports_compaction = true,
            .storage_kind = .overlay,
            .journal_path = try allocator.dupe(u8, self_.journal_path),
            .checkpoint_path = try allocator.dupe(u8, self_.checkpoint_path),
            .compacted_through_sequence = self_.compacted_through_sequence,
            .oldest_available_sequence = self_.compacted_through_sequence + 1,
        };
    }

    fn implCompactEvents(ptr: *anyopaque) anyerror!u64 {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        return self_.compactEventsInternal();
    }

    fn implExportCheckpoint(ptr: *anyopaque, allocator: std.mem.Allocator) anyerror![]u8 {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        var file = try self_.openJournalShared();
        defer file.close();
        try self_.refreshJournalLocked(&file);
        return self_.serializeCheckpointPayload(allocator);
    }

    fn implApplyCheckpoint(ptr: *anyopaque, payload: []const u8) anyerror!void {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        try self_.applyCheckpointPayload(payload);
    }

    fn implCount(ptr: *anyopaque) anyerror!usize {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        var file = try self_.openJournalShared();
        defer file.close();
        try self_.refreshJournalLocked(&file);
        return self_.state_entries.count();
    }

    fn implHealthCheck(ptr: *anyopaque) bool {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        return self_.backend.healthCheck();
    }

    fn implDeinit(ptr: *anyopaque) void {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        self_.deinit();
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

    fn storeLocal(self: *Self, key: []const u8, content: []const u8, category: MemoryCategory, session_id: ?[]const u8) !void {
        var file = try self.openJournalExclusive();
        defer file.close();

        try self.refreshJournalLocked(&file);
        const input = try self.makeLocalInputLocked(.put, key, session_id, category, content);
        _ = try self.recordEventLocked(&file, input);
        try self.replayProjectionLocked(&file);
    }

    fn deleteLocalKey(self: *Self, key: []const u8) !bool {
        var file = try self.openJournalExclusive();
        defer file.close();

        try self.refreshJournalLocked(&file);
        const had_entry = self.hasAnyStateForKey(key);
        const input = try self.makeLocalInputLocked(.delete_all, key, null, null, null);
        _ = try self.recordEventLocked(&file, input);
        try self.replayProjectionLocked(&file);
        return had_entry;
    }

    fn deleteLocalScoped(self: *Self, key: []const u8, session_id: ?[]const u8) !bool {
        var file = try self.openJournalExclusive();
        defer file.close();

        try self.refreshJournalLocked(&file);
        const storage_key = try key_codec.encode(self.allocator, key, session_id);
        defer self.allocator.free(storage_key);
        const had_entry = self.state_entries.contains(storage_key);
        const input = try self.makeLocalInputLocked(.delete_scoped, key, session_id, null, null);
        _ = try self.recordEventLocked(&file, input);
        try self.replayProjectionLocked(&file);
        return had_entry;
    }

    fn applyRemoteEvent(self: *Self, input: MemoryEventInput) !void {
        var file = try self.openJournalExclusive();
        defer file.close();

        try self.refreshJournalLocked(&file);
        _ = try self.recordEventLocked(&file, input);
        try self.replayProjectionLocked(&file);
    }

    fn ensureProjectionUpToDate(self: *Self) !void {
        var file = try self.openJournalExclusive();
        defer file.close();
        try self.refreshJournalLocked(&file);
        try self.replayProjectionLocked(&file);
    }

    fn makeLocalInputLocked(
        self: *Self,
        operation: root.MemoryEventOp,
        key: []const u8,
        session_id: ?[]const u8,
        category: ?MemoryCategory,
        content: ?[]const u8,
    ) !MemoryEventInput {
        const next_origin_sequence = (self.origin_frontiers.get(self.instance_id) orelse 0) + 1;
        const now_ms = std.time.milliTimestamp();
        const timestamp_ms = if (now_ms > self.last_timestamp_ms) now_ms else self.last_timestamp_ms + 1;
        return .{
            .origin_instance_id = self.instance_id,
            .origin_sequence = next_origin_sequence,
            .timestamp_ms = timestamp_ms,
            .operation = operation,
            .key = key,
            .session_id = session_id,
            .category = category,
            .content = content,
        };
    }

    fn recordEventLocked(self: *Self, file: *std.fs.File, input: MemoryEventInput) !bool {
        const frontier = self.origin_frontiers.get(input.origin_instance_id) orelse 0;
        if (input.origin_sequence <= frontier) return false;

        var decision = try self.computeDecision(input);
        defer decision.deinit(self.allocator);
        const next_sequence = self.last_sequence + 1;
        const end_offset = try self.appendEventLineLocked(file, next_sequence, input, decision.effect, decision.resolved_state);
        try self.applyMetadataUpdate(next_sequence, input, decision.effect, decision.resolved_state);

        // The journal is canonical. The backend is a projection that may lag
        // behind temporarily if replay fails after the append.
        self.loaded_size_bytes = end_offset;
        return true;
    }

    fn replayProjectionLocked(self: *Self, file: *std.fs.File) !void {
        if (self.projection_offset_bytes >= self.loaded_size_bytes) return;

        try file.seekTo(self.projection_offset_bytes);

        const read_buf = try self.allocator.alloc(u8, MAX_EVENT_LINE_BYTES);
        defer self.allocator.free(read_buf);
        var reader = file.readerStreaming(read_buf);
        while (try reader.interface.takeDelimiter('\n')) |line_with_no_delim| {
            const line_end = try file.getPos();
            const line = std.mem.trim(u8, line_with_no_delim, " \t\r\n");
            if (line.len == 0) {
                self.projection_offset_bytes = line_end;
                continue;
            }

            var recorded = try parseRecordedEventLine(self.allocator, line);
            defer recorded.deinit(self.allocator);

            const input = memoryEventInput(recorded.event);
            try self.applyProjectionEffect(input, recorded.effect, recorded.resolved_state);
            self.projection_offset_bytes = line_end;
        }
    }

    fn refreshJournalLocked(self: *Self, file: *std.fs.File) !void {
        const end_pos = try file.getEndPos();
        if (end_pos < self.loaded_size_bytes) {
            self.clearJournalState();
            try self.loadCheckpoint();
            try self.loadJournalFromOffsetLocked(file, 0);
            return;
        }

        if (end_pos == self.loaded_size_bytes) return;
        try self.loadJournalFromOffsetLocked(file, self.loaded_size_bytes);
    }

    fn loadCheckpoint(self: *Self) !void {
        var file = std.fs.openFileAbsolute(self.checkpoint_path, .{
            .mode = .read_only,
        }) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };
        defer file.close();

        const read_buf = try self.allocator.alloc(u8, MAX_EVENT_LINE_BYTES);
        defer self.allocator.free(read_buf);
        var reader = file.readerStreaming(read_buf);
        while (try reader.interface.takeDelimiter('\n')) |line_with_no_delim| {
            const line = std.mem.trim(u8, line_with_no_delim, " \t\r\n");
            if (line.len == 0) continue;
            try self.applyCheckpointLine(line);
        }
    }

    fn loadJournalFromOffsetLocked(self: *Self, file: *std.fs.File, start_offset: u64) !void {
        try file.seekTo(start_offset);

        const read_buf = try self.allocator.alloc(u8, MAX_EVENT_LINE_BYTES);
        defer self.allocator.free(read_buf);
        var reader = file.readerStreaming(read_buf);
        while (try reader.interface.takeDelimiter('\n')) |line_with_no_delim| {
            const line_end = try file.getPos();
            const line = std.mem.trim(u8, line_with_no_delim, " \t\r\n");
            if (line.len == 0) {
                self.loaded_size_bytes = line_end;
                continue;
            }

            var recorded = try parseRecordedEventLine(self.allocator, line);
            defer recorded.deinit(self.allocator);

            const input = memoryEventInput(recorded.event);
            try self.applyMetadataUpdate(recorded.event.sequence, input, recorded.effect, recorded.resolved_state);
            self.loaded_size_bytes = line_end;
        }
    }

    fn compactEventsInternal(self: *Self) !u64 {
        var file = try self.openJournalExclusive();
        defer file.close();

        try self.refreshJournalLocked(&file);
        try self.writeCheckpoint();
        try file.setEndPos(0);
        try file.sync();

        self.loaded_size_bytes = 0;
        self.projection_offset_bytes = 0;
        self.compacted_through_sequence = self.last_sequence;
        return self.compacted_through_sequence;
    }

    fn rebuildProjectionFromCanonical(self: *Self) !void {
        const projected = try self.backend.list(self.allocator, null, null);
        defer root.freeEntries(self.allocator, projected);

        for (projected) |entry| {
            const storage_key = try key_codec.encode(self.allocator, entry.key, entry.session_id);
            defer self.allocator.free(storage_key);

            const canonical = self.state_entries.getPtr(storage_key);
            const matches = canonical != null and
                std.mem.eql(u8, canonical.?.content, entry.content) and
                canonical.?.category.eql(entry.category);
            if (!matches) {
                _ = try self.backend.forgetScoped(self.allocator, entry.key, entry.session_id);
            }
        }

        var it = self.state_entries.iterator();
        while (it.next()) |kv| {
            const state = kv.value_ptr.*;
            const existing = try self.backend.getScoped(self.allocator, state.key, state.session_id);
            defer if (existing) |entry| entry.deinit(self.allocator);

            const needs_upsert = if (existing) |entry|
                !std.mem.eql(u8, entry.content, state.content) or !entry.category.eql(state.category)
            else
                true;
            if (needs_upsert) {
                try self.backend.store(state.key, state.content, state.category, state.session_id);
            }
        }
    }

    fn writeCheckpoint(self: *Self) !void {
        var file = try std.fs.createFileAbsolute(self.checkpoint_path, .{
            .truncate = true,
            .read = true,
        });
        defer file.close();

        var buf: [4096]u8 = undefined;
        var file_writer = file.writer(&buf);
        const writer = &file_writer.interface;
        try self.writeCheckpointToWriter(writer);
        try writer.flush();
        try file.sync();
    }

    fn writeCheckpointToWriter(self: *Self, writer: anytype) !void {
        try self.writeCheckpointMetaLine(writer);

        var frontier_it = self.origin_frontiers.iterator();
        while (frontier_it.next()) |kv| {
            try self.writeCheckpointFrontierLine(writer, kv.key_ptr.*, kv.value_ptr.*);
        }

        var state_it = self.state_entries.iterator();
        while (state_it.next()) |kv| {
            try self.writeCheckpointStateLine(writer, kv.value_ptr.*);
        }

        var scoped_it = self.scoped_tombstones.iterator();
        while (scoped_it.next()) |kv| {
            try self.writeCheckpointTombstoneLine(writer, "scoped_tombstone", kv.key_ptr.*, kv.value_ptr.*);
        }

        var key_it = self.key_tombstones.iterator();
        while (key_it.next()) |kv| {
            try self.writeCheckpointTombstoneLine(writer, "key_tombstone", kv.key_ptr.*, kv.value_ptr.*);
        }
    }

    fn serializeCheckpointPayload(self: *Self, allocator: std.mem.Allocator) ![]u8 {
        var out: std.ArrayListUnmanaged(u8) = .empty;
        errdefer out.deinit(allocator);
        const writer = out.writer(allocator);
        try self.writeCheckpointToWriter(writer);
        return out.toOwnedSlice(allocator);
    }

    fn applyCheckpointPayload(self: *Self, payload: []const u8) !void {
        var scratch = Self{
            .allocator = self.allocator,
            .backend = self.backend,
            .journal_path = try self.allocator.dupe(u8, ""),
            .checkpoint_path = try self.allocator.dupe(u8, ""),
            .instance_id = try self.allocator.dupe(u8, self.instance_id),
        };
        defer scratch.deinitMembers();

        var lines = std.mem.splitScalar(u8, payload, '\n');
        while (lines.next()) |raw_line| {
            const line = std.mem.trim(u8, raw_line, " \t\r\n");
            if (line.len == 0) continue;
            try scratch.applyCheckpointLine(line);
        }

        var checkpoint_file = try std.fs.createFileAbsolute(self.checkpoint_path, .{
            .truncate = true,
            .read = true,
        });
        defer checkpoint_file.close();
        try checkpoint_file.writeAll(payload);
        try checkpoint_file.sync();

        var journal_file = try self.openJournalExclusive();
        defer journal_file.close();
        try journal_file.setEndPos(0);
        try journal_file.sync();

        self.clearJournalState();
        try self.loadCheckpoint();
        try self.rebuildProjectionFromCanonical();
        self.loaded_size_bytes = 0;
        self.projection_offset_bytes = 0;
    }

    fn writeCheckpointMetaLine(self: *Self, writer: anytype) !void {
        var payload: std.ArrayListUnmanaged(u8) = .empty;
        defer payload.deinit(self.allocator);

        try payload.append(self.allocator, '{');
        try json_util.appendJsonKeyValue(&payload, self.allocator, "kind", "meta");
        try payload.append(self.allocator, ',');
        try json_util.appendJsonInt(&payload, self.allocator, "schema_version", 1);
        try payload.append(self.allocator, ',');
        try json_util.appendJsonKey(&payload, self.allocator, "last_sequence");
        try payload.writer(self.allocator).print("{d}", .{self.last_sequence});
        try payload.append(self.allocator, ',');
        try json_util.appendJsonKey(&payload, self.allocator, "last_timestamp_ms");
        try payload.writer(self.allocator).print("{d}", .{self.last_timestamp_ms});
        try payload.append(self.allocator, ',');
        try json_util.appendJsonKey(&payload, self.allocator, "compacted_through_sequence");
        try payload.writer(self.allocator).print("{d}", .{self.last_sequence});
        try payload.appendSlice(self.allocator, "}\n");
        try writer.writeAll(payload.items);
    }

    fn writeCheckpointFrontierLine(self: *Self, writer: anytype, origin_instance_id: []const u8, origin_sequence: u64) !void {
        var payload: std.ArrayListUnmanaged(u8) = .empty;
        defer payload.deinit(self.allocator);

        try payload.append(self.allocator, '{');
        try json_util.appendJsonKeyValue(&payload, self.allocator, "kind", "frontier");
        try payload.append(self.allocator, ',');
        try json_util.appendJsonKeyValue(&payload, self.allocator, "origin_instance_id", origin_instance_id);
        try payload.append(self.allocator, ',');
        try json_util.appendJsonKey(&payload, self.allocator, "origin_sequence");
        try payload.writer(self.allocator).print("{d}", .{origin_sequence});
        try payload.appendSlice(self.allocator, "}\n");
        try writer.writeAll(payload.items);
    }

    fn writeCheckpointStateLine(self: *Self, writer: anytype, state: StoredState) !void {
        var payload: std.ArrayListUnmanaged(u8) = .empty;
        defer payload.deinit(self.allocator);

        try payload.append(self.allocator, '{');
        try json_util.appendJsonKeyValue(&payload, self.allocator, "kind", "state");
        try payload.append(self.allocator, ',');
        try json_util.appendJsonKeyValue(&payload, self.allocator, "key", state.key);
        try payload.append(self.allocator, ',');
        try json_util.appendJsonKey(&payload, self.allocator, "session_id");
        if (state.session_id) |sid| {
            try json_util.appendJsonString(&payload, self.allocator, sid);
        } else {
            try payload.appendSlice(self.allocator, "null");
        }
        try payload.append(self.allocator, ',');
        try json_util.appendJsonKeyValue(&payload, self.allocator, "category", state.category.toString());
        try payload.append(self.allocator, ',');
        try json_util.appendJsonKey(&payload, self.allocator, "value_kind");
        if (state.value_kind) |value_kind| {
            try json_util.appendJsonString(&payload, self.allocator, value_kind.toString());
        } else {
            try payload.appendSlice(self.allocator, "null");
        }
        try payload.append(self.allocator, ',');
        try json_util.appendJsonKeyValue(&payload, self.allocator, "content", state.content);
        try payload.append(self.allocator, ',');
        try json_util.appendJsonKey(&payload, self.allocator, "timestamp_ms");
        try payload.writer(self.allocator).print("{d}", .{state.timestamp_ms});
        try payload.append(self.allocator, ',');
        try json_util.appendJsonKeyValue(&payload, self.allocator, "origin_instance_id", state.origin_instance_id);
        try payload.append(self.allocator, ',');
        try json_util.appendJsonKey(&payload, self.allocator, "origin_sequence");
        try payload.writer(self.allocator).print("{d}", .{state.origin_sequence});
        try payload.appendSlice(self.allocator, "}\n");
        try writer.writeAll(payload.items);
    }

    fn writeCheckpointTombstoneLine(self: *Self, writer: anytype, kind: []const u8, key: []const u8, meta: EventMeta) !void {
        var payload: std.ArrayListUnmanaged(u8) = .empty;
        defer payload.deinit(self.allocator);

        try payload.append(self.allocator, '{');
        try json_util.appendJsonKeyValue(&payload, self.allocator, "kind", kind);
        try payload.append(self.allocator, ',');
        try json_util.appendJsonKeyValue(&payload, self.allocator, "key", key);
        try payload.append(self.allocator, ',');
        try json_util.appendJsonKey(&payload, self.allocator, "timestamp_ms");
        try payload.writer(self.allocator).print("{d}", .{meta.timestamp_ms});
        try payload.append(self.allocator, ',');
        try json_util.appendJsonKeyValue(&payload, self.allocator, "origin_instance_id", meta.origin_instance_id);
        try payload.append(self.allocator, ',');
        try json_util.appendJsonKey(&payload, self.allocator, "origin_sequence");
        try payload.writer(self.allocator).print("{d}", .{meta.origin_sequence});
        try payload.appendSlice(self.allocator, "}\n");
        try writer.writeAll(payload.items);
    }

    fn applyCheckpointLine(self: *Self, line: []const u8) !void {
        var parsed = try std.json.parseFromSlice(std.json.Value, self.allocator, line, .{});
        defer parsed.deinit();

        const kind = jsonStringField(parsed.value, "kind") orelse return error.InvalidEvent;
        if (std.mem.eql(u8, kind, "meta")) {
            self.last_sequence = jsonUnsignedField(parsed.value, "last_sequence") orelse 0;
            self.last_timestamp_ms = jsonIntegerField(parsed.value, "last_timestamp_ms") orelse 0;
            self.compacted_through_sequence = jsonUnsignedField(parsed.value, "compacted_through_sequence") orelse self.last_sequence;
            return;
        }
        if (std.mem.eql(u8, kind, "frontier")) {
            const origin_instance_id = jsonStringField(parsed.value, "origin_instance_id") orelse return error.InvalidEvent;
            const origin_sequence = jsonUnsignedField(parsed.value, "origin_sequence") orelse return error.InvalidEvent;
            try self.rememberOriginFrontier(origin_instance_id, origin_sequence);
            return;
        }
        if (std.mem.eql(u8, kind, "state")) {
            const key = jsonStringField(parsed.value, "key") orelse return error.InvalidEvent;
            const content = jsonStringField(parsed.value, "content") orelse return error.InvalidEvent;
            const category_str = jsonStringField(parsed.value, "category") orelse return error.InvalidEvent;
            const category = try dupCategory(self.allocator, MemoryCategory.fromString(category_str));
            errdefer switch (category) {
                .custom => |name| self.allocator.free(name),
                else => {},
            };
            const value_kind = if (jsonNullableStringField(parsed.value, "value_kind")) |kind_name|
                MemoryValueKind.fromString(kind_name) orelse return error.InvalidEvent
            else
                null;
            const timestamp_ms = jsonIntegerField(parsed.value, "timestamp_ms") orelse return error.InvalidEvent;
            const origin_instance_id = jsonStringField(parsed.value, "origin_instance_id") orelse return error.InvalidEvent;
            const origin_sequence = jsonUnsignedField(parsed.value, "origin_sequence") orelse return error.InvalidEvent;
            try self.restoreCheckpointState(
                key,
                if (jsonNullableStringField(parsed.value, "session_id")) |sid| sid else null,
                category,
                value_kind,
                content,
                timestamp_ms,
                origin_instance_id,
                origin_sequence,
            );
            return;
        }
        if (std.mem.eql(u8, kind, "scoped_tombstone") or std.mem.eql(u8, kind, "key_tombstone")) {
            const key = jsonStringField(parsed.value, "key") orelse return error.InvalidEvent;
            const timestamp_ms = jsonIntegerField(parsed.value, "timestamp_ms") orelse return error.InvalidEvent;
            const origin_instance_id = jsonStringField(parsed.value, "origin_instance_id") orelse return error.InvalidEvent;
            const origin_sequence = jsonUnsignedField(parsed.value, "origin_sequence") orelse return error.InvalidEvent;
            try self.restoreCheckpointTombstone(kind, key, timestamp_ms, origin_instance_id, origin_sequence);
            return;
        }
        return error.InvalidEvent;
    }

    fn restoreCheckpointState(
        self: *Self,
        key: []const u8,
        session_id: ?[]const u8,
        category: MemoryCategory,
        value_kind: ?MemoryValueKind,
        content: []const u8,
        timestamp_ms: i64,
        origin_instance_id: []const u8,
        origin_sequence: u64,
    ) !void {
        const storage_key = try key_codec.encode(self.allocator, key, session_id);
        defer self.allocator.free(storage_key);

        const state = StoredState{
            .key = try self.allocator.dupe(u8, key),
            .content = try self.allocator.dupe(u8, content),
            .category = category,
            .value_kind = value_kind,
            .session_id = if (session_id) |sid| try self.allocator.dupe(u8, sid) else null,
            .timestamp_ms = timestamp_ms,
            .origin_instance_id = try self.allocator.dupe(u8, origin_instance_id),
            .origin_sequence = origin_sequence,
        };
        errdefer self.freeStoredState(state);

        if (self.state_entries.getPtr(storage_key)) |existing| {
            self.freeStoredState(existing.*);
            existing.* = state;
            return;
        }
        try self.state_entries.put(self.allocator, try self.allocator.dupe(u8, storage_key), state);
    }

    fn restoreCheckpointTombstone(
        self: *Self,
        kind: []const u8,
        key: []const u8,
        timestamp_ms: i64,
        origin_instance_id: []const u8,
        origin_sequence: u64,
    ) !void {
        const meta = EventMeta{
            .timestamp_ms = timestamp_ms,
            .origin_instance_id = try self.allocator.dupe(u8, origin_instance_id),
            .origin_sequence = origin_sequence,
        };
        errdefer self.freeEventMeta(meta);

        if (std.mem.eql(u8, kind, "scoped_tombstone")) {
            if (self.scoped_tombstones.getPtr(key)) |existing| {
                self.freeEventMeta(existing.*);
                existing.* = meta;
                return;
            }
            try self.scoped_tombstones.put(self.allocator, try self.allocator.dupe(u8, key), meta);
            return;
        }

        if (self.key_tombstones.getPtr(key)) |existing| {
            self.freeEventMeta(existing.*);
            existing.* = meta;
            return;
        }
        try self.key_tombstones.put(self.allocator, try self.allocator.dupe(u8, key), meta);
    }

    fn computeDecision(self: *Self, input: MemoryEventInput) !EventDecision {
        return switch (input.operation) {
            .put, .merge_object, .merge_string_set => blk: {
                const storage_key = try key_codec.encode(self.allocator, input.key, input.session_id);
                defer self.allocator.free(storage_key);

                if (self.key_tombstones.get(input.key)) |meta| {
                    if (compareMeta(meta, input) <= 0) break :blk .{ .effect = .none };
                }
                if (self.scoped_tombstones.get(storage_key)) |meta| {
                    if (compareMeta(meta, input) <= 0) break :blk .{ .effect = .none };
                }
                const existing = self.state_entries.getPtr(storage_key);
                if (existing) |state| {
                    if (compareStoredState(state.*, input) <= 0) break :blk .{ .effect = .none };
                }
                break :blk .{
                    .effect = .put,
                    .resolved_state = try root.resolveMemoryEventState(
                        self.allocator,
                        if (existing) |state| state.content else null,
                        if (existing) |state| state.category else null,
                        if (existing) |state| state.value_kind else null,
                        input,
                    ) orelse return error.InvalidEvent,
                };
            },
            .delete_scoped => blk: {
                const storage_key = try key_codec.encode(self.allocator, input.key, input.session_id);
                defer self.allocator.free(storage_key);

                if (self.key_tombstones.get(input.key)) |meta| {
                    if (compareMeta(meta, input) <= 0) break :blk .{ .effect = .none };
                }
                if (self.scoped_tombstones.get(storage_key)) |meta| {
                    if (compareMeta(meta, input) <= 0) break :blk .{ .effect = .none };
                }
                break :blk .{ .effect = .delete_scoped };
            },
            .delete_all => blk: {
                if (self.key_tombstones.get(input.key)) |meta| {
                    if (compareMeta(meta, input) <= 0) break :blk .{ .effect = .none };
                }
                break :blk .{ .effect = .delete_all };
            },
        };
    }

    fn applyMetadataUpdate(
        self: *Self,
        sequence: u64,
        input: MemoryEventInput,
        effect: Effect,
        resolved_state: ?ResolvedMemoryState,
    ) !void {
        self.last_sequence = @max(self.last_sequence, sequence);
        self.last_timestamp_ms = @max(self.last_timestamp_ms, input.timestamp_ms);
        try self.rememberOriginFrontier(input.origin_instance_id, input.origin_sequence);

        switch (effect) {
            .none => {},
            .put => {
                const storage_key = try key_codec.encode(self.allocator, input.key, input.session_id);
                defer self.allocator.free(storage_key);
                const state = resolved_state orelse return error.InvalidEvent;
                try self.upsertStateEntry(storage_key, input, state);
            },
            .delete_scoped => {
                const storage_key = try key_codec.encode(self.allocator, input.key, input.session_id);
                defer self.allocator.free(storage_key);
                try self.removeStateEntry(storage_key);
                try self.rememberScopedTombstone(storage_key, input);
            },
            .delete_all => {
                try self.removeStateEntriesForKey(input.key);
                try self.rememberKeyTombstone(input.key, input);
            },
        }
    }

    fn applyProjectionEffect(
        self: *Self,
        input: MemoryEventInput,
        effect: Effect,
        resolved_state: ?ResolvedMemoryState,
    ) !void {
        switch (effect) {
            .none => {},
            .put => {
                const state = resolved_state orelse return error.InvalidEvent;
                try self.backend.store(input.key, state.content, state.category, input.session_id);
            },
            .delete_scoped => {
                _ = try self.backend.forgetScoped(self.allocator, input.key, input.session_id);
            },
            .delete_all => {
                _ = try self.backend.forget(input.key);
            },
        }
    }

    fn rememberOriginFrontier(self: *Self, origin_instance_id: []const u8, origin_sequence: u64) !void {
        if (self.origin_frontiers.getPtr(origin_instance_id)) |existing| {
            existing.* = @max(existing.*, origin_sequence);
            return;
        }
        try self.origin_frontiers.put(self.allocator, try self.allocator.dupe(u8, origin_instance_id), origin_sequence);
    }

    fn upsertStateEntry(self: *Self, storage_key: []const u8, input: MemoryEventInput, resolved_state: ResolvedMemoryState) !void {
        const state = try self.dupStoredState(input, resolved_state);
        errdefer self.freeStoredState(state);
        if (self.state_entries.getPtr(storage_key)) |existing| {
            self.freeStoredState(existing.*);
            existing.* = state;
            return;
        }
        try self.state_entries.put(self.allocator, try self.allocator.dupe(u8, storage_key), state);
    }

    fn removeStateEntry(self: *Self, storage_key: []const u8) !void {
        if (self.state_entries.fetchRemove(storage_key)) |removed| {
            self.allocator.free(removed.key);
            self.freeStoredState(removed.value);
        }
    }

    fn rememberScopedTombstone(self: *Self, storage_key: []const u8, input: MemoryEventInput) !void {
        const meta = try self.dupEventMeta(input);
        errdefer self.freeEventMeta(meta);
        if (self.scoped_tombstones.getPtr(storage_key)) |existing| {
            if (compareMeta(existing.*, input) >= 0) {
                self.freeEventMeta(meta);
                return;
            }
            self.freeEventMeta(existing.*);
            existing.* = meta;
            return;
        }
        try self.scoped_tombstones.put(self.allocator, try self.allocator.dupe(u8, storage_key), meta);
    }

    fn rememberKeyTombstone(self: *Self, key: []const u8, input: MemoryEventInput) !void {
        const meta = try self.dupEventMeta(input);
        errdefer self.freeEventMeta(meta);
        if (self.key_tombstones.getPtr(key)) |existing| {
            if (compareMeta(existing.*, input) >= 0) {
                self.freeEventMeta(meta);
                return;
            }
            self.freeEventMeta(existing.*);
            existing.* = meta;
            return;
        }
        try self.key_tombstones.put(self.allocator, try self.allocator.dupe(u8, key), meta);
    }

    fn dupEventMeta(self: *Self, input: MemoryEventInput) !EventMeta {
        return .{
            .timestamp_ms = input.timestamp_ms,
            .origin_instance_id = try self.allocator.dupe(u8, input.origin_instance_id),
            .origin_sequence = input.origin_sequence,
        };
    }

    fn dupStoredState(self: *Self, input: MemoryEventInput, resolved_state: ResolvedMemoryState) !StoredState {
        return .{
            .key = try self.allocator.dupe(u8, input.key),
            .content = try self.allocator.dupe(u8, resolved_state.content),
            .category = try dupCategory(self.allocator, resolved_state.category),
            .value_kind = resolved_state.value_kind,
            .session_id = if (input.session_id) |sid| try self.allocator.dupe(u8, sid) else null,
            .timestamp_ms = input.timestamp_ms,
            .origin_instance_id = try self.allocator.dupe(u8, input.origin_instance_id),
            .origin_sequence = input.origin_sequence,
        };
    }

    fn freeEventMeta(self: *Self, meta: EventMeta) void {
        self.allocator.free(meta.origin_instance_id);
    }

    fn freeStoredState(self: *Self, state: StoredState) void {
        self.allocator.free(state.key);
        self.allocator.free(state.content);
        self.allocator.free(state.origin_instance_id);
        if (state.session_id) |sid| self.allocator.free(sid);
        switch (state.category) {
            .custom => |name| self.allocator.free(name),
            else => {},
        }
    }

    fn hasAnyStateForKey(self: *Self, key: []const u8) bool {
        var it = self.state_entries.iterator();
        while (it.next()) |kv| {
            if (std.mem.eql(u8, kv.value_ptr.key, key)) return true;
        }
        return false;
    }

    fn removeStateEntriesForKey(self: *Self, key: []const u8) !void {
        var to_remove: std.ArrayListUnmanaged([]u8) = .empty;
        defer {
            for (to_remove.items) |owned| self.allocator.free(owned);
            to_remove.deinit(self.allocator);
        }

        var it = self.state_entries.iterator();
        while (it.next()) |kv| {
            if (std.mem.eql(u8, kv.value_ptr.key, key)) {
                try to_remove.append(self.allocator, try self.allocator.dupe(u8, kv.key_ptr.*));
            }
        }

        for (to_remove.items) |storage_key| {
            try self.removeStateEntry(storage_key);
        }
    }

    fn findDefaultStatePtr(self: *Self, key: []const u8) ?*StoredState {
        var it = self.state_entries.iterator();
        while (it.next()) |kv| {
            if (!std.mem.eql(u8, kv.value_ptr.key, key)) continue;
            if (kv.value_ptr.session_id == null) return kv.value_ptr;
        }
        return null;
    }

    fn cloneStateEntry(self: *Self, allocator: std.mem.Allocator, state: StoredState) !MemoryEntry {
        _ = self;
        return .{
            .id = try key_codec.encode(allocator, state.key, state.session_id),
            .key = try allocator.dupe(u8, state.key),
            .content = try allocator.dupe(u8, state.content),
            .category = try dupCategory(allocator, state.category),
            .timestamp = try std.fmt.allocPrint(allocator, "{d}", .{state.timestamp_ms}),
            .session_id = if (state.session_id) |sid| try allocator.dupe(u8, sid) else null,
        };
    }

    fn listCanonical(self: *Self, allocator: std.mem.Allocator, category: ?MemoryCategory, session_id: ?[]const u8) ![]MemoryEntry {
        var results: std.ArrayListUnmanaged(MemoryEntry) = .empty;
        errdefer {
            for (results.items) |*entry| entry.deinit(allocator);
            results.deinit(allocator);
        }

        var it = self.state_entries.iterator();
        while (it.next()) |kv| {
            const state = kv.value_ptr.*;
            if (category) |cat| {
                if (!state.category.eql(cat)) continue;
            }
            if (session_id) |sid| {
                if (state.session_id) |entry_sid| {
                    if (!std.mem.eql(u8, entry_sid, sid)) continue;
                } else continue;
            }
            try results.append(allocator, try self.cloneStateEntry(allocator, state));
        }

        return results.toOwnedSlice(allocator);
    }

    fn recallCanonical(self: *Self, allocator: std.mem.Allocator, query: []const u8, limit: usize, session_id: ?[]const u8) ![]MemoryEntry {
        const trimmed = std.mem.trim(u8, query, " \t\r\n");
        if (trimmed.len == 0 or limit == 0) return allocator.alloc(MemoryEntry, 0);

        const Match = struct {
            state: StoredState,
        };
        var matches: std.ArrayListUnmanaged(Match) = .empty;
        defer matches.deinit(allocator);

        var it = self.state_entries.iterator();
        while (it.next()) |kv| {
            const state = kv.value_ptr.*;
            if (session_id) |sid| {
                if (state.session_id) |entry_sid| {
                    if (!std.mem.eql(u8, entry_sid, sid)) continue;
                } else continue;
            }
            if (std.mem.indexOf(u8, state.key, trimmed) == null and std.mem.indexOf(u8, state.content, trimmed) == null) continue;
            try matches.append(allocator, .{ .state = state });
        }

        std.mem.sort(Match, matches.items, {}, struct {
            fn lessThan(_: void, a: Match, b: Match) bool {
                return compareStoredStates(a.state, b.state) > 0;
            }
        }.lessThan);

        const result_len = @min(matches.items.len, limit);
        const results = try allocator.alloc(MemoryEntry, result_len);
        var filled: usize = 0;
        errdefer {
            for (results[0..filled]) |*entry| entry.deinit(allocator);
            allocator.free(results);
        }

        for (results, 0..) |*slot, idx| {
            slot.* = try self.cloneStateEntry(allocator, matches.items[idx].state);
            filled += 1;
        }

        return results;
    }

    fn appendEventLineLocked(
        self: *Self,
        file: *std.fs.File,
        sequence: u64,
        input: MemoryEventInput,
        effect: Effect,
        resolved_state: ?ResolvedMemoryState,
    ) !u64 {
        var payload: std.ArrayListUnmanaged(u8) = .empty;
        defer payload.deinit(self.allocator);

        try payload.append(self.allocator, '{');
        try json_util.appendJsonInt(&payload, self.allocator, "schema_version", 1);
        try payload.append(self.allocator, ',');
        try json_util.appendJsonKey(&payload, self.allocator, "sequence");
        try payload.writer(self.allocator).print("{d}", .{sequence});
        try payload.append(self.allocator, ',');
        try json_util.appendJsonKeyValue(&payload, self.allocator, "origin_instance_id", input.origin_instance_id);
        try payload.append(self.allocator, ',');
        try json_util.appendJsonKey(&payload, self.allocator, "origin_sequence");
        try payload.writer(self.allocator).print("{d}", .{input.origin_sequence});
        try payload.append(self.allocator, ',');
        try json_util.appendJsonKey(&payload, self.allocator, "timestamp_ms");
        try payload.writer(self.allocator).print("{d}", .{input.timestamp_ms});
        try payload.append(self.allocator, ',');
        try json_util.appendJsonKeyValue(&payload, self.allocator, "operation", input.operation.toString());
        try payload.append(self.allocator, ',');
        try json_util.appendJsonKeyValue(&payload, self.allocator, "effect", effect.toString());
        try payload.append(self.allocator, ',');
        try json_util.appendJsonKeyValue(&payload, self.allocator, "key", input.key);
        try payload.append(self.allocator, ',');
        try json_util.appendJsonKey(&payload, self.allocator, "session_id");
        if (input.session_id) |sid| {
            try json_util.appendJsonString(&payload, self.allocator, sid);
        } else {
            try payload.appendSlice(self.allocator, "null");
        }
        try payload.append(self.allocator, ',');
        try json_util.appendJsonKey(&payload, self.allocator, "category");
        if (input.category) |category| {
            try json_util.appendJsonString(&payload, self.allocator, category.toString());
        } else {
            try payload.appendSlice(self.allocator, "null");
        }
        try payload.append(self.allocator, ',');
        try json_util.appendJsonKey(&payload, self.allocator, "value_kind");
        if (input.value_kind) |value_kind| {
            try json_util.appendJsonString(&payload, self.allocator, value_kind.toString());
        } else {
            try payload.appendSlice(self.allocator, "null");
        }
        try payload.append(self.allocator, ',');
        try json_util.appendJsonKey(&payload, self.allocator, "content");
        if (input.content) |content| {
            try json_util.appendJsonString(&payload, self.allocator, content);
        } else {
            try payload.appendSlice(self.allocator, "null");
        }
        if (resolved_state) |state| {
            try payload.append(self.allocator, ',');
            try json_util.appendJsonKey(&payload, self.allocator, "resolved_category");
            try json_util.appendJsonString(&payload, self.allocator, state.category.toString());
            try payload.append(self.allocator, ',');
            try json_util.appendJsonKey(&payload, self.allocator, "resolved_value_kind");
            if (state.value_kind) |value_kind| {
                try json_util.appendJsonString(&payload, self.allocator, value_kind.toString());
            } else {
                try payload.appendSlice(self.allocator, "null");
            }
            try payload.append(self.allocator, ',');
            try json_util.appendJsonKey(&payload, self.allocator, "resolved_content");
            try json_util.appendJsonString(&payload, self.allocator, state.content);
        }
        try payload.appendSlice(self.allocator, "}\n");

        try file.seekFromEnd(0);
        try file.writeAll(payload.items);
        try file.sync();
        return try file.getPos();
    }

    fn readEvents(self: *Self, allocator: std.mem.Allocator, after_sequence: u64, limit: usize) ![]MemoryEvent {
        if (limit == 0) return allocator.alloc(MemoryEvent, 0);

        var file = try self.openJournalShared();
        defer file.close();

        try file.seekTo(0);

        const read_buf = try self.allocator.alloc(u8, MAX_EVENT_LINE_BYTES);
        defer self.allocator.free(read_buf);
        var reader = file.readerStreaming(read_buf);
        var events: std.ArrayListUnmanaged(MemoryEvent) = .empty;
        errdefer {
            for (events.items) |*event| event.deinit(allocator);
            events.deinit(allocator);
        }

        while (try reader.interface.takeDelimiter('\n')) |line_with_no_delim| {
            const line = std.mem.trim(u8, line_with_no_delim, " \t\r\n");
            if (line.len == 0) continue;

            var recorded = try parseRecordedEventLine(allocator, line);
            errdefer recorded.deinit(allocator);
            if (recorded.event.sequence <= after_sequence) {
                recorded.deinit(allocator);
                continue;
            }

            if (recorded.resolved_state) |*state| {
                state.deinit(allocator);
                recorded.resolved_state = null;
            }
            try events.append(allocator, recorded.event);
            recorded.event = undefined;
            if (events.items.len >= limit) break;
        }

        return events.toOwnedSlice(allocator);
    }

    fn openJournalExclusive(self: *Self) !std.fs.File {
        return std.fs.createFileAbsolute(self.journal_path, .{
            .read = true,
            .truncate = false,
            .lock = .exclusive,
        });
    }

    fn openJournalShared(self: *Self) !std.fs.File {
        return std.fs.openFileAbsolute(self.journal_path, .{
            .mode = .read_only,
            .lock = .shared,
        }) catch |err| switch (err) {
            error.FileNotFound => blk: {
                var created = try std.fs.createFileAbsolute(self.journal_path, .{
                    .read = true,
                    .truncate = false,
                });
                created.close();
                break :blk try std.fs.openFileAbsolute(self.journal_path, .{
                    .mode = .read_only,
                    .lock = .shared,
                });
            },
            else => err,
        };
    }
};

fn compareMeta(meta: EventFeedOverlay.EventMeta, input: MemoryEventInput) i8 {
    if (input.timestamp_ms < meta.timestamp_ms) return -1;
    if (input.timestamp_ms > meta.timestamp_ms) return 1;

    const order = std.mem.order(u8, input.origin_instance_id, meta.origin_instance_id);
    if (order == .lt) return -1;
    if (order == .gt) return 1;

    if (input.origin_sequence < meta.origin_sequence) return -1;
    if (input.origin_sequence > meta.origin_sequence) return 1;
    return 0;
}

fn compareStoredState(state: EventFeedOverlay.StoredState, input: MemoryEventInput) i8 {
    if (input.timestamp_ms < state.timestamp_ms) return -1;
    if (input.timestamp_ms > state.timestamp_ms) return 1;

    const order = std.mem.order(u8, input.origin_instance_id, state.origin_instance_id);
    if (order == .lt) return -1;
    if (order == .gt) return 1;

    if (input.origin_sequence < state.origin_sequence) return -1;
    if (input.origin_sequence > state.origin_sequence) return 1;
    return 0;
}

fn compareStoredStates(a: EventFeedOverlay.StoredState, b: EventFeedOverlay.StoredState) i8 {
    if (a.timestamp_ms < b.timestamp_ms) return -1;
    if (a.timestamp_ms > b.timestamp_ms) return 1;

    const order = std.mem.order(u8, a.origin_instance_id, b.origin_instance_id);
    if (order == .lt) return -1;
    if (order == .gt) return 1;

    if (a.origin_sequence < b.origin_sequence) return -1;
    if (a.origin_sequence > b.origin_sequence) return 1;
    return 0;
}

fn ensureJournalParent(journal_path: []const u8) !void {
    const parent = std.fs.path.dirname(journal_path) orelse return;
    std.fs.makeDirAbsolute(parent) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => try fs_compat.makePath(parent),
    };
}

fn buildJournalPath(allocator: std.mem.Allocator, journal_root_dir: []const u8, journal_identity: []const u8) ![]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(journal_identity, &digest, .{});
    const hex = std.fmt.bytesToHex(digest, .lower);
    const filename = try std.fmt.allocPrint(allocator, ".nullclaw-feed.{s}.jsonl", .{hex[0..]});
    defer allocator.free(filename);
    return std.fs.path.join(allocator, &.{ journal_root_dir, filename });
}

fn buildCheckpointPath(allocator: std.mem.Allocator, journal_root_dir: []const u8, journal_identity: []const u8) ![]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(journal_identity, &digest, .{});
    const hex = std.fmt.bytesToHex(digest, .lower);
    const filename = try std.fmt.allocPrint(allocator, ".nullclaw-feed.{s}.checkpoint.jsonl", .{hex[0..]});
    defer allocator.free(filename);
    return std.fs.path.join(allocator, &.{ journal_root_dir, filename });
}

fn memoryEventInput(event: MemoryEvent) MemoryEventInput {
    return .{
        .origin_instance_id = event.origin_instance_id,
        .origin_sequence = event.origin_sequence,
        .timestamp_ms = event.timestamp_ms,
        .operation = event.operation,
        .key = event.key,
        .session_id = event.session_id,
        .category = event.category,
        .value_kind = event.value_kind,
        .content = event.content,
    };
}

fn parseRecordedEventLine(allocator: std.mem.Allocator, line: []const u8) !EventFeedOverlay.RecordedEvent {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, line, .{});
    defer parsed.deinit();

    const val = parsed.value;
    const sequence = jsonUnsignedField(val, "sequence") orelse return error.InvalidEvent;
    const origin_instance_id = jsonStringField(val, "origin_instance_id") orelse return error.InvalidEvent;
    const origin_sequence = jsonUnsignedField(val, "origin_sequence") orelse return error.InvalidEvent;
    const timestamp_ms = jsonIntegerField(val, "timestamp_ms") orelse return error.InvalidEvent;
    const operation_str = jsonStringField(val, "operation") orelse return error.InvalidEvent;
    const effect_str = jsonStringField(val, "effect") orelse return error.InvalidEvent;
    const key = jsonStringField(val, "key") orelse return error.InvalidEvent;
    const schema_version_raw = jsonUnsignedField(val, "schema_version") orelse return error.InvalidEvent;
    if (schema_version_raw == 0) return error.InvalidEvent;

    const event_category = if (jsonNullableStringField(val, "category")) |cat|
        try root.cloneMemoryCategory(allocator, root.MemoryCategory.fromString(cat))
    else
        null;
    errdefer if (event_category) |category| switch (category) {
        .custom => |name| allocator.free(name),
        else => {},
    };

    const effect = EventFeedOverlay.Effect.fromString(effect_str) orelse return error.InvalidEvent;
    const resolved_state = blk: {
        const resolved_category_text = jsonNullableStringField(val, "resolved_category") orelse break :blk null;
        const resolved_content = jsonNullableStringField(val, "resolved_content") orelse return error.InvalidEvent;
        const resolved_category = try root.cloneMemoryCategory(allocator, root.MemoryCategory.fromString(resolved_category_text));
        errdefer switch (resolved_category) {
            .custom => |name| allocator.free(name),
            else => {},
        };
        break :blk ResolvedMemoryState{
            .content = try allocator.dupe(u8, resolved_content),
            .category = resolved_category,
            .value_kind = if (jsonNullableStringField(val, "resolved_value_kind")) |kind|
                MemoryValueKind.fromString(kind) orelse return error.InvalidEvent
            else
                null,
        };
    };
    errdefer if (resolved_state) |*state| state.deinit(allocator);
    if (effect == .put and resolved_state == null) return error.InvalidEvent;

    return .{
        .event = .{
            .schema_version = @intCast(schema_version_raw),
            .sequence = sequence,
            .origin_instance_id = try allocator.dupe(u8, origin_instance_id),
            .origin_sequence = origin_sequence,
            .timestamp_ms = timestamp_ms,
            .operation = root.MemoryEventOp.fromString(operation_str) orelse return error.InvalidEvent,
            .key = try allocator.dupe(u8, key),
            .session_id = if (jsonNullableStringField(val, "session_id")) |sid| try allocator.dupe(u8, sid) else null,
            .category = event_category,
            .value_kind = if (jsonNullableStringField(val, "value_kind")) |kind|
                MemoryValueKind.fromString(kind) orelse return error.InvalidEvent
            else
                null,
            .content = if (jsonNullableStringField(val, "content")) |content| try allocator.dupe(u8, content) else null,
        },
        .effect = effect,
        .resolved_state = resolved_state,
    };
}

fn jsonStringField(val: std.json.Value, key: []const u8) ?[]const u8 {
    if (val != .object) return null;
    const field = val.object.get(key) orelse return null;
    return if (field == .string) field.string else null;
}

fn jsonNullableStringField(val: std.json.Value, key: []const u8) ?[]const u8 {
    if (val != .object) return null;
    const field = val.object.get(key) orelse return null;
    if (field == .null) return null;
    return if (field == .string) field.string else null;
}

fn jsonIntegerField(val: std.json.Value, key: []const u8) ?i64 {
    if (val != .object) return null;
    const field = val.object.get(key) orelse return null;
    return switch (field) {
        .integer => field.integer,
        else => null,
    };
}

fn jsonUnsignedField(val: std.json.Value, key: []const u8) ?u64 {
    const value = jsonIntegerField(val, key) orelse return null;
    if (value < 0) return null;
    return @intCast(value);
}

fn dupCategory(allocator: std.mem.Allocator, cat: MemoryCategory) !MemoryCategory {
    return switch (cat) {
        .custom => |name| .{ .custom = try allocator.dupe(u8, name) },
        else => cat,
    };
}

const FailingProjectionBackend = struct {
    allocator: std.mem.Allocator,
    state: std.StringHashMapUnmanaged([]u8) = .{},
    fail_writes: bool = true,
    fail_recalls: bool = false,
    recall_calls: usize = 0,

    fn init(allocator: std.mem.Allocator) FailingProjectionBackend {
        return .{ .allocator = allocator };
    }

    fn deinit(self: *FailingProjectionBackend) void {
        var it = self.state.iterator();
        while (it.next()) |kv| {
            self.allocator.free(kv.key_ptr.*);
            self.allocator.free(kv.value_ptr.*);
        }
        self.state.deinit(self.allocator);
    }

    fn memory(self: *FailingProjectionBackend) Memory {
        return .{ .ptr = @ptrCast(self), .vtable = &vtable };
    }

    fn implName(_: *anyopaque) []const u8 {
        return "failing-projection";
    }

    fn implStore(ptr: *anyopaque, key: []const u8, content: []const u8, _: MemoryCategory, _: ?[]const u8) anyerror!void {
        const self_: *FailingProjectionBackend = @ptrCast(@alignCast(ptr));
        if (self_.fail_writes) return error.BackendUnavailable;
        const owned_key = try self_.allocator.dupe(u8, key);
        errdefer self_.allocator.free(owned_key);
        const owned_content = try self_.allocator.dupe(u8, content);
        errdefer self_.allocator.free(owned_content);
        if (try self_.state.fetchPut(self_.allocator, owned_key, owned_content)) |existing| {
            self_.allocator.free(existing.key);
            self_.allocator.free(existing.value);
        }
    }

    fn implRecall(ptr: *anyopaque, allocator: std.mem.Allocator, query: []const u8, limit: usize, _: ?[]const u8) anyerror![]MemoryEntry {
        const self_: *FailingProjectionBackend = @ptrCast(@alignCast(ptr));
        self_.recall_calls += 1;
        if (self_.fail_recalls) return error.BackendUnavailable;
        var out: std.ArrayListUnmanaged(MemoryEntry) = .empty;
        errdefer {
            for (out.items) |*entry| entry.deinit(allocator);
            out.deinit(allocator);
        }

        var it = self_.state.iterator();
        while (it.next()) |kv| {
            if (!std.mem.containsAtLeast(u8, kv.value_ptr.*, 1, query)) continue;
            try out.append(allocator, .{
                .id = try allocator.dupe(u8, kv.key_ptr.*),
                .key = try allocator.dupe(u8, kv.key_ptr.*),
                .content = try allocator.dupe(u8, kv.value_ptr.*),
                .category = .core,
                .timestamp = try allocator.dupe(u8, "0"),
                .session_id = null,
            });
            if (out.items.len >= limit) break;
        }

        return out.toOwnedSlice(allocator);
    }

    fn implGet(ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8) anyerror!?MemoryEntry {
        const self_: *FailingProjectionBackend = @ptrCast(@alignCast(ptr));
        const value = self_.state.get(key) orelse return null;
        return .{
            .id = try allocator.dupe(u8, key),
            .key = try allocator.dupe(u8, key),
            .content = try allocator.dupe(u8, value),
            .category = .core,
            .timestamp = try allocator.dupe(u8, "0"),
            .session_id = null,
        };
    }

    fn implGetScoped(ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8, _: ?[]const u8) anyerror!?MemoryEntry {
        return implGet(ptr, allocator, key);
    }

    fn implList(ptr: *anyopaque, allocator: std.mem.Allocator, _: ?MemoryCategory, _: ?[]const u8) anyerror![]MemoryEntry {
        const self_: *FailingProjectionBackend = @ptrCast(@alignCast(ptr));
        var out: std.ArrayListUnmanaged(MemoryEntry) = .empty;
        errdefer {
            for (out.items) |*entry| entry.deinit(allocator);
            out.deinit(allocator);
        }

        var it = self_.state.iterator();
        while (it.next()) |kv| {
            try out.append(allocator, .{
                .id = try allocator.dupe(u8, kv.key_ptr.*),
                .key = try allocator.dupe(u8, kv.key_ptr.*),
                .content = try allocator.dupe(u8, kv.value_ptr.*),
                .category = .core,
                .timestamp = try allocator.dupe(u8, "0"),
                .session_id = null,
            });
        }

        return out.toOwnedSlice(allocator);
    }

    fn implForget(ptr: *anyopaque, key: []const u8) anyerror!bool {
        const self_: *FailingProjectionBackend = @ptrCast(@alignCast(ptr));
        if (self_.fail_writes) return error.BackendUnavailable;
        if (self_.state.fetchRemove(key)) |removed| {
            self_.allocator.free(removed.key);
            self_.allocator.free(removed.value);
            return true;
        }
        return false;
    }

    fn implForgetScoped(ptr: *anyopaque, key: []const u8, _: ?[]const u8) anyerror!bool {
        return implForget(ptr, key);
    }

    fn implCount(ptr: *anyopaque) anyerror!usize {
        const self_: *FailingProjectionBackend = @ptrCast(@alignCast(ptr));
        return self_.state.count();
    }

    fn implHealthCheck(_: *anyopaque) bool {
        return true;
    }

    fn implDeinit(ptr: *anyopaque) void {
        const self_: *FailingProjectionBackend = @ptrCast(@alignCast(ptr));
        self_.deinit();
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
        .count = &implCount,
        .healthCheck = &implHealthCheck,
        .deinit = &implDeinit,
    };
};

test "feed overlay does not auto-bootstrap backend state" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const workspace = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(workspace);

    var backend = try root.markdown.MarkdownProjectionMemory.init(std.testing.allocator, workspace);
    const memory = backend.memory();
    try memory.store("preferences.theme", "dark", .core, null);
    try memory.store("preferences.locale", "en", .core, "sess-a");

    var overlay = try EventFeedOverlay.init(std.testing.allocator, memory, workspace, "markdown-bootstrap", "agent-a");
    defer overlay.deinit();

    const events = try overlay.memory().listEvents(std.testing.allocator, 0, 10);
    defer root.freeEvents(std.testing.allocator, events);
    try std.testing.expectEqual(@as(usize, 0), events.len);

    const listed = try overlay.memory().list(std.testing.allocator, null, null);
    defer root.freeEntries(std.testing.allocator, listed);
    try std.testing.expectEqual(@as(usize, 0), listed.len);
}

test "feed overlay converges markdown replicas" {
    var tmp_a = std.testing.tmpDir(.{});
    defer tmp_a.cleanup();
    const ws_a = try tmp_a.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(ws_a);

    var tmp_b = std.testing.tmpDir(.{});
    defer tmp_b.cleanup();
    const ws_b = try tmp_b.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(ws_b);

    var source_backend = try root.markdown.MarkdownProjectionMemory.init(std.testing.allocator, ws_a);
    var replica_backend = try root.markdown.MarkdownProjectionMemory.init(std.testing.allocator, ws_b);

    var source = try EventFeedOverlay.init(std.testing.allocator, source_backend.memory(), ws_a, "markdown-source", "agent-a");
    defer source.deinit();
    var replica = try EventFeedOverlay.init(std.testing.allocator, replica_backend.memory(), ws_b, "markdown-replica", "agent-b");
    defer replica.deinit();

    const source_mem = source.memory();
    const replica_mem = replica.memory();

    try source_mem.store("preferences.tone", "formal", .core, null);
    try source_mem.store("preferences.locale", "ru", .core, "sess-a");
    try std.testing.expect(try source_mem.forgetScoped(std.testing.allocator, "preferences.locale", "sess-a"));

    const events = try source_mem.listEvents(std.testing.allocator, 0, 16);
    defer root.freeEvents(std.testing.allocator, events);

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

    const tone = (try replica_mem.getScoped(std.testing.allocator, "preferences.tone", null)).?;
    defer tone.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("formal", tone.content);

    const locale = try replica_mem.getScoped(std.testing.allocator, "preferences.locale", "sess-a");
    defer if (locale) |entry| entry.deinit(std.testing.allocator);
    try std.testing.expect(locale == null);
}

test "feed overlay journals before backend projection" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const workspace = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(workspace);

    var backend = FailingProjectionBackend.init(std.testing.allocator);
    var overlay = try EventFeedOverlay.init(std.testing.allocator, backend.memory(), workspace, "failing-backend", "agent-a");
    defer overlay.deinit();

    const mem = overlay.memory();
    try std.testing.expectError(error.BackendUnavailable, mem.store("preferences.theme", "dark", .core, null));

    const events = try mem.listEvents(std.testing.allocator, 0, 8);
    defer root.freeEvents(std.testing.allocator, events);
    try std.testing.expectEqual(@as(usize, 1), events.len);
    try std.testing.expectEqualStrings("preferences.theme", events[0].key);

    backend.fail_writes = false;
    const entry = (try mem.getScoped(std.testing.allocator, "preferences.theme", null)).?;
    defer entry.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("dark", entry.content);
}

test "feed overlay canonical reads ignore projection drift" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const workspace = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(workspace);

    var backend = FailingProjectionBackend.init(std.testing.allocator);
    backend.fail_writes = false;
    var overlay = try EventFeedOverlay.init(std.testing.allocator, backend.memory(), workspace, "drift-check", "agent-a");
    defer overlay.deinit();

    const mem = overlay.memory();
    try mem.store("preferences.theme", "dark", .core, null);

    try backend.memory().store("outofband.key", "should-not-appear", .core, null);

    const out_of_band = try mem.getScoped(std.testing.allocator, "outofband.key", null);
    defer if (out_of_band) |entry| entry.deinit(std.testing.allocator);
    try std.testing.expect(out_of_band == null);

    const listed = try mem.list(std.testing.allocator, null, null);
    defer root.freeEntries(std.testing.allocator, listed);
    try std.testing.expectEqual(@as(usize, 1), listed.len);
    try std.testing.expectEqualStrings("preferences.theme", listed[0].key);
}

test "feed overlay get ignores scoped-only entries" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const workspace = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(workspace);

    var backend = FailingProjectionBackend.init(std.testing.allocator);
    backend.fail_writes = false;
    var overlay = try EventFeedOverlay.init(std.testing.allocator, backend.memory(), workspace, "scoped-get", "agent-a");
    defer overlay.deinit();

    const mem = overlay.memory();
    try mem.store("preferences.locale", "ru", .core, "sess-a");

    const global = try mem.get(std.testing.allocator, "preferences.locale");
    defer if (global) |entry| entry.deinit(std.testing.allocator);
    try std.testing.expect(global == null);

    const scoped = (try mem.getScoped(std.testing.allocator, "preferences.locale", "sess-a")).?;
    defer scoped.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("ru", scoped.content);
}

test "feed overlay delegates recall to projection backend when available" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const workspace = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(workspace);

    var backend = FailingProjectionBackend.init(std.testing.allocator);
    backend.fail_writes = false;
    var overlay = try EventFeedOverlay.init(std.testing.allocator, backend.memory(), workspace, "recall-projection", "agent-a");
    defer overlay.deinit();

    const mem = overlay.memory();
    try mem.store("preferences.theme", "dark", .core, null);

    const recalled = try mem.recall(std.testing.allocator, "dark", 8, null);
    defer root.freeEntries(std.testing.allocator, recalled);
    try std.testing.expectEqual(@as(usize, 1), recalled.len);
    try std.testing.expectEqual(@as(usize, 1), backend.recall_calls);

    const info = try mem.eventFeedInfo(std.testing.allocator);
    defer info.deinit(std.testing.allocator);
    try std.testing.expectEqual(root.MemoryEventFeedStorage.overlay, info.storage_kind);
    try std.testing.expect(info.journal_path != null);
}

test "feed overlay falls back to canonical recall when projection recall fails" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const workspace = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(workspace);

    var backend = FailingProjectionBackend.init(std.testing.allocator);
    backend.fail_writes = false;
    var overlay = try EventFeedOverlay.init(std.testing.allocator, backend.memory(), workspace, "recall-fallback", "agent-a");
    defer overlay.deinit();

    const mem = overlay.memory();
    try mem.store("traits.tags", "friendly", .core, null);
    backend.fail_recalls = true;

    const recalled = try mem.recall(std.testing.allocator, "friendly", 8, null);
    defer root.freeEntries(std.testing.allocator, recalled);
    try std.testing.expectEqual(@as(usize, 1), recalled.len);
    try std.testing.expectEqualStrings("traits.tags", recalled[0].key);
    try std.testing.expectEqual(@as(usize, 1), backend.recall_calls);
}

test "feed overlay compaction creates checkpoint, enforces cursor floor, and survives restart" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const workspace = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(workspace);

    var compacted_through: u64 = 0;
    {
        var backend = FailingProjectionBackend.init(std.testing.allocator);
        backend.fail_writes = false;
        var overlay = try EventFeedOverlay.init(std.testing.allocator, backend.memory(), workspace, "compaction-check", "agent-a");
        defer overlay.deinit();

        const mem = overlay.memory();
        try mem.store("preferences.theme", "dark", .core, null);
        compacted_through = try mem.compactEvents();
        try std.testing.expect(compacted_through > 0);
        try std.testing.expectError(error.CursorExpired, mem.listEvents(std.testing.allocator, 0, 8));

        const info = try mem.eventFeedInfo(std.testing.allocator);
        defer info.deinit(std.testing.allocator);
        try std.testing.expect(info.supports_compaction);
        try std.testing.expectEqual(compacted_through, info.compacted_through_sequence);
        try std.testing.expectEqual(compacted_through + 1, info.oldest_available_sequence);
        try std.testing.expect(info.checkpoint_path != null);
    }

    {
        var backend = FailingProjectionBackend.init(std.testing.allocator);
        backend.fail_writes = false;
        var overlay = try EventFeedOverlay.init(std.testing.allocator, backend.memory(), workspace, "compaction-check", "agent-a");
        defer overlay.deinit();

        const mem = overlay.memory();
        const entry = (try mem.getScoped(std.testing.allocator, "preferences.theme", null)).?;
        defer entry.deinit(std.testing.allocator);
        try std.testing.expectEqualStrings("dark", entry.content);

        const no_tail = try mem.listEvents(std.testing.allocator, compacted_through, 8);
        defer root.freeEvents(std.testing.allocator, no_tail);
        try std.testing.expectEqual(@as(usize, 0), no_tail.len);

        try mem.store("preferences.locale", "en", .core, null);
        const tail = try mem.listEvents(std.testing.allocator, compacted_through, 8);
        defer root.freeEvents(std.testing.allocator, tail);
        try std.testing.expectEqual(@as(usize, 1), tail.len);
        try std.testing.expectEqual(compacted_through + 1, tail[0].sequence);
    }
}

test "feed overlay checkpoint restores replica and preserves local origin frontier" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const workspace = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(workspace);

    var source_backend = FailingProjectionBackend.init(std.testing.allocator);
    source_backend.fail_writes = false;
    var source = try EventFeedOverlay.init(std.testing.allocator, source_backend.memory(), workspace, "checkpoint-source", "agent-a");
    defer source.deinit();

    const source_mem = source.memory();
    try source_mem.store("preferences.theme", "dark", .core, null);
    _ = try source_mem.compactEvents();

    const checkpoint = try source_mem.exportCheckpoint(std.testing.allocator);
    defer std.testing.allocator.free(checkpoint);

    var replica_backend = FailingProjectionBackend.init(std.testing.allocator);
    replica_backend.fail_writes = false;
    var replica = try EventFeedOverlay.init(std.testing.allocator, replica_backend.memory(), workspace, "checkpoint-replica", "agent-a");
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
