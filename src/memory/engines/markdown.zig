//! Markdown-based memory with native deterministic event feed.
//!
//! Layout:
//!   workspace/MEMORY.md                          — projected core memory
//!   workspace/memory/<category>.md               — projected non-core categories
//!   workspace/.nullclaw/markdown-feed/*.jsonl    — native feed journal/checkpoint
//!
//! Markdown files stay human-readable, but the canonical shared-memory feed is
//! owned by this backend directly rather than via the generic overlay runtime.

const std = @import("std");
const fs_compat = @import("../../fs_compat.zig");
const json_util = @import("../../json_util.zig");
const root = @import("../root.zig");
const key_codec = @import("../vector/key_codec.zig");
const Memory = root.Memory;
const MemoryCategory = root.MemoryCategory;
const MemoryEntry = root.MemoryEntry;
const MemoryEvent = root.MemoryEvent;
const MemoryEventInput = root.MemoryEventInput;
const MemoryEventFeedInfo = root.MemoryEventFeedInfo;
const MemoryValueKind = root.MemoryValueKind;
const ResolvedMemoryState = root.ResolvedMemoryState;
const log = std.log.scoped(.memory_markdown);

const MAX_EVENT_LINE_BYTES: usize = 1024 * 1024;

pub const MarkdownProjectionMemory = struct {
    workspace_dir: []const u8,
    allocator: std.mem.Allocator,
    owns_self: bool = false,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, workspace_dir: []const u8) !Self {
        return Self{
            .workspace_dir = try allocator.dupe(u8, workspace_dir),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.workspace_dir);
    }

    fn corePath(self: *const Self, allocator: std.mem.Allocator) ![]u8 {
        return std.fmt.allocPrint(allocator, "{s}/MEMORY.md", .{self.workspace_dir});
    }

    fn memoryDir(self: *const Self, allocator: std.mem.Allocator) ![]u8 {
        return std.fmt.allocPrint(allocator, "{s}/memory", .{self.workspace_dir});
    }

    fn categoryFileStem(allocator: std.mem.Allocator, category: MemoryCategory) ![]u8 {
        const raw = category.toString();
        var buf: std.ArrayListUnmanaged(u8) = .empty;
        errdefer buf.deinit(allocator);

        for (raw) |ch| {
            if (std.ascii.isAlphanumeric(ch) or ch == '-' or ch == '_' or ch == '.') {
                try buf.append(allocator, ch);
            } else {
                try buf.append(allocator, '_');
            }
        }

        if (buf.items.len == 0) try buf.appendSlice(allocator, "custom");
        return buf.toOwnedSlice(allocator);
    }

    fn categoryPath(self: *const Self, allocator: std.mem.Allocator, category: MemoryCategory) ![]u8 {
        if (category.eql(.core)) return self.corePath(allocator);

        const stem = try categoryFileStem(allocator, category);
        defer allocator.free(stem);
        return std.fmt.allocPrint(allocator, "{s}/memory/{s}.md", .{ self.workspace_dir, stem });
    }

    fn ensureDir(path: []const u8) !void {
        if (std.fs.path.dirname(path)) |dir| {
            std.fs.makeDirAbsolute(dir) catch |err| switch (err) {
                error.PathAlreadyExists => {},
                else => return err,
            };
        }
    }

    fn writeFileContents(path: []const u8, content: []const u8, allocator: std.mem.Allocator) !void {
        try ensureDir(path);
        const file = try std.fs.cwd().createFile(path, .{ .truncate = true, .read = true });
        defer file.close();
        _ = allocator;
        try file.writeAll(content);
        if (content.len == 0 or content[content.len - 1] != '\n') {
            try file.writeAll("\n");
        }
    }

    const ParsedMeta = struct {
        category: MemoryCategory,
        session_id: ?[]u8 = null,

        fn deinit(self: ParsedMeta, allocator: std.mem.Allocator) void {
            if (self.session_id) |sid| allocator.free(sid);
            switch (self.category) {
                .custom => |name| allocator.free(name),
                else => {},
            }
        }
    };

    fn parseMetaComment(meta: []const u8, fallback_category: MemoryCategory, allocator: std.mem.Allocator) !ParsedMeta {
        var parsed = ParsedMeta{
            .category = switch (fallback_category) {
                .custom => |name| .{ .custom = try allocator.dupe(u8, name) },
                else => fallback_category,
            },
            .session_id = null,
        };
        errdefer parsed.deinit(allocator);

        var iter = std.mem.splitScalar(u8, meta, ';');
        while (iter.next()) |part| {
            const trimmed = std.mem.trim(u8, part, " \t");
            const eq = std.mem.indexOfScalar(u8, trimmed, '=') orelse continue;
            const name = trimmed[0..eq];
            const value = trimmed[eq + 1 ..];

            if (std.mem.eql(u8, name, "category")) {
                switch (parsed.category) {
                    .custom => |existing| allocator.free(existing),
                    else => {},
                }
                const cat = MemoryCategory.fromString(value);
                parsed.category = switch (cat) {
                    .custom => |custom_name| .{ .custom = try allocator.dupe(u8, custom_name) },
                    else => cat,
                };
            } else if (std.mem.eql(u8, name, "session")) {
                if (parsed.session_id) |sid| allocator.free(sid);
                parsed.session_id = if (value.len > 0) try allocator.dupe(u8, value) else null;
            }
        }

        return parsed;
    }

    fn sameSession(entry_session: ?[]const u8, target_session: ?[]const u8) bool {
        if (entry_session == null and target_session == null) return true;
        if (entry_session == null or target_session == null) return false;
        return std.mem.eql(u8, entry_session.?, target_session.?);
    }

    fn serializeEntry(allocator: std.mem.Allocator, entry: MemoryEntry) ![]u8 {
        return std.fmt.allocPrint(allocator, "- **{s}**: {s} <!-- nullclaw:category={s};session={s} -->", .{
            entry.key,
            entry.content,
            entry.category.toString(),
            entry.session_id orelse "",
        });
    }

    fn clearManagedFiles(self: *Self, allocator: std.mem.Allocator) !void {
        const core = try self.corePath(allocator);
        defer allocator.free(core);
        std.fs.deleteFileAbsolute(core) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };

        const legacy_core = try std.fmt.allocPrint(allocator, "{s}/memory.md", .{self.workspace_dir});
        defer allocator.free(legacy_core);
        std.fs.deleteFileAbsolute(legacy_core) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };

        const md = try self.memoryDir(allocator);
        defer allocator.free(md);
        if (std.fs.openDirAbsolute(md, .{ .iterate = true })) |*dir_handle| {
            var dir = dir_handle.*;
            defer dir.close();
            var it = dir.iterate();
            while (try it.next()) |entry| {
                if (!std.mem.endsWith(u8, entry.name, ".md")) continue;
                const path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ md, entry.name });
                defer allocator.free(path);
                std.fs.deleteFileAbsolute(path) catch |err| switch (err) {
                    error.FileNotFound => {},
                    else => return err,
                };
            }
        } else |_| {}
    }

    fn writeEntries(self: *Self, entries: []const MemoryEntry, allocator: std.mem.Allocator) !void {
        const Group = struct {
            path: []u8,
            buffer: std.ArrayListUnmanaged(u8) = .empty,
        };

        var groups: std.ArrayListUnmanaged(Group) = .empty;
        defer {
            for (groups.items) |*group| {
                allocator.free(group.path);
                group.buffer.deinit(allocator);
            }
            groups.deinit(allocator);
        }

        try self.clearManagedFiles(allocator);
        if (entries.len == 0) return;

        for (entries) |entry| {
            const path = try self.categoryPath(allocator, entry.category);
            defer allocator.free(path);

            var found_index: ?usize = null;
            for (groups.items, 0..) |group, idx| {
                if (std.mem.eql(u8, group.path, path)) {
                    found_index = idx;
                    break;
                }
            }

            if (found_index == null) {
                try groups.append(allocator, .{
                    .path = try allocator.dupe(u8, path),
                });
                found_index = groups.items.len - 1;
            }

            const line = try serializeEntry(allocator, entry);
            defer allocator.free(line);

            var group = &groups.items[found_index.?];
            if (group.buffer.items.len > 0) try group.buffer.append(allocator, '\n');
            try group.buffer.appendSlice(allocator, line);
        }

        for (groups.items) |group| {
            try writeFileContents(group.path, group.buffer.items, allocator);
        }
    }

    fn parseEntries(text: []const u8, filename: []const u8, category: MemoryCategory, allocator: std.mem.Allocator) ![]MemoryEntry {
        var entries: std.ArrayList(MemoryEntry) = .empty;
        errdefer {
            for (entries.items) |*e| e.deinit(allocator);
            entries.deinit(allocator);
        }

        var line_idx: usize = 0;
        var iter = std.mem.splitScalar(u8, text, '\n');
        while (iter.next()) |line| {
            const trimmed = std.mem.trim(u8, line, " \t\r");
            if (trimmed.len == 0 or trimmed[0] == '#') {
                continue;
            }

            const clean = if (std.mem.startsWith(u8, trimmed, "- "))
                trimmed[2..]
            else
                trimmed;

            const metadata_prefix = "<!-- nullclaw:";
            const metadata_start = std.mem.indexOf(u8, clean, metadata_prefix);
            const content_part = if (metadata_start) |idx|
                std.mem.trimRight(u8, clean[0..idx], " \t")
            else
                clean;

            var parsed_meta = ParsedMeta{
                .category = switch (category) {
                    .custom => |name| .{ .custom = try allocator.dupe(u8, name) },
                    else => category,
                },
                .session_id = null,
            };
            errdefer parsed_meta.deinit(allocator);

            if (metadata_start) |idx| {
                const meta_with_suffix = clean[idx + metadata_prefix.len ..];
                if (std.mem.indexOf(u8, meta_with_suffix, "-->")) |end_idx| {
                    parsed_meta.deinit(allocator);
                    parsed_meta = try parseMetaComment(meta_with_suffix[0..end_idx], category, allocator);
                }
            }

            const id = try std.fmt.allocPrint(allocator, "{s}:{d}", .{ filename, line_idx });
            errdefer allocator.free(id);
            const explicit_key = blk: {
                if (!std.mem.startsWith(u8, content_part, "**")) break :blk null;
                const rest = content_part[2..];
                const suffix = std.mem.indexOf(u8, rest, "**:") orelse break :blk null;
                if (suffix == 0) break :blk null;
                break :blk rest[0..suffix];
            };
            const value_slice = if (explicit_key != null)
                std.mem.trim(u8, content_part[(2 + explicit_key.?.len + 3)..], " \t")
            else
                content_part;

            const key = try allocator.dupe(u8, explicit_key orelse id);
            errdefer allocator.free(key);
            const content_dup = try allocator.dupe(u8, value_slice);
            errdefer allocator.free(content_dup);
            const timestamp = try allocator.dupe(u8, filename);
            errdefer allocator.free(timestamp);

            try entries.append(allocator, MemoryEntry{
                .id = id,
                .key = key,
                .content = content_dup,
                .category = parsed_meta.category,
                .timestamp = timestamp,
                .session_id = parsed_meta.session_id,
            });
            parsed_meta = .{ .category = .core, .session_id = null };

            line_idx += 1;
        }

        return entries.toOwnedSlice(allocator);
    }

    fn readAllEntries(self: *Self, allocator: std.mem.Allocator) ![]MemoryEntry {
        var all: std.ArrayList(MemoryEntry) = .empty;
        errdefer {
            for (all.items) |*e| e.deinit(allocator);
            all.deinit(allocator);
        }

        const root_path = try self.corePath(allocator);
        defer allocator.free(root_path);
        if (fs_compat.readFileAlloc(std.fs.cwd(), allocator, root_path, 1024 * 1024)) |content| {
            defer allocator.free(content);
            const entries = try parseEntries(content, "MEMORY", .core, allocator);
            defer allocator.free(entries);
            for (entries) |e| try all.append(allocator, e);
        } else |_| {}

        const md = try self.memoryDir(allocator);
        defer allocator.free(md);
        if (std.fs.cwd().openDir(md, .{ .iterate = true })) |*dir_handle| {
            var dir = dir_handle.*;
            defer dir.close();
            var it = dir.iterate();
            while (try it.next()) |entry| {
                if (!std.mem.endsWith(u8, entry.name, ".md")) continue;
                const fpath = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ md, entry.name });
                defer allocator.free(fpath);
                if (fs_compat.readFileAlloc(std.fs.cwd(), allocator, fpath, 1024 * 1024)) |content| {
                    defer allocator.free(content);
                    const fname = entry.name[0 .. entry.name.len - 3];
                    const inferred_category = MemoryCategory.fromString(fname);
                    const entries = try parseEntries(content, fname, inferred_category, allocator);
                    defer allocator.free(entries);
                    for (entries) |e| try all.append(allocator, e);
                } else |_| {}
            }
        } else |_| {}

        return all.toOwnedSlice(allocator);
    }

    // ── Memory vtable impl ────────────────────────────────────────

    fn implName(_: *anyopaque) []const u8 {
        return "markdown";
    }

    fn implStore(ptr: *anyopaque, key: []const u8, content: []const u8, category: MemoryCategory, session_id: ?[]const u8) anyerror!void {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        var entries = try self_.readAllEntries(self_.allocator);
        defer root.freeEntries(self_.allocator, entries);

        var updated = false;
        for (entries) |*entry| {
            if (!std.mem.eql(u8, entry.key, key)) continue;
            if (!sameSession(entry.session_id, session_id)) continue;

            self_.allocator.free(entry.content);
            entry.content = try self_.allocator.dupe(u8, content);
            self_.allocator.free(entry.timestamp);
            entry.timestamp = try std.fmt.allocPrint(self_.allocator, "{d}", .{std.time.timestamp()});
            if (entry.session_id) |sid| self_.allocator.free(sid);
            entry.session_id = if (session_id) |sid| try self_.allocator.dupe(u8, sid) else null;
            switch (entry.category) {
                .custom => |name| self_.allocator.free(name),
                else => {},
            }
            entry.category = switch (category) {
                .custom => |name| .{ .custom = try self_.allocator.dupe(u8, name) },
                else => category,
            };
            updated = true;
            break;
        }

        if (!updated) {
            const id = try std.fmt.allocPrint(self_.allocator, "md:{d}", .{std.time.nanoTimestamp()});
            errdefer self_.allocator.free(id);
            const stored_key = try self_.allocator.dupe(u8, key);
            errdefer self_.allocator.free(stored_key);
            const stored_content = try self_.allocator.dupe(u8, content);
            errdefer self_.allocator.free(stored_content);
            const timestamp = try std.fmt.allocPrint(self_.allocator, "{d}", .{std.time.timestamp()});
            errdefer self_.allocator.free(timestamp);
            const stored_category: MemoryCategory = switch (category) {
                .custom => |name| .{ .custom = try self_.allocator.dupe(u8, name) },
                else => category,
            };
            errdefer switch (stored_category) {
                .custom => |name| self_.allocator.free(name),
                else => {},
            };
            const stored_session = if (session_id) |sid| try self_.allocator.dupe(u8, sid) else null;
            errdefer if (stored_session) |sid| self_.allocator.free(sid);

            const new_entries = try self_.allocator.realloc(entries, entries.len + 1);
            entries = new_entries;
            entries[entries.len - 1] = .{
                .id = id,
                .key = stored_key,
                .content = stored_content,
                .category = stored_category,
                .timestamp = timestamp,
                .session_id = stored_session,
            };
        }

        try self_.writeEntries(entries, self_.allocator);
    }

    fn implRecall(ptr: *anyopaque, allocator: std.mem.Allocator, query: []const u8, limit: usize, session_id: ?[]const u8) anyerror![]MemoryEntry {
        const self_: *Self = @ptrCast(@alignCast(ptr));

        const all = try self_.readAllEntries(allocator);
        defer allocator.free(all);

        const query_lower = try std.ascii.allocLowerString(allocator, query);
        defer allocator.free(query_lower);

        var keywords: std.ArrayList([]const u8) = .empty;
        defer keywords.deinit(allocator);
        var kw_iter = std.mem.tokenizeAny(u8, query_lower, " \t\n\r");
        while (kw_iter.next()) |word| try keywords.append(allocator, word);

        if (keywords.items.len == 0) {
            for (all) |*e| @constCast(e).deinit(allocator);
            return allocator.alloc(MemoryEntry, 0);
        }

        var scored: std.ArrayList(MemoryEntry) = .empty;
        errdefer {
            for (scored.items) |*e| e.deinit(allocator);
            scored.deinit(allocator);
        }

        for (all) |*entry_ptr| {
            var entry = entry_ptr.*;
            if (!sameSession(entry.session_id, session_id)) {
                @constCast(entry_ptr).deinit(allocator);
                continue;
            }
            const content_lower = try std.ascii.allocLowerString(allocator, entry.content);
            defer allocator.free(content_lower);
            const key_lower = try std.ascii.allocLowerString(allocator, entry.key);
            defer allocator.free(key_lower);

            var matched: usize = 0;
            for (keywords.items) |kw| {
                if (std.mem.indexOf(u8, content_lower, kw) != null) matched += 1;
                if (std.mem.indexOf(u8, key_lower, kw) != null) matched += 1;
            }

            if (matched > 0) {
                const score: f64 = @as(f64, @floatFromInt(matched)) / @as(f64, @floatFromInt(keywords.items.len));
                entry.score = score;
                try scored.append(allocator, entry);
            } else {
                @constCast(entry_ptr).deinit(allocator);
            }
        }

        std.mem.sort(MemoryEntry, scored.items, {}, struct {
            fn lessThan(_: void, a: MemoryEntry, b: MemoryEntry) bool {
                return (b.score orelse 0) < (a.score orelse 0);
            }
        }.lessThan);

        if (scored.items.len > limit) {
            for (scored.items[limit..]) |*e| e.deinit(allocator);
            scored.shrinkRetainingCapacity(limit);
        }

        return scored.toOwnedSlice(allocator);
    }

    fn implGet(ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8) anyerror!?MemoryEntry {
        const self_: *Self = @ptrCast(@alignCast(ptr));

        const all = try self_.readAllEntries(allocator);
        defer allocator.free(all);

        var found: ?MemoryEntry = null;
        for (all) |*entry_ptr| {
            const entry = entry_ptr.*;
            if (std.mem.eql(u8, entry.key, key) and entry.session_id == null) {
                if (found) |*prev| prev.deinit(allocator);
                found = entry;
            } else {
                @constCast(entry_ptr).deinit(allocator);
            }
        }

        return found;
    }

    fn implGetScoped(ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8, session_id: ?[]const u8) anyerror!?MemoryEntry {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const all = try self_.readAllEntries(allocator);
        defer allocator.free(all);

        var found: ?MemoryEntry = null;
        for (all) |*entry_ptr| {
            if (std.mem.eql(u8, entry_ptr.key, key) and sameSession(entry_ptr.session_id, session_id)) {
                if (found) |*prev| prev.deinit(allocator);
                found = entry_ptr.*;
            } else {
                @constCast(entry_ptr).deinit(allocator);
            }
        }

        return found;
    }

    fn implList(ptr: *anyopaque, allocator: std.mem.Allocator, category: ?MemoryCategory, session_id: ?[]const u8) anyerror![]MemoryEntry {
        const self_: *Self = @ptrCast(@alignCast(ptr));

        const all = try self_.readAllEntries(allocator);
        defer allocator.free(all);

        if (category == null) {
            var filtered_all: std.ArrayList(MemoryEntry) = .empty;
            errdefer {
                for (filtered_all.items) |*e| e.deinit(allocator);
                filtered_all.deinit(allocator);
            }

            for (all) |*entry_ptr| {
                if (sameSession(entry_ptr.session_id, session_id)) {
                    try filtered_all.append(allocator, entry_ptr.*);
                } else {
                    @constCast(entry_ptr).deinit(allocator);
                }
            }

            return filtered_all.toOwnedSlice(allocator);
        }

        var filtered: std.ArrayList(MemoryEntry) = .empty;
        errdefer {
            for (filtered.items) |*e| e.deinit(allocator);
            filtered.deinit(allocator);
        }

        for (all) |*entry_ptr| {
            var entry = entry_ptr.*;
            if (entry.category.eql(category.?) and sameSession(entry.session_id, session_id)) {
                try filtered.append(allocator, entry);
            } else {
                @constCast(entry_ptr).deinit(allocator);
            }
        }

        return filtered.toOwnedSlice(allocator);
    }

    fn implForget(ptr: *anyopaque, key: []const u8) anyerror!bool {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const all = try self_.readAllEntries(self_.allocator);
        defer root.freeEntries(self_.allocator, all);

        var kept: std.ArrayListUnmanaged(MemoryEntry) = .empty;
        defer {
            for (kept.items) |*entry| entry.deinit(self_.allocator);
            kept.deinit(self_.allocator);
        }

        var deleted = false;
        for (all) |entry| {
            if (std.mem.eql(u8, entry.key, key)) {
                deleted = true;
                continue;
            }
            try kept.append(self_.allocator, .{
                .id = try self_.allocator.dupe(u8, entry.id),
                .key = try self_.allocator.dupe(u8, entry.key),
                .content = try self_.allocator.dupe(u8, entry.content),
                .category = switch (entry.category) {
                    .custom => |name| .{ .custom = try self_.allocator.dupe(u8, name) },
                    else => entry.category,
                },
                .timestamp = try self_.allocator.dupe(u8, entry.timestamp),
                .session_id = if (entry.session_id) |sid| try self_.allocator.dupe(u8, sid) else null,
            });
        }

        if (deleted) try self_.writeEntries(kept.items, self_.allocator);
        return deleted;
    }

    fn implForgetScoped(ptr: *anyopaque, key: []const u8, session_id: ?[]const u8) anyerror!bool {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const all = try self_.readAllEntries(self_.allocator);
        defer root.freeEntries(self_.allocator, all);

        var kept: std.ArrayListUnmanaged(MemoryEntry) = .empty;
        defer {
            for (kept.items) |*entry| entry.deinit(self_.allocator);
            kept.deinit(self_.allocator);
        }

        var deleted = false;
        for (all) |entry| {
            if (std.mem.eql(u8, entry.key, key) and sameSession(entry.session_id, session_id)) {
                deleted = true;
                continue;
            }
            try kept.append(self_.allocator, .{
                .id = try self_.allocator.dupe(u8, entry.id),
                .key = try self_.allocator.dupe(u8, entry.key),
                .content = try self_.allocator.dupe(u8, entry.content),
                .category = switch (entry.category) {
                    .custom => |name| .{ .custom = try self_.allocator.dupe(u8, name) },
                    else => entry.category,
                },
                .timestamp = try self_.allocator.dupe(u8, entry.timestamp),
                .session_id = if (entry.session_id) |sid| try self_.allocator.dupe(u8, sid) else null,
            });
        }

        if (deleted) try self_.writeEntries(kept.items, self_.allocator);
        return deleted;
    }

    fn implCount(ptr: *anyopaque) anyerror!usize {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const all = try self_.readAllEntries(self_.allocator);
        defer {
            for (all) |*entry| {
                @constCast(entry).deinit(self_.allocator);
            }
            self_.allocator.free(all);
        }
        return all.len;
    }

    fn implHealthCheck(_: *anyopaque) bool {
        return true;
    }

    fn implDeinit(ptr: *anyopaque) void {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        self_.deinit();
        if (self_.owns_self) {
            self_.allocator.destroy(self_);
        }
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

    pub fn memory(self: *Self) Memory {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    pub fn exportAllEntries(self: *Self, allocator: std.mem.Allocator) ![]MemoryEntry {
        return self.readAllEntries(allocator);
    }
};

pub const MarkdownMemory = struct {
    allocator: std.mem.Allocator,
    projection: *MarkdownProjectionMemory,
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

    pub fn init(allocator: std.mem.Allocator, workspace_dir: []const u8) !Self {
        return initWithInstanceId(allocator, workspace_dir, "");
    }

    pub fn initWithInstanceId(allocator: std.mem.Allocator, workspace_dir: []const u8, instance_id: []const u8) !Self {
        const effective_instance_id = if (instance_id.len > 0) instance_id else "default";

        const projection = try allocator.create(MarkdownProjectionMemory);
        errdefer allocator.destroy(projection);
        projection.* = try MarkdownProjectionMemory.init(allocator, workspace_dir);
        projection.owns_self = true;

        const journal_root = try std.fmt.allocPrint(allocator, "{s}/.nullclaw/markdown-feed", .{workspace_dir});
        defer allocator.free(journal_root);
        const journal_identity = try std.fmt.allocPrint(allocator, "markdown-native-v2\n{s}\n{s}", .{ workspace_dir, effective_instance_id });
        defer allocator.free(journal_identity);

        var self = Self{
            .allocator = allocator,
            .projection = projection,
            .journal_path = try buildNativeJournalPath(allocator, journal_root, journal_identity),
            .checkpoint_path = try buildNativeCheckpointPath(allocator, journal_root, journal_identity),
            .instance_id = try allocator.dupe(u8, effective_instance_id),
        };
        errdefer self.deinit();

        try ensureJournalParent(self.journal_path);
        try ensureJournalParent(self.checkpoint_path);

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
        self.projection.deinit();
        if (self.projection.owns_self) self.allocator.destroy(self.projection);
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

    pub fn exportAllEntries(self: *Self, allocator: std.mem.Allocator) ![]MemoryEntry {
        return self.memory().list(allocator, null, null);
    }

    pub fn parseEntries(text: []const u8, filename: []const u8, category: MemoryCategory, allocator: std.mem.Allocator) ![]MemoryEntry {
        return MarkdownProjectionMemory.parseEntries(text, filename, category, allocator);
    }

    fn projectionMemory(self: *Self) Memory {
        return self.projection.memory();
    }

    fn implName(_: *anyopaque) []const u8 {
        return "markdown";
    }

    fn implStore(ptr: *anyopaque, key: []const u8, content: []const u8, category: MemoryCategory, session_id: ?[]const u8) anyerror!void {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        try self_.storeLocal(key, content, category, session_id);
    }

    fn implRecall(ptr: *anyopaque, allocator: std.mem.Allocator, query: []const u8, limit: usize, session_id: ?[]const u8) anyerror![]MemoryEntry {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        var file = try self_.openJournalShared();
        defer file.close();
        try self_.refreshJournalLocked(&file);
        return self_.recallCanonical(allocator, query, limit, session_id);
    }

    fn implGet(ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8) anyerror!?MemoryEntry {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        var file = try self_.openJournalShared();
        defer file.close();
        try self_.refreshJournalLocked(&file);
        if (self_.findDefaultStatePtr(key)) |state| return try self_.cloneStateEntry(allocator, state.*);
        return null;
    }

    fn implGetScoped(ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8, session_id: ?[]const u8) anyerror!?MemoryEntry {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        var file = try self_.openJournalShared();
        defer file.close();
        try self_.refreshJournalLocked(&file);
        const storage_key = try key_codec.encode(allocator, key, session_id);
        defer allocator.free(storage_key);
        const state = self_.state_entries.getPtr(storage_key) orelse return null;
        return try self_.cloneStateEntry(allocator, state.*);
    }

    fn implList(ptr: *anyopaque, allocator: std.mem.Allocator, category: ?MemoryCategory, session_id: ?[]const u8) anyerror![]MemoryEntry {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        var file = try self_.openJournalShared();
        defer file.close();
        try self_.refreshJournalLocked(&file);
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
            .storage_kind = .native,
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
        return self_.projectionMemory().healthCheck();
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

    fn storeLocal(self: *Self, key: []const u8, content: []const u8, category: MemoryCategory, session_id: ?[]const u8) !void {
        var file = try self.openJournalExclusive();
        defer file.close();

        try self.refreshJournalLocked(&file);
        const input = try self.makeLocalInputLocked(.put, key, session_id, category, null, content);
        _ = try self.recordEventLocked(&file, input);
        try self.replayProjectionLocked(&file);
    }

    fn deleteLocalKey(self: *Self, key: []const u8) !bool {
        var file = try self.openJournalExclusive();
        defer file.close();

        try self.refreshJournalLocked(&file);
        const had_entry = self.hasAnyStateForKey(key);
        const input = try self.makeLocalInputLocked(.delete_all, key, null, null, null, null);
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
        const input = try self.makeLocalInputLocked(.delete_scoped, key, session_id, null, null, null);
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
        value_kind: ?MemoryValueKind,
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
            .value_kind = value_kind,
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
        self.loaded_size_bytes = end_offset;
        self.applyMetadataUpdate(next_sequence, input, decision.effect, decision.resolved_state) catch |err| {
            log.warn("markdown journal append committed before metadata update; reloading canonical state from disk: {}", .{err});
            try self.reloadCanonicalStateLocked(file);
            return true;
        };
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
        var file = std.fs.openFileAbsolute(self.checkpoint_path, .{ .mode = .read_only }) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };
        defer file.close();

        const read_buf = try self.allocator.alloc(u8, MAX_EVENT_LINE_BYTES);
        defer self.allocator.free(read_buf);
        var reader = file.readerStreaming(read_buf);
        var saw_any = false;
        var saw_meta = false;
        while (try reader.interface.takeDelimiter('\n')) |line_with_no_delim| {
            const line = std.mem.trim(u8, line_with_no_delim, " \t\r\n");
            if (line.len == 0) continue;
            saw_any = true;
            if (try self.applyCheckpointLine(line)) saw_meta = true;
        }
        if (saw_any and !saw_meta) return error.InvalidEvent;
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

    fn reloadCanonicalStateLocked(self: *Self, file: *std.fs.File) !void {
        self.clearJournalState();
        try self.loadCheckpoint();
        try self.loadJournalFromOffsetLocked(file, 0);
        try self.rebuildProjectionFromCanonical();
        self.projection_offset_bytes = self.loaded_size_bytes;
    }

    fn rebuildProjectionFromCanonical(self: *Self) !void {
        const projected = try self.projectionMemory().list(self.allocator, null, null);
        defer root.freeEntries(self.allocator, projected);

        for (projected) |entry| {
            const storage_key = try key_codec.encode(self.allocator, entry.key, entry.session_id);
            defer self.allocator.free(storage_key);

            const canonical = self.state_entries.getPtr(storage_key);
            const matches = canonical != null and
                std.mem.eql(u8, canonical.?.content, entry.content) and
                canonical.?.category.eql(entry.category);
            if (!matches) {
                _ = try self.projectionMemory().forgetScoped(self.allocator, entry.key, entry.session_id);
            }
        }

        var it = self.state_entries.iterator();
        while (it.next()) |kv| {
            const state = kv.value_ptr.*;
            const existing = try self.projectionMemory().getScoped(self.allocator, state.key, state.session_id);
            defer if (existing) |entry| entry.deinit(self.allocator);

            const needs_upsert = if (existing) |entry|
                !std.mem.eql(u8, entry.content, state.content) or !entry.category.eql(state.category)
            else
                true;
            if (needs_upsert) {
                try self.projectionMemory().store(state.key, state.content, state.category, state.session_id);
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
            .projection = self.projection,
            .journal_path = try self.allocator.dupe(u8, ""),
            .checkpoint_path = try self.allocator.dupe(u8, ""),
            .instance_id = try self.allocator.dupe(u8, self.instance_id),
        };
        defer scratch.deinitMembers();

        var lines = std.mem.splitScalar(u8, payload, '\n');
        var saw_any = false;
        var saw_meta = false;
        while (lines.next()) |raw_line| {
            const line = std.mem.trim(u8, raw_line, " \t\r\n");
            if (line.len == 0) continue;
            saw_any = true;
            if (try scratch.applyCheckpointLine(line)) saw_meta = true;
        }
        if (!saw_any or !saw_meta) return error.InvalidEvent;

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

    fn applyCheckpointLine(self: *Self, line: []const u8) !bool {
        var parsed = try std.json.parseFromSlice(std.json.Value, self.allocator, line, .{});
        defer parsed.deinit();

        const kind = jsonStringField(parsed.value, "kind") orelse return error.InvalidEvent;
        if (std.mem.eql(u8, kind, "meta")) {
            const schema_version = jsonUnsignedField(parsed.value, "schema_version") orelse return error.InvalidEvent;
            if (schema_version != 1) return error.InvalidEvent;
            self.last_sequence = jsonUnsignedField(parsed.value, "last_sequence") orelse 0;
            self.last_timestamp_ms = jsonIntegerField(parsed.value, "last_timestamp_ms") orelse 0;
            self.compacted_through_sequence = jsonUnsignedField(parsed.value, "compacted_through_sequence") orelse self.last_sequence;
            return true;
        }
        if (std.mem.eql(u8, kind, "frontier")) {
            const origin_instance_id = jsonStringField(parsed.value, "origin_instance_id") orelse return error.InvalidEvent;
            const origin_sequence = jsonUnsignedField(parsed.value, "origin_sequence") orelse return error.InvalidEvent;
            try self.rememberOriginFrontier(origin_instance_id, origin_sequence);
            return false;
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
            return false;
        }
        if (std.mem.eql(u8, kind, "scoped_tombstone") or std.mem.eql(u8, kind, "key_tombstone")) {
            const key = jsonStringField(parsed.value, "key") orelse return error.InvalidEvent;
            const timestamp_ms = jsonIntegerField(parsed.value, "timestamp_ms") orelse return error.InvalidEvent;
            const origin_instance_id = jsonStringField(parsed.value, "origin_instance_id") orelse return error.InvalidEvent;
            const origin_sequence = jsonUnsignedField(parsed.value, "origin_sequence") orelse return error.InvalidEvent;
            try self.restoreCheckpointTombstone(kind, key, timestamp_ms, origin_instance_id, origin_sequence);
            return false;
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
                try self.projectionMemory().store(input.key, state.content, state.category, input.session_id);
            },
            .delete_scoped => {
                _ = try self.projectionMemory().forgetScoped(self.allocator, input.key, input.session_id);
            },
            .delete_all => {
                _ = try self.projectionMemory().forget(input.key);
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

fn buildNativeJournalPath(allocator: std.mem.Allocator, journal_root_dir: []const u8, journal_identity: []const u8) ![]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(journal_identity, &digest, .{});
    const hex = std.fmt.bytesToHex(digest, .lower);
    const filename = try std.fmt.allocPrint(allocator, "feed.{s}.jsonl", .{hex[0..]});
    defer allocator.free(filename);
    return std.fs.path.join(allocator, &.{ journal_root_dir, filename });
}

fn buildNativeCheckpointPath(allocator: std.mem.Allocator, journal_root_dir: []const u8, journal_identity: []const u8) ![]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(journal_identity, &digest, .{});
    const hex = std.fmt.bytesToHex(digest, .lower);
    const filename = try std.fmt.allocPrint(allocator, "feed.{s}.checkpoint.jsonl", .{hex[0..]});
    defer allocator.free(filename);
    return std.fs.path.join(allocator, &.{ journal_root_dir, filename });
}

fn compareMeta(meta: MarkdownMemory.EventMeta, input: MemoryEventInput) i8 {
    if (input.timestamp_ms < meta.timestamp_ms) return -1;
    if (input.timestamp_ms > meta.timestamp_ms) return 1;

    const order = std.mem.order(u8, input.origin_instance_id, meta.origin_instance_id);
    if (order == .lt) return -1;
    if (order == .gt) return 1;

    if (input.origin_sequence < meta.origin_sequence) return -1;
    if (input.origin_sequence > meta.origin_sequence) return 1;
    return 0;
}

fn compareStoredState(state: MarkdownMemory.StoredState, input: MemoryEventInput) i8 {
    if (input.timestamp_ms < state.timestamp_ms) return -1;
    if (input.timestamp_ms > state.timestamp_ms) return 1;

    const order = std.mem.order(u8, input.origin_instance_id, state.origin_instance_id);
    if (order == .lt) return -1;
    if (order == .gt) return 1;

    if (input.origin_sequence < state.origin_sequence) return -1;
    if (input.origin_sequence > state.origin_sequence) return 1;
    return 0;
}

fn compareStoredStates(a: MarkdownMemory.StoredState, b: MarkdownMemory.StoredState) i8 {
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

fn parseRecordedEventLine(allocator: std.mem.Allocator, line: []const u8) !MarkdownMemory.RecordedEvent {
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
    if (schema_version_raw != 1) return error.InvalidEvent;

    const event_category = if (jsonNullableStringField(val, "category")) |cat|
        try root.cloneMemoryCategory(allocator, root.MemoryCategory.fromString(cat))
    else
        null;
    errdefer if (event_category) |category| switch (category) {
        .custom => |name| allocator.free(name),
        else => {},
    };

    const effect = MarkdownMemory.Effect.fromString(effect_str) orelse return error.InvalidEvent;
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
            .schema_version = 1,
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

// ── Tests ──────────────────────────────────────────────────────────

test "markdown forget removes matching entry" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    var mem = try MarkdownMemory.init(std.testing.allocator, base);
    defer mem.deinit();
    const m = mem.memory();

    try m.store("key1", "value1", .core, null);
    try m.store("key2", "value2", .core, "sess-a");

    try std.testing.expect(try m.forgetScoped(std.testing.allocator, "key2", "sess-a"));
    try std.testing.expect((try m.getScoped(std.testing.allocator, "key2", "sess-a")) == null);
    try std.testing.expect(try m.forget("key1"));
    try std.testing.expect((try m.getScoped(std.testing.allocator, "key1", null)) == null);
}

test "markdown parseEntries skips empty lines" {
    const text = "line one\n\n\nline two\n";
    const entries = try MarkdownMemory.parseEntries(text, "test", .core, std.testing.allocator);
    defer {
        for (entries) |*e| e.deinit(std.testing.allocator);
        std.testing.allocator.free(entries);
    }
    try std.testing.expectEqual(@as(usize, 2), entries.len);
    try std.testing.expectEqualStrings("line one", entries[0].content);
    try std.testing.expectEqualStrings("line two", entries[1].content);
}

test "markdown parseEntries skips headings" {
    const text = "# Heading\nContent under heading\n## Sub\nMore content";
    const entries = try MarkdownMemory.parseEntries(text, "test", .core, std.testing.allocator);
    defer {
        for (entries) |*e| e.deinit(std.testing.allocator);
        std.testing.allocator.free(entries);
    }
    try std.testing.expectEqual(@as(usize, 2), entries.len);
    try std.testing.expectEqualStrings("Content under heading", entries[0].content);
    try std.testing.expectEqualStrings("More content", entries[1].content);
}

test "markdown parseEntries strips bullet prefix" {
    const text = "- Item one\n- Item two\nPlain line";
    const entries = try MarkdownMemory.parseEntries(text, "test", .core, std.testing.allocator);
    defer {
        for (entries) |*e| e.deinit(std.testing.allocator);
        std.testing.allocator.free(entries);
    }
    try std.testing.expectEqual(@as(usize, 3), entries.len);
    try std.testing.expectEqualStrings("Item one", entries[0].content);
    try std.testing.expectEqualStrings("Item two", entries[1].content);
    try std.testing.expectEqualStrings("Plain line", entries[2].content);
}

test "markdown parseEntries generates sequential ids" {
    const text = "a\nb\nc";
    const entries = try MarkdownMemory.parseEntries(text, "myfile", .core, std.testing.allocator);
    defer {
        for (entries) |*e| e.deinit(std.testing.allocator);
        std.testing.allocator.free(entries);
    }
    try std.testing.expectEqual(@as(usize, 3), entries.len);
    try std.testing.expectEqualStrings("myfile:0", entries[0].id);
    try std.testing.expectEqualStrings("myfile:1", entries[1].id);
    try std.testing.expectEqualStrings("myfile:2", entries[2].id);
}

test "markdown parseEntries empty text returns empty" {
    const entries = try MarkdownMemory.parseEntries("", "test", .core, std.testing.allocator);
    defer std.testing.allocator.free(entries);
    try std.testing.expectEqual(@as(usize, 0), entries.len);
}

test "markdown parseEntries only headings returns empty" {
    const text = "# Heading\n## Another\n### Third";
    const entries = try MarkdownMemory.parseEntries(text, "test", .core, std.testing.allocator);
    defer std.testing.allocator.free(entries);
    try std.testing.expectEqual(@as(usize, 0), entries.len);
}

test "markdown parseEntries preserves category" {
    const text = "content";
    const entries = try MarkdownMemory.parseEntries(text, "test", .daily, std.testing.allocator);
    defer {
        for (entries) |*e| e.deinit(std.testing.allocator);
        std.testing.allocator.free(entries);
    }
    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expect(entries[0].category.eql(.daily));
}

test "markdown persists exact session_id namespaces" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    var mem = try MarkdownMemory.init(std.testing.allocator, base);
    defer mem.deinit();
    const m = mem.memory();

    try m.store("sess_key", "session data", .core, "session-123");
    try m.store("sess_key", "global data", .core, null);

    const recalled = try m.recall(std.testing.allocator, "session", 10, "session-123");
    defer {
        for (recalled) |*e| e.deinit(std.testing.allocator);
        std.testing.allocator.free(recalled);
    }

    try std.testing.expectEqual(@as(usize, 1), recalled.len);
    try std.testing.expect(recalled[0].session_id != null);
    try std.testing.expectEqualStrings("session-123", recalled[0].session_id.?);

    const listed = try m.list(std.testing.allocator, null, "session-123");
    defer {
        for (listed) |*e| e.deinit(std.testing.allocator);
        std.testing.allocator.free(listed);
    }
    try std.testing.expectEqual(@as(usize, 1), listed.len);
    try std.testing.expect(listed[0].session_id != null);
    try std.testing.expectEqualStrings("session-123", listed[0].session_id.?);
}

test "markdown getScoped returns entry inside isolated workspace" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    var mem = try MarkdownMemory.init(std.testing.allocator, base);
    defer mem.deinit();
    const m = mem.memory();

    try m.store("scoped_key", "session data", .core, "session-123");

    const entry = (try m.getScoped(std.testing.allocator, "scoped_key", "session-123")).?;
    defer entry.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("session data", entry.content);
    try std.testing.expectEqualStrings("session-123", entry.session_id.?);
}

test "markdown get returns latest matching entry for duplicate key" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    var mem = try MarkdownMemory.init(std.testing.allocator, base);
    defer mem.deinit();
    const m = mem.memory();

    try m.store("dup_key", "old", .core, null);
    try m.store("dup_key", "new", .core, null);

    const entry = (try m.get(std.testing.allocator, "dup_key")).?;
    defer entry.deinit(std.testing.allocator);
    try std.testing.expect(std.mem.indexOf(u8, entry.content, "new") != null);
}

test "markdown get ignores session-scoped entries" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    var mem = try MarkdownMemory.init(std.testing.allocator, base);
    defer mem.deinit();
    const m = mem.memory();

    try m.store("scoped_only", "session data", .core, "sess-a");
    try std.testing.expect((try m.get(std.testing.allocator, "scoped_only")) == null);
}

test "markdown native feed does not auto-bootstrap legacy markdown state" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    {
        var legacy = try MarkdownProjectionMemory.init(std.testing.allocator, base);
        defer legacy.deinit();
        const mem = legacy.memory();
        try mem.store("prefs/theme", "solarized", .core, null);
        try mem.store("prefs/locale", "ru", .core, "sess-a");
    }

    var upgraded = try MarkdownMemory.initWithInstanceId(std.testing.allocator, base, "agent-a");
    defer upgraded.deinit();
    const mem = upgraded.memory();

    try std.testing.expect((try mem.get(std.testing.allocator, "prefs/theme")) == null);
    try std.testing.expect((try mem.getScoped(std.testing.allocator, "prefs/locale", "sess-a")) == null);

    const info = try mem.eventFeedInfo(std.testing.allocator);
    defer info.deinit(std.testing.allocator);
    try std.testing.expectEqual(root.MemoryEventFeedStorage.native, info.storage_kind);
    try std.testing.expect(info.journal_path != null);
    try std.testing.expect(info.checkpoint_path != null);
    try std.testing.expectEqual(@as(u64, 0), info.last_sequence);
    try std.testing.expectEqual(@as(u64, 0), info.compacted_through_sequence);

    const events = try mem.listEvents(std.testing.allocator, 0, 8);
    defer root.freeEvents(std.testing.allocator, events);
    try std.testing.expectEqual(@as(usize, 0), events.len);
}

test "markdown native feed roundtrip across workspaces" {
    var tmp_a = std.testing.tmpDir(.{});
    defer tmp_a.cleanup();
    const ws_a = try tmp_a.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(ws_a);

    var tmp_b = std.testing.tmpDir(.{});
    defer tmp_b.cleanup();
    const ws_b = try tmp_b.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(ws_b);

    var source = try MarkdownMemory.initWithInstanceId(std.testing.allocator, ws_a, "agent-a");
    defer source.deinit();
    var replica = try MarkdownMemory.initWithInstanceId(std.testing.allocator, ws_b, "agent-b");
    defer replica.deinit();

    const src = source.memory();
    const dst = replica.memory();
    try src.store("traits/tone", "formal", .core, null);
    try src.store("prefs/lang", "zig", .core, "sess-b");

    const events = try src.listEvents(std.testing.allocator, 0, 16);
    defer root.freeEvents(std.testing.allocator, events);
    try std.testing.expectEqual(@as(usize, 2), events.len);

    for (events) |event| {
        try dst.applyEvent(.{
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

    const tone = (try dst.get(std.testing.allocator, "traits/tone")).?;
    defer tone.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("formal", tone.content);

    const lang = (try dst.getScoped(std.testing.allocator, "prefs/lang", "sess-b")).?;
    defer lang.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("zig", lang.content);
}

test "markdown recall ignores out-of-band projection edits" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    var mem = try MarkdownMemory.initWithInstanceId(std.testing.allocator, base, "agent-a");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("traits/tone", "formal", .core, null);

    const memory_path = try std.fs.path.join(std.testing.allocator, &.{ base, "MEMORY.md" });
    defer std.testing.allocator.free(memory_path);
    try MarkdownProjectionMemory.writeFileContents(
        memory_path,
        "- **traits/tone**: casual <!-- nullclaw:category=core;session= -->",
        std.testing.allocator,
    );

    const formal = try m.recall(std.testing.allocator, "formal", 8, null);
    defer root.freeEntries(std.testing.allocator, formal);
    try std.testing.expectEqual(@as(usize, 1), formal.len);
    try std.testing.expectEqualStrings("formal", formal[0].content);

    const casual = try m.recall(std.testing.allocator, "casual", 8, null);
    defer root.freeEntries(std.testing.allocator, casual);
    try std.testing.expectEqual(@as(usize, 0), casual.len);
}

test "markdown checkpoint requires meta and preserves state on invalid payload" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    var mem = try MarkdownMemory.initWithInstanceId(std.testing.allocator, base, "agent-a");
    defer mem.deinit();
    const m = mem.memory();

    try m.store("prefs/theme", "solarized", .core, null);

    const invalid_checkpoint =
        \\{"kind":"state","key":"prefs/theme","session_id":null,"category":"core","value_kind":null,"content":"dracula","timestamp_ms":1,"origin_instance_id":"agent-a","origin_sequence":1}
        \\
    ;

    try std.testing.expectError(error.InvalidEvent, m.applyCheckpoint(invalid_checkpoint));

    const entry = (try m.get(std.testing.allocator, "prefs/theme")).?;
    defer entry.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("solarized", entry.content);
}

test "markdown compaction enforces cursor floor and survives reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    var compacted_through: u64 = 0;
    {
        var mem = try MarkdownMemory.initWithInstanceId(std.testing.allocator, base, "agent-a");
        defer mem.deinit();
        const m = mem.memory();

        try m.store("prefs/theme", "dark", .core, null);
        compacted_through = try m.compactEvents();
        try std.testing.expect(compacted_through > 0);
        try std.testing.expectError(error.CursorExpired, m.listEvents(std.testing.allocator, 0, 8));

        const info = try m.eventFeedInfo(std.testing.allocator);
        defer info.deinit(std.testing.allocator);
        try std.testing.expectEqual(compacted_through, info.compacted_through_sequence);
        try std.testing.expectEqual(compacted_through + 1, info.oldest_available_sequence);
    }

    {
        var reopened = try MarkdownMemory.initWithInstanceId(std.testing.allocator, base, "agent-a");
        defer reopened.deinit();
        const m = reopened.memory();

        const entry = (try m.get(std.testing.allocator, "prefs/theme")).?;
        defer entry.deinit(std.testing.allocator);
        try std.testing.expectEqualStrings("dark", entry.content);
        try std.testing.expectEqual(compacted_through, try m.lastEventSequence());

        const tail = try m.listEvents(std.testing.allocator, compacted_through, 8);
        defer root.freeEvents(std.testing.allocator, tail);
        try std.testing.expectEqual(@as(usize, 0), tail.len);
    }
}
