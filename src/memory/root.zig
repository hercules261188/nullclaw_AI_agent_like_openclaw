//! Memory module — persistent knowledge storage for nullclaw.
//!
//! Mirrors ZeroClaw's memory architecture:
//!   - Memory vtable interface (store, recall, get, list, forget, count)
//!   - MemoryEntry, MemoryCategory
//!   - Multiple backends: SQLite (FTS5), Markdown (file-based), None (no-op)
//!   - ResponseCache for LLM response deduplication
//!   - Document chunking for large markdown files

const std = @import("std");
const build_options = @import("build_options");
const config_types = @import("../config_types.zig");
const fs_compat = @import("../fs_compat.zig");
const json_util = @import("../json_util.zig");
const provider_api_key = @import("../providers/api_key.zig");
const log = std.log.scoped(.memory);

// engines/ (Layer A: Primary Store)
pub const sqlite = if (build_options.enable_sqlite) @import("engines/sqlite.zig") else @import("engines/sqlite_disabled.zig");
pub const markdown = @import("engines/markdown.zig");
pub const none = @import("engines/none.zig");
pub const memory_lru = @import("engines/memory_lru.zig");
pub const lucid = if (build_options.enable_memory_lucid) @import("engines/lucid.zig") else struct {
    pub const LucidMemory = struct {};
};
pub const postgres = if (build_options.enable_postgres) @import("engines/postgres.zig") else struct {};
pub const redis = @import("engines/redis.zig");
pub const lancedb = if (build_options.enable_memory_lancedb) @import("engines/lancedb.zig") else struct {
    pub const LanceDbMemory = struct {};
};
pub const api = @import("engines/api.zig");
pub const clickhouse = @import("engines/clickhouse.zig");
pub const registry = @import("engines/registry.zig");
pub const context_core = @import("context_core.zig");
pub const memory_feed = @import("feed.zig");

// retrieval/ (Layer B: Retrieval Engine)
pub const retrieval = @import("retrieval/engine.zig");
pub const retrieval_qmd = @import("retrieval/qmd.zig");
pub const rrf = @import("retrieval/rrf.zig");
pub const query_expansion = @import("retrieval/query_expansion.zig");
pub const temporal_decay = @import("retrieval/temporal_decay.zig");
pub const mmr = @import("retrieval/mmr.zig");
pub const adaptive = @import("retrieval/adaptive.zig");
pub const llm_reranker = @import("retrieval/llm_reranker.zig");

// vector/ (Layer C: Vector Plane)
pub const vector = @import("vector/math.zig");
pub const vector_store = @import("vector/store.zig");
pub const embeddings = @import("vector/embeddings.zig");
pub const embeddings_gemini = @import("vector/embeddings_gemini.zig");
pub const embeddings_voyage = @import("vector/embeddings_voyage.zig");
pub const embeddings_ollama = @import("vector/embeddings_ollama.zig");
pub const provider_router = @import("vector/provider_router.zig");
pub const store_qdrant = @import("vector/store_qdrant.zig");
pub const store_pgvector = @import("vector/store_pgvector.zig");
pub const vector_key = @import("vector/key_codec.zig");
pub const circuit_breaker = @import("vector/circuit_breaker.zig");
pub const outbox = @import("vector/outbox.zig");
pub const chunker = @import("vector/chunker.zig");

// lifecycle/ (Layer D: Runtime Orchestrator)
pub const cache = @import("lifecycle/cache.zig");
pub const semantic_cache = @import("lifecycle/semantic_cache.zig");
pub const hygiene = @import("lifecycle/hygiene.zig");
pub const snapshot = @import("lifecycle/snapshot.zig");
pub const rollout = @import("lifecycle/rollout.zig");
pub const migrate = @import("lifecycle/migrate.zig");
pub const diagnostics = @import("lifecycle/diagnostics.zig");
pub const summarizer = @import("lifecycle/summarizer.zig");

pub const SqliteMemory = sqlite.SqliteMemory;
pub const MarkdownMemory = markdown.MarkdownMemory;
pub const NoneMemory = none.NoneMemory;
pub const InMemoryLruMemory = memory_lru.InMemoryLruMemory;
pub const LucidMemory = lucid.LucidMemory;
pub const PostgresMemory = if (build_options.enable_postgres) postgres.PostgresMemory else struct {};
pub const RedisMemory = redis.RedisMemory;
pub const ClickHouseMemory = clickhouse.ClickHouseMemory;
pub const LanceDbMemory = lancedb.LanceDbMemory;
pub const ApiMemory = api.ApiMemory;
pub const ResponseCache = cache.ResponseCache;
pub const Chunk = chunker.Chunk;
pub const chunkMarkdown = chunker.chunkMarkdown;
pub const EmbeddingProvider = embeddings.EmbeddingProvider;
pub const NoopEmbedding = embeddings.NoopEmbedding;
pub const cosineSimilarity = vector.cosineSimilarity;
pub const ScoredResult = vector.ScoredResult;
pub const hybridMerge = vector.hybridMerge;
pub const HygieneReport = hygiene.HygieneReport;
pub const exportSnapshot = snapshot.exportSnapshot;
pub const hydrateFromSnapshot = snapshot.hydrateFromSnapshot;
pub const shouldHydrate = snapshot.shouldHydrate;
pub const BackendDescriptor = registry.BackendDescriptor;
pub const BackendConfig = registry.BackendConfig;
pub const BackendInstance = registry.BackendInstance;
pub const BackendCapabilities = registry.BackendCapabilities;
pub const findBackend = registry.findBackend;
pub const RetrievalCandidate = retrieval.RetrievalCandidate;
pub const RetrievalSourceAdapter = retrieval.RetrievalSourceAdapter;
pub const PrimaryAdapter = retrieval.PrimaryAdapter;
pub const RetrievalEngine = retrieval.RetrievalEngine;
pub const QmdAdapter = retrieval_qmd.QmdAdapter;
pub const rrfMerge = rrf.rrfMerge;
pub const applyTemporalDecay = temporal_decay.applyTemporalDecay;
pub const VectorStore = vector_store.VectorStore;
pub const VectorResult = vector_store.VectorResult;
pub const HealthStatus = vector_store.HealthStatus;
pub const SqliteSharedVectorStore = vector_store.SqliteSharedVectorStore;
pub const SqliteSidecarVectorStore = vector_store.SqliteSidecarVectorStore;
pub const QdrantVectorStore = store_qdrant.QdrantVectorStore;
pub const freeVectorResults = vector_store.freeVectorResults;
pub const VectorOutbox = outbox.VectorOutbox;
pub const CircuitBreaker = circuit_breaker.CircuitBreaker;
pub const RolloutMode = rollout.RolloutMode;
pub const RolloutPolicy = rollout.RolloutPolicy;
pub const RolloutDecision = rollout.RolloutDecision;
pub const SqliteSourceEntry = migrate.SqliteSourceEntry;
pub const readBrainDb = migrate.readBrainDb;
pub const freeSqliteEntries = migrate.freeSqliteEntries;
pub const DiagnosticReport = diagnostics.DiagnosticReport;
pub const CacheStats = diagnostics.CacheStats;
pub const diagnoseRuntime = diagnostics.diagnose;
pub const formatDiagnosticReport = diagnostics.formatReport;
pub const ContextCore = context_core.ContextCore;
pub const ContextApplyResult = context_core.ContextApplyResult;
pub const MemoryFeed = memory_feed.MemoryFeed;

// Extended retrieval stages
pub const expandQuery = query_expansion.expandQuery;
pub const ExpandedQuery = query_expansion.ExpandedQuery;
pub const analyzeQuery = adaptive.analyzeQuery;
pub const AdaptiveConfig = adaptive.AdaptiveConfig;
pub const QueryAnalysis = adaptive.QueryAnalysis;
pub const RetrievalStrategy = adaptive.RetrievalStrategy;
pub const buildRerankPrompt = llm_reranker.buildRerankPrompt;
pub const parseRerankResponse = llm_reranker.parseRerankResponse;
pub const LlmRerankerConfig = llm_reranker.LlmRerankerConfig;

// Lifecycle: summarizer
pub const SummarizerConfig = summarizer.SummarizerConfig;
pub const SummaryResult = summarizer.SummaryResult;
pub const shouldSummarize = summarizer.shouldSummarize;
pub const buildSummarizationPrompt = summarizer.buildSummarizationPrompt;
pub const parseSummaryResponse = summarizer.parseSummaryResponse;

// Lifecycle: semantic cache
pub const SemanticCache = semantic_cache.SemanticCache;

// ── Session message types ─────────────────────────────────────────

pub const RUNTIME_COMMAND_ROLE = "__runtime_command__";

pub fn isRuntimeCommandRole(role: []const u8) bool {
    return std.mem.eql(u8, role, RUNTIME_COMMAND_ROLE);
}

pub const MessageEntry = struct {
    role: []const u8,
    content: []const u8,
};

pub fn freeMessages(allocator: std.mem.Allocator, messages: []MessageEntry) void {
    for (messages) |entry| {
        allocator.free(entry.role);
        allocator.free(entry.content);
    }
    allocator.free(messages);
}

/// Session summary for listing sessions.
pub const SessionInfo = struct {
    session_id: []const u8,
    message_count: u64,
    first_message_at: []const u8,
    last_message_at: []const u8,

    pub fn deinit(self: SessionInfo, allocator: std.mem.Allocator) void {
        allocator.free(self.session_id);
        allocator.free(self.first_message_at);
        allocator.free(self.last_message_at);
    }
};

pub fn freeSessionInfos(allocator: std.mem.Allocator, infos: []SessionInfo) void {
    for (infos) |info| info.deinit(allocator);
    allocator.free(infos);
}

/// Message with timestamp for detailed history.
pub const DetailedMessageEntry = struct {
    role: []const u8,
    content: []const u8,
    created_at: []const u8,
};

pub fn freeDetailedMessages(allocator: std.mem.Allocator, entries: []DetailedMessageEntry) void {
    for (entries) |entry| {
        allocator.free(entry.role);
        allocator.free(entry.content);
        allocator.free(entry.created_at);
    }
    allocator.free(entries);
}

// ── SessionStore vtable interface ─────────────────────────────────

pub const SessionStore = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        saveMessage: *const fn (ptr: *anyopaque, session_id: []const u8, role: []const u8, content: []const u8) anyerror!void,
        loadMessages: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator, session_id: []const u8) anyerror![]MessageEntry,
        clearMessages: *const fn (ptr: *anyopaque, session_id: []const u8) anyerror!void,
        clearAutoSaved: *const fn (ptr: *anyopaque, session_id: ?[]const u8) anyerror!void,
        saveUsage: ?*const fn (ptr: *anyopaque, session_id: []const u8, total_tokens: u64) anyerror!void = null,
        loadUsage: ?*const fn (ptr: *anyopaque, session_id: []const u8) anyerror!?u64 = null,
        countSessions: ?*const fn (ptr: *anyopaque) anyerror!u64 = null,
        listSessions: ?*const fn (ptr: *anyopaque, allocator: std.mem.Allocator, limit: usize, offset: usize) anyerror![]SessionInfo = null,
        countDetailedMessages: ?*const fn (ptr: *anyopaque, session_id: []const u8) anyerror!u64 = null,
        loadMessagesDetailed: ?*const fn (ptr: *anyopaque, allocator: std.mem.Allocator, session_id: []const u8, limit: usize, offset: usize) anyerror![]DetailedMessageEntry = null,
    };

    pub fn saveMessage(self: SessionStore, session_id: []const u8, role: []const u8, content: []const u8) !void {
        return self.vtable.saveMessage(self.ptr, session_id, role, content);
    }

    pub fn loadMessages(self: SessionStore, allocator: std.mem.Allocator, session_id: []const u8) ![]MessageEntry {
        return self.vtable.loadMessages(self.ptr, allocator, session_id);
    }

    pub fn clearMessages(self: SessionStore, session_id: []const u8) !void {
        return self.vtable.clearMessages(self.ptr, session_id);
    }

    pub fn clearAutoSaved(self: SessionStore, session_id: ?[]const u8) !void {
        return self.vtable.clearAutoSaved(self.ptr, session_id);
    }

    pub fn saveUsage(self: SessionStore, session_id: []const u8, total_tokens: u64) !void {
        const func = self.vtable.saveUsage orelse return error.NotSupported;
        return func(self.ptr, session_id, total_tokens);
    }

    pub fn loadUsage(self: SessionStore, session_id: []const u8) !?u64 {
        const func = self.vtable.loadUsage orelse return null;
        return func(self.ptr, session_id);
    }

    pub fn countSessions(self: SessionStore) !u64 {
        const func = self.vtable.countSessions orelse return error.NotSupported;
        return func(self.ptr);
    }

    pub fn listSessions(self: SessionStore, allocator: std.mem.Allocator, limit: usize, offset: usize) ![]SessionInfo {
        const func = self.vtable.listSessions orelse return error.NotSupported;
        return func(self.ptr, allocator, limit, offset);
    }

    pub fn countDetailedMessages(self: SessionStore, session_id: []const u8) !u64 {
        const func = self.vtable.countDetailedMessages orelse return error.NotSupported;
        return func(self.ptr, session_id);
    }

    pub fn loadMessagesDetailed(self: SessionStore, allocator: std.mem.Allocator, session_id: []const u8, limit: usize, offset: usize) ![]DetailedMessageEntry {
        const func = self.vtable.loadMessagesDetailed orelse return error.NotSupported;
        return func(self.ptr, allocator, session_id, limit, offset);
    }
};

// ── Memory categories ──────────────────────────────────────────────

pub const MemoryCategory = union(enum) {
    core,
    daily,
    conversation,
    custom: []const u8,

    pub fn toString(self: MemoryCategory) []const u8 {
        return switch (self) {
            .core => "core",
            .daily => "daily",
            .conversation => "conversation",
            .custom => |name| name,
        };
    }

    pub fn fromString(s: []const u8) MemoryCategory {
        if (std.mem.eql(u8, s, "core")) return .core;
        if (std.mem.eql(u8, s, "daily")) return .daily;
        if (std.mem.eql(u8, s, "conversation")) return .conversation;
        return .{ .custom = s };
    }

    pub fn eql(a: MemoryCategory, b: MemoryCategory) bool {
        const TagType = @typeInfo(MemoryCategory).@"union".tag_type.?;
        const tag_a: TagType = a;
        const tag_b: TagType = b;
        if (tag_a != tag_b) return false;
        if (tag_a == .custom) {
            return std.mem.eql(u8, a.custom, b.custom);
        }
        return true;
    }
};

// ── Memory entry ───────────────────────────────────────────────────

pub const MemoryEntry = struct {
    id: []const u8,
    key: []const u8,
    content: []const u8,
    category: MemoryCategory,
    timestamp: []const u8,
    session_id: ?[]const u8 = null,
    score: ?f64 = null,

    /// Free all allocated strings owned by this entry.
    pub fn deinit(self: *const MemoryEntry, allocator: std.mem.Allocator) void {
        allocator.free(self.id);
        allocator.free(self.key);
        allocator.free(self.content);
        allocator.free(self.timestamp);
        if (self.session_id) |sid| allocator.free(sid);
        switch (self.category) {
            .custom => |name| allocator.free(name),
            else => {},
        }
    }
};

pub fn freeEntries(allocator: std.mem.Allocator, entries: []MemoryEntry) void {
    for (entries) |*entry| {
        entry.deinit(allocator);
    }
    allocator.free(entries);
}

pub const MemoryEventOp = enum {
    put,
    delete_all,
    delete_scoped,

    pub fn toString(self: MemoryEventOp) []const u8 {
        return switch (self) {
            .put => "put",
            .delete_all => "delete_all",
            .delete_scoped => "delete_scoped",
        };
    }

    pub fn fromString(value: []const u8) ?MemoryEventOp {
        if (std.mem.eql(u8, value, "put")) return .put;
        if (std.mem.eql(u8, value, "delete_all")) return .delete_all;
        if (std.mem.eql(u8, value, "delete_scoped")) return .delete_scoped;
        return null;
    }
};

pub const MEMORY_EVENT_SCHEMA_VERSION: u16 = 1;

pub const MemoryEventInput = struct {
    schema_version: ?u16 = null,
    operation: MemoryEventOp,
    key: []const u8,
    content: ?[]const u8 = null,
    category: ?MemoryCategory = null,
    session_id: ?[]const u8 = null,
    origin_instance_id: ?[]const u8 = null,
    origin_sequence: ?u64 = null,
    timestamp_ms: ?i64 = null,
};

pub const MemoryEvent = struct {
    schema_version: u16,
    sequence: u64,
    timestamp_ms: i64,
    origin_instance_id: []const u8,
    origin_sequence: u64,
    operation: MemoryEventOp,
    key: []const u8,
    content: ?[]const u8 = null,
    category: ?MemoryCategory = null,
    session_id: ?[]const u8 = null,

    pub fn deinit(self: *const MemoryEvent, allocator: std.mem.Allocator) void {
        allocator.free(self.origin_instance_id);
        allocator.free(self.key);
        if (self.content) |content| allocator.free(content);
        if (self.session_id) |session_id| allocator.free(session_id);
        if (self.category) |category| switch (category) {
            .custom => |name| allocator.free(name),
            else => {},
        };
    }
};

pub fn freeEvents(allocator: std.mem.Allocator, events: []MemoryEvent) void {
    for (events) |*event| {
        event.deinit(allocator);
    }
    allocator.free(events);
}

pub const MemoryEventFeedInfo = struct {
    schema_version: u16 = MEMORY_EVENT_SCHEMA_VERSION,
    compacted_through_sequence: u64 = 0,
    latest_sequence: u64 = 0,
    core_backend: []const u8 = "sqlite",
    projection_backend: []const u8 = "none",
    projection_last_applied_sequence: u64 = 0,
    projection_lag: u64 = 0,
    recall_source: []const u8 = "core",
};

pub const MemoryApplyResult = struct {
    accepted: bool,
    materialized: bool,
    sequence: u64 = 0,
};

pub const PromptBootstrapKeyPrefix = "__bootstrap.prompt.";

pub const PromptBootstrapDoc = struct {
    filename: []const u8,
    memory_key: []const u8,
};

pub const prompt_bootstrap_docs = [_]PromptBootstrapDoc{
    .{ .filename = "AGENTS.md", .memory_key = "__bootstrap.prompt.AGENTS.md" },
    .{ .filename = "SOUL.md", .memory_key = "__bootstrap.prompt.SOUL.md" },
    .{ .filename = "TOOLS.md", .memory_key = "__bootstrap.prompt.TOOLS.md" },
    .{ .filename = "CONFIG.md", .memory_key = "__bootstrap.prompt.CONFIG.md" },
    .{ .filename = "IDENTITY.md", .memory_key = "__bootstrap.prompt.IDENTITY.md" },
    .{ .filename = "USER.md", .memory_key = "__bootstrap.prompt.USER.md" },
    .{ .filename = "HEARTBEAT.md", .memory_key = "__bootstrap.prompt.HEARTBEAT.md" },
    .{ .filename = "BOOTSTRAP.md", .memory_key = "__bootstrap.prompt.BOOTSTRAP.md" },
    .{ .filename = "MEMORY.md", .memory_key = "__bootstrap.prompt.MEMORY.md" },
};

pub fn promptBootstrapMemoryKey(filename: []const u8) ?[]const u8 {
    for (prompt_bootstrap_docs) |doc| {
        if (std.mem.eql(u8, doc.filename, filename)) return doc.memory_key;
    }
    return null;
}

/// markdown and hybrid backends keep bootstrap identity in workspace files;
/// all other backends use backend-native key/value entries.
pub fn usesWorkspaceBootstrapFiles(memory_backend: ?[]const u8) bool {
    const backend = memory_backend orelse return true;
    return std.mem.eql(u8, backend, "markdown") or std.mem.eql(u8, backend, "hybrid");
}

pub fn isInternalMemoryKey(key: []const u8) bool {
    return std.mem.startsWith(u8, key, "autosave_user_") or
        std.mem.startsWith(u8, key, "autosave_assistant_") or
        std.mem.eql(u8, key, "last_hygiene_at") or
        std.mem.startsWith(u8, key, PromptBootstrapKeyPrefix);
}

pub fn extractMarkdownMemoryKey(content: []const u8) ?[]const u8 {
    const trimmed = std.mem.trim(u8, content, " \t");
    if (!std.mem.startsWith(u8, trimmed, "**")) return null;
    const rest = trimmed[2..];
    const suffix = std.mem.indexOf(u8, rest, "**:") orelse return null;
    if (suffix == 0) return null;
    return rest[0..suffix];
}

pub fn isInternalMemoryEntryKeyOrContent(key: []const u8, content: []const u8) bool {
    if (isInternalMemoryKey(key)) return true;
    if (extractMarkdownMemoryKey(content)) |extracted| {
        if (isInternalMemoryKey(extracted)) return true;
    }
    return false;
}

fn trimCandidatesToLimit(allocator: std.mem.Allocator, candidates: []RetrievalCandidate, limit: usize) ![]RetrievalCandidate {
    if (candidates.len <= limit) return candidates;

    // If allocation fails while trimming, free the original result to avoid leaks.
    errdefer retrieval.freeCandidates(allocator, candidates);

    var trimmed = try allocator.alloc(RetrievalCandidate, limit);
    for (candidates[0..limit], 0..) |candidate, i| {
        trimmed[i] = candidate;
    }
    for (candidates[limit..]) |*candidate| {
        candidate.deinit(allocator);
    }
    allocator.free(candidates);

    return trimmed;
}

// ── Memory vtable interface ────────────────────────────────────────

pub const Memory = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        name: *const fn (ptr: *anyopaque) []const u8,
        store: *const fn (ptr: *anyopaque, key: []const u8, content: []const u8, category: MemoryCategory, session_id: ?[]const u8) anyerror!void,
        recall: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator, query: []const u8, limit: usize, session_id: ?[]const u8) anyerror![]MemoryEntry,
        get: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8) anyerror!?MemoryEntry,
        getScoped: ?*const fn (ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8, session_id: ?[]const u8) anyerror!?MemoryEntry = null,
        list: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator, category: ?MemoryCategory, session_id: ?[]const u8) anyerror![]MemoryEntry,
        forget: *const fn (ptr: *anyopaque, key: []const u8) anyerror!bool,
        forgetScoped: ?*const fn (ptr: *anyopaque, key: []const u8, session_id: ?[]const u8) anyerror!bool = null,
        listEvents: ?*const fn (ptr: *anyopaque, allocator: std.mem.Allocator, after_sequence: ?u64, limit: usize) anyerror![]MemoryEvent = null,
        applyEvent: ?*const fn (ptr: *anyopaque, event: MemoryEventInput) anyerror!bool = null,
        lastEventSequence: ?*const fn (ptr: *anyopaque) anyerror!u64 = null,
        eventFeedInfo: ?*const fn (ptr: *anyopaque) anyerror!MemoryEventFeedInfo = null,
        compactEvents: ?*const fn (ptr: *anyopaque) anyerror!usize = null,
        rebuildProjection: ?*const fn (ptr: *anyopaque) anyerror!void = null,
        count: *const fn (ptr: *anyopaque) anyerror!usize,
        healthCheck: *const fn (ptr: *anyopaque) bool,
        deinit: *const fn (ptr: *anyopaque) void,
    };

    pub fn name(self: Memory) []const u8 {
        return self.vtable.name(self.ptr);
    }

    pub fn store(self: Memory, key: []const u8, content: []const u8, category: MemoryCategory, session_id: ?[]const u8) !void {
        return self.vtable.store(self.ptr, key, content, category, session_id);
    }

    pub fn recall(self: Memory, allocator: std.mem.Allocator, query: []const u8, limit: usize, session_id: ?[]const u8) ![]MemoryEntry {
        return self.vtable.recall(self.ptr, allocator, query, limit, session_id);
    }

    pub fn get(self: Memory, allocator: std.mem.Allocator, key: []const u8) !?MemoryEntry {
        return self.vtable.get(self.ptr, allocator, key);
    }

    pub fn getScoped(self: Memory, allocator: std.mem.Allocator, key: []const u8, session_id: ?[]const u8) !?MemoryEntry {
        if (self.vtable.getScoped) |func| {
            return func(self.ptr, allocator, key, session_id);
        }

        if (session_id == null) {
            return self.vtable.get(self.ptr, allocator, key);
        }

        const entries = try self.vtable.list(self.ptr, allocator, null, session_id);
        defer allocator.free(entries);

        var found: ?MemoryEntry = null;
        errdefer if (found) |value| value.deinit(allocator);

        for (entries) |*entry_ptr| {
            if (std.mem.eql(u8, entry_ptr.key, key)) {
                if (found) |*prev| prev.deinit(allocator);
                found = entry_ptr.*;
            } else {
                @constCast(entry_ptr).deinit(allocator);
            }
        }

        return found;
    }

    pub fn list(self: Memory, allocator: std.mem.Allocator, category: ?MemoryCategory, session_id: ?[]const u8) ![]MemoryEntry {
        return self.vtable.list(self.ptr, allocator, category, session_id);
    }

    pub fn forget(self: Memory, key: []const u8) !bool {
        return self.vtable.forget(self.ptr, key);
    }

    pub fn forgetScoped(self: Memory, allocator: std.mem.Allocator, key: []const u8, session_id: ?[]const u8) !bool {
        if (self.vtable.forgetScoped) |func| {
            return func(self.ptr, key, session_id);
        }

        _ = allocator;
        if (session_id != null) {
            return error.NotSupported;
        }
        return self.vtable.forget(self.ptr, key);
    }

    pub fn listEvents(self: Memory, allocator: std.mem.Allocator, after_sequence: ?u64, limit: usize) ![]MemoryEvent {
        const func = self.vtable.listEvents orelse return error.NotSupported;
        return func(self.ptr, allocator, after_sequence, limit);
    }

    pub fn applyEvent(self: Memory, event: MemoryEventInput) !bool {
        const func = self.vtable.applyEvent orelse return error.NotSupported;
        return func(self.ptr, event);
    }

    pub fn lastEventSequence(self: Memory) !u64 {
        const func = self.vtable.lastEventSequence orelse return error.NotSupported;
        return func(self.ptr);
    }

    pub fn eventFeedInfo(self: Memory) !MemoryEventFeedInfo {
        const func = self.vtable.eventFeedInfo orelse return error.NotSupported;
        return func(self.ptr);
    }

    pub fn compactEvents(self: Memory) !usize {
        const func = self.vtable.compactEvents orelse return error.NotSupported;
        return func(self.ptr);
    }

    pub fn rebuildProjection(self: Memory) !void {
        const func = self.vtable.rebuildProjection orelse return error.NotSupported;
        return func(self.ptr);
    }

    pub fn count(self: Memory) !usize {
        return self.vtable.count(self.ptr);
    }

    pub fn healthCheck(self: Memory) bool {
        return self.vtable.healthCheck(self.ptr);
    }

    pub fn deinit(self: Memory) void {
        self.vtable.deinit(self.ptr);
    }

    /// Hybrid search: combine keyword recall with optional vector similarity.
    /// This is a convenience method that wraps recall() and merges results.
    /// If an embedding provider is available, it can be used for vector search;
    /// otherwise falls back to keyword-only search via recall().
    pub fn search(self: Memory, allocator: std.mem.Allocator, query: []const u8, limit: usize) ![]MemoryEntry {
        // For now, delegate to recall() which uses FTS5/keyword search.
        // When embeddings are integrated at a higher level, this serves as
        // the standard entry point that can be upgraded to hybrid search.
        return self.recall(allocator, query, limit, null);
    }
};

const EVENT_JOURNAL_FILENAME_PREFIX = ".nullclaw-memory.";
const EVENT_JOURNAL_FILENAME_SUFFIX = ".events.jsonl";
const EVENT_CHECKPOINT_FILENAME_SUFFIX = ".checkpoint.json";
const MAX_EVENT_LINE_BYTES = 16 * 1024 * 1024;

const EventSourcedMemory = struct {
    allocator: std.mem.Allocator,
    backend: Memory,
    journal_path: []const u8,
    checkpoint_path: []const u8,
    local_instance_id: []const u8,
    projection: std.StringHashMapUnmanaged(ProjectedEntry) = .empty,
    origin_frontiers: std.StringHashMapUnmanaged(u64) = .empty,
    last_sequence_value: u64 = 0,
    compacted_through_sequence: u64 = 0,
    use_backend_recall: bool = false,
    owns_self: bool = false,

    const Self = @This();

    const ProjectedEntry = struct {
        key: []const u8,
        content: []const u8,
        category: MemoryCategory,
        session_id: ?[]const u8,
        timestamp: []const u8,
        sequence: u64,
    };

    const PreparedProjectionMutation = union(enum) {
        none,
        put: struct {
            storage_key: []u8,
            entry: ProjectedEntry,
        },
        delete_scoped: []u8,
        delete_all: std.ArrayListUnmanaged([]u8),
    };

    pub fn init(allocator: std.mem.Allocator, backend: Memory, workspace_dir: []const u8, instance_id: []const u8) !Self {
        const stable_instance_id = if (instance_id.len > 0) instance_id else "default";
        const journal_path = try buildJournalPath(allocator, workspace_dir, stable_instance_id);
        errdefer allocator.free(journal_path);
        const checkpoint_path = try buildCheckpointPath(allocator, workspace_dir, stable_instance_id);
        errdefer allocator.free(checkpoint_path);

        const local_instance_id = try allocator.dupe(u8, stable_instance_id);
        errdefer allocator.free(local_instance_id);

        var self = Self{
            .allocator = allocator,
            .backend = backend,
            .journal_path = journal_path,
            .checkpoint_path = checkpoint_path,
            .local_instance_id = local_instance_id,
            .use_backend_recall = backendSupportsExactProjection(backend),
        };
        errdefer self.release();

        const loaded_checkpoint = try self.loadCheckpoint();
        try self.loadJournal(!loaded_checkpoint and self.use_backend_recall);
        if (loaded_checkpoint and self.use_backend_recall) {
            try self.rebuildBackendProjection();
        }
        if (self.last_sequence_value == 0) {
            try self.bootstrapFromBackend();
        }
        return self;
    }

    pub fn deinit(self: *Self) void {
        self.release();
        if (self.owns_self) self.allocator.destroy(self);
    }

    fn release(self: *Self) void {
        var projection_it = self.projection.iterator();
        while (projection_it.next()) |entry| {
            self.freeProjectedEntry(entry.value_ptr.*);
            self.allocator.free(entry.key_ptr.*);
        }
        self.projection.deinit(self.allocator);

        var origin_it = self.origin_frontiers.iterator();
        while (origin_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.origin_frontiers.deinit(self.allocator);

        self.allocator.free(self.journal_path);
        self.allocator.free(self.checkpoint_path);
        self.allocator.free(self.local_instance_id);
        self.backend.deinit();
    }

    pub fn memory(self: *Self) Memory {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    fn buildJournalPath(allocator: std.mem.Allocator, workspace_dir: []const u8, stable_instance_id: []const u8) ![]u8 {
        return buildArtifactPath(allocator, workspace_dir, stable_instance_id, EVENT_JOURNAL_FILENAME_SUFFIX);
    }

    fn buildCheckpointPath(allocator: std.mem.Allocator, workspace_dir: []const u8, stable_instance_id: []const u8) ![]u8 {
        return buildArtifactPath(allocator, workspace_dir, stable_instance_id, EVENT_CHECKPOINT_FILENAME_SUFFIX);
    }

    fn buildArtifactPath(allocator: std.mem.Allocator, workspace_dir: []const u8, stable_instance_id: []const u8, suffix: []const u8) ![]u8 {
        var hash_buf: [32]u8 = undefined;
        const hash = std.hash.Wyhash.hash(0, stable_instance_id);
        const hash_str = try std.fmt.bufPrint(&hash_buf, "{x}", .{hash});
        const filename = try std.fmt.allocPrint(
            allocator,
            "{s}{s}{s}",
            .{ EVENT_JOURNAL_FILENAME_PREFIX, hash_str, suffix },
        );
        defer allocator.free(filename);
        return std.fs.path.join(allocator, &.{ workspace_dir, filename });
    }

    fn backendSupportsExactProjection(backend: Memory) bool {
        const name = backend.name();
        return std.mem.eql(u8, name, "sqlite") or
            std.mem.eql(u8, name, "lucid") or
            std.mem.eql(u8, name, "memory_lru") or
            std.mem.eql(u8, name, "api");
    }

    fn dupCategory(allocator: std.mem.Allocator, category: MemoryCategory) !MemoryCategory {
        return switch (category) {
            .custom => |name| .{ .custom = try allocator.dupe(u8, name) },
            else => category,
        };
    }

    fn cloneProjectedEntry(self: *Self, event: MemoryEvent) !ProjectedEntry {
        const content = event.content orelse return error.InvalidEvent;
        const category = event.category orelse return error.InvalidEvent;
        const dup_key = try self.allocator.dupe(u8, event.key);
        errdefer self.allocator.free(dup_key);
        const dup_content = try self.allocator.dupe(u8, content);
        errdefer self.allocator.free(dup_content);
        const dup_category = try dupCategory(self.allocator, category);
        errdefer switch (dup_category) {
            .custom => |name| self.allocator.free(name),
            else => {},
        };
        const dup_session_id = if (event.session_id) |session_id| try self.allocator.dupe(u8, session_id) else null;
        errdefer if (dup_session_id) |session_id| self.allocator.free(session_id);
        const dup_timestamp = try std.fmt.allocPrint(self.allocator, "{d}", .{event.timestamp_ms});
        errdefer self.allocator.free(dup_timestamp);

        return .{
            .key = dup_key,
            .content = dup_content,
            .category = dup_category,
            .session_id = dup_session_id,
            .timestamp = dup_timestamp,
            .sequence = event.sequence,
        };
    }

    fn freeProjectedEntry(self: *Self, entry: ProjectedEntry) void {
        self.allocator.free(entry.key);
        self.allocator.free(entry.content);
        self.allocator.free(entry.timestamp);
        if (entry.session_id) |session_id| self.allocator.free(session_id);
        switch (entry.category) {
            .custom => |name| self.allocator.free(name),
            else => {},
        }
    }

    fn projectedToEntry(allocator: std.mem.Allocator, entry: ProjectedEntry) !MemoryEntry {
        const id = try std.fmt.allocPrint(allocator, "{d}", .{entry.sequence});
        errdefer allocator.free(id);
        const key = try allocator.dupe(u8, entry.key);
        errdefer allocator.free(key);
        const content = try allocator.dupe(u8, entry.content);
        errdefer allocator.free(content);
        const category = try dupCategory(allocator, entry.category);
        errdefer switch (category) {
            .custom => |name| allocator.free(name),
            else => {},
        };
        const timestamp = try allocator.dupe(u8, entry.timestamp);
        errdefer allocator.free(timestamp);
        const session_id = if (entry.session_id) |value| try allocator.dupe(u8, value) else null;
        errdefer if (session_id) |value| allocator.free(value);

        return .{
            .id = id,
            .key = key,
            .content = content,
            .category = category,
            .timestamp = timestamp,
            .session_id = session_id,
            .score = null,
        };
    }

    fn cloneEvent(
        allocator: std.mem.Allocator,
        schema_version: u16,
        sequence: u64,
        timestamp_ms: i64,
        origin_instance_id: []const u8,
        origin_sequence: u64,
        operation: MemoryEventOp,
        key: []const u8,
        content: ?[]const u8,
        category: ?MemoryCategory,
        session_id: ?[]const u8,
    ) !MemoryEvent {
        const dup_origin_instance_id = try allocator.dupe(u8, origin_instance_id);
        errdefer allocator.free(dup_origin_instance_id);
        const dup_key = try allocator.dupe(u8, key);
        errdefer allocator.free(dup_key);
        const dup_content = if (content) |value| try allocator.dupe(u8, value) else null;
        errdefer if (dup_content) |value| allocator.free(value);
        const dup_category = if (category) |value| try dupCategory(allocator, value) else null;
        errdefer if (dup_category) |value| switch (value) {
            .custom => |name| allocator.free(name),
            else => {},
        };
        const dup_session_id = if (session_id) |value| try allocator.dupe(u8, value) else null;
        errdefer if (dup_session_id) |value| allocator.free(value);

        return .{
            .schema_version = schema_version,
            .sequence = sequence,
            .timestamp_ms = timestamp_ms,
            .origin_instance_id = dup_origin_instance_id,
            .origin_sequence = origin_sequence,
            .operation = operation,
            .key = dup_key,
            .content = dup_content,
            .category = dup_category,
            .session_id = dup_session_id,
        };
    }

    fn markAppliedOrigin(self: *Self, origin_instance_id: []const u8, origin_sequence: u64) !void {
        if (self.origin_frontiers.getPtr(origin_instance_id)) |frontier| {
            if (origin_sequence > frontier.*) frontier.* = origin_sequence;
            return;
        }

        const key = try self.allocator.dupe(u8, origin_instance_id);
        errdefer self.allocator.free(key);
        try self.origin_frontiers.put(self.allocator, key, origin_sequence);
    }

    fn hasAppliedOrigin(self: *Self, origin_instance_id: []const u8, origin_sequence: u64) bool {
        const frontier = self.origin_frontiers.get(origin_instance_id) orelse return false;
        return origin_sequence <= frontier;
    }

    fn tryLockFile(file: std.fs.File, lock: std.fs.File.Lock) !bool {
        file.lock(lock) catch |err| switch (err) {
            error.FileLocksNotSupported => return false,
            else => return err,
        };
        return true;
    }

    fn writeJsonEscaped(out: anytype, text: []const u8) !void {
        for (text) |ch| {
            switch (ch) {
                '"' => try out.writeAll("\\\""),
                '\\' => try out.writeAll("\\\\"),
                '\n' => try out.writeAll("\\n"),
                '\r' => try out.writeAll("\\r"),
                '\t' => try out.writeAll("\\t"),
                else => {
                    if (ch < 0x20) {
                        var escape_buf: [6]u8 = undefined;
                        const escape = try std.fmt.bufPrint(&escape_buf, "\\u{x:0>4}", .{ch});
                        try out.writeAll(escape);
                    } else {
                        try out.writeByte(ch);
                    }
                },
            }
        }
    }

    fn writeJsonString(out: anytype, text: []const u8) !void {
        try out.writeByte('"');
        try writeJsonEscaped(out, text);
        try out.writeByte('"');
    }

    fn writeJsonNullableString(out: anytype, text: ?[]const u8) !void {
        if (text) |value| {
            try writeJsonString(out, value);
        } else {
            try out.writeAll("null");
        }
    }

    fn writeJsonNullableCategory(out: anytype, category: ?MemoryCategory) !void {
        if (category) |value| {
            try writeJsonString(out, value.toString());
        } else {
            try out.writeAll("null");
        }
    }

    fn appendEventLine(self: *Self, event: MemoryEvent) !void {
        var file = self.openJournalForAppend() catch |err| {
            log.warn("memory event journal append open failed for '{s}': {}", .{ self.journal_path, err });
            return err;
        };
        defer file.close();
        const locked = try tryLockFile(file, .exclusive);
        defer if (locked) file.unlock();

        try file.seekFromEnd(0);

        var line: std.ArrayListUnmanaged(u8) = .empty;
        defer line.deinit(self.allocator);

        try line.append(self.allocator, '{');
        try json_util.appendJsonKey(&line, self.allocator, "schema_version");
        try line.writer(self.allocator).print("{d}", .{event.schema_version});
        try line.append(self.allocator, ',');
        try json_util.appendJsonKey(&line, self.allocator, "sequence");
        try line.writer(self.allocator).print("{d}", .{event.sequence});
        try line.append(self.allocator, ',');
        try json_util.appendJsonInt(&line, self.allocator, "timestamp_ms", event.timestamp_ms);
        try line.append(self.allocator, ',');
        try json_util.appendJsonKeyValue(&line, self.allocator, "origin_instance_id", event.origin_instance_id);
        try line.append(self.allocator, ',');
        try json_util.appendJsonKey(&line, self.allocator, "origin_sequence");
        try line.writer(self.allocator).print("{d}", .{event.origin_sequence});
        try line.append(self.allocator, ',');
        try json_util.appendJsonKeyValue(&line, self.allocator, "operation", event.operation.toString());
        try line.append(self.allocator, ',');
        try json_util.appendJsonKeyValue(&line, self.allocator, "key", event.key);

        if (event.content) |content| {
            try line.append(self.allocator, ',');
            try json_util.appendJsonKeyValue(&line, self.allocator, "content", content);
        } else {
            try line.append(self.allocator, ',');
            try json_util.appendJsonKey(&line, self.allocator, "content");
            try line.appendSlice(self.allocator, "null");
        }

        if (event.category) |category| {
            try line.append(self.allocator, ',');
            try json_util.appendJsonKeyValue(&line, self.allocator, "category", category.toString());
        } else {
            try line.append(self.allocator, ',');
            try json_util.appendJsonKey(&line, self.allocator, "category");
            try line.appendSlice(self.allocator, "null");
        }

        if (event.session_id) |session_id| {
            try line.append(self.allocator, ',');
            try json_util.appendJsonKeyValue(&line, self.allocator, "session_id", session_id);
        } else {
            try line.append(self.allocator, ',');
            try json_util.appendJsonKey(&line, self.allocator, "session_id");
            try line.appendSlice(self.allocator, "null");
        }

        try line.appendSlice(self.allocator, "}\n");
        try file.writeAll(line.items);
        try file.sync();
    }

    fn openJournalForRead(self: *Self) !std.fs.File {
        if (std.fs.path.isAbsolute(self.journal_path)) {
            return std.fs.openFileAbsolute(self.journal_path, .{});
        }
        return std.fs.cwd().openFile(self.journal_path, .{});
    }

    fn ensureJournalParentDir(self: *Self) !void {
        const parent = std.fs.path.dirname(self.journal_path) orelse return;
        try fs_compat.makePath(parent);
    }

    fn ensureCheckpointParentDir(self: *Self) !void {
        const parent = std.fs.path.dirname(self.checkpoint_path) orelse return;
        try fs_compat.makePath(parent);
    }

    fn openCheckpointForRead(self: *Self) !std.fs.File {
        if (std.fs.path.isAbsolute(self.checkpoint_path)) {
            return std.fs.openFileAbsolute(self.checkpoint_path, .{});
        }
        return std.fs.cwd().openFile(self.checkpoint_path, .{});
    }

    fn openCheckpointForWrite(self: *Self, path: []const u8) !std.fs.File {
        try self.ensureCheckpointParentDir();
        if (std.fs.path.isAbsolute(path)) {
            return std.fs.createFileAbsolute(path, .{ .read = true, .truncate = true });
        }
        return std.fs.cwd().createFile(path, .{ .read = true, .truncate = true });
    }

    fn readCheckpointContent(self: *Self) ![]u8 {
        var file = try self.openCheckpointForRead();
        defer file.close();
        const locked = try tryLockFile(file, .shared);
        defer if (locked) file.unlock();

        const meta = fs_compat.stat(file) catch |err| switch (err) {
            error.SystemResources => return file.readToEndAlloc(self.allocator, 1024 * 1024),
            else => return err,
        };
        const max_bytes: u64 = @intCast(std.math.maxInt(usize));
        return file.readToEndAlloc(self.allocator, @intCast(@min(meta.size + 1, max_bytes)));
    }

    fn openJournalForAppend(self: *Self) !std.fs.File {
        try self.ensureJournalParentDir();
        if (std.fs.path.isAbsolute(self.journal_path)) {
            return std.fs.openFileAbsolute(self.journal_path, .{ .mode = .read_write }) catch |err| switch (err) {
                error.FileNotFound => std.fs.createFileAbsolute(self.journal_path, .{ .read = true, .truncate = false }),
                else => err,
            };
        }
        return std.fs.cwd().openFile(self.journal_path, .{ .mode = .read_write }) catch |err| switch (err) {
            error.FileNotFound => std.fs.cwd().createFile(self.journal_path, .{ .read = true, .truncate = false }),
            else => err,
        };
    }

    fn openJournalForCompact(self: *Self) !std.fs.File {
        return self.openJournalForAppend();
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

    fn parseRequiredI64(obj: std.json.ObjectMap, key: []const u8) !i64 {
        const value = obj.get(key) orelse return error.InvalidEventJournal;
        return switch (value) {
            .integer => |n| n,
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

    fn parseEventLineAlloc(self: *Self, allocator: std.mem.Allocator, line: []const u8) !MemoryEvent {
        _ = self;
        var parsed = try std.json.parseFromSlice(std.json.Value, allocator, line, .{});
        defer parsed.deinit();

        if (parsed.value != .object) return error.InvalidEventJournal;
        const obj = parsed.value.object;

        const op_name = try parseRequiredString(obj, "operation");
        const operation = MemoryEventOp.fromString(op_name) orelse return error.InvalidEventJournal;
        const category_name = try parseOptionalString(obj, "category");
        const schema_version_u64 = (try parseOptionalU64(obj, "schema_version")) orelse MEMORY_EVENT_SCHEMA_VERSION;
        if (schema_version_u64 != MEMORY_EVENT_SCHEMA_VERSION) return error.UnsupportedEventSchema;

        return cloneEvent(
            allocator,
            @intCast(schema_version_u64),
            try parseRequiredU64(obj, "sequence"),
            try parseRequiredI64(obj, "timestamp_ms"),
            try parseRequiredString(obj, "origin_instance_id"),
            try parseRequiredU64(obj, "origin_sequence"),
            operation,
            try parseRequiredString(obj, "key"),
            try parseOptionalString(obj, "content"),
            if (category_name) |name| MemoryCategory.fromString(name) else null,
            try parseOptionalString(obj, "session_id"),
        );
    }

    fn parseEventLine(self: *Self, line: []const u8) !MemoryEvent {
        return self.parseEventLineAlloc(self.allocator, line);
    }

    fn loadCheckpoint(self: *Self) !bool {
        const content = self.readCheckpointContent() catch |err| switch (err) {
            error.FileNotFound => return false,
            else => return err,
        };
        defer self.allocator.free(content);

        if (content.len == 0) return false;

        var parsed = try std.json.parseFromSlice(std.json.Value, self.allocator, content, .{});
        defer parsed.deinit();

        if (parsed.value != .object) return error.InvalidEventCheckpoint;
        const obj = parsed.value.object;

        const schema_version_u64 = (try parseOptionalU64(obj, "schema_version")) orelse MEMORY_EVENT_SCHEMA_VERSION;
        if (schema_version_u64 != MEMORY_EVENT_SCHEMA_VERSION) return error.UnsupportedEventSchema;

        const included_sequence = try parseRequiredU64(obj, "included_sequence");
        const entries_value = obj.get("entries") orelse return error.InvalidEventCheckpoint;
        const entries_array = switch (entries_value) {
            .array => |value| value,
            else => return error.InvalidEventCheckpoint,
        };

        for (entries_array.items) |item| {
            const entry_obj = switch (item) {
                .object => |value| value,
                else => return error.InvalidEventCheckpoint,
            };

            const category_name = try parseRequiredString(entry_obj, "category");
            const session_id = try parseOptionalString(entry_obj, "session_id");
            const key = try parseRequiredString(entry_obj, "key");
            const entry_content = try parseRequiredString(entry_obj, "content");
            const timestamp = try parseRequiredString(entry_obj, "timestamp");
            const storage_key = try vector_key.encode(self.allocator, key, session_id);
            errdefer self.allocator.free(storage_key);
            const dup_key = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(dup_key);
            const dup_content = try self.allocator.dupe(u8, entry_content);
            errdefer self.allocator.free(dup_content);
            const dup_category = try dupCategory(self.allocator, MemoryCategory.fromString(category_name));
            errdefer switch (dup_category) {
                .custom => |name| self.allocator.free(name),
                else => {},
            };
            const dup_session_id = if (session_id) |value| try self.allocator.dupe(u8, value) else null;
            errdefer if (dup_session_id) |value| self.allocator.free(value);
            const dup_timestamp = try self.allocator.dupe(u8, timestamp);
            errdefer self.allocator.free(dup_timestamp);

            const projected = ProjectedEntry{
                .key = dup_key,
                .content = dup_content,
                .category = dup_category,
                .session_id = dup_session_id,
                .timestamp = dup_timestamp,
                .sequence = try parseRequiredU64(entry_obj, "sequence"),
            };

            try self.projection.put(self.allocator, storage_key, projected);
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
                const origin_id = try self.allocator.dupe(u8, entry.key_ptr.*);
                errdefer self.allocator.free(origin_id);
                try self.origin_frontiers.put(self.allocator, origin_id, frontier);
            }
        }

        self.compacted_through_sequence = included_sequence;
        self.last_sequence_value = included_sequence;
        return true;
    }

    fn loadJournal(self: *Self, sync_backend: bool) !void {
        var file = self.openJournalForRead() catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };
        defer file.close();
        const locked = try tryLockFile(file, .shared);
        defer if (locked) file.unlock();

        var reader = file.deprecatedReader();
        while (try reader.readUntilDelimiterOrEofAlloc(self.allocator, '\n', MAX_EVENT_LINE_BYTES)) |raw_line| {
            defer self.allocator.free(raw_line);
            const line = std.mem.trimRight(u8, raw_line, "\r");
            if (line.len == 0) continue;

            const event = try self.parseEventLine(line);
            errdefer event.deinit(self.allocator);
            if (event.sequence <= self.compacted_through_sequence) {
                event.deinit(self.allocator);
                continue;
            }
            try self.commitLoadedEvent(event, sync_backend);
        }
    }

    fn bootstrapFromBackend(self: *Self) !void {
        const entries = try self.backend.list(self.allocator, null, null);
        defer freeEntries(self.allocator, entries);

        for (entries) |entry| {
            _ = try self.recordEvent(.{
                .operation = .put,
                .key = entry.key,
                .content = entry.content,
                .category = entry.category,
                .session_id = entry.session_id,
            }, false);
        }
    }

    fn commitLoadedEvent(self: *Self, event: MemoryEvent, sync_backend: bool) !void {
        defer event.deinit(self.allocator);
        try self.markAppliedOrigin(event.origin_instance_id, event.origin_sequence);
        self.last_sequence_value = event.sequence;
        try self.applyProjection(event);
        if (sync_backend) {
            self.syncBackendProjection(event);
        }
    }

    fn writeCheckpointFile(self: *Self) !usize {
        const EntryView = struct {
            key: []const u8,
            entry: ProjectedEntry,
        };
        const FrontierView = struct {
            origin_id: []const u8,
            frontier: u64,
        };

        const temp_path = try std.fmt.allocPrint(self.allocator, "{s}.tmp", .{self.checkpoint_path});
        defer self.allocator.free(temp_path);

        var file = try self.openCheckpointForWrite(temp_path);
        defer file.close();
        errdefer if (std.fs.path.isAbsolute(temp_path)) {
            std.fs.deleteFileAbsolute(temp_path) catch {};
        } else {
            std.fs.cwd().deleteFile(temp_path) catch {};
        };

        var entries = try self.allocator.alloc(EntryView, self.projection.count());
        defer self.allocator.free(entries);
        var entry_index: usize = 0;
        var projection_it = self.projection.iterator();
        while (projection_it.next()) |entry| {
            entries[entry_index] = .{
                .key = entry.key_ptr.*,
                .entry = entry.value_ptr.*,
            };
            entry_index += 1;
        }
        std.mem.sort(EntryView, entries, {}, struct {
            fn lessThan(_: void, a: EntryView, b: EntryView) bool {
                const key_order = std.mem.order(u8, a.entry.key, b.entry.key);
                if (key_order != .eq) return key_order == .lt;
                const a_sid = a.entry.session_id orelse "";
                const b_sid = b.entry.session_id orelse "";
                const sid_order = std.mem.order(u8, a_sid, b_sid);
                if (sid_order != .eq) return sid_order == .lt;
                return a.entry.sequence < b.entry.sequence;
            }
        }.lessThan);

        var frontiers = try self.allocator.alloc(FrontierView, self.origin_frontiers.count());
        defer self.allocator.free(frontiers);
        var frontier_index: usize = 0;
        var frontier_it = self.origin_frontiers.iterator();
        while (frontier_it.next()) |entry| {
            frontiers[frontier_index] = .{
                .origin_id = entry.key_ptr.*,
                .frontier = entry.value_ptr.*,
            };
            frontier_index += 1;
        }
        std.mem.sort(FrontierView, frontiers, {}, struct {
            fn lessThan(_: void, a: FrontierView, b: FrontierView) bool {
                return std.mem.order(u8, a.origin_id, b.origin_id) == .lt;
            }
        }.lessThan);

        var writer_buf: [4096]u8 = undefined;
        var bw = file.writer(&writer_buf);
        const out = &bw.interface;

        try out.writeAll("{\"schema_version\":");
        try out.print("{d}", .{MEMORY_EVENT_SCHEMA_VERSION});
        try out.writeAll(",\"included_sequence\":");
        try out.print("{d}", .{self.last_sequence_value});
        try out.writeAll(",\"entries\":[");
        for (entries, 0..) |view, idx| {
            if (idx > 0) try out.writeAll(",");
            try out.writeAll("{\"key\":");
            try writeJsonString(out, view.entry.key);
            try out.writeAll(",\"content\":");
            try writeJsonString(out, view.entry.content);
            try out.writeAll(",\"category\":");
            try writeJsonString(out, view.entry.category.toString());
            try out.writeAll(",\"timestamp\":");
            try writeJsonString(out, view.entry.timestamp);
            try out.writeAll(",\"session_id\":");
            try writeJsonNullableString(out, view.entry.session_id);
            try out.writeAll(",\"sequence\":");
            try out.print("{d}", .{view.entry.sequence});
            try out.writeAll("}");
        }
        try out.writeAll("],\"origin_frontiers\":{");
        for (frontiers, 0..) |view, idx| {
            if (idx > 0) try out.writeAll(",");
            try writeJsonString(out, view.origin_id);
            try out.writeAll(":");
            try out.print("{d}", .{view.frontier});
        }
        try out.writeAll("}}\n");
        try out.flush();
        try file.sync();

        if (std.fs.path.isAbsolute(temp_path)) {
            try std.fs.renameAbsolute(temp_path, self.checkpoint_path);
        } else {
            try std.fs.rename(std.fs.cwd(), temp_path, std.fs.cwd(), self.checkpoint_path);
        }
        return entries.len;
    }

    fn truncateJournalLocked(file: std.fs.File) !void {
        try file.setEndPos(0);
        try file.seekTo(0);
        try file.sync();
    }

    fn rebuildBackendProjection(self: *Self) !void {
        const backend_entries = try self.backend.list(self.allocator, null, null);
        defer freeEntries(self.allocator, backend_entries);

        var reset_keys: std.StringHashMapUnmanaged(void) = .empty;
        defer {
            var it = reset_keys.iterator();
            while (it.next()) |entry| self.allocator.free(entry.key_ptr.*);
            reset_keys.deinit(self.allocator);
        }

        for (backend_entries) |entry| {
            const storage_key = try vector_key.encode(self.allocator, entry.key, entry.session_id);
            defer self.allocator.free(storage_key);
            if (self.projection.contains(storage_key)) continue;

            if (!reset_keys.contains(entry.key)) {
                const dup_key = try self.allocator.dupe(u8, entry.key);
                errdefer self.allocator.free(dup_key);
                try reset_keys.put(self.allocator, dup_key, {});
            }
        }

        var reset_it = reset_keys.iterator();
        while (reset_it.next()) |entry| {
            _ = try self.backend.forget(entry.key_ptr.*);
        }

        var projection_it = self.projection.iterator();
        while (projection_it.next()) |entry| {
            try self.backend.store(
                entry.value_ptr.key,
                entry.value_ptr.content,
                entry.value_ptr.category,
                entry.value_ptr.session_id,
            );
        }
    }

    fn compactJournal(self: *Self) !usize {
        var journal = try self.openJournalForCompact();
        defer journal.close();
        const locked = try tryLockFile(journal, .exclusive);
        defer if (locked) journal.unlock();

        const compacted_entries = try self.writeCheckpointFile();
        try truncateJournalLocked(journal);
        self.compacted_through_sequence = self.last_sequence_value;
        return compacted_entries;
    }

    fn freePreparedProjectionMutation(self: *Self, mutation: *PreparedProjectionMutation) void {
        switch (mutation.*) {
            .none => {},
            .put => |value| {
                self.allocator.free(value.storage_key);
                self.freeProjectedEntry(value.entry);
            },
            .delete_scoped => |storage_key| {
                self.allocator.free(storage_key);
            },
            .delete_all => |*keys| {
                for (keys.items) |storage_key| self.allocator.free(storage_key);
                keys.deinit(self.allocator);
            },
        }
        mutation.* = .none;
    }

    fn prepareProjectionMutation(self: *Self, event: MemoryEvent) !PreparedProjectionMutation {
        return switch (event.operation) {
            .put => .{
                .put = .{
                    .storage_key = try vector_key.encode(self.allocator, event.key, event.session_id),
                    .entry = try self.cloneProjectedEntry(event),
                },
            },
            .delete_scoped => .{
                .delete_scoped = try vector_key.encode(self.allocator, event.key, event.session_id),
            },
            .delete_all => blk: {
                var keys: std.ArrayListUnmanaged([]u8) = .empty;
                errdefer {
                    for (keys.items) |storage_key| self.allocator.free(storage_key);
                    keys.deinit(self.allocator);
                }

                var it = self.projection.iterator();
                while (it.next()) |entry| {
                    if (!std.mem.eql(u8, entry.value_ptr.key, event.key)) continue;
                    try keys.append(self.allocator, try self.allocator.dupe(u8, entry.key_ptr.*));
                }
                break :blk .{ .delete_all = keys };
            },
        };
    }

    fn applyPreparedProjectionMutation(self: *Self, mutation: *PreparedProjectionMutation) void {
        switch (mutation.*) {
            .none => {},
            .put => |value| {
                if (self.projection.getPtr(value.storage_key)) |entry| {
                    self.freeProjectedEntry(entry.*);
                    entry.* = value.entry;
                    self.allocator.free(value.storage_key);
                } else {
                    self.projection.put(self.allocator, value.storage_key, value.entry) catch unreachable;
                }
            },
            .delete_scoped => |storage_key| {
                if (self.projection.fetchRemove(storage_key)) |removed| {
                    self.freeProjectedEntry(removed.value);
                    self.allocator.free(removed.key);
                }
                self.allocator.free(storage_key);
            },
            .delete_all => |*keys| {
                for (keys.items) |storage_key| {
                    if (self.projection.fetchRemove(storage_key)) |removed| {
                        self.freeProjectedEntry(removed.value);
                        self.allocator.free(removed.key);
                    }
                    self.allocator.free(storage_key);
                }
                keys.deinit(self.allocator);
            },
        }
        mutation.* = .none;
    }

    fn recordEvent(self: *Self, input: MemoryEventInput, sync_backend: bool) !bool {
        if (input.operation == .put and (input.content == null or input.category == null)) {
            return error.InvalidEvent;
        }
        const schema_version = input.schema_version orelse MEMORY_EVENT_SCHEMA_VERSION;
        if (schema_version != MEMORY_EVENT_SCHEMA_VERSION) {
            return error.UnsupportedEventSchema;
        }

        const next_sequence = self.last_sequence_value + 1;
        const origin_instance_id = input.origin_instance_id orelse self.local_instance_id;
        const origin_sequence = input.origin_sequence orelse next_sequence;
        if (self.hasAppliedOrigin(origin_instance_id, origin_sequence)) return false;

        const event = try cloneEvent(
            self.allocator,
            schema_version,
            next_sequence,
            input.timestamp_ms orelse std.time.milliTimestamp(),
            origin_instance_id,
            origin_sequence,
            input.operation,
            input.key,
            input.content,
            input.category,
            input.session_id,
        );
        errdefer event.deinit(self.allocator);

        var prepared_projection = try self.prepareProjectionMutation(event);
        defer self.freePreparedProjectionMutation(&prepared_projection);

        try self.origin_frontiers.ensureUnusedCapacity(self.allocator, 1);
        if (input.operation == .put) {
            try self.projection.ensureUnusedCapacity(self.allocator, 1);
        }
        try self.appendEventLine(event);
        try self.markAppliedOrigin(origin_instance_id, origin_sequence);
        self.last_sequence_value = event.sequence;
        self.applyPreparedProjectionMutation(&prepared_projection);
        if (sync_backend) {
            self.syncBackendProjection(event);
        }
        event.deinit(self.allocator);
        return true;
    }

    fn syncBackendProjection(self: *Self, event: MemoryEvent) void {
        switch (event.operation) {
            .put => {
                const content = event.content orelse return;
                const category = event.category orelse return;
                self.backend.store(event.key, content, category, event.session_id) catch |err| {
                    log.warn("memory backend projection store failed for key '{s}': {}", .{ event.key, err });
                };
            },
            .delete_all => {
                _ = self.backend.forget(event.key) catch |err| {
                    log.warn("memory backend projection delete-all failed for key '{s}': {}", .{ event.key, err });
                };
            },
            .delete_scoped => {
                if (self.backend.vtable.forgetScoped) |func| {
                    _ = func(self.backend.ptr, event.key, event.session_id) catch |err| {
                        log.warn("memory backend projection delete-scoped failed for key '{s}': {}", .{ event.key, err });
                    };
                } else {
                    log.info("memory backend '{s}' does not support exact scoped delete for key '{s}'", .{ self.backend.name(), event.key });
                }
            },
        }
    }

    fn applyProjection(self: *Self, event: MemoryEvent) !void {
        switch (event.operation) {
            .put => {
                const storage_key = try vector_key.encode(self.allocator, event.key, event.session_id);
                errdefer self.allocator.free(storage_key);

                const projected = try self.cloneProjectedEntry(event);
                errdefer self.freeProjectedEntry(projected);

                if (self.projection.getPtr(storage_key)) |entry| {
                    self.freeProjectedEntry(entry.*);
                    entry.* = projected;
                    self.allocator.free(storage_key);
                    return;
                }

                try self.projection.put(self.allocator, storage_key, projected);
            },
            .delete_all => {
                var keys_to_remove: std.ArrayListUnmanaged([]u8) = .empty;
                defer {
                    for (keys_to_remove.items) |storage_key| self.allocator.free(storage_key);
                    keys_to_remove.deinit(self.allocator);
                }

                var it = self.projection.iterator();
                while (it.next()) |entry| {
                    if (!std.mem.eql(u8, entry.value_ptr.key, event.key)) continue;
                    try keys_to_remove.append(self.allocator, try self.allocator.dupe(u8, entry.key_ptr.*));
                }

                for (keys_to_remove.items) |storage_key| {
                    if (self.projection.fetchRemove(storage_key)) |removed| {
                        self.freeProjectedEntry(removed.value);
                        self.allocator.free(removed.key);
                    }
                }
            },
            .delete_scoped => {
                const storage_key = try vector_key.encode(self.allocator, event.key, event.session_id);
                defer self.allocator.free(storage_key);

                if (self.projection.fetchRemove(storage_key)) |removed| {
                    self.freeProjectedEntry(removed.value);
                    self.allocator.free(removed.key);
                }
            },
        }
    }

    fn matchesSession(entry_session_id: ?[]const u8, filter_session_id: ?[]const u8) bool {
        if (filter_session_id) |session_id| {
            if (entry_session_id) |entry_id| {
                return std.mem.eql(u8, entry_id, session_id);
            }
            return false;
        }
        return true;
    }

    fn matchesCategory(entry_category: MemoryCategory, filter_category: ?MemoryCategory) bool {
        if (filter_category) |category| {
            return entry_category.eql(category);
        }
        return true;
    }

    fn implName(ptr: *anyopaque) []const u8 {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.backend.name();
    }

    fn implStore(ptr: *anyopaque, key: []const u8, content: []const u8, category: MemoryCategory, session_id: ?[]const u8) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        _ = try self.recordEvent(.{
            .operation = .put,
            .key = key,
            .content = content,
            .category = category,
            .session_id = session_id,
        }, true);
    }

    fn implRecall(ptr: *anyopaque, allocator: std.mem.Allocator, query: []const u8, limit: usize, session_id: ?[]const u8) anyerror![]MemoryEntry {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (self.use_backend_recall) {
            return self.backend.recall(allocator, query, limit, session_id);
        }

        const Match = struct {
            key: []const u8,
            entry: ProjectedEntry,
        };

        var matches: std.ArrayListUnmanaged(Match) = .empty;
        defer matches.deinit(allocator);

        var it = self.projection.iterator();
        while (it.next()) |entry| {
            if (!matchesSession(entry.value_ptr.session_id, session_id)) continue;
            if (std.mem.indexOf(u8, entry.value_ptr.key, query) == null and
                std.mem.indexOf(u8, entry.value_ptr.content, query) == null)
                continue;
            try matches.append(allocator, .{ .key = entry.key_ptr.*, .entry = entry.value_ptr.* });
        }

        std.mem.sort(Match, matches.items, {}, struct {
            fn lessThan(_: void, a: Match, b: Match) bool {
                return a.entry.sequence > b.entry.sequence;
            }
        }.lessThan);

        const result_len = @min(limit, matches.items.len);
        const results = try allocator.alloc(MemoryEntry, result_len);
        var initialized: usize = 0;
        errdefer {
            for (results[0..result_len], 0..) |*entry, idx| {
                if (idx >= initialized) break;
                entry.deinit(allocator);
            }
            allocator.free(results);
        }

        for (matches.items[0..result_len], 0..) |match, idx| {
            _ = match.key;
            results[idx] = try projectedToEntry(allocator, match.entry);
            initialized += 1;
        }
        return results;
    }

    fn implGet(ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8) anyerror!?MemoryEntry {
        const self: *Self = @ptrCast(@alignCast(ptr));

        var best: ?ProjectedEntry = null;
        var best_global = false;
        var best_sequence: u64 = 0;

        var it = self.projection.iterator();
        while (it.next()) |entry| {
            if (!std.mem.eql(u8, entry.value_ptr.key, key)) continue;
            const is_global = entry.value_ptr.session_id == null;
            if (best == null or
                (is_global and !best_global) or
                (is_global == best_global and entry.value_ptr.sequence > best_sequence))
            {
                best = entry.value_ptr.*;
                best_global = is_global;
                best_sequence = entry.value_ptr.sequence;
            }
        }

        if (best) |entry| {
            return try projectedToEntry(allocator, entry);
        }
        return null;
    }

    fn implGetScoped(ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8, session_id: ?[]const u8) anyerror!?MemoryEntry {
        const self: *Self = @ptrCast(@alignCast(ptr));
        const storage_key = try vector_key.encode(allocator, key, session_id);
        defer allocator.free(storage_key);

        const entry = self.projection.get(storage_key) orelse return null;
        return try projectedToEntry(allocator, entry);
    }

    fn implList(ptr: *anyopaque, allocator: std.mem.Allocator, category: ?MemoryCategory, session_id: ?[]const u8) anyerror![]MemoryEntry {
        const self: *Self = @ptrCast(@alignCast(ptr));

        var matches: std.ArrayListUnmanaged(ProjectedEntry) = .empty;
        defer matches.deinit(allocator);

        var it = self.projection.iterator();
        while (it.next()) |entry| {
            if (!matchesSession(entry.value_ptr.session_id, session_id)) continue;
            if (!matchesCategory(entry.value_ptr.category, category)) continue;
            try matches.append(allocator, entry.value_ptr.*);
        }

        std.mem.sort(ProjectedEntry, matches.items, {}, struct {
            fn lessThan(_: void, a: ProjectedEntry, b: ProjectedEntry) bool {
                return a.sequence > b.sequence;
            }
        }.lessThan);

        const results = try allocator.alloc(MemoryEntry, matches.items.len);
        var initialized: usize = 0;
        errdefer {
            for (results[0..matches.items.len], 0..) |*entry, idx| {
                if (idx >= initialized) break;
                entry.deinit(allocator);
            }
            allocator.free(results);
        }

        for (matches.items, 0..) |entry, idx| {
            results[idx] = try projectedToEntry(allocator, entry);
            initialized += 1;
        }
        return results;
    }

    fn implForget(ptr: *anyopaque, key: []const u8) anyerror!bool {
        const self: *Self = @ptrCast(@alignCast(ptr));
        var found = false;

        var it = self.projection.iterator();
        while (it.next()) |entry| {
            if (std.mem.eql(u8, entry.value_ptr.key, key)) {
                found = true;
                break;
            }
        }
        if (!found) return false;

        _ = try self.recordEvent(.{
            .operation = .delete_all,
            .key = key,
        }, true);
        return true;
    }

    fn implForgetScoped(ptr: *anyopaque, key: []const u8, session_id: ?[]const u8) anyerror!bool {
        const self: *Self = @ptrCast(@alignCast(ptr));
        const storage_key = try vector_key.encode(self.allocator, key, session_id);
        defer self.allocator.free(storage_key);
        if (!self.projection.contains(storage_key)) return false;

        _ = try self.recordEvent(.{
            .operation = .delete_scoped,
            .key = key,
            .session_id = session_id,
        }, true);
        return true;
    }

    fn implListEvents(ptr: *anyopaque, allocator: std.mem.Allocator, after_sequence: ?u64, limit: usize) anyerror![]MemoryEvent {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (after_sequence) |after| {
            if (after < self.compacted_through_sequence) return error.CursorExpired;
        }

        var out: std.ArrayListUnmanaged(MemoryEvent) = .empty;
        errdefer {
            for (out.items) |*event| event.deinit(allocator);
            out.deinit(allocator);
        }

        var file = self.openJournalForRead() catch |err| switch (err) {
            error.FileNotFound => return allocator.alloc(MemoryEvent, 0),
            else => return err,
        };
        defer file.close();
        const locked = try tryLockFile(file, .shared);
        defer if (locked) file.unlock();

        var reader = file.deprecatedReader();
        while (try reader.readUntilDelimiterOrEofAlloc(allocator, '\n', MAX_EVENT_LINE_BYTES)) |raw_line| {
            defer allocator.free(raw_line);
            const line = std.mem.trimRight(u8, raw_line, "\r");
            if (line.len == 0) continue;

            const event = try self.parseEventLineAlloc(allocator, line);
            errdefer event.deinit(allocator);
            if (after_sequence) |after| {
                if (event.sequence <= after) {
                    event.deinit(allocator);
                    continue;
                }
            }
            try out.append(allocator, event);
            if (limit > 0 and out.items.len >= limit) break;
        }
        return out.toOwnedSlice(allocator);
    }

    fn implApplyEvent(ptr: *anyopaque, event: MemoryEventInput) anyerror!bool {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.recordEvent(event, true);
    }

    fn implLastEventSequence(ptr: *anyopaque) anyerror!u64 {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.last_sequence_value;
    }

    fn implEventFeedInfo(ptr: *anyopaque) anyerror!MemoryEventFeedInfo {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return .{
            .schema_version = MEMORY_EVENT_SCHEMA_VERSION,
            .compacted_through_sequence = self.compacted_through_sequence,
            .latest_sequence = self.last_sequence_value,
        };
    }

    fn implCompactEvents(ptr: *anyopaque) anyerror!usize {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.compactJournal();
    }

    fn implRebuildProjection(ptr: *anyopaque) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.rebuildBackendProjection();
    }

    fn implCount(ptr: *anyopaque) anyerror!usize {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.projection.count();
    }

    fn implHealthCheck(ptr: *anyopaque) bool {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.backend.healthCheck();
    }

    fn implDeinit(ptr: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.deinit();
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
        .rebuildProjection = &implRebuildProjection,
        .count = &implCount,
        .healthCheck = &implHealthCheck,
        .deinit = &implDeinit,
    };
};

fn wrapEventSourcedMemory(
    allocator: std.mem.Allocator,
    backend: Memory,
    workspace_dir: []const u8,
    instance_id: []const u8,
) !Memory {
    const wrapped = try allocator.create(EventSourcedMemory);
    errdefer allocator.destroy(wrapped);
    wrapped.* = try EventSourcedMemory.init(allocator, backend, workspace_dir, instance_id);
    wrapped.owns_self = true;
    return wrapped.memory();
}

const ContextBackedMemory = struct {
    allocator: std.mem.Allocator,
    core: *ContextCore,
    projection: Memory,
    projection_capabilities: BackendCapabilities,
    projection_backend_name: []const u8,
    projection_last_applied_sequence: u64 = 0,
    projection_trusted: bool = false,
    native_recall_enabled: bool = false,
    owns_self: bool = false,

    const Self = @This();

    fn init(
        allocator: std.mem.Allocator,
        projection: Memory,
        projection_backend_name: []const u8,
        projection_capabilities: BackendCapabilities,
        workspace_dir: []const u8,
        instance_id: []const u8,
    ) !Self {
        const core_ptr = try allocator.create(ContextCore);
        errdefer allocator.destroy(core_ptr);
        core_ptr.* = try ContextCore.init(allocator, workspace_dir, instance_id);
        errdefer core_ptr.deinit();

        const owned_backend_name = try allocator.dupe(u8, projection_backend_name);
        errdefer allocator.free(owned_backend_name);

        var self = Self{
            .allocator = allocator,
            .core = core_ptr,
            .projection = projection,
            .projection_capabilities = projection_capabilities,
            .projection_backend_name = owned_backend_name,
        };
        errdefer self.release();

        if (try self.core.isEmpty()) {
            try self.core.bootstrapFromMemory(self.projection);
            try self.core.markMigrationComplete("backend_bootstrap");
            const latest_after_bootstrap = try self.core.lastEventSequence();
            try self.core.setProjectionSequence(self.projection_backend_name, latest_after_bootstrap);
            self.projection_last_applied_sequence = latest_after_bootstrap;
            self.projection_trusted = true;
        } else {
            self.projection_last_applied_sequence = try self.core.getProjectionSequence(self.projection_backend_name);
        }

        try self.reconcileProjection();
        return self;
    }

    fn memory(self: *Self) Memory {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    fn release(self: *Self) void {
        self.projection.deinit();
        self.core.deinit();
        self.allocator.destroy(self.core);
        self.allocator.free(self.projection_backend_name);
    }

    fn deinit(self: *Self) void {
        self.release();
        if (self.owns_self) self.allocator.destroy(self);
    }

    fn refreshNativeRecallEnabled(self: *Self) void {
        self.native_recall_enabled = self.projection_trusted and self.projection_capabilities.supports_native_recall;
    }

    fn reconcileProjection(self: *Self) !void {
        const latest = try self.core.lastEventSequence();
        if (latest == 0) {
            self.projection_trusted = true;
            self.refreshNativeRecallEnabled();
            return;
        }

        if (self.projection_capabilities.supports_safe_rebuild) {
            if (self.projection_last_applied_sequence < latest) {
                try self.rebuildProjectionFromCore();
                return;
            }
            self.projection_trusted = true;
            self.refreshNativeRecallEnabled();
            return;
        }

        if (self.projection_trusted and self.projection_last_applied_sequence >= latest) {
            self.refreshNativeRecallEnabled();
            return;
        }

        self.projection_trusted = false;
        self.refreshNativeRecallEnabled();
    }

    fn shouldUseNativeRecall(self: *Self, session_id: ?[]const u8) bool {
        if (!self.native_recall_enabled) return false;
        if (!self.projection_capabilities.supports_native_recall) return false;
        if (session_id != null and !self.projection_capabilities.supports_scoped_native_recall) return false;
        return true;
    }

    fn rememberProjectionProgress(self: *Self, sequence: u64) !void {
        self.projection_last_applied_sequence = sequence;
        try self.core.setProjectionSequence(self.projection_backend_name, sequence);
    }

    fn projectEvent(self: *Self, event: MemoryEventInput, sequence: u64) void {
        var success = false;
        switch (event.operation) {
            .put => {
                const content = event.content orelse return;
                const category = event.category orelse return;
                self.projection.store(event.key, content, category, event.session_id) catch |err| {
                    log.warn("memory projection put failed for backend '{s}' key '{s}': {}", .{ self.projection_backend_name, event.key, err });
                    self.projection_trusted = false;
                    self.refreshNativeRecallEnabled();
                    return;
                };
                success = true;
            },
            .delete_all => {
                const entries = self.projection.list(self.allocator, null, null) catch |err| {
                    log.warn("memory projection delete-all list failed for backend '{s}' key '{s}': {}", .{ self.projection_backend_name, event.key, err });
                    self.projection_trusted = false;
                    self.refreshNativeRecallEnabled();
                    return;
                };
                defer freeEntries(self.allocator, entries);

                var any_failed = false;
                var matched = false;
                for (entries) |entry| {
                    if (!std.mem.eql(u8, entry.key, event.key)) continue;
                    matched = true;
                    if (entry.session_id != null and self.projection.vtable.forgetScoped != null) {
                        _ = self.projection.forgetScoped(self.allocator, event.key, entry.session_id) catch {
                            any_failed = true;
                        };
                    } else {
                        _ = self.projection.forget(event.key) catch {
                            any_failed = true;
                        };
                    }
                }

                if (!matched) {
                    _ = self.projection.forget(event.key) catch {};
                }

                if (any_failed) {
                    log.warn("memory projection delete-all degraded for backend '{s}' key '{s}'", .{ self.projection_backend_name, event.key });
                    self.projection_trusted = false;
                    self.refreshNativeRecallEnabled();
                    return;
                }
                success = true;
            },
            .delete_scoped => {
                if (self.projection.vtable.forgetScoped) |_| {
                    _ = self.projection.forgetScoped(self.allocator, event.key, event.session_id) catch |err| {
                        log.warn("memory projection scoped delete failed for backend '{s}' key '{s}': {}", .{ self.projection_backend_name, event.key, err });
                        self.projection_trusted = false;
                        self.refreshNativeRecallEnabled();
                        return;
                    };
                    success = true;
                } else {
                    log.info("memory projection backend '{s}' does not support scoped delete for key '{s}'", .{ self.projection_backend_name, event.key });
                    self.projection_trusted = false;
                    self.refreshNativeRecallEnabled();
                    return;
                }
            },
        }

        if (success) {
            self.rememberProjectionProgress(sequence) catch |err| {
                log.warn("memory projection checkpoint update failed for backend '{s}': {}", .{ self.projection_backend_name, err });
                self.projection_trusted = false;
                self.refreshNativeRecallEnabled();
            };
        }
    }

    fn storeDetailed(
        self: *Self,
        key: []const u8,
        content: []const u8,
        category: MemoryCategory,
        session_id: ?[]const u8,
    ) !MemoryApplyResult {
        const result = try self.core.store(key, content, category, session_id);
        if (result.accepted and result.materialized) {
            self.projectEvent(.{
                .operation = .put,
                .key = key,
                .content = content,
                .category = category,
                .session_id = session_id,
            }, result.sequence);
        }
        return .{
            .accepted = result.accepted,
            .materialized = result.materialized,
            .sequence = result.sequence,
        };
    }

    fn forgetDetailed(self: *Self, key: []const u8) !MemoryApplyResult {
        const result = try self.core.forget(key);
        if (result.accepted and result.materialized) {
            self.projectEvent(.{
                .operation = .delete_all,
                .key = key,
            }, result.sequence);
        }
        return .{
            .accepted = result.accepted,
            .materialized = result.materialized,
            .sequence = result.sequence,
        };
    }

    fn forgetScopedDetailed(self: *Self, key: []const u8, session_id: ?[]const u8) !MemoryApplyResult {
        const result = try self.core.forgetScoped(key, session_id);
        if (result.accepted and result.materialized) {
            self.projectEvent(.{
                .operation = .delete_scoped,
                .key = key,
                .session_id = session_id,
            }, result.sequence);
        }
        return .{
            .accepted = result.accepted,
            .materialized = result.materialized,
            .sequence = result.sequence,
        };
    }

    fn applyDetailed(self: *Self, event: MemoryEventInput) !MemoryApplyResult {
        const result = try self.core.applyInput(event);
        if (result.accepted and result.materialized) {
            self.projectEvent(event, result.sequence);
        }
        return .{
            .accepted = result.accepted,
            .materialized = result.materialized,
            .sequence = result.sequence,
        };
    }

    fn rebuildProjectionFromCore(self: *Self) !void {
        if (!self.projection_capabilities.supports_safe_rebuild) return error.NotSupported;

        const existing = try self.projection.list(self.allocator, null, null);
        defer freeEntries(self.allocator, existing);

        for (existing) |entry| {
            if (entry.session_id != null and self.projection.vtable.forgetScoped != null) {
                _ = self.projection.forgetScoped(self.allocator, entry.key, entry.session_id) catch |err| {
                    log.warn("memory projection scoped clear failed for backend '{s}' key '{s}': {}", .{ self.projection_backend_name, entry.key, err });
                    return err;
                };
            } else {
                _ = self.projection.forget(entry.key) catch |err| {
                    log.warn("memory projection clear failed for backend '{s}' key '{s}': {}", .{ self.projection_backend_name, entry.key, err });
                    return err;
                };
            }
        }

        const entries = try self.core.list(self.allocator, null, null);
        defer freeEntries(self.allocator, entries);
        for (entries) |entry| {
            try self.projection.store(entry.key, entry.content, entry.category, entry.session_id);
        }

        try self.rememberProjectionProgress(try self.core.lastEventSequence());
        self.projection_trusted = true;
        self.refreshNativeRecallEnabled();
    }

    fn feedInfo(self: *Self) !MemoryEventFeedInfo {
        const latest_sequence = try self.core.lastEventSequence();
        const compacted_through_sequence = try self.core.compactedThroughSequence();
        return .{
            .schema_version = MEMORY_EVENT_SCHEMA_VERSION,
            .compacted_through_sequence = compacted_through_sequence,
            .latest_sequence = latest_sequence,
            .core_backend = "sqlite",
            .projection_backend = self.projection_backend_name,
            .projection_last_applied_sequence = self.projection_last_applied_sequence,
            .projection_lag = latest_sequence -| self.projection_last_applied_sequence,
            .recall_source = if (self.native_recall_enabled) "projection" else "core",
        };
    }

    fn implName(ptr: *anyopaque) []const u8 {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.projection_backend_name;
    }

    fn implStore(ptr: *anyopaque, key: []const u8, content: []const u8, category: MemoryCategory, session_id: ?[]const u8) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        _ = try self.storeDetailed(key, content, category, session_id);
    }

    fn implRecall(ptr: *anyopaque, allocator: std.mem.Allocator, query: []const u8, limit: usize, session_id: ?[]const u8) anyerror![]MemoryEntry {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (self.shouldUseNativeRecall(session_id)) {
            return self.projection.recall(allocator, query, limit, session_id);
        }
        return self.core.recall(allocator, query, limit, session_id);
    }

    fn implGet(ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8) anyerror!?MemoryEntry {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.core.get(allocator, key);
    }

    fn implGetScoped(ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8, session_id: ?[]const u8) anyerror!?MemoryEntry {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.core.getScoped(allocator, key, session_id);
    }

    fn implList(ptr: *anyopaque, allocator: std.mem.Allocator, category: ?MemoryCategory, session_id: ?[]const u8) anyerror![]MemoryEntry {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.core.list(allocator, category, session_id);
    }

    fn implForget(ptr: *anyopaque, key: []const u8) anyerror!bool {
        const self: *Self = @ptrCast(@alignCast(ptr));
        const result = try self.forgetDetailed(key);
        return result.accepted;
    }

    fn implForgetScoped(ptr: *anyopaque, key: []const u8, session_id: ?[]const u8) anyerror!bool {
        const self: *Self = @ptrCast(@alignCast(ptr));
        const result = try self.forgetScopedDetailed(key, session_id);
        return result.accepted;
    }

    fn implListEvents(ptr: *anyopaque, allocator: std.mem.Allocator, after_sequence: ?u64, limit: usize) anyerror![]MemoryEvent {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.core.listEvents(allocator, after_sequence, limit);
    }

    fn implApplyEvent(ptr: *anyopaque, event: MemoryEventInput) anyerror!bool {
        const self: *Self = @ptrCast(@alignCast(ptr));
        const result = try self.applyDetailed(event);
        return result.accepted;
    }

    fn implLastEventSequence(ptr: *anyopaque) anyerror!u64 {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.core.lastEventSequence();
    }

    fn implEventFeedInfo(ptr: *anyopaque) anyerror!MemoryEventFeedInfo {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.feedInfo();
    }

    fn implCompactEvents(ptr: *anyopaque) anyerror!usize {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.core.compactEvents();
    }

    fn implRebuildProjection(ptr: *anyopaque) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.rebuildProjectionFromCore();
    }

    fn implCount(ptr: *anyopaque) anyerror!usize {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.core.count();
    }

    fn implHealthCheck(ptr: *anyopaque) bool {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.core.db != null and self.projection.healthCheck();
    }

    fn implDeinit(ptr: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.deinit();
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
        .rebuildProjection = &implRebuildProjection,
        .count = &implCount,
        .healthCheck = &implHealthCheck,
        .deinit = &implDeinit,
    };
};

fn wrapContextBackedMemory(
    allocator: std.mem.Allocator,
    projection: Memory,
    projection_backend_name: []const u8,
    projection_capabilities: BackendCapabilities,
    workspace_dir: []const u8,
    instance_id: []const u8,
) !Memory {
    const wrapped = try allocator.create(ContextBackedMemory);
    errdefer allocator.destroy(wrapped);
    wrapped.* = try ContextBackedMemory.init(
        allocator,
        projection,
        projection_backend_name,
        projection_capabilities,
        workspace_dir,
        instance_id,
    );
    wrapped.owns_self = true;
    return wrapped.memory();
}

// ── MemoryRuntime — bundled memory + session store + capabilities ──

/// Resolved configuration snapshot — captures what was actually resolved during init.
/// Stored in MemoryRuntime for diagnostics, `/doctor`, and runtime inspection.
pub const ResolvedConfig = struct {
    primary_backend: []const u8,
    retrieval_mode: []const u8, // "disabled" | "keyword" | "hybrid"
    vector_mode: []const u8, // "none" | "sqlite_shared" | "sqlite_sidecar" | "sqlite_ann" | "qdrant" | "pgvector"
    embedding_provider: []const u8, // "none" | "openai" | "gemini" | "voyage" | "ollama" | "auto"
    rollout_mode: []const u8,
    vector_sync_mode: []const u8, // "best_effort" | "durable_outbox"
    hygiene_enabled: bool,
    snapshot_enabled: bool,
    cache_enabled: bool,
    semantic_cache_enabled: bool,
    summarizer_enabled: bool,
    source_count: usize,
    fallback_policy: []const u8, // "degrade" | "fail_fast"
};

pub const MemoryRuntime = struct {
    memory: Memory,
    session_store: ?SessionStore,
    response_cache: ?*cache.ResponseCache,
    capabilities: BackendCapabilities,
    resolved: ResolvedConfig,

    // Internal: owned resources for cleanup
    _db_path: ?[*:0]const u8,
    _cache_db_path: ?[*:0]const u8,
    _engine: ?*retrieval.RetrievalEngine,
    _allocator: std.mem.Allocator,
    _search_enabled: bool = true,

    // P5: rollout policy
    _rollout_policy: rollout.RolloutPolicy = .{ .mode = .on, .canary_percent = 0, .shadow_percent = 0 },

    // Lifecycle: summarizer config
    _summarizer_cfg: summarizer.SummarizerConfig = .{},

    // Lifecycle: semantic cache (optional, extends response cache with cosine similarity)
    _semantic_cache: ?*semantic_cache.SemanticCache = null,
    _semantic_cache_db_path: ?[*:0]const u8 = null,

    // P3: vector plane components (all optional)
    _embedding_provider: ?embeddings.EmbeddingProvider = null,
    _vector_store: ?vector_store.VectorStore = null,
    _circuit_breaker: ?*circuit_breaker.CircuitBreaker = null,
    _outbox: ?*outbox.VectorOutbox = null,
    _sidecar_db_path: ?[*:0]const u8 = null,

    /// High-level search: uses rollout policy to decide keyword-only vs hybrid.
    pub fn search(self: *MemoryRuntime, allocator: std.mem.Allocator, query: []const u8, limit: usize, session_id: ?[]const u8) ![]RetrievalCandidate {
        if (!self._search_enabled) return allocator.alloc(RetrievalCandidate, 0);

        const decision = self._rollout_policy.decide(session_id);

        switch (decision) {
            .keyword_only => {
                // Bypass engine, use recall() directly
                const entries = try self.memory.recall(allocator, query, limit, session_id);
                defer freeEntries(allocator, entries);
                return retrieval.entriesToCandidates(allocator, entries);
            },
            .hybrid => {
                // Use engine if available, else fall back
                if (self._engine) |engine| {
                    const candidates = try engine.search(allocator, query, session_id);
                    return trimCandidatesToLimit(allocator, candidates, limit);
                }
                const entries = try self.memory.recall(allocator, query, limit, session_id);
                defer freeEntries(allocator, entries);
                return retrieval.entriesToCandidates(allocator, entries);
            },
            .shadow_hybrid => {
                // Run both, serve keyword result, log hybrid for comparison
                const keyword_entries = try self.memory.recall(allocator, query, limit, session_id);
                defer freeEntries(allocator, keyword_entries);
                const keyword_results = try retrieval.entriesToCandidates(allocator, keyword_entries);

                if (self._engine) |engine| {
                    const hybrid_results = engine.search(allocator, query, session_id) catch |err| {
                        log.warn("shadow hybrid search failed: {}", .{err});
                        return keyword_results;
                    };
                    defer retrieval.freeCandidates(allocator, hybrid_results);

                    log.info("shadow: keyword={d} hybrid={d} results", .{ keyword_results.len, hybrid_results.len });
                }

                return keyword_results;
            },
        }
    }

    /// Get current rollout mode.
    pub fn rolloutMode(self: *const MemoryRuntime) rollout.RolloutMode {
        return self._rollout_policy.mode;
    }

    /// Best-effort vector sync after a store() call.
    /// Embeds the content and upserts into the vector store.
    /// Errors are caught and logged, never propagated.
    pub fn syncVectorAfterStore(
        self: *MemoryRuntime,
        allocator: std.mem.Allocator,
        key: []const u8,
        content: []const u8,
        session_id: ?[]const u8,
    ) void {
        syncVectorUpsertWithComponents(
            allocator,
            key,
            content,
            session_id,
            self._outbox,
            self._embedding_provider,
            self._vector_store,
            self._circuit_breaker,
            "",
        );
    }

    /// Drain the durable outbox (if configured).
    /// Call periodically (e.g., after each agent turn).
    pub fn drainOutbox(self: *MemoryRuntime, allocator: std.mem.Allocator) u32 {
        const ob = self._outbox orelse return 0;
        const provider = self._embedding_provider orelse return 0;
        const vs = self._vector_store orelse return 0;
        return ob.drain(allocator, provider, vs, self._circuit_breaker) catch 0;
    }

    /// Best-effort delete from vector store after a forget() call.
    /// Errors are caught and logged, never propagated.
    pub fn deleteFromVectorStore(self: *MemoryRuntime, key: []const u8, session_id: ?[]const u8) void {
        const encoded_key = vector_key.encode(self._allocator, key, session_id) catch return;
        defer self._allocator.free(encoded_key);

        if (self._outbox) |ob| {
            ob.enqueue(encoded_key, "delete") catch |err| {
                log.warn("outbox enqueue failed for key '{s}': {}", .{ encoded_key, err });
            };
            return;
        }

        const vs = self._vector_store orelse return;
        vs.delete(encoded_key) catch |err| {
            log.warn("vector store delete failed for key '{s}': {}", .{ encoded_key, err });
        };
        deleteLegacyVectorKey(vs, encoded_key, "vector store delete") catch {};
    }

    /// Rebuild the entire vector store from primary memory entries.
    /// Used for recovery after vector store corruption, embedding model changes,
    /// or migration to a different vector store backend.
    /// Returns the number of entries reindexed, or 0 if no vector plane is configured.
    pub fn reindex(self: *MemoryRuntime, allocator: std.mem.Allocator) u32 {
        const provider = self._embedding_provider orelse return 0;
        const vs = self._vector_store orelse return 0;

        // List all entries from primary store
        const entries = self.memory.list(allocator, null, null) catch |err| {
            log.warn("reindex: failed to list primary entries: {}", .{err});
            return 0;
        };
        defer freeEntries(allocator, entries);

        var reindexed: u32 = 0;
        for (entries) |entry| {
            const emb = provider.embed(allocator, entry.content) catch |err| {
                log.warn("reindex: embed failed for key '{s}': {}", .{ entry.key, err });
                continue;
            };
            defer allocator.free(emb);
            if (emb.len == 0) continue;

            const encoded_key = vector_key.encode(allocator, entry.key, entry.session_id) catch |err| {
                log.warn("reindex: failed to encode vector key '{s}': {}", .{ entry.key, err });
                continue;
            };
            defer allocator.free(encoded_key);

            vs.upsert(encoded_key, emb) catch |err| {
                log.warn("reindex: upsert failed for key '{s}': {}", .{ encoded_key, err });
                continue;
            };
            deleteLegacyVectorKey(vs, encoded_key, "reindex cleanup") catch {};
            reindexed += 1;
        }

        log.info("reindex complete: {d}/{d} entries reindexed", .{ reindexed, entries.len });
        return reindexed;
    }

    /// Enqueue a key for vector sync via the outbox (if configured).
    pub fn enqueueVectorSync(self: *MemoryRuntime, key: []const u8, session_id: ?[]const u8, operation: []const u8) void {
        const ob = self._outbox orelse return;
        const encoded_key = vector_key.encode(self._allocator, key, session_id) catch return;
        defer self._allocator.free(encoded_key);
        ob.enqueue(encoded_key, operation) catch |err| {
            log.warn("outbox enqueue failed for key '{s}': {}", .{ encoded_key, err });
        };
    }

    /// Get the summarizer configuration (for the agent/session layer to use).
    pub fn summarizerConfig(self: *const MemoryRuntime) summarizer.SummarizerConfig {
        return self._summarizer_cfg;
    }

    /// Get the semantic cache (for the agent/session layer to use).
    pub fn semanticCache(self: *MemoryRuntime) ?*semantic_cache.SemanticCache {
        return self._semantic_cache;
    }

    /// Run memory doctor diagnostics and return a report.
    pub fn diagnose(self: *MemoryRuntime) diagnostics.DiagnosticReport {
        return diagnostics.diagnose(self);
    }

    pub fn feedListEvents(self: *MemoryRuntime, allocator: std.mem.Allocator, after_sequence: ?u64, limit: usize) ![]MemoryEvent {
        return self.memory.listEvents(allocator, after_sequence, limit);
    }

    pub fn feedStatus(self: *MemoryRuntime) !MemoryEventFeedInfo {
        return self.memory.eventFeedInfo();
    }

    pub fn feedApply(self: *MemoryRuntime, event: MemoryEventInput) !MemoryApplyResult {
        if (self.memory.vtable == &ContextBackedMemory.vtable) {
            const wrapped: *ContextBackedMemory = @ptrCast(@alignCast(self.memory.ptr));
            return wrapped.applyDetailed(event);
        }

        const accepted = try self.memory.applyEvent(event);
        return .{
            .accepted = accepted,
            .materialized = accepted,
        };
    }

    pub fn feedCompact(self: *MemoryRuntime) !usize {
        return self.memory.compactEvents();
    }

    pub fn feedRebuild(self: *MemoryRuntime) !void {
        return self.memory.rebuildProjection();
    }

    pub fn deinit(self: *MemoryRuntime) void {
        // Best-effort: drain any pending vector sync operations before teardown.
        // Must happen while embedding provider, vector store, and circuit breaker
        // are still alive (drainOutbox uses all three).
        _ = self.drainOutbox(self._allocator);

        // Engine first: it holds references to P3 components (vector store,
        // embedding provider, circuit breaker) — must deinit before them.
        if (self._engine) |engine| {
            engine.deinit();
            self._allocator.destroy(engine);
        }

        // P3 cleanup (outbox borrows db from vector store or primary — deinit before them)
        if (self._outbox) |ob| {
            ob.deinit(); // handles owns_self destroy
        }
        if (self._circuit_breaker) |cb| {
            self._allocator.destroy(cb);
        }
        if (self._vector_store) |vs| {
            vs.deinitStore(); // vtable deinit handles owns_self destroy
        }
        if (self._sidecar_db_path) |p| self._allocator.free(std.mem.span(p));
        if (self._embedding_provider) |ep| {
            ep.deinit();
        }
        if (self._semantic_cache) |sc| {
            sc.deinit();
            self._allocator.destroy(sc);
        }
        if (self._semantic_cache_db_path) |p| self._allocator.free(std.mem.span(p));
        if (self.response_cache) |rc| {
            rc.deinit();
            self._allocator.destroy(rc);
        }
        if (self._cache_db_path) |p| self._allocator.free(std.mem.span(p));
        self.memory.deinit();
        if (self._db_path) |p| self._allocator.free(std.mem.span(p));
    }
};

const HygienePreserveSyncCtx = struct {
    outbox: ?*outbox.VectorOutbox = null,
    embed_provider: ?embeddings.EmbeddingProvider = null,
    vector_store: ?vector_store.VectorStore = null,
    circuit_breaker: ?*circuit_breaker.CircuitBreaker = null,
};

fn syncVectorUpsertWithComponents(
    allocator: std.mem.Allocator,
    key: []const u8,
    content: []const u8,
    session_id: ?[]const u8,
    outbox_inst: ?*outbox.VectorOutbox,
    embed_provider: ?embeddings.EmbeddingProvider,
    vector_store_inst: ?vector_store.VectorStore,
    circuit_breaker_inst: ?*circuit_breaker.CircuitBreaker,
    log_prefix: []const u8,
) void {
    const encoded_key = vector_key.encode(allocator, key, session_id) catch return;
    defer allocator.free(encoded_key);

    // Durable mode: enqueue and return.
    if (outbox_inst) |ob| {
        ob.enqueue(encoded_key, "upsert") catch |err| {
            log.warn("{s}outbox enqueue failed for key '{s}': {}", .{ log_prefix, encoded_key, err });
        };
        return;
    }

    const provider = embed_provider orelse return;
    const vs = vector_store_inst orelse return;

    if (circuit_breaker_inst) |cb| {
        if (!cb.allow()) return;
    }

    const emb = provider.embed(allocator, content) catch |err| {
        log.warn("{s}vector sync embed failed for key '{s}': {}", .{ log_prefix, encoded_key, err });
        if (circuit_breaker_inst) |cb| cb.recordFailure();
        return;
    };
    defer allocator.free(emb);

    if (circuit_breaker_inst) |cb| cb.recordSuccess();
    if (emb.len == 0) return;

    vs.upsert(encoded_key, emb) catch |err| {
        log.warn("{s}vector sync upsert failed for key '{s}': {}", .{ log_prefix, encoded_key, err });
        return;
    };
    deleteLegacyVectorKey(vs, encoded_key, "vector legacy cleanup") catch {};
}

fn deleteLegacyVectorKey(vs: vector_store.VectorStore, encoded_key: []const u8, log_prefix: []const u8) !void {
    const decoded = vector_key.decode(encoded_key);
    if (decoded.is_legacy) return;
    vs.delete(decoded.logical_key) catch |err| {
        log.warn("{s} failed for legacy key '{s}': {}", .{ log_prefix, decoded.logical_key, err });
    };
}

fn syncPreservedChunkToVector(
    ctx_ptr: *anyopaque,
    allocator: std.mem.Allocator,
    key: []const u8,
    content: []const u8,
) void {
    const ctx: *HygienePreserveSyncCtx = @ptrCast(@alignCast(ctx_ptr));
    syncVectorUpsertWithComponents(
        allocator,
        key,
        content,
        null,
        ctx.outbox,
        ctx.embed_provider,
        ctx.vector_store,
        ctx.circuit_breaker,
        "hygiene ",
    );
}

/// Create a MemoryRuntime from a MemoryConfig and workspace directory.
/// Goes through the registry to find the backend, resolve paths, and
/// create the instance. Returns null on any error (unknown backend,
/// path resolution failure, backend init failure).
pub fn initRuntime(
    allocator: std.mem.Allocator,
    config: *const config_types.MemoryConfig,
    workspace_dir: []const u8,
) ?MemoryRuntime {
    const desc = registry.findBackend(config.backend) orelse {
        const enabled_backends = registry.formatEnabledBackends(allocator) catch null;
        defer if (enabled_backends) |names| allocator.free(names);

        if (registry.isKnownBackend(config.backend)) {
            const engine_token = registry.engineTokenForBackend(config.backend) orelse config.backend;
            log.warn("memory backend '{s}' is configured but disabled in this build", .{config.backend});
            log.warn("rebuild with -Dengines={s} (or include it in your -Dengines=... list)", .{engine_token});
        } else {
            log.warn("unknown memory backend '{s}' — check config.memory.backend", .{config.backend});
            log.warn("known memory backends: {s}", .{registry.known_backends_csv});
        }
        if (enabled_backends) |names| {
            log.warn("enabled memory backends in this build: {s}", .{names});
        }
        return null;
    };

    const pg_cfg: ?config_types.MemoryPostgresConfig = if (std.mem.eql(u8, config.backend, "postgres")) config.postgres else null;
    const redis_cfg: ?config_types.MemoryRedisConfig = if (std.mem.eql(u8, config.backend, "redis")) config.redis else null;
    const api_cfg: ?config_types.MemoryApiConfig = if (std.mem.eql(u8, config.backend, "api")) config.api else null;
    const clickhouse_cfg: ?config_types.MemoryClickHouseConfig = if (std.mem.eql(u8, config.backend, "clickhouse")) config.clickhouse else null;
    var cfg = registry.resolvePaths(allocator, desc, workspace_dir, pg_cfg, redis_cfg, api_cfg, clickhouse_cfg) catch |err| {
        log.warn("memory path resolution failed for backend '{s}': {}", .{ config.backend, err });
        return null;
    };
    cfg.instance_id = config.instance_id;

    var instance = desc.create(allocator, cfg) catch |err| {
        log.warn("memory backend '{s}' init failed: {}", .{ config.backend, err });
        if (std.mem.eql(u8, config.backend, "sqlite") and err == error.MigrationFailed) {
            const db_path = if (cfg.db_path) |p| std.mem.span(p) else "(unknown path)";
            log.warn("sqlite migration failed for {s}", .{db_path});
            log.warn("common causes: database locked/read-only, corrupt sqlite file, or sqlite build without FTS5", .{});
            log.warn("hint: stop other nullclaw processes; if needed, back up/remove the db file and retry", .{});
        }
        if (cfg.postgres_url) |pu| allocator.free(std.mem.span(pu));
        if (cfg.db_path) |p| allocator.free(std.mem.span(p));
        return null;
    };

    // ── Lifecycle: snapshot hydrate (before hygiene) ──
    if (config.lifecycle.auto_hydrate) {
        if (snapshot.shouldHydrate(allocator, instance.memory, workspace_dir)) {
            _ = snapshot.hydrateFromSnapshot(allocator, instance.memory, workspace_dir) catch |e| {
                log.warn("snapshot hydration failed: {}", .{e});
            };
        }
    }

    instance.memory = wrapContextBackedMemory(
        allocator,
        instance.memory,
        config.backend,
        desc.capabilities,
        workspace_dir,
        config.instance_id,
    ) catch |err| {
        log.warn("memory context core init failed for backend '{s}': {}", .{ config.backend, err });
        instance.memory.deinit();
        if (cfg.postgres_url) |pu| allocator.free(std.mem.span(pu));
        if (cfg.db_path) |p| allocator.free(std.mem.span(p));
        return null;
    };

    // ── Lifecycle: response cache ──
    var resp_cache: ?*cache.ResponseCache = null;
    var cache_db_path: ?[*:0]const u8 = null;
    if (build_options.enable_sqlite and config.response_cache.enabled) blk: {
        const cp_slice = std.fs.path.joinZ(allocator, &.{ workspace_dir, "response_cache.db" }) catch break :blk;
        const cp: [*:0]const u8 = cp_slice.ptr;
        const rc = allocator.create(cache.ResponseCache) catch {
            allocator.free(std.mem.span(cp));
            break :blk;
        };
        rc.* = cache.ResponseCache.init(cp, config.response_cache.ttl_minutes, config.response_cache.max_entries) catch {
            allocator.destroy(rc);
            allocator.free(std.mem.span(cp));
            break :blk;
        };
        resp_cache = rc;
        cache_db_path = cp;
    }

    // ── Retrieval engine ──
    var engine: ?*retrieval.RetrievalEngine = null;
    if (config.search.enabled) build_engine: {
        const eng = allocator.create(retrieval.RetrievalEngine) catch break :build_engine;
        eng.* = retrieval.RetrievalEngine.init(allocator, config.search.query);

        // Add primary adapter unless QMD-only mode is explicitly requested.
        const include_primary = !config.qmd.enabled or config.qmd.include_default_memory;
        if (include_primary) {
            const primary = allocator.create(retrieval.PrimaryAdapter) catch {
                allocator.destroy(eng);
                break :build_engine;
            };
            primary.* = retrieval.PrimaryAdapter.init(instance.memory);
            primary.owns_self = true;
            primary.allocator = allocator;
            eng.addSource(primary.adapter()) catch {
                allocator.destroy(primary);
                eng.deinit();
                allocator.destroy(eng);
                break :build_engine;
            };
        }

        // QMD adapter (optional — alloc failure just skips it, engine remains usable)
        if (config.qmd.enabled) {
            if (allocator.create(retrieval_qmd.QmdAdapter)) |qmd| {
                qmd.* = retrieval_qmd.QmdAdapter.init(allocator, config.qmd, workspace_dir);
                qmd.owns_self = true;
                eng.addSource(qmd.adapter()) catch {
                    allocator.destroy(qmd);
                };
            } else |_| {}
        }

        // Configure extended pipeline stages (query expansion, adaptive, LLM reranker)
        eng.setRetrievalStages(config.retrieval_stages);

        engine = eng;
    }

    // ── P3: Vector plane wiring ──
    var embed_provider: ?embeddings.EmbeddingProvider = null;
    var vs_iface: ?vector_store.VectorStore = null;
    var cb_inst: ?*circuit_breaker.CircuitBreaker = null;
    var outbox_inst: ?*outbox.VectorOutbox = null;
    var sidecar_db_path: ?[*:0]const u8 = null;
    var resolved_vector_mode: []const u8 = "none";
    var resolved_vector_sync_mode: []const u8 = "best_effort";
    if (config.search.enabled and !std.mem.eql(u8, config.search.provider, "none") and config.search.query.hybrid.enabled) vec_plane: {
        const primary_api_key = provider_api_key.resolveApiKey(allocator, config.search.provider, null) catch null;
        defer if (primary_api_key) |k| allocator.free(k);

        // 1. Create EmbeddingProvider (with optional fallback via ProviderRouter)
        const primary_ep = embeddings.createEmbeddingProvider(
            allocator,
            config.search.provider,
            primary_api_key,
            config.search.model,
            config.search.dimensions,
        ) catch break :vec_plane;

        embed_provider = primary_ep;

        // Wrap primary + fallback in a ProviderRouter when fallback is configured
        if (!std.mem.eql(u8, config.search.fallback_provider, "none") and
            config.search.fallback_provider.len > 0)
        wrap_router: {
            const fallback_api_key = provider_api_key.resolveApiKey(allocator, config.search.fallback_provider, null) catch null;
            defer if (fallback_api_key) |k| allocator.free(k);

            const fallback_ep = embeddings.createEmbeddingProvider(
                allocator,
                config.search.fallback_provider,
                fallback_api_key,
                config.search.model,
                config.search.dimensions,
            ) catch {
                log.warn("fallback embedding provider '{s}' init failed, using primary only", .{config.search.fallback_provider});
                break :wrap_router;
            };
            const router = provider_router.ProviderRouter.init(
                allocator,
                primary_ep,
                &.{fallback_ep},
                &.{},
            ) catch {
                fallback_ep.deinit();
                break :wrap_router;
            };
            embed_provider = router.provider();
        }

        // 2. Resolve vector store mode based on config.search.store.kind
        //    "auto"           → sqlite_shared if primary is sqlite-based, else sqlite_sidecar
        //    "qdrant"         → QdrantVectorStore via REST API
        //    "pgvector"       → PgvectorVectorStore via libpq (requires enable_postgres)
        //    "sqlite_shared"  → explicit sqlite shared (requires sqlite-based primary)
        //    "sqlite_sidecar" → explicit sqlite sidecar (separate vectors.db)
        //    "sqlite_ann"     → sqlite shared + ANN prefilter (experimental)
        var db_handle_for_outbox: ?*c.sqlite3 = null;
        const store_kind = config.search.store.kind;

        if (std.mem.eql(u8, store_kind, "qdrant")) {
            // Qdrant via REST API
            if (config.search.store.qdrant_url.len == 0) {
                log.warn("vector store kind 'qdrant' requires search.store.qdrant_url to be set", .{});
                break :vec_plane;
            }
            const qdrant = store_qdrant.QdrantVectorStore.init(allocator, .{
                .url = config.search.store.qdrant_url,
                .api_key = if (config.search.store.qdrant_api_key.len > 0) config.search.store.qdrant_api_key else null,
                .collection_name = config.search.store.qdrant_collection,
                .dimensions = config.search.dimensions,
            }) catch |err| {
                log.warn("qdrant vector store init failed: {}", .{err});
                break :vec_plane;
            };
            vs_iface = qdrant.store();
            resolved_vector_mode = "qdrant";
        } else if (std.mem.eql(u8, store_kind, "pgvector")) {
            // pgvector via PostgreSQL
            if (build_options.enable_postgres) {
                const pg_url = if (config.postgres.url.len > 0)
                    config.postgres.url
                else {
                    log.warn("vector store kind 'pgvector' requires postgres.url to be set", .{});
                    break :vec_plane;
                };
                const pgvs = store_pgvector.PgvectorVectorStore.init(allocator, .{
                    .connection_url = pg_url,
                    .table_name = config.search.store.pgvector_table,
                    .dimensions = config.search.dimensions,
                }) catch |err| {
                    log.warn("pgvector vector store init failed: {}", .{err});
                    break :vec_plane;
                };
                vs_iface = pgvs.store();
                resolved_vector_mode = "pgvector";
            } else {
                log.warn("vector store kind 'pgvector' requires build with enable_postgres=true", .{});
                break :vec_plane;
            }
        } else if (!build_options.enable_sqlite) {
            log.warn("vector store kind '{s}' requires build with enable_sqlite=true", .{store_kind});
            break :vec_plane;
        } else {
            // auto / sqlite_shared / sqlite_sidecar
            if (std.mem.eql(u8, store_kind, "sqlite_ann")) {
                if (extractSqliteDb(instance.memory)) |db_handle| {
                    const vs = allocator.create(vector_store.SqliteAnnVectorStore) catch break :vec_plane;
                    vs.* = vector_store.SqliteAnnVectorStore.init(
                        allocator,
                        db_handle,
                        config.search.store.ann_candidate_multiplier,
                        config.search.store.ann_min_candidates,
                    ) catch |err| {
                        allocator.destroy(vs);
                        log.warn("sqlite_ann vector store init failed: {}", .{err});
                        break :vec_plane;
                    };
                    vs.owns_self = true;
                    vs_iface = vs.store();
                    db_handle_for_outbox = db_handle;
                    resolved_vector_mode = "sqlite_ann";
                } else {
                    log.warn("vector store kind 'sqlite_ann' requires a sqlite-based primary backend", .{});
                    break :vec_plane;
                }
            } else {
                const use_shared = std.mem.eql(u8, store_kind, "auto") or std.mem.eql(u8, store_kind, "sqlite_shared");
                if (use_shared) {
                    if (extractSqliteDb(instance.memory)) |db_handle| {
                        // sqlite_shared: reuse existing sqlite db handle
                        const vs = allocator.create(vector_store.SqliteSharedVectorStore) catch break :vec_plane;
                        vs.* = vector_store.SqliteSharedVectorStore.init(allocator, db_handle);
                        vs.owns_self = true;
                        vs_iface = vs.store();
                        db_handle_for_outbox = db_handle;
                        resolved_vector_mode = "sqlite_shared";
                    } else if (std.mem.eql(u8, store_kind, "sqlite_shared")) {
                        log.warn("vector store kind 'sqlite_shared' requires a sqlite-based primary backend", .{});
                        break :vec_plane;
                    }
                    // else: auto fallthrough to sidecar below
                }

                // sqlite_sidecar: explicit or auto fallback for non-sqlite backends
                if (vs_iface == null) {
                    const sidecar_path_slice = blk: {
                        const configured = config.search.store.sidecar_path;
                        if (configured.len == 0) {
                            break :blk std.fs.path.joinZ(allocator, &.{ workspace_dir, "vectors.db" }) catch break :vec_plane;
                        }
                        if (std.fs.path.isAbsolute(configured)) {
                            break :blk allocator.dupeZ(u8, configured) catch break :vec_plane;
                        }
                        break :blk std.fs.path.joinZ(allocator, &.{ workspace_dir, configured }) catch break :vec_plane;
                    };
                    const sidecar_path: [*:0]const u8 = sidecar_path_slice.ptr;
                    const vs = allocator.create(vector_store.SqliteSidecarVectorStore) catch {
                        allocator.free(sidecar_path_slice);
                        break :vec_plane;
                    };
                    vs.* = vector_store.SqliteSidecarVectorStore.init(allocator, sidecar_path) catch {
                        allocator.destroy(vs);
                        allocator.free(sidecar_path_slice);
                        break :vec_plane;
                    };
                    vs.owns_self = true;
                    vs_iface = vs.store();
                    db_handle_for_outbox = vs.db; // sidecar's own db for outbox
                    sidecar_db_path = sidecar_path;
                    resolved_vector_mode = "sqlite_sidecar";
                }
            }
        }

        // 3. Create CircuitBreaker
        const cb = allocator.create(circuit_breaker.CircuitBreaker) catch break :vec_plane;
        cb.* = circuit_breaker.CircuitBreaker.init(
            config.reliability.circuit_breaker_failures,
            config.reliability.circuit_breaker_cooldown_ms,
        );
        cb_inst = cb;

        // 4. Create VectorOutbox if not best_effort
        if (!std.mem.eql(u8, config.search.sync.mode, "best_effort")) {
            if (db_handle_for_outbox) |db_h| {
                const ob = allocator.create(outbox.VectorOutbox) catch break :vec_plane;
                const outbox_retries = @max(config.search.sync.embed_max_retries, config.search.sync.vector_max_retries);
                ob.* = outbox.VectorOutbox.init(allocator, db_h, outbox_retries);
                ob.owns_self = true;
                ob.migrate() catch {
                    allocator.destroy(ob);
                    break :vec_plane;
                };
                outbox_inst = ob;
                resolved_vector_sync_mode = "durable_outbox";
            }
        }

        // 5. Wire into retrieval engine
        if (engine) |eng| {
            eng.setVectorSearch(embed_provider.?, vs_iface.?, cb, config.search.query.hybrid);
        }
    }

    // ── Lifecycle: hygiene ──
    if (config.lifecycle.hygiene_enabled) {
        var preserve_sync_ctx = HygienePreserveSyncCtx{
            .outbox = outbox_inst,
            .embed_provider = embed_provider,
            .vector_store = vs_iface,
            .circuit_breaker = cb_inst,
        };
        const preserve_sync_hook: ?hygiene.PreserveSyncHook = if (config.lifecycle.preserve_before_purge and
            (outbox_inst != null or (embed_provider != null and vs_iface != null)))
            .{
                .ptr = @ptrCast(&preserve_sync_ctx),
                .callback = syncPreservedChunkToVector,
            }
        else
            null;
        const hygiene_cfg = hygiene.HygieneConfig{
            .hygiene_enabled = true,
            .archive_after_days = config.lifecycle.archive_after_days,
            .purge_after_days = config.lifecycle.purge_after_days,
            .preserve_before_purge = config.lifecycle.preserve_before_purge,
            .conversation_retention_days = config.lifecycle.conversation_retention_days,
            .workspace_dir = workspace_dir,
        };
        const report = hygiene.runIfDue(allocator, hygiene_cfg, instance.memory, preserve_sync_hook);

        // Snapshot after hygiene if configured and hygiene did work
        if (config.lifecycle.snapshot_on_hygiene and report.totalActions() > 0) {
            _ = snapshot.exportSnapshot(allocator, instance.memory, workspace_dir) catch |e| {
                log.warn("snapshot export after hygiene failed: {}", .{e});
            };
        }
    }

    // Enforce fallback_policy: if fail_fast and vector plane was expected but failed, abort.
    if (std.mem.eql(u8, config.reliability.fallback_policy, "fail_fast")) {
        const vector_expected = config.search.enabled and
            !std.mem.eql(u8, config.search.provider, "none") and
            config.search.query.hybrid.enabled;
        const durable_requested = !std.mem.eql(u8, config.search.sync.mode, "best_effort");
        const vector_plane_failed = vector_expected and vs_iface == null;
        const durable_outbox_unavailable = vector_expected and durable_requested and outbox_inst == null;
        if (vector_plane_failed or durable_outbox_unavailable) {
            if (vector_plane_failed) {
                log.warn("fallback_policy=fail_fast: vector plane init failed, aborting runtime creation", .{});
            } else {
                log.warn("fallback_policy=fail_fast: durable vector sync unavailable, aborting runtime creation", .{});
            }
            // Clean up partially-created P3 resources
            if (outbox_inst) |ob| ob.deinit();
            if (vs_iface) |vs| vs.deinitStore();
            if (embed_provider) |ep| ep.deinit();
            if (cb_inst) |cb| allocator.destroy(cb);
            if (sidecar_db_path) |p| allocator.free(std.mem.span(p));
            // Clean up response cache
            if (resp_cache) |rc| {
                rc.deinit();
                allocator.destroy(rc);
            }
            if (cache_db_path) |p| allocator.free(std.mem.span(p));
            if (engine) |eng| {
                eng.deinit();
                allocator.destroy(eng);
            }
            instance.memory.deinit();
            if (cfg.postgres_url) |pu| allocator.free(std.mem.span(pu));
            if (cfg.db_path) |p| allocator.free(std.mem.span(p));
            return null;
        }
    }

    // Free postgres_url after backend creation (backend dupes what it needs)
    if (cfg.postgres_url) |pu| allocator.free(std.mem.span(pu));

    // ── Lifecycle: semantic cache ──
    var sem_cache: ?*semantic_cache.SemanticCache = null;
    var sem_cache_db_path: ?[*:0]const u8 = null;
    if (build_options.enable_sqlite and config.response_cache.enabled and embed_provider != null) sem_cache_blk: {
        const sc_path = std.fs.path.joinZ(allocator, &.{ workspace_dir, "semantic_cache.db" }) catch break :sem_cache_blk;
        const sc = allocator.create(semantic_cache.SemanticCache) catch {
            allocator.free(std.mem.span(sc_path.ptr));
            break :sem_cache_blk;
        };
        sc.* = semantic_cache.SemanticCache.init(
            sc_path.ptr,
            config.response_cache.ttl_minutes,
            config.response_cache.max_entries,
            0.95, // cosine similarity threshold
            embed_provider,
        ) catch {
            allocator.destroy(sc);
            allocator.free(std.mem.span(sc_path.ptr));
            break :sem_cache_blk;
        };
        sem_cache = sc;
        sem_cache_db_path = sc_path.ptr;
    }

    // ── Lifecycle: summarizer config ──
    const summarizer_cfg = summarizer.SummarizerConfig{
        .enabled = config.summarizer.enabled,
        .window_size_tokens = @intCast(config.summarizer.window_size_tokens),
        .summary_max_tokens = @intCast(config.summarizer.summary_max_tokens),
        .auto_extract_semantic = config.summarizer.auto_extract_semantic,
    };

    // ── Startup diagnostic ──
    const retrieval_mode: []const u8 = if (!config.search.enabled)
        "disabled"
    else if (config.search.query.hybrid.enabled)
        "hybrid"
    else
        "keyword";
    const source_count: usize = if (engine) |eng| eng.sources.items.len else 0;
    const vector_mode: []const u8 = if (vs_iface == null) "none" else resolved_vector_mode;
    const cache_enabled = resp_cache != null;
    log.info("memory plan resolved: backend={s} retrieval={s} vector={s} rollout={s} hygiene={} snapshot={} cache={} semantic_cache={} summarizer={} sources={d}", .{
        config.backend,
        retrieval_mode,
        vector_mode,
        config.reliability.rollout_mode,
        config.lifecycle.hygiene_enabled,
        config.lifecycle.snapshot_enabled,
        cache_enabled,
        sem_cache != null,
        config.summarizer.enabled,
        source_count,
    });

    const embed_name: []const u8 = if (embed_provider) |ep_| ep_.getName() else "none";

    return .{
        .memory = instance.memory,
        .session_store = instance.session_store,
        .response_cache = resp_cache,
        .capabilities = desc.capabilities,
        .resolved = .{
            .primary_backend = config.backend,
            .retrieval_mode = retrieval_mode,
            .vector_mode = vector_mode,
            .embedding_provider = embed_name,
            .rollout_mode = config.reliability.rollout_mode,
            .vector_sync_mode = resolved_vector_sync_mode,
            .hygiene_enabled = config.lifecycle.hygiene_enabled,
            .snapshot_enabled = config.lifecycle.snapshot_enabled,
            .cache_enabled = cache_enabled,
            .semantic_cache_enabled = sem_cache != null,
            .summarizer_enabled = config.summarizer.enabled,
            .source_count = source_count,
            .fallback_policy = config.reliability.fallback_policy,
        },
        ._db_path = cfg.db_path,
        ._cache_db_path = cache_db_path,
        ._engine = engine,
        ._allocator = allocator,
        ._search_enabled = config.search.enabled,
        ._rollout_policy = rollout.RolloutPolicy.init(config.reliability),
        ._summarizer_cfg = summarizer_cfg,
        ._semantic_cache = sem_cache,
        ._semantic_cache_db_path = sem_cache_db_path,
        ._embedding_provider = embed_provider,
        ._vector_store = vs_iface,
        ._circuit_breaker = cb_inst,
        ._outbox = outbox_inst,
        ._sidecar_db_path = sidecar_db_path,
    };
}

// ── Helpers ────────────────────────────────────────────────────────

const c = sqlite.c;

/// Extract the raw sqlite3* handle from a Memory vtable, if the backend is sqlite-based.
fn extractSqliteDb(mem: Memory) ?*c.sqlite3 {
    if (!build_options.enable_sqlite) return null;

    if (mem.vtable == &ContextBackedMemory.vtable) {
        const wrapped: *ContextBackedMemory = @ptrCast(@alignCast(mem.ptr));
        return extractSqliteDb(wrapped.projection);
    }

    if (mem.vtable == &EventSourcedMemory.vtable) {
        const wrapped: *EventSourcedMemory = @ptrCast(@alignCast(mem.ptr));
        return extractSqliteDb(wrapped.backend);
    }

    const name_str = mem.name();
    if (std.mem.eql(u8, name_str, "sqlite")) {
        const impl_: *SqliteMemory = @ptrCast(@alignCast(mem.ptr));
        return impl_.db;
    }
    if (build_options.enable_memory_lucid and std.mem.eql(u8, name_str, "lucid")) {
        const impl_: *LucidMemory = @ptrCast(@alignCast(mem.ptr));
        return impl_.local.db;
    }
    return null;
}

// ── Tests ──────────────────────────────────────────────────────────

const test_resolved_cfg: ResolvedConfig = .{
    .primary_backend = "test",
    .retrieval_mode = "keyword",
    .vector_mode = "none",
    .embedding_provider = "none",
    .rollout_mode = "off",
    .vector_sync_mode = "best_effort",
    .hygiene_enabled = false,
    .snapshot_enabled = false,
    .cache_enabled = false,
    .semantic_cache_enabled = false,
    .summarizer_enabled = false,
    .source_count = 0,
    .fallback_policy = "degrade",
};

test "MemoryCategory toString roundtrip" {
    const core: MemoryCategory = .core;
    try std.testing.expectEqualStrings("core", core.toString());

    const daily: MemoryCategory = .daily;
    try std.testing.expectEqualStrings("daily", daily.toString());

    const conversation: MemoryCategory = .conversation;
    try std.testing.expectEqualStrings("conversation", conversation.toString());

    const custom: MemoryCategory = .{ .custom = "project" };
    try std.testing.expectEqualStrings("project", custom.toString());
}

test "MemoryCategory fromString" {
    const core = MemoryCategory.fromString("core");
    try std.testing.expect(core.eql(.core));

    const daily = MemoryCategory.fromString("daily");
    try std.testing.expect(daily.eql(.daily));

    const conversation = MemoryCategory.fromString("conversation");
    try std.testing.expect(conversation.eql(.conversation));

    const custom = MemoryCategory.fromString("project");
    try std.testing.expectEqualStrings("project", custom.custom);
}

test "MemoryCategory equality" {
    const core: MemoryCategory = .core;
    try std.testing.expect(core.eql(.core));
    try std.testing.expect(!core.eql(.daily));
    const c1: MemoryCategory = .{ .custom = "a" };
    const c2: MemoryCategory = .{ .custom = "a" };
    const c3: MemoryCategory = .{ .custom = "b" };
    try std.testing.expect(c1.eql(c2));
    try std.testing.expect(!c1.eql(c3));
}

test "MemoryCategory custom toString" {
    const cat: MemoryCategory = .{ .custom = "my_project" };
    try std.testing.expectEqualStrings("my_project", cat.toString());
}

test "MemoryCategory fromString custom" {
    const cat = MemoryCategory.fromString("unknown_category");
    try std.testing.expectEqualStrings("unknown_category", cat.custom);
}

test "MemoryCategory eql different tags" {
    const core: MemoryCategory = .core;
    const daily: MemoryCategory = .daily;
    const conv: MemoryCategory = .conversation;
    try std.testing.expect(!core.eql(daily));
    try std.testing.expect(!core.eql(conv));
    try std.testing.expect(!daily.eql(conv));
}

test "Memory convenience store accepts session_id" {
    var backend = none.NoneMemory.init();
    defer backend.deinit();
    const m = backend.memory();
    try m.store("key", "value", .core, null);
    try m.store("key2", "value2", .daily, "session-abc");
}

test "Memory convenience recall accepts session_id" {
    var backend = none.NoneMemory.init();
    defer backend.deinit();
    const m = backend.memory();
    const results = try m.recall(std.testing.allocator, "query", 5, null);
    defer std.testing.allocator.free(results);
    try std.testing.expectEqual(@as(usize, 0), results.len);

    const results2 = try m.recall(std.testing.allocator, "query", 5, "session-abc");
    defer std.testing.allocator.free(results2);
    try std.testing.expectEqual(@as(usize, 0), results2.len);
}

test "Memory convenience list accepts session_id" {
    var backend = none.NoneMemory.init();
    defer backend.deinit();
    const m = backend.memory();
    const results = try m.list(std.testing.allocator, null, null);
    defer std.testing.allocator.free(results);
    try std.testing.expectEqual(@as(usize, 0), results.len);

    const results2 = try m.list(std.testing.allocator, .core, "session-abc");
    defer std.testing.allocator.free(results2);
    try std.testing.expectEqual(@as(usize, 0), results2.len);
}

test "Memory getScoped fallback uses scoped list when backend lacks native getter" {
    const TestMemory = struct {
        fn makeEntry(allocator: std.mem.Allocator, key: []const u8, content: []const u8, session_id: ?[]const u8) !MemoryEntry {
            return .{
                .id = try allocator.dupe(u8, key),
                .key = try allocator.dupe(u8, key),
                .content = try allocator.dupe(u8, content),
                .category = .core,
                .timestamp = try allocator.dupe(u8, "now"),
                .session_id = if (session_id) |sid| try allocator.dupe(u8, sid) else null,
                .score = null,
            };
        }

        fn implName(_: *anyopaque) []const u8 {
            return "fallback";
        }

        fn implStore(_: *anyopaque, _: []const u8, _: []const u8, _: MemoryCategory, _: ?[]const u8) anyerror!void {}

        fn implRecall(_: *anyopaque, allocator: std.mem.Allocator, _: []const u8, _: usize, _: ?[]const u8) anyerror![]MemoryEntry {
            return allocator.alloc(MemoryEntry, 0);
        }

        fn implGet(_: *anyopaque, allocator: std.mem.Allocator, _: []const u8) anyerror!?MemoryEntry {
            return try makeEntry(allocator, "shared", "global", null);
        }

        fn implList(_: *anyopaque, allocator: std.mem.Allocator, _: ?MemoryCategory, session_id: ?[]const u8) anyerror![]MemoryEntry {
            if (session_id == null) return allocator.alloc(MemoryEntry, 0);
            var entries = try allocator.alloc(MemoryEntry, 1);
            entries[0] = try makeEntry(allocator, "shared", "scoped", session_id);
            return entries;
        }

        fn implForget(_: *anyopaque, _: []const u8) anyerror!bool {
            return true;
        }

        fn implCount(_: *anyopaque) anyerror!usize {
            return 0;
        }

        fn implHealthCheck(_: *anyopaque) bool {
            return true;
        }

        fn implDeinit(_: *anyopaque) void {}

        const vtable = Memory.VTable{
            .name = &implName,
            .store = &implStore,
            .recall = &implRecall,
            .get = &implGet,
            .list = &implList,
            .forget = &implForget,
            .count = &implCount,
            .healthCheck = &implHealthCheck,
            .deinit = &implDeinit,
        };
    };

    const mem = Memory{ .ptr = undefined, .vtable = &TestMemory.vtable };
    const entry = (try mem.getScoped(std.testing.allocator, "shared", "sess-a")).?;
    defer entry.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("scoped", entry.content);
    try std.testing.expectEqualStrings("sess-a", entry.session_id.?);
}

test "Memory forgetScoped fallback fails closed without native support" {
    const TestMemory = struct {
        var forget_calls: usize = 0;

        fn implName(_: *anyopaque) []const u8 {
            return "fallback";
        }

        fn implStore(_: *anyopaque, _: []const u8, _: []const u8, _: MemoryCategory, _: ?[]const u8) anyerror!void {}

        fn implRecall(_: *anyopaque, allocator: std.mem.Allocator, _: []const u8, _: usize, _: ?[]const u8) anyerror![]MemoryEntry {
            return allocator.alloc(MemoryEntry, 0);
        }

        fn implGet(_: *anyopaque, _: std.mem.Allocator, _: []const u8) anyerror!?MemoryEntry {
            return null;
        }

        fn implList(_: *anyopaque, allocator: std.mem.Allocator, _: ?MemoryCategory, _: ?[]const u8) anyerror![]MemoryEntry {
            return allocator.alloc(MemoryEntry, 0);
        }

        fn implForget(_: *anyopaque, _: []const u8) anyerror!bool {
            forget_calls += 1;
            return true;
        }

        fn implCount(_: *anyopaque) anyerror!usize {
            return 0;
        }

        fn implHealthCheck(_: *anyopaque) bool {
            return true;
        }

        fn implDeinit(_: *anyopaque) void {}

        const vtable = Memory.VTable{
            .name = &implName,
            .store = &implStore,
            .recall = &implRecall,
            .get = &implGet,
            .list = &implList,
            .forget = &implForget,
            .count = &implCount,
            .healthCheck = &implHealthCheck,
            .deinit = &implDeinit,
        };
    };

    const mem = Memory{ .ptr = undefined, .vtable = &TestMemory.vtable };
    TestMemory.forget_calls = 0;
    try std.testing.expectError(error.NotSupported, mem.forgetScoped(std.testing.allocator, "shared", "sess-a"));
    try std.testing.expectEqual(@as(usize, 0), TestMemory.forget_calls);
}

test "Memory event hooks fail closed without native support" {
    var backend = none.NoneMemory.init();
    defer backend.deinit();
    const mem = backend.memory();

    try std.testing.expectError(error.NotSupported, mem.listEvents(std.testing.allocator, null, 16));
    try std.testing.expectError(error.NotSupported, mem.applyEvent(.{
        .operation = .put,
        .key = "k",
        .content = "v",
        .category = .core,
    }));
    try std.testing.expectError(error.NotSupported, mem.lastEventSequence());
    try std.testing.expectError(error.NotSupported, mem.eventFeedInfo());
    try std.testing.expectError(error.NotSupported, mem.compactEvents());
    try std.testing.expectError(error.NotSupported, mem.rebuildProjection());
}

test "event sourced memory bootstraps backend state into journal" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const workspace = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(workspace);

    var base = memory_lru.InMemoryLruMemory.init(std.testing.allocator, 32);
    try base.memory().store("lang", "zig", .core, null);

    const mem = try wrapEventSourcedMemory(std.testing.allocator, base.memory(), workspace, "agent-a");
    defer mem.deinit();

    try std.testing.expectEqualStrings("memory_lru", mem.name());
    try std.testing.expectEqual(@as(usize, 1), try mem.count());
    try std.testing.expectEqual(@as(u64, 1), try mem.lastEventSequence());

    const bootstrap_events = try mem.listEvents(std.testing.allocator, null, 8);
    defer freeEvents(std.testing.allocator, bootstrap_events);
    try std.testing.expectEqual(@as(usize, 1), bootstrap_events.len);
    try std.testing.expectEqual(MemoryEventOp.put, bootstrap_events[0].operation);
    try std.testing.expectEqualStrings("lang", bootstrap_events[0].key);
    try std.testing.expectEqualStrings("zig", bootstrap_events[0].content.?);
    try std.testing.expectEqualStrings("agent-a", bootstrap_events[0].origin_instance_id);

    try mem.store("timezone", "utc", .daily, "sess-1");
    try std.testing.expectEqual(@as(u64, 2), try mem.lastEventSequence());

    const scoped = (try mem.getScoped(std.testing.allocator, "timezone", "sess-1")).?;
    defer scoped.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("utc", scoped.content);

    const tail = try mem.listEvents(std.testing.allocator, 1, 8);
    defer freeEvents(std.testing.allocator, tail);
    try std.testing.expectEqual(@as(usize, 1), tail.len);
    try std.testing.expectEqual(MemoryEventOp.put, tail[0].operation);
    try std.testing.expectEqualStrings("timezone", tail[0].key);
    try std.testing.expectEqualStrings("sess-1", tail[0].session_id.?);
}

test "event sourced memory keeps exact delete semantics over append-only backend" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const workspace = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(workspace);

    var base = try markdown.MarkdownMemory.init(std.testing.allocator, workspace);
    const mem = try wrapEventSourcedMemory(std.testing.allocator, base.memory(), workspace, "agent-a");
    defer mem.deinit();

    try mem.store("shared", "global", .core, null);
    try mem.store("shared", "scoped", .core, "sess-a");
    try std.testing.expectEqual(@as(usize, 2), try mem.count());

    try std.testing.expect(try mem.forgetScoped(std.testing.allocator, "shared", "sess-a"));
    try std.testing.expect((try mem.getScoped(std.testing.allocator, "shared", "sess-a")) == null);

    const global = (try mem.getScoped(std.testing.allocator, "shared", null)).?;
    defer global.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("global", global.content);

    try std.testing.expect(try mem.forget("shared"));
    try std.testing.expectEqual(@as(usize, 0), try mem.count());

    const events = try mem.listEvents(std.testing.allocator, null, 16);
    defer freeEvents(std.testing.allocator, events);
    try std.testing.expectEqual(@as(usize, 4), events.len);
    try std.testing.expectEqual(MemoryEventOp.put, events[0].operation);
    try std.testing.expectEqual(MemoryEventOp.put, events[1].operation);
    try std.testing.expectEqual(MemoryEventOp.delete_scoped, events[2].operation);
    try std.testing.expectEqual(MemoryEventOp.delete_all, events[3].operation);
}

test "event sourced memory deduplicates replayed remote events" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const workspace = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(workspace);

    var base = memory_lru.InMemoryLruMemory.init(std.testing.allocator, 32);
    const mem = try wrapEventSourcedMemory(std.testing.allocator, base.memory(), workspace, "agent-b");
    defer mem.deinit();

    try std.testing.expect(try mem.applyEvent(.{
        .operation = .put,
        .key = "pref",
        .content = "vim",
        .category = .core,
        .origin_instance_id = "agent-a",
        .origin_sequence = 7,
        .timestamp_ms = 1234,
    }));
    try std.testing.expect(!(try mem.applyEvent(.{
        .operation = .put,
        .key = "pref",
        .content = "vim",
        .category = .core,
        .origin_instance_id = "agent-a",
        .origin_sequence = 7,
        .timestamp_ms = 1234,
    })));

    try std.testing.expectEqual(@as(u64, 1), try mem.lastEventSequence());
    const entry = (try mem.get(std.testing.allocator, "pref")).?;
    defer entry.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("vim", entry.content);
}

test "event sourced memory default origin identity is stable" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const workspace = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(workspace);

    var base = memory_lru.InMemoryLruMemory.init(std.testing.allocator, 32);
    const mem = try wrapEventSourcedMemory(std.testing.allocator, base.memory(), workspace, "");
    defer mem.deinit();

    try mem.store("lang", "zig", .core, null);

    const events = try mem.listEvents(std.testing.allocator, null, 8);
    defer freeEvents(std.testing.allocator, events);
    try std.testing.expectEqual(@as(usize, 1), events.len);
    try std.testing.expectEqualStrings("default", events[0].origin_instance_id);
}

test "event sourced memory delegates recall to exact backend" {
    const MockExactMemory = struct {
        recall_calls: usize = 0,
        store_calls: usize = 0,

        fn makeEntry(allocator: std.mem.Allocator, key: []const u8, content: []const u8) !MemoryEntry {
            return .{
                .id = try allocator.dupe(u8, "1"),
                .key = try allocator.dupe(u8, key),
                .content = try allocator.dupe(u8, content),
                .category = .core,
                .timestamp = try allocator.dupe(u8, "now"),
                .session_id = null,
                .score = 1.0,
            };
        }

        fn implName(_: *anyopaque) []const u8 {
            return "sqlite";
        }

        fn implStore(ptr: *anyopaque, _: []const u8, _: []const u8, _: MemoryCategory, _: ?[]const u8) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.store_calls += 1;
        }

        fn implRecall(ptr: *anyopaque, allocator: std.mem.Allocator, query: []const u8, _: usize, _: ?[]const u8) anyerror![]MemoryEntry {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.recall_calls += 1;

            if (!std.mem.eql(u8, query, "needle")) return allocator.alloc(MemoryEntry, 0);
            var entries = try allocator.alloc(MemoryEntry, 1);
            entries[0] = try makeEntry(allocator, "k", "needle result");
            return entries;
        }

        fn implGet(_: *anyopaque, _: std.mem.Allocator, _: []const u8) anyerror!?MemoryEntry {
            return null;
        }

        fn implList(_: *anyopaque, allocator: std.mem.Allocator, _: ?MemoryCategory, _: ?[]const u8) anyerror![]MemoryEntry {
            return allocator.alloc(MemoryEntry, 0);
        }

        fn implForget(_: *anyopaque, _: []const u8) anyerror!bool {
            return false;
        }

        fn implCount(_: *anyopaque) anyerror!usize {
            return 0;
        }

        fn implHealthCheck(_: *anyopaque) bool {
            return true;
        }

        fn implDeinit(_: *anyopaque) void {}

        const vtable = Memory.VTable{
            .name = &implName,
            .store = &implStore,
            .recall = &implRecall,
            .get = &implGet,
            .list = &implList,
            .forget = &implForget,
            .count = &implCount,
            .healthCheck = &implHealthCheck,
            .deinit = &implDeinit,
        };
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const workspace = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(workspace);

    var mock = MockExactMemory{};
    const backend = Memory{ .ptr = @ptrCast(&mock), .vtable = &MockExactMemory.vtable };
    const mem = try wrapEventSourcedMemory(std.testing.allocator, backend, workspace, "agent-a");
    defer mem.deinit();

    try mem.store("k", "needle", .core, null);
    const recalled = try mem.recall(std.testing.allocator, "needle", 5, null);
    defer freeEntries(std.testing.allocator, recalled);

    try std.testing.expectEqual(@as(usize, 1), mock.recall_calls);
    try std.testing.expectEqual(@as(usize, 1), recalled.len);
    try std.testing.expectEqualStrings("needle result", recalled[0].content);
}

test "event sourced memory loads legacy schema-less journal entries" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const workspace = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(workspace);

    const journal_path = try EventSourcedMemory.buildJournalPath(std.testing.allocator, workspace, "agent-legacy");
    defer std.testing.allocator.free(journal_path);

    const legacy_line =
        "{\"sequence\":1,\"timestamp_ms\":1234,\"origin_instance_id\":\"agent-legacy\",\"origin_sequence\":1,\"operation\":\"put\",\"key\":\"legacy\",\"content\":\"value\",\"category\":\"core\",\"session_id\":null}\n";

    var file = try std.fs.createFileAbsolute(journal_path, .{ .read = true, .truncate = true });
    defer file.close();
    try file.writeAll(legacy_line);

    var base = memory_lru.InMemoryLruMemory.init(std.testing.allocator, 32);
    const mem = try wrapEventSourcedMemory(std.testing.allocator, base.memory(), workspace, "agent-legacy");
    defer mem.deinit();

    const entry = (try mem.get(std.testing.allocator, "legacy")).?;
    defer entry.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("value", entry.content);
    try std.testing.expectEqual(@as(u64, 1), try mem.lastEventSequence());
}

test "event sourced memory compacts to checkpoint and expires stale cursors" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const workspace = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(workspace);

    {
        var base = memory_lru.InMemoryLruMemory.init(std.testing.allocator, 32);
        const mem = try wrapEventSourcedMemory(std.testing.allocator, base.memory(), workspace, "agent-compact");
        defer mem.deinit();

        try mem.store("lang", "zig", .core, null);
        try mem.store("editor", "nvim", .daily, "sess-a");

        const compacted = try mem.compactEvents();
        try std.testing.expectEqual(@as(usize, 2), compacted);

        const info = try mem.eventFeedInfo();
        try std.testing.expectEqual(@as(u64, 2), info.compacted_through_sequence);
        try std.testing.expectEqual(@as(u64, 2), info.latest_sequence);
        try std.testing.expectError(error.CursorExpired, mem.listEvents(std.testing.allocator, 0, 16));

        const tail = try mem.listEvents(std.testing.allocator, info.compacted_through_sequence, 16);
        defer freeEvents(std.testing.allocator, tail);
        try std.testing.expectEqual(@as(usize, 0), tail.len);
    }

    var reopened_base = memory_lru.InMemoryLruMemory.init(std.testing.allocator, 32);
    const reopened = try wrapEventSourcedMemory(std.testing.allocator, reopened_base.memory(), workspace, "agent-compact");
    defer reopened.deinit();

    try std.testing.expectEqual(@as(u64, 2), try reopened.lastEventSequence());
    const reopened_entry = (try reopened.get(std.testing.allocator, "lang")).?;
    defer reopened_entry.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("zig", reopened_entry.content);

    const recalled = try reopened.recall(std.testing.allocator, "nvim", 4, "sess-a");
    defer freeEntries(std.testing.allocator, recalled);
    try std.testing.expectEqual(@as(usize, 1), recalled.len);
}

test "event sourced memory rebuilds backend projection from checkpoint state" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const workspace = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(workspace);

    var base = memory_lru.InMemoryLruMemory.init(std.testing.allocator, 32);
    const raw_backend = base.memory();
    const mem = try wrapEventSourcedMemory(std.testing.allocator, raw_backend, workspace, "agent-rebuild");
    defer mem.deinit();

    try mem.store("shared", "current", .core, null);
    try raw_backend.store("stale", "ghost", .core, null);
    try raw_backend.store("shared", "drifted", .core, null);

    try mem.rebuildProjection();

    try std.testing.expect((try raw_backend.get(std.testing.allocator, "stale")) == null);
    const entry = (try raw_backend.get(std.testing.allocator, "shared")).?;
    defer entry.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("current", entry.content);
}

test "context backed memory bootstraps projection state into canonical core and reports feed status" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const workspace = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(workspace);

    var projection = memory_lru.InMemoryLruMemory.init(std.testing.allocator, 32);
    try projection.memory().store("lang", "zig", .core, null);

    const mem = try wrapContextBackedMemory(
        std.testing.allocator,
        projection.memory(),
        "memory",
        .{
            .supports_keyword_rank = false,
            .supports_session_store = false,
            .supports_transactions = false,
            .supports_outbox = false,
            .supports_native_recall = true,
            .supports_scoped_native_recall = true,
            .supports_safe_rebuild = true,
            .has_remote_side_effects = false,
        },
        workspace,
        "agent-a",
    );
    defer mem.deinit();

    try std.testing.expectEqualStrings("memory", mem.name());
    try std.testing.expectEqual(@as(usize, 1), try mem.count());

    const info = try mem.eventFeedInfo();
    try std.testing.expectEqualStrings("sqlite", info.core_backend);
    try std.testing.expectEqualStrings("memory", info.projection_backend);
    try std.testing.expectEqual(@as(u64, 1), info.latest_sequence);
    try std.testing.expectEqual(@as(u64, 1), info.projection_last_applied_sequence);
    try std.testing.expectEqual(@as(u64, 0), info.projection_lag);
    try std.testing.expectEqualStrings("projection", info.recall_source);

    const events = try mem.listEvents(std.testing.allocator, null, 8);
    defer freeEvents(std.testing.allocator, events);
    try std.testing.expectEqual(@as(usize, 1), events.len);
    try std.testing.expectEqualStrings("lang", events[0].key);
    try std.testing.expectEqualStrings("zig", events[0].content.?);

    const entry = (try mem.get(std.testing.allocator, "lang")).?;
    defer entry.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("zig", entry.content);
}

test "context backed memory falls back to core for scoped recall and degrades unsafe rebuilds" {
    const ProjectionRecallMock = struct {
        recall_calls: usize = 0,
        scoped_recall_calls: usize = 0,
        store_calls: usize = 0,

        fn makeEntry(allocator: std.mem.Allocator, key: []const u8, content: []const u8, session_id: ?[]const u8) !MemoryEntry {
            return .{
                .id = try allocator.dupe(u8, "projection-1"),
                .key = try allocator.dupe(u8, key),
                .content = try allocator.dupe(u8, content),
                .category = .core,
                .timestamp = try allocator.dupe(u8, "now"),
                .session_id = if (session_id) |sid| try allocator.dupe(u8, sid) else null,
                .score = 1.0,
            };
        }

        fn implName(_: *anyopaque) []const u8 {
            return "api";
        }

        fn implStore(ptr: *anyopaque, _: []const u8, _: []const u8, _: MemoryCategory, _: ?[]const u8) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.store_calls += 1;
        }

        fn implRecall(ptr: *anyopaque, allocator: std.mem.Allocator, query: []const u8, _: usize, session_id: ?[]const u8) anyerror![]MemoryEntry {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.recall_calls += 1;
            if (session_id != null) self.scoped_recall_calls += 1;

            if (!std.mem.eql(u8, query, "pref")) return allocator.alloc(MemoryEntry, 0);

            var entries = try allocator.alloc(MemoryEntry, 1);
            entries[0] = try makeEntry(allocator, "pref", "projection-result", null);
            return entries;
        }

        fn implGet(_: *anyopaque, _: std.mem.Allocator, _: []const u8) anyerror!?MemoryEntry {
            return null;
        }

        fn implList(_: *anyopaque, allocator: std.mem.Allocator, _: ?MemoryCategory, _: ?[]const u8) anyerror![]MemoryEntry {
            return allocator.alloc(MemoryEntry, 0);
        }

        fn implForget(_: *anyopaque, _: []const u8) anyerror!bool {
            return false;
        }

        fn implCount(_: *anyopaque) anyerror!usize {
            return 0;
        }

        fn implHealthCheck(_: *anyopaque) bool {
            return true;
        }

        fn implDeinit(_: *anyopaque) void {}

        const vtable = Memory.VTable{
            .name = &implName,
            .store = &implStore,
            .recall = &implRecall,
            .get = &implGet,
            .list = &implList,
            .forget = &implForget,
            .count = &implCount,
            .healthCheck = &implHealthCheck,
            .deinit = &implDeinit,
        };
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const workspace = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(workspace);

    var projection = ProjectionRecallMock{};
    const backend = Memory{ .ptr = @ptrCast(&projection), .vtable = &ProjectionRecallMock.vtable };
    const mem = try wrapContextBackedMemory(
        std.testing.allocator,
        backend,
        "api",
        .{
            .supports_keyword_rank = false,
            .supports_session_store = false,
            .supports_transactions = false,
            .supports_outbox = false,
            .supports_native_recall = true,
            .supports_scoped_native_recall = false,
            .supports_safe_rebuild = false,
            .has_remote_side_effects = true,
        },
        workspace,
        "agent-a",
    );
    defer mem.deinit();

    try mem.store("pref", "global from core", .core, null);
    try mem.store("pref", "scoped from core", .core, "sess-a");
    try std.testing.expectEqual(@as(usize, 2), projection.store_calls);

    const unscoped = try mem.recall(std.testing.allocator, "pref", 4, null);
    defer freeEntries(std.testing.allocator, unscoped);
    try std.testing.expectEqual(@as(usize, 1), projection.recall_calls);
    try std.testing.expectEqual(@as(usize, 0), projection.scoped_recall_calls);
    try std.testing.expectEqual(@as(usize, 1), unscoped.len);
    try std.testing.expectEqualStrings("projection-result", unscoped[0].content);

    const scoped = try mem.recall(std.testing.allocator, "pref", 4, "sess-a");
    defer freeEntries(std.testing.allocator, scoped);
    try std.testing.expectEqual(@as(usize, 1), projection.recall_calls);
    try std.testing.expectEqual(@as(usize, 0), projection.scoped_recall_calls);
    try std.testing.expectEqual(@as(usize, 1), scoped.len);
    try std.testing.expectEqualStrings("scoped from core", scoped[0].content);

    try std.testing.expect(try mem.forgetScoped(std.testing.allocator, "pref", "sess-a"));
    try std.testing.expect((try mem.getScoped(std.testing.allocator, "pref", "sess-a")) == null);

    const info = try mem.eventFeedInfo();
    try std.testing.expectEqual(@as(u64, 3), info.latest_sequence);
    try std.testing.expectEqual(@as(u64, 2), info.projection_last_applied_sequence);
    try std.testing.expectEqual(@as(u64, 1), info.projection_lag);
    try std.testing.expectEqualStrings("core", info.recall_source);

    try std.testing.expectError(error.NotSupported, mem.rebuildProjection());
}

test "context backed memory does not trust persisted projection progress for unsafe backends after restart" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const workspace = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(workspace);

    {
        var projection = memory_lru.InMemoryLruMemory.init(std.testing.allocator, 32);
        const mem = try wrapContextBackedMemory(
            std.testing.allocator,
            projection.memory(),
            "api",
            .{
                .supports_keyword_rank = false,
                .supports_session_store = false,
                .supports_transactions = false,
                .supports_outbox = false,
                .supports_native_recall = true,
                .supports_scoped_native_recall = false,
                .supports_safe_rebuild = false,
                .has_remote_side_effects = true,
            },
            workspace,
            "agent-a",
        );
        defer mem.deinit();

        try mem.store("pref", "core-value", .core, null);

        const info = try mem.eventFeedInfo();
        try std.testing.expectEqualStrings("projection", info.recall_source);
    }

    {
        var projection = memory_lru.InMemoryLruMemory.init(std.testing.allocator, 32);
        const mem = try wrapContextBackedMemory(
            std.testing.allocator,
            projection.memory(),
            "api",
            .{
                .supports_keyword_rank = false,
                .supports_session_store = false,
                .supports_transactions = false,
                .supports_outbox = false,
                .supports_native_recall = true,
                .supports_scoped_native_recall = false,
                .supports_safe_rebuild = false,
                .has_remote_side_effects = true,
            },
            workspace,
            "agent-a",
        );
        defer mem.deinit();

        const info = try mem.eventFeedInfo();
        try std.testing.expectEqual(@as(u64, 1), info.latest_sequence);
        try std.testing.expectEqual(@as(u64, 1), info.projection_last_applied_sequence);
        try std.testing.expectEqualStrings("core", info.recall_source);

        const recalled = try mem.recall(std.testing.allocator, "pref", 4, null);
        defer freeEntries(std.testing.allocator, recalled);
        try std.testing.expectEqual(@as(usize, 1), recalled.len);
        try std.testing.expectEqualStrings("core-value", recalled[0].content);
    }
}

test "SessionStore delegates through vtable" {
    const TestSessionStore = struct {
        call_count: usize = 0,

        fn implSaveMessage(ptr: *anyopaque, _: []const u8, _: []const u8, _: []const u8) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
        }
        fn implLoadMessages(_: *anyopaque, allocator: std.mem.Allocator, _: []const u8) anyerror![]MessageEntry {
            return allocator.alloc(MessageEntry, 0);
        }
        fn implClearMessages(ptr: *anyopaque, _: []const u8) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
        }
        fn implClearAutoSaved(ptr: *anyopaque, _: ?[]const u8) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
        }
        fn implSaveUsage(ptr: *anyopaque, _: []const u8, _: u64) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
        }
        fn implLoadUsage(_: *anyopaque, _: []const u8) anyerror!?u64 {
            return 42;
        }

        const sess_vtable = SessionStore.VTable{
            .saveMessage = &implSaveMessage,
            .loadMessages = &implLoadMessages,
            .clearMessages = &implClearMessages,
            .clearAutoSaved = &implClearAutoSaved,
            .saveUsage = &implSaveUsage,
            .loadUsage = &implLoadUsage,
        };
    };

    var mock = TestSessionStore{};
    const store = SessionStore{ .ptr = @ptrCast(&mock), .vtable = &TestSessionStore.sess_vtable };

    try store.saveMessage("s1", "user", "hello");
    try std.testing.expectEqual(@as(usize, 1), mock.call_count);

    const msgs = try store.loadMessages(std.testing.allocator, "s1");
    defer std.testing.allocator.free(msgs);
    try std.testing.expectEqual(@as(usize, 0), msgs.len);

    try store.clearMessages("s1");
    try std.testing.expectEqual(@as(usize, 2), mock.call_count);

    try store.clearAutoSaved(null);
    try std.testing.expectEqual(@as(usize, 3), mock.call_count);

    try store.saveUsage("s1", 7);
    try std.testing.expectEqual(@as(usize, 4), mock.call_count);

    const usage = try store.loadUsage("s1");
    try std.testing.expectEqual(@as(?u64, 42), usage);
}

test "freeMessages frees all entries" {
    const allocator = std.testing.allocator;
    var messages = try allocator.alloc(MessageEntry, 2);
    messages[0] = .{ .role = try allocator.dupe(u8, "user"), .content = try allocator.dupe(u8, "hello") };
    messages[1] = .{ .role = try allocator.dupe(u8, "assistant"), .content = try allocator.dupe(u8, "hi") };
    freeMessages(allocator, messages);
    // No leak = pass (allocator is testing allocator with leak detection)
}

fn requireBackendEnabledForTests(name: []const u8) !void {
    if (findBackend(name) == null) return error.SkipZigTest;
}

const TestTmpDir = @TypeOf(std.testing.tmpDir(.{}));
const TestWorkspace = struct {
    tmp: TestTmpDir,
    path: []u8,

    fn init(allocator: std.mem.Allocator) !TestWorkspace {
        var tmp = std.testing.tmpDir(.{});
        const path = try tmp.dir.realpathAlloc(allocator, ".");
        return .{ .tmp = tmp, .path = path };
    }

    fn deinit(self: *TestWorkspace, allocator: std.mem.Allocator) void {
        allocator.free(self.path);
        self.tmp.cleanup();
    }
};

test "initRuntime none returns valid runtime" {
    try requireBackendEnabledForTests("none");

    var rt = initRuntime(std.testing.allocator, &.{ .backend = "none" }, "/tmp") orelse
        return error.TestUnexpectedResult;
    defer rt.deinit();

    try std.testing.expectEqualStrings("none", rt.memory.name());
    try std.testing.expect(rt.session_store == null);
    try std.testing.expect(!rt.capabilities.supports_session_store);
    try std.testing.expect(!rt.capabilities.supports_keyword_rank);
}

test "initRuntime unknown backend returns null" {
    try std.testing.expect(initRuntime(std.testing.allocator, &.{ .backend = "unknown_backend" }, "/tmp") == null);
}

test "initRuntime none deinit does not leak" {
    try requireBackendEnabledForTests("none");

    var rt = initRuntime(std.testing.allocator, &.{ .backend = "none" }, "/tmp") orelse
        return error.TestUnexpectedResult;
    rt.deinit();
    // testing allocator detects leaks — if we get here, no leak
}

test "initRuntime none has null db_path" {
    try requireBackendEnabledForTests("none");

    var rt = initRuntime(std.testing.allocator, &.{ .backend = "none" }, "/tmp") orelse
        return error.TestUnexpectedResult;
    defer rt.deinit();

    try std.testing.expect(rt._db_path == null);
    try std.testing.expect(rt.response_cache == null);
}

test "initRuntime sqlite returns full runtime" {
    if (!build_options.enable_memory_sqlite) return;
    var ws = try TestWorkspace.init(std.testing.allocator);
    defer ws.deinit(std.testing.allocator);

    var rt = initRuntime(std.testing.allocator, &.{ .backend = "sqlite" }, ws.path) orelse
        return error.TestUnexpectedResult;
    defer rt.deinit();

    try std.testing.expectEqualStrings("sqlite", rt.memory.name());
    try std.testing.expect(rt.session_store != null);
    try std.testing.expect(rt.capabilities.supports_session_store);
    try std.testing.expect(rt.capabilities.supports_keyword_rank);
    try std.testing.expect(rt.capabilities.supports_transactions);
    try std.testing.expect(rt._db_path != null);
    const path_slice = std.mem.span(rt._db_path.?);
    try std.testing.expect(std.mem.endsWith(u8, path_slice, "memory.db"));
}

test "initRuntime with lifecycle defaults does not crash" {
    try requireBackendEnabledForTests("none");

    var rt = initRuntime(std.testing.allocator, &.{ .backend = "none" }, "/tmp/test_lifecycle");
    if (rt) |*r| r.deinit();
}

test "initRuntime with cache disabled leaves response_cache null" {
    try requireBackendEnabledForTests("none");

    var rt = initRuntime(std.testing.allocator, &.{ .backend = "none" }, "/tmp/test_nocache") orelse return;
    defer rt.deinit();
    try std.testing.expect(rt.response_cache == null);
    try std.testing.expect(rt._cache_db_path == null);
}

test "initRuntime with cache enabled creates ResponseCache" {
    if (!build_options.enable_sqlite) return error.SkipZigTest;
    try requireBackendEnabledForTests("none");

    var ws = try TestWorkspace.init(std.testing.allocator);
    defer ws.deinit(std.testing.allocator);

    var rt = initRuntime(std.testing.allocator, &.{
        .backend = "none",
        .response_cache = .{
            .enabled = true,
            .ttl_minutes = 5,
            .max_entries = 100,
        },
    }, ws.path) orelse return;
    defer rt.deinit();
    try std.testing.expect(rt.response_cache != null);
    try std.testing.expect(rt._cache_db_path != null);
}

test "initRuntime creates engine with primary source" {
    try requireBackendEnabledForTests("none");

    var rt = initRuntime(std.testing.allocator, &.{ .backend = "none" }, "/tmp") orelse
        return error.TestUnexpectedResult;
    defer rt.deinit();
    try std.testing.expect(rt._engine != null);
}

test "initRuntime engine with qmd disabled has one source" {
    try requireBackendEnabledForTests("none");

    var rt = initRuntime(std.testing.allocator, &.{ .backend = "none" }, "/tmp") orelse
        return error.TestUnexpectedResult;
    defer rt.deinit();
    if (rt._engine) |eng| {
        try std.testing.expectEqual(@as(usize, 1), eng.sources.items.len);
    }
}

test "initRuntime engine with qmd enabled and include_default_memory=true has primary and qmd sources" {
    try requireBackendEnabledForTests("none");

    var rt = initRuntime(std.testing.allocator, &.{
        .backend = "none",
        .qmd = .{
            .enabled = true,
            .include_default_memory = true,
        },
    }, "/tmp") orelse return error.TestUnexpectedResult;
    defer rt.deinit();

    if (rt._engine) |eng| {
        try std.testing.expectEqual(@as(usize, 2), eng.sources.items.len);
        try std.testing.expectEqualStrings("primary", eng.sources.items[0].getName());
        try std.testing.expectEqualStrings("qmd", eng.sources.items[1].getName());
    } else return error.TestUnexpectedResult;
}

test "initRuntime engine with qmd enabled and include_default_memory=false has qmd-only source" {
    try requireBackendEnabledForTests("none");

    var rt = initRuntime(std.testing.allocator, &.{
        .backend = "none",
        .qmd = .{
            .enabled = true,
            .include_default_memory = false,
        },
    }, "/tmp") orelse return error.TestUnexpectedResult;
    defer rt.deinit();

    if (rt._engine) |eng| {
        try std.testing.expectEqual(@as(usize, 1), eng.sources.items.len);
        try std.testing.expectEqualStrings("qmd", eng.sources.items[0].getName());
    } else return error.TestUnexpectedResult;
}

test "MemoryRuntime.search without engine falls back to recall" {
    var backend = none.NoneMemory.init();
    defer backend.deinit();
    var rt = MemoryRuntime{
        .memory = backend.memory(),
        .session_store = null,
        .response_cache = null,
        .capabilities = .{
            .supports_keyword_rank = false,
            .supports_session_store = false,
            .supports_transactions = false,
            .supports_outbox = false,
            .supports_native_recall = false,
            .supports_scoped_native_recall = false,
            .supports_safe_rebuild = false,
            .has_remote_side_effects = false,
        },
        .resolved = test_resolved_cfg,
        ._db_path = null,
        ._cache_db_path = null,
        ._engine = null,
        ._allocator = std.testing.allocator,
        ._embedding_provider = null,
        ._vector_store = null,
        ._circuit_breaker = null,
        ._outbox = null,
    };
    const results = try rt.search(std.testing.allocator, "query", 5, null);
    defer retrieval.freeCandidates(std.testing.allocator, results);
    try std.testing.expectEqual(@as(usize, 0), results.len);
}

test "MemoryRuntime.search with engine delegates" {
    try requireBackendEnabledForTests("none");

    var rt = initRuntime(std.testing.allocator, &.{ .backend = "none" }, "/tmp") orelse
        return error.TestUnexpectedResult;
    defer rt.deinit();
    const results = try rt.search(std.testing.allocator, "query", 5, null);
    defer retrieval.freeCandidates(std.testing.allocator, results);
    try std.testing.expectEqual(@as(usize, 0), results.len);
}

test "MemoryRuntime.search hybrid path respects caller limit" {
    if (findBackend("memory") == null) return error.SkipZigTest;

    var rt = initRuntime(std.testing.allocator, &.{ .backend = "memory" }, "/tmp") orelse
        return error.TestUnexpectedResult;
    defer rt.deinit();

    try rt.memory.store("k1", "alpha one", .core, null);
    try rt.memory.store("k2", "alpha two", .core, null);
    try rt.memory.store("k3", "alpha three", .core, null);

    rt._rollout_policy = .{ .mode = .on, .canary_percent = 0, .shadow_percent = 0 };

    const results = try rt.search(std.testing.allocator, "alpha", 1, null);
    defer retrieval.freeCandidates(std.testing.allocator, results);
    try std.testing.expectEqual(@as(usize, 1), results.len);
}

test "initRuntime with hybrid disabled has no embedding provider" {
    try requireBackendEnabledForTests("none");

    var rt = initRuntime(std.testing.allocator, &.{ .backend = "none" }, "/tmp") orelse
        return error.TestUnexpectedResult;
    defer rt.deinit();

    try std.testing.expect(rt._embedding_provider == null);
    try std.testing.expect(rt._vector_store == null);
    try std.testing.expect(rt._circuit_breaker == null);
    try std.testing.expect(rt._outbox == null);
}

test "initRuntime with search.provider=none has no vector store" {
    try requireBackendEnabledForTests("none");

    var rt = initRuntime(std.testing.allocator, &.{
        .backend = "none",
        .search = .{
            .provider = "none",
            .query = .{ .hybrid = .{ .enabled = true } },
        },
    }, "/tmp") orelse
        return error.TestUnexpectedResult;
    defer rt.deinit();

    try std.testing.expect(rt._embedding_provider == null);
    try std.testing.expect(rt._vector_store == null);
}

test "initRuntime resolves sqlite_sidecar mode when explicitly configured" {
    if (!build_options.enable_memory_sqlite) return;
    var ws = try TestWorkspace.init(std.testing.allocator);
    defer ws.deinit(std.testing.allocator);

    var rt = initRuntime(std.testing.allocator, &.{
        .backend = "sqlite",
        .search = .{
            .provider = "openai",
            .query = .{ .hybrid = .{ .enabled = true } },
            .store = .{ .kind = "sqlite_sidecar" },
        },
    }, ws.path) orelse return error.TestUnexpectedResult;
    defer rt.deinit();

    try std.testing.expect(rt._vector_store != null);
    try std.testing.expectEqualStrings("sqlite_sidecar", rt.resolved.vector_mode);
}

test "initRuntime resolves sqlite_ann mode when explicitly configured" {
    if (!build_options.enable_memory_sqlite) return;
    var ws = try TestWorkspace.init(std.testing.allocator);
    defer ws.deinit(std.testing.allocator);

    var rt = initRuntime(std.testing.allocator, &.{
        .backend = "sqlite",
        .search = .{
            .provider = "openai",
            .query = .{ .hybrid = .{ .enabled = true } },
            .store = .{
                .kind = "sqlite_ann",
                .ann_candidate_multiplier = 10,
                .ann_min_candidates = 80,
            },
        },
    }, ws.path) orelse return error.TestUnexpectedResult;
    defer rt.deinit();

    try std.testing.expect(rt._vector_store != null);
    try std.testing.expectEqualStrings("sqlite_ann", rt.resolved.vector_mode);
}

test "initRuntime uses configured relative sqlite_sidecar path" {
    if (!build_options.enable_memory_sqlite) return;
    var ws = try TestWorkspace.init(std.testing.allocator);
    defer ws.deinit(std.testing.allocator);

    var rt = initRuntime(std.testing.allocator, &.{
        .backend = "sqlite",
        .search = .{
            .provider = "openai",
            .query = .{ .hybrid = .{ .enabled = true } },
            .store = .{
                .kind = "sqlite_sidecar",
                .sidecar_path = "vectors-custom.db",
            },
        },
    }, ws.path) orelse return error.TestUnexpectedResult;
    defer rt.deinit();

    const expected_path = try std.fs.path.join(std.testing.allocator, &.{ ws.path, "vectors-custom.db" });
    defer std.testing.allocator.free(expected_path);

    try std.testing.expect(rt._sidecar_db_path != null);
    try std.testing.expectEqualStrings(expected_path, std.mem.span(rt._sidecar_db_path.?));
}

test "initRuntime uses configured absolute sqlite_sidecar path" {
    if (!build_options.enable_memory_sqlite) return;
    var ws = try TestWorkspace.init(std.testing.allocator);
    defer ws.deinit(std.testing.allocator);
    const absolute_sidecar_path = try std.fs.path.join(std.testing.allocator, &.{ ws.path, "vectors-absolute.db" });
    defer std.testing.allocator.free(absolute_sidecar_path);

    var rt = initRuntime(std.testing.allocator, &.{
        .backend = "sqlite",
        .search = .{
            .provider = "openai",
            .query = .{ .hybrid = .{ .enabled = true } },
            .store = .{
                .kind = "sqlite_sidecar",
                .sidecar_path = absolute_sidecar_path,
            },
        },
    }, ws.path) orelse return error.TestUnexpectedResult;
    defer rt.deinit();

    try std.testing.expect(rt._sidecar_db_path != null);
    try std.testing.expectEqualStrings(absolute_sidecar_path, std.mem.span(rt._sidecar_db_path.?));
}

test "initRuntime respects search.enabled=false" {
    if (!build_options.enable_memory_sqlite) return;
    var ws = try TestWorkspace.init(std.testing.allocator);
    defer ws.deinit(std.testing.allocator);

    var rt = initRuntime(std.testing.allocator, &.{
        .backend = "sqlite",
        .search = .{
            .enabled = false,
            .provider = "openai",
            .query = .{ .hybrid = .{ .enabled = true } },
        },
    }, ws.path) orelse return error.TestUnexpectedResult;
    defer rt.deinit();

    try std.testing.expect(rt._engine == null);
    try std.testing.expect(rt._embedding_provider == null);
    try std.testing.expect(rt._vector_store == null);
    try std.testing.expectEqualStrings("disabled", rt.resolved.retrieval_mode);

    const candidates = try rt.search(std.testing.allocator, "query", 5, null);
    defer retrieval.freeCandidates(std.testing.allocator, candidates);
    try std.testing.expectEqual(@as(usize, 0), candidates.len);
}

test "initRuntime durable_outbox uses max of embed/vector retry config" {
    if (!build_options.enable_memory_sqlite) return;
    var ws = try TestWorkspace.init(std.testing.allocator);
    defer ws.deinit(std.testing.allocator);

    var rt = initRuntime(std.testing.allocator, &.{
        .backend = "sqlite",
        .search = .{
            .provider = "openai",
            .query = .{ .hybrid = .{ .enabled = true } },
            .sync = .{
                .mode = "durable_outbox",
                .embed_max_retries = 1,
                .vector_max_retries = 5,
            },
        },
    }, ws.path) orelse return error.TestUnexpectedResult;
    defer rt.deinit();

    const ob = rt._outbox orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u32, 5), ob.max_retries);
    try std.testing.expectEqualStrings("durable_outbox", rt.resolved.vector_sync_mode);
}

test "initRuntime resolves best_effort vector sync when outbox backend unavailable" {
    try requireBackendEnabledForTests("none");

    var rt = initRuntime(std.testing.allocator, &.{
        .backend = "none",
        .search = .{
            .provider = "openai",
            .query = .{ .hybrid = .{ .enabled = true } },
            .store = .{
                .kind = "qdrant",
                .qdrant_url = "http://127.0.0.1:6333",
            },
            .sync = .{
                .mode = "durable_outbox",
            },
        },
    }, "/tmp") orelse return error.TestUnexpectedResult;
    defer rt.deinit();

    try std.testing.expect(rt._vector_store != null);
    try std.testing.expect(rt._outbox == null);
    try std.testing.expectEqualStrings("best_effort", rt.resolved.vector_sync_mode);
}

test "initRuntime fail_fast returns null when durable outbox is unavailable" {
    try requireBackendEnabledForTests("none");

    const rt = initRuntime(std.testing.allocator, &.{
        .backend = "none",
        .search = .{
            .provider = "openai",
            .query = .{ .hybrid = .{ .enabled = true } },
            .store = .{
                .kind = "qdrant",
                .qdrant_url = "http://127.0.0.1:6333",
            },
            .sync = .{
                .mode = "durable_outbox",
            },
        },
        .reliability = .{
            .fallback_policy = "fail_fast",
        },
    }, "/tmp");
    try std.testing.expect(rt == null);
}

test "syncVectorAfterStore enqueues when durable outbox is active" {
    if (!build_options.enable_memory_sqlite) return;
    var ws = try TestWorkspace.init(std.testing.allocator);
    defer ws.deinit(std.testing.allocator);

    var rt = initRuntime(std.testing.allocator, &.{
        .backend = "sqlite",
        .search = .{
            .provider = "openai",
            .query = .{ .hybrid = .{ .enabled = true } },
            .sync = .{
                .mode = "durable_outbox",
            },
        },
    }, ws.path) orelse return error.TestUnexpectedResult;
    defer rt.deinit();

    const ob = rt._outbox orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 0), try ob.pendingCount());

    rt.syncVectorAfterStore(std.testing.allocator, "k1", "content", null);
    try std.testing.expectEqual(@as(usize, 1), try ob.pendingCount());
}

test "deleteFromVectorStore enqueues delete when durable outbox is active" {
    if (!build_options.enable_memory_sqlite) return;
    var ws = try TestWorkspace.init(std.testing.allocator);
    defer ws.deinit(std.testing.allocator);

    var rt = initRuntime(std.testing.allocator, &.{
        .backend = "sqlite",
        .search = .{
            .provider = "openai",
            .query = .{ .hybrid = .{ .enabled = true } },
            .sync = .{
                .mode = "durable_outbox",
            },
        },
    }, ws.path) orelse return error.TestUnexpectedResult;
    defer rt.deinit();

    const ob = rt._outbox orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 0), try ob.pendingCount());

    rt.deleteFromVectorStore("k1", null);
    try std.testing.expectEqual(@as(usize, 1), try ob.pendingCount());
}

test "initRuntime hygiene preserve enqueues vector sync when durable outbox is active" {
    if (!build_options.enable_memory_sqlite) return;

    var ws = try TestWorkspace.init(std.testing.allocator);
    defer ws.deinit(std.testing.allocator);

    const archive_path = try std.fs.path.join(std.testing.allocator, &.{ ws.path, "memory", "archive" });
    defer std.testing.allocator.free(archive_path);
    try fs_compat.makePath(archive_path);

    var archive_dir = try std.fs.cwd().openDir(archive_path, .{});
    defer archive_dir.close();

    var file = try archive_dir.createFile("old-memory.md", .{});
    defer file.close();
    try file.writeAll("Archived markdown content that should be preserved and vector-synced.");
    try file.updateTimes(0, 0);

    var rt = initRuntime(std.testing.allocator, &.{
        .backend = "sqlite",
        .search = .{
            .provider = "openai",
            .query = .{ .hybrid = .{ .enabled = true } },
            .sync = .{
                .mode = "durable_outbox",
            },
        },
        .lifecycle = .{
            .hygiene_enabled = true,
            .archive_after_days = 0,
            .purge_after_days = 1,
            .preserve_before_purge = true,
            .conversation_retention_days = 0,
        },
    }, ws.path) orelse return error.TestUnexpectedResult;
    defer rt.deinit();

    try std.testing.expect((archive_dir.statFile("old-memory.md") catch null) == null);

    const preserved = try rt.memory.list(std.testing.allocator, .{ .custom = "archive" }, null);
    defer freeEntries(std.testing.allocator, preserved);
    try std.testing.expect(preserved.len > 0);

    const ob = rt._outbox orelse return error.TestUnexpectedResult;
    try std.testing.expect(try ob.pendingCount() > 0);
}

test "MemoryRuntime.syncVectorAfterStore with no provider is no-op" {
    var backend = none.NoneMemory.init();
    defer backend.deinit();
    var rt = MemoryRuntime{
        .memory = backend.memory(),
        .session_store = null,
        .response_cache = null,
        .capabilities = .{
            .supports_keyword_rank = false,
            .supports_session_store = false,
            .supports_transactions = false,
            .supports_outbox = false,
            .supports_native_recall = false,
            .supports_scoped_native_recall = false,
            .supports_safe_rebuild = false,
            .has_remote_side_effects = false,
        },
        .resolved = test_resolved_cfg,
        ._db_path = null,
        ._cache_db_path = null,
        ._engine = null,
        ._allocator = std.testing.allocator,
        ._embedding_provider = null,
        ._vector_store = null,
        ._circuit_breaker = null,
        ._outbox = null,
    };
    // Should not crash — just a no-op
    rt.syncVectorAfterStore(std.testing.allocator, "key", "content", null);
}

test "MemoryRuntime.drainOutbox with no outbox returns 0" {
    var backend = none.NoneMemory.init();
    defer backend.deinit();
    var rt = MemoryRuntime{
        .memory = backend.memory(),
        .session_store = null,
        .response_cache = null,
        .capabilities = .{
            .supports_keyword_rank = false,
            .supports_session_store = false,
            .supports_transactions = false,
            .supports_outbox = false,
            .supports_native_recall = false,
            .supports_scoped_native_recall = false,
            .supports_safe_rebuild = false,
            .has_remote_side_effects = false,
        },
        .resolved = test_resolved_cfg,
        ._db_path = null,
        ._cache_db_path = null,
        ._engine = null,
        ._allocator = std.testing.allocator,
        ._embedding_provider = null,
        ._vector_store = null,
        ._circuit_breaker = null,
        ._outbox = null,
    };
    try std.testing.expectEqual(@as(u32, 0), rt.drainOutbox(std.testing.allocator));
}

test "MemoryRuntime.deinit cleans up P3 resources" {
    try requireBackendEnabledForTests("none");

    var rt = initRuntime(std.testing.allocator, &.{ .backend = "none" }, "/tmp") orelse
        return error.TestUnexpectedResult;
    // P3 fields are null for "none" backend with hybrid disabled, but deinit should handle that.
    rt.deinit();
    // testing allocator detects leaks
}

test {
    // engines/ (Layer A)
    _ = sqlite;
    _ = markdown;
    _ = none;
    _ = memory_lru;
    _ = lucid;
    _ = postgres;
    _ = redis;
    _ = lancedb;
    _ = clickhouse;
    _ = registry;
    _ = @import("engines/contract_test.zig");

    // retrieval/ (Layer B)
    _ = retrieval;
    _ = retrieval_qmd;
    _ = rrf;
    _ = query_expansion;
    _ = temporal_decay;
    _ = mmr;
    _ = adaptive;
    _ = llm_reranker;

    // vector/ (Layer C)
    _ = vector;
    _ = vector_store;
    _ = embeddings;
    _ = embeddings_gemini;
    _ = embeddings_voyage;
    _ = embeddings_ollama;
    _ = provider_router;
    _ = store_qdrant;
    _ = store_pgvector;
    _ = circuit_breaker;
    _ = outbox;
    _ = chunker;

    // lifecycle/ (Layer D)
    _ = cache;
    _ = semantic_cache;
    _ = hygiene;
    _ = snapshot;
    _ = rollout;
    _ = migrate;
    _ = diagnostics;
    _ = summarizer;
}
