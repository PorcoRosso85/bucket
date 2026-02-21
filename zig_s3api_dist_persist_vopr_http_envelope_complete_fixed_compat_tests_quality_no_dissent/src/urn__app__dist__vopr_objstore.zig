const std = @import("std");
const MemStore = @import("urn__app__core__memstore.zig").Store;

/// Deterministic Simulation Testing (DST) harness for a replicated object-store core.
///
/// This file intentionally blends:
/// - a small replication protocol (fixed membership)
/// - deterministic simulation of network/process/disk
/// - an oracle checking safety + eventual convergence
///
/// Implemented milestones:
/// - (1) leader crash tolerance via deterministic Raft-style elections (term/vote/heartbeat)
/// - (2) persistence model extension: durable log + snapshot + fsync boundary + restart replay
///
/// Non-goals (for now):
/// - membership change
/// - real sockets
/// - full S3 compatibility (SigV4/multipart)
pub const MaxNodes: usize = 9;

// -----------------------------
// Command encoding/decoding
// -----------------------------

const CmdTag = enum(u8) {
    create_bucket = 1,
    delete_bucket = 2,
    put_object = 3,
    delete_object = 4,
};

const CmdView = union(CmdTag) {
    create_bucket: []const u8,
    delete_bucket: []const u8,
    put_object: struct { bucket: []const u8, key: []const u8, data: []const u8 },
    delete_object: struct { bucket: []const u8, key: []const u8 },
};

fn appendU16(out: *std.ArrayList(u8), allocator: std.mem.Allocator, v: u16) !void {
    var buf: [2]u8 = undefined;
    std.mem.writeInt(u16, &buf, v, .big);
    try out.appendSlice(allocator, &buf);
}

fn readU16(r: anytype) !u16 {
    var buf: [2]u8 = undefined;
    try r.readNoEof(&buf);
    return std.mem.readInt(u16, &buf, .big);
}

fn appendU32(out: *std.ArrayList(u8), allocator: std.mem.Allocator, v: u32) !void {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, v, .big);
    try out.appendSlice(allocator, &buf);
}

fn readU32(r: anytype) !u32 {
    var buf: [4]u8 = undefined;
    try r.readNoEof(&buf);
    return std.mem.readInt(u32, &buf, .big);
}

// Minimal fixed-buffer reader (Zig 0.16 removed `std.io.fixedBufferStream`).
const BufReader = struct {
    buf: []const u8,
    pos: usize = 0,

    fn readByte(self: *BufReader) !u8 {
        if (self.pos >= self.buf.len) return error.EndOfStream;
        const b = self.buf[self.pos];
        self.pos += 1;
        return b;
    }

    fn readNoEof(self: *BufReader, out: []u8) !void {
        if (self.pos + out.len > self.buf.len) return error.EndOfStream;
        @memcpy(out, self.buf[self.pos .. self.pos + out.len]);
        self.pos += out.len;
    }
};

pub fn encodeCmd(allocator: std.mem.Allocator, cmd: CmdView) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    switch (cmd) {
        .create_bucket => |b| {
            try out.append(allocator, @intFromEnum(CmdTag.create_bucket));
            try appendU16(&out, allocator, @intCast(b.len));
            try out.appendSlice(allocator, b);
        },
        .delete_bucket => |b| {
            try out.append(allocator, @intFromEnum(CmdTag.delete_bucket));
            try appendU16(&out, allocator, @intCast(b.len));
            try out.appendSlice(allocator, b);
        },
        .put_object => |p| {
            try out.append(allocator, @intFromEnum(CmdTag.put_object));
            try appendU16(&out, allocator, @intCast(p.bucket.len));
            try out.appendSlice(allocator, p.bucket);
            try appendU16(&out, allocator, @intCast(p.key.len));
            try out.appendSlice(allocator, p.key);
            try appendU32(&out, allocator, @intCast(p.data.len));
            try out.appendSlice(allocator, p.data);
        },
        .delete_object => |p| {
            try out.append(allocator, @intFromEnum(CmdTag.delete_object));
            try appendU16(&out, allocator, @intCast(p.bucket.len));
            try out.appendSlice(allocator, p.bucket);
            try appendU16(&out, allocator, @intCast(p.key.len));
            try out.appendSlice(allocator, p.key);
        },
    }

    return out.toOwnedSlice(allocator);
}

pub fn decodeCmd(buf: []const u8) !CmdView {
    if (buf.len < 1) return error.BadCmd;
    var br = BufReader{ .buf = buf };
    const tag_u8 = try br.readByte();
    const tag: CmdTag = @enumFromInt(tag_u8);
    return switch (tag) {
        .create_bucket => blk: {
            const blen = try readU16(&br);
            const start = br.pos;
            const end = start + blen;
            if (end > buf.len) return error.BadCmd;
            br.pos = end;
            break :blk CmdView{ .create_bucket = buf[start..end] };
        },
        .delete_bucket => blk: {
            const blen = try readU16(&br);
            const start = br.pos;
            const end = start + blen;
            if (end > buf.len) return error.BadCmd;
            br.pos = end;
            break :blk CmdView{ .delete_bucket = buf[start..end] };
        },
        .put_object => blk: {
            const blen = try readU16(&br);
            const bstart = br.pos;
            const bend = bstart + blen;
            if (bend > buf.len) return error.BadCmd;
            br.pos = bend;
            const klen = try readU16(&br);
            const kstart = br.pos;
            const kend = kstart + klen;
            if (kend > buf.len) return error.BadCmd;
            br.pos = kend;
            const dlen = try readU32(&br);
            const dstart = br.pos;
            const dend = dstart + dlen;
            if (dend > buf.len) return error.BadCmd;
            br.pos = dend;
            break :blk CmdView{ .put_object = .{ .bucket = buf[bstart..bend], .key = buf[kstart..kend], .data = buf[dstart..dend] } };
        },
        .delete_object => blk: {
            const blen = try readU16(&br);
            const bstart = br.pos;
            const bend = bstart + blen;
            if (bend > buf.len) return error.BadCmd;
            br.pos = bend;
            const klen = try readU16(&br);
            const kstart = br.pos;
            const kend = kstart + klen;
            if (kend > buf.len) return error.BadCmd;
            br.pos = kend;
            break :blk CmdView{ .delete_object = .{ .bucket = buf[bstart..bend], .key = buf[kstart..kend] } };
        },
    };
}

fn cmdChecksum(buf: []const u8) u64 {
    // Not cryptographic; just for corruption detection inside the simulation.
    return std.hash.Wyhash.hash(0, buf);
}

/// Deterministic digest of the current object-store state.
///
/// Used by the oracle to assert that replicas converged.
pub fn storeDigest(allocator: std.mem.Allocator, store: *MemStore) ![32]u8 {
    var h = std.crypto.hash.sha2.Sha256.init(.{});

    const names = try store.listBucketNames(allocator);
    defer allocator.free(names);

    // Small N: O(n^2) sort is fine and keeps stdlib usage minimal.
    var sorted_names = try allocator.alloc([]const u8, names.len);
    defer allocator.free(sorted_names);
    for (names, 0..) |n, i| sorted_names[i] = n;
    var i: usize = 0;
    while (i < sorted_names.len) : (i += 1) {
        var j: usize = i + 1;
        while (j < sorted_names.len) : (j += 1) {
            if (std.mem.lessThan(u8, sorted_names[j], sorted_names[i])) {
                const tmp = sorted_names[i];
                sorted_names[i] = sorted_names[j];
                sorted_names[j] = tmp;
            }
        }
    }

    for (sorted_names) |bucket| {
        h.update(bucket);
        h.update("\x00");
        const keys = store.listObjectKeys(bucket, allocator) catch |err| switch (err) {
            error.NoSuchBucket => continue,
            else => return err,
        };
        defer allocator.free(keys);
        var sorted_keys = try allocator.alloc([]const u8, keys.len);
        defer allocator.free(sorted_keys);
        for (keys, 0..) |k, ki| sorted_keys[ki] = k;
        var a: usize = 0;
        while (a < sorted_keys.len) : (a += 1) {
            var b: usize = a + 1;
            while (b < sorted_keys.len) : (b += 1) {
                if (std.mem.lessThan(u8, sorted_keys[b], sorted_keys[a])) {
                    const t = sorted_keys[a];
                    sorted_keys[a] = sorted_keys[b];
                    sorted_keys[b] = t;
                }
            }
        }
        for (sorted_keys) |key| {
            h.update(key);
            h.update("\x00");
            const data = store.getObject(bucket, key) catch continue;
            h.update(data);
            h.update("\x00");
        }
    }

    var digest: [32]u8 = undefined;
    h.final(&digest);
    return digest;
}

// -----------------------------
// Snapshot encoding (state snapshot, not log prefix)
// -----------------------------

// Format:
// [bucket_count:u16]
//   repeat bucket_count:
//     [bucket_len:u16][bucket_bytes]
//     [obj_count:u16]
//       repeat obj_count:
//         [key_len:u16][key_bytes]
//         [data_len:u32][data_bytes]
fn encodeStoreSnapshot(allocator: std.mem.Allocator, store: *MemStore) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);

    const names = try store.listBucketNames(allocator);
    defer allocator.free(names);

    if (names.len > std.math.maxInt(u16)) return error.TooMany;

    // deterministic ordering
    var sorted_names = try allocator.alloc([]const u8, names.len);
    defer allocator.free(sorted_names);
    for (names, 0..) |n, i| sorted_names[i] = n;
    var i: usize = 0;
    while (i < sorted_names.len) : (i += 1) {
        var j: usize = i + 1;
        while (j < sorted_names.len) : (j += 1) {
            if (std.mem.lessThan(u8, sorted_names[j], sorted_names[i])) {
                const tmp = sorted_names[i];
                sorted_names[i] = sorted_names[j];
                sorted_names[j] = tmp;
            }
        }
    }

    try appendU16(&out, allocator, @intCast(sorted_names.len));
    for (sorted_names) |bucket| {
        try appendU16(&out, allocator, @intCast(bucket.len));
        try out.appendSlice(allocator, bucket);

        const keys = store.listObjectKeys(bucket, allocator) catch |err| switch (err) {
            error.NoSuchBucket => {
                try appendU16(&out, allocator, 0);
                continue;
            },
            else => return err,
        };
        defer allocator.free(keys);

        if (keys.len > std.math.maxInt(u16)) return error.TooMany;
        var sorted_keys = try allocator.alloc([]const u8, keys.len);
        defer allocator.free(sorted_keys);
        for (keys, 0..) |k, ki| sorted_keys[ki] = k;
        var a: usize = 0;
        while (a < sorted_keys.len) : (a += 1) {
            var b: usize = a + 1;
            while (b < sorted_keys.len) : (b += 1) {
                if (std.mem.lessThan(u8, sorted_keys[b], sorted_keys[a])) {
                    const t = sorted_keys[a];
                    sorted_keys[a] = sorted_keys[b];
                    sorted_keys[b] = t;
                }
            }
        }

        try appendU16(&out, allocator, @intCast(sorted_keys.len));
        for (sorted_keys) |key| {
            try appendU16(&out, allocator, @intCast(key.len));
            try out.appendSlice(allocator, key);
            const data = store.getObject(bucket, key) catch "";
            try appendU32(&out, allocator, @intCast(data.len));
            try out.appendSlice(allocator, data);
        }
    }

    return out.toOwnedSlice(allocator);
}

fn decodeStoreSnapshot(payload: []const u8, allocator: std.mem.Allocator) !MemStore {
    var br = BufReader{ .buf = payload };

    var store = MemStore.init(allocator);
    errdefer store.deinit();

    const bucket_count = try readU16(&br);
    var bi: usize = 0;
    while (bi < bucket_count) : (bi += 1) {
        const blen = try readU16(&br);
        const bstart = br.pos;
        const bend = bstart + blen;
        if (bend > payload.len) return error.BadSnapshot;
        br.pos = bend;
        const bucket = payload[bstart..bend];
        store.createBucket(bucket) catch |err| switch (err) {
            error.BucketAlreadyExists => {},
            else => return err,
        };

        const obj_count = try readU16(&br);
        var oi: usize = 0;
        while (oi < obj_count) : (oi += 1) {
            const klen = try readU16(&br);
            const kstart = br.pos;
            const kend = kstart + klen;
            if (kend > payload.len) return error.BadSnapshot;
            br.pos = kend;
            const key = payload[kstart..kend];

            const dlen = try readU32(&br);
            const dstart = br.pos;
            const dend = dstart + dlen;
            if (dend > payload.len) return error.BadSnapshot;
            br.pos = dend;
            const data = payload[dstart..dend];

            store.putObject(bucket, key, data) catch |err| switch (err) {
                error.NoSuchBucket => return error.BadSnapshot,
                else => return err,
            };
        }
    }

    return store;
}

// -----------------------------
// Cluster protocol messages
// -----------------------------

const MsgKind = enum(u8) {
    append,
    ack,
    commit,
    heartbeat,
    resync_req,
    snapshot,
    request_vote,
    vote,
};

const AppendMsg = struct {
    term: u64,
    from: u8,
    to: u8,
    index: u64,
    checksum: u64,
    cmd: []u8, // owned

    fn deinit(self: *AppendMsg, allocator: std.mem.Allocator) void {
        allocator.free(self.cmd);
    }
};

const SnapshotMsg = struct {
    term: u64,
    from: u8,
    to: u8,
    commit_index: u64,
    snapshot_index: u64,
    payload: []u8, // owned (store snapshot)

    fn deinit(self: *SnapshotMsg, allocator: std.mem.Allocator) void {
        allocator.free(self.payload);
    }
};

const NetMsg = union(MsgKind) {
    append: AppendMsg,
    ack: struct { term: u64, from: u8, to: u8, index: u64, ok: bool },
    commit: struct { term: u64, from: u8, to: u8, commit_index: u64 },
    heartbeat: struct { term: u64, from: u8, to: u8, commit_index: u64 },
    resync_req: struct { term: u64, from: u8, to: u8 },
    snapshot: SnapshotMsg,
    request_vote: struct { term: u64, from: u8, to: u8, last_index: u64, commit_index: u64 },
    vote: struct { term: u64, from: u8, to: u8, granted: bool },

    fn deinit(self: *NetMsg, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .append => |*m| m.deinit(allocator),
            .snapshot => |*s| s.deinit(allocator),
            else => {},
        }
    }
};

// -----------------------------
// Node / state
// -----------------------------

const Entry = struct {
    index: u64,
    checksum: u64,
    cmd: []u8, // owned

    fn deinit(self: *Entry, allocator: std.mem.Allocator) void {
        allocator.free(self.cmd);
    }
};

const Role = enum {
    follower,
    candidate,
    leader,
};

const Pending = struct {
    index: u64,
    acks_mask: u64,
    leader_fsynced: bool = false,
    committed: bool = false,
};

pub const DiskFaults = struct {
    write_lost_pct: u8 = 0,
    write_corrupt_pct: u8 = 0,
    write_torn_pct: u8 = 0,
    write_misdirect_pct: u8 = 0,

    scrub_corrupt_pct: u8 = 0,
};

const Node = struct {
    id: u8,

    // process state
    alive: bool = true,
    paused: bool = false,
    needs_resync: bool = false,

    // consensus
    term: u64 = 0,
    voted_for: ?u8 = null,
    role: Role = .follower,

    // durable disk state
    snapshot_index: u64 = 0,
    snapshot: []u8 = &[_]u8{}, // owned; empty => genesis
    durable_log: std.ArrayListUnmanaged(Entry) = .{}, // entries with index > snapshot_index
    unstable_log: std.ArrayListUnmanaged(Entry) = .{}, // not fsynced yet; lost on crash
    commit_index: u64 = 0,

    // volatile derived state
    store: MemStore,
    applied_index: u64 = 0,

    // election timers (volatile)
    election_deadline: u64 = 0,
    votes_mask: u64 = 0,

    fn init(id: u8, allocator: std.mem.Allocator) Node {
        return .{ .id = id, .store = MemStore.init(allocator) };
    }

    fn deinit(self: *Node, allocator: std.mem.Allocator) void {
        self.store.deinit();
        allocator.free(@constCast(self.snapshot));
        for (self.durable_log.items) |*e| e.deinit(allocator);
        self.durable_log.deinit(allocator);
        for (self.unstable_log.items) |*e| e.deinit(allocator);
        self.unstable_log.deinit(allocator);
    }

    fn durableLastIndex(self: *const Node) u64 {
        return self.snapshot_index + @as(u64, @intCast(self.durable_log.items.len));
    }

    fn lastIndex(self: *const Node) u64 {
        return self.snapshot_index + @as(u64, @intCast(self.durable_log.items.len + self.unstable_log.items.len));
    }

    fn getDurableEntry(self: *Node, index: u64) ?*Entry {
        if (index <= self.snapshot_index) return null;
        if (index > self.durableLastIndex()) return null;
        const off: usize = @intCast(index - self.snapshot_index - 1);
        return &self.durable_log.items[off];
    }

    fn dropUnstable(self: *Node, allocator: std.mem.Allocator) void {
        for (self.unstable_log.items) |*e| e.deinit(allocator);
        self.unstable_log.clearRetainingCapacity();
    }

    fn truncateTo(self: *Node, allocator: std.mem.Allocator, index: u64) void {
        // Drops any (durable or unstable) entries with global index > index.
        // Keeps snapshot as-is.
        if (index < self.snapshot_index) {
            // If asked to truncate below snapshot, treat as "wipe to snapshot".
            self.dropUnstable(allocator);
            for (self.durable_log.items) |*e| e.deinit(allocator);
            self.durable_log.clearRetainingCapacity();
            self.commit_index = self.snapshot_index;
            return;
        }

        // First drop unstable tail.
        while (self.unstable_log.items.len != 0) {
            const li = self.lastIndex();
            if (li <= index) break;
            var e = self.unstable_log.pop().?;
            e.deinit(allocator);
        }

        // Then drop durable tail.
        while (self.durable_log.items.len != 0) {
            const li = self.durableLastIndex();
            if (li <= index) break;
            var e = self.durable_log.pop().?;
            e.deinit(allocator);
        }

        if (self.commit_index > index) self.commit_index = index;
        if (self.applied_index > self.commit_index) self.applied_index = self.commit_index;
    }
};

// -----------------------------
// Deterministic simulator
// -----------------------------

pub const FaultProfile = struct {
    nodes: u8 = 3,

    // network
    drop_pct: u8 = 0,
    dup_pct: u8 = 0,
    corrupt_pct: u8 = 0,
    misdirect_pct: u8 = 0,
    max_delay: u64 = 0,

    // disk
    disk: DiskFaults = .{},
    disk_fsync_max_delay: u64 = 0,

    // process faults toggles
    enable_crash: bool = true,
    enable_pause: bool = true,
    enable_partition: bool = true,

    // leader crash is now modeled
    enable_leader_crash: bool = true,

    // protocol timers
    heartbeat_interval: u64 = 5,
    election_timeout_base: u64 = 15,
    election_timeout_jitter: u64 = 10,

    // snapshotting
    snapshot_every: u64 = 25,
};

const Payload = union(enum) {
    client_op: void,
    net_deliver: NetMsg,
    fault: Fault,
    disk_fsync: struct { node: u8, index: u64, term: u64, leader: u8 },
    tick: void,
};

const FaultKind = enum {
    crash,
    recover,
    pause,
    resume_node,
    partition_on,
    partition_off,
    scrub_corrupt,
};

const Fault = struct {
    kind: FaultKind,
    a: u8 = 0,
    b: u8 = 0,
};

const Event = struct {
    time: u64,
    seq: u64,
    payload: Payload,
};

fn eventOrder(_: void, a: Event, b: Event) std.math.Order {
    if (a.time < b.time) return .lt;
    if (a.time > b.time) return .gt;
    if (a.seq < b.seq) return .lt;
    if (a.seq > b.seq) return .gt;
    return .eq;
}

const Net = struct {
    n: u8,
    drop_pct: u8,
    dup_pct: u8,
    corrupt_pct: u8,
    misdirect_pct: u8,
    max_delay: u64,
    allowed: [MaxNodes]u64,

    fn init(n: u8, fp: FaultProfile) Net {
        var allowed: [MaxNodes]u64 = undefined;
        @memset(&allowed, 0);
        var from: u8 = 0;
        while (from < n) : (from += 1) {
            var mask: u64 = 0;
            var to: u8 = 0;
            while (to < n) : (to += 1) mask |= (@as(u64, 1) << @intCast(to));
            allowed[from] = mask;
        }
        return .{ .n = n, .drop_pct = fp.drop_pct, .dup_pct = fp.dup_pct, .corrupt_pct = fp.corrupt_pct, .misdirect_pct = fp.misdirect_pct, .max_delay = fp.max_delay, .allowed = allowed };
    }

    fn setLink(self: *Net, from: u8, to: u8, on: bool) void {
        const bit: u64 = (@as(u64, 1) << @intCast(to));
        if (on) self.allowed[from] |= bit else self.allowed[from] &= ~bit;
    }

    fn linkAllowed(self: *const Net, from: u8, to: u8) bool {
        const bit: u64 = (@as(u64, 1) << @intCast(to));
        return (self.allowed[from] & bit) != 0;
    }
};

pub const SimResult = struct {
    seed: u64,
    trace_sha256: [32]u8,
    oracle_ok: bool,
};

pub const Sim = struct {
    allocator: std.mem.Allocator,
    seed: u64,
    prng: std.Random.DefaultPrng,
    hasher: std.crypto.hash.sha2.Sha256,

    fp: FaultProfile,
    now: u64 = 0,
    seq: u64 = 0,

    pq: std.PriorityQueue(Event, void, eventOrder),

    nodes: []Node,
    leader: u8 = 0,
    pending: std.ArrayListUnmanaged(Pending) = .{},

    net: Net,

    pub fn init(allocator: std.mem.Allocator, seed: u64, fp: FaultProfile) !Sim {
        if (fp.nodes < 1 or fp.nodes > MaxNodes) return error.BadArgs;
        if (fp.heartbeat_interval == 0) return error.BadArgs;

        var pq = std.PriorityQueue(Event, void, eventOrder).init(allocator, {});
        errdefer pq.deinit();

        const nodes = try allocator.alloc(Node, fp.nodes);
        errdefer allocator.free(nodes);
        var i: usize = 0;
        while (i < nodes.len) : (i += 1) nodes[i] = Node.init(@intCast(i), allocator);

        var sim = Sim{
            .allocator = allocator,
            .seed = seed,
            .prng = std.Random.DefaultPrng.init(seed),
            .hasher = std.crypto.hash.sha2.Sha256.init(.{}),
            .fp = fp,
            .pq = pq,
            .nodes = nodes,
            .leader = 0,
            .net = Net.init(fp.nodes, fp),
        };

        // Initial leader is node 0.
        sim.nodes[0].role = .leader;
        sim.leader = 0;

        // Initialize election deadlines.
        var nid: u8 = 0;
        while (nid < fp.nodes) : (nid += 1) {
            sim.resetElectionDeadline(nid);
        }

        return sim;
    }

    pub fn deinit(self: *Sim) void {
        // The priority queue may still contain scheduled network deliveries that own
        // heap memory (append payloads / snapshots). Drain and deinit them before
        // freeing allocator-backed structures.
        while (self.pq.removeOrNull()) |ev| {
            switch (ev.payload) {
                .net_deliver => |m| {
                    var tmp = m;
                    tmp.deinit(self.allocator);
                },
                else => {},
            }
        }
        self.pq.deinit();

        for (self.nodes) |*n| n.deinit(self.allocator);
        self.allocator.free(self.nodes);
        self.pending.deinit(self.allocator);
    }

    fn trace(self: *Sim, comptime fmt: []const u8, args: anytype) void {
        var buf: [256]u8 = undefined;
        const s = std.fmt.bufPrint(&buf, fmt, args) catch return;
        self.hasher.update(s);
        self.hasher.update("\n");
    }

    fn schedule(self: *Sim, time: u64, payload: Payload) !void {
        self.seq += 1;
        try self.pq.add(.{ .time = time, .seq = self.seq, .payload = payload });
    }

    fn majority(self: *const Sim) u8 {
        return @intCast((self.nodes.len / 2) + 1);
    }

    fn ackCount(self: *const Sim, mask: u64) u8 {
        var c: u8 = 0;
        var i: u8 = 0;
        while (i < self.nodes.len) : (i += 1) {
            const bit: u64 = (@as(u64, 1) << @intCast(i));
            if ((mask & bit) != 0) c += 1;
        }
        return c;
    }

    fn resetElectionDeadline(self: *Sim, node_id: u8) void {
        var n = &self.nodes[node_id];
        // Leaders don't use election deadlines, but keeping a value helps determinism.
        const jitter = if (self.fp.election_timeout_jitter == 0) 0 else self.prng.random().uintLessThan(u64, self.fp.election_timeout_jitter + 1);
        n.election_deadline = self.now + self.fp.election_timeout_base + jitter;
    }

    // -----------------------------
    // Network send/clone helpers
    // -----------------------------

    fn patchTo(self: *Sim, msg: *NetMsg, to: u8) void {
        _ = self;
        switch (msg.*) {
            .append => |*m| m.to = to,
            .ack => |*m| m.to = to,
            .commit => |*m| m.to = to,
            .heartbeat => |*m| m.to = to,
            .resync_req => |*m| m.to = to,
            .snapshot => |*m| m.to = to,
            .request_vote => |*m| m.to = to,
            .vote => |*m| m.to = to,
        }
    }

    fn msgKindName(msg: NetMsg) []const u8 {
        return switch (msg) {
            .append => "append",
            .ack => "ack",
            .commit => "commit",
            .heartbeat => "heartbeat",
            .resync_req => "resync_req",
            .snapshot => "snapshot",
            .request_vote => "request_vote",
            .vote => "vote",
        };
    }

    fn cloneMsg(self: *Sim, msg: NetMsg) !NetMsg {
        return switch (msg) {
            .append => |m| blk: {
                const bytes = try self.allocator.dupe(u8, m.cmd);
                break :blk NetMsg{ .append = .{ .term = m.term, .from = m.from, .to = m.to, .index = m.index, .checksum = m.checksum, .cmd = bytes } };
            },
            .snapshot => |s| blk: {
                const bytes = try self.allocator.dupe(u8, s.payload);
                break :blk NetMsg{ .snapshot = .{ .term = s.term, .from = s.from, .to = s.to, .commit_index = s.commit_index, .snapshot_index = s.snapshot_index, .payload = bytes } };
            },
            .ack => |a| NetMsg{ .ack = a },
            .commit => |c| NetMsg{ .commit = c },
            .heartbeat => |h| NetMsg{ .heartbeat = h },
            .resync_req => |r| NetMsg{ .resync_req = r },
            .request_vote => |rv| NetMsg{ .request_vote = rv },
            .vote => |v| NetMsg{ .vote = v },
        };
    }

    fn send(self: *Sim, msg: NetMsg) !void {
        // Apply network faults deterministically.
        var out = msg;
        const from = switch (out) {
            .append => |m| m.from,
            .ack => |m| m.from,
            .commit => |m| m.from,
            .heartbeat => |m| m.from,
            .resync_req => |m| m.from,
            .snapshot => |m| m.from,
            .request_vote => |m| m.from,
            .vote => |m| m.from,
        };
        const to_decl = switch (out) {
            .append => |m| m.to,
            .ack => |m| m.to,
            .commit => |m| m.to,
            .heartbeat => |m| m.to,
            .resync_req => |m| m.to,
            .snapshot => |m| m.to,
            .request_vote => |m| m.to,
            .vote => |m| m.to,
        };

        // Misdirect (changes actual delivery target)
        var to_actual: u8 = to_decl;
        if (self.fp.misdirect_pct != 0 and self.prng.random().uintLessThan(u8, 100) < self.fp.misdirect_pct) {
            to_actual = self.prng.random().uintLessThan(u8, self.fp.nodes);
            self.patchTo(&out, to_actual);
        }

        // Partition (evaluated on actual delivery target)
        if (!self.net.linkAllowed(from, to_actual)) {
            out.deinit(self.allocator);
            return;
        }

        // Drop
        if (self.fp.drop_pct != 0 and self.prng.random().uintLessThan(u8, 100) < self.fp.drop_pct) {
            out.deinit(self.allocator);
            return;
        }

        // Corrupt (flip 1 bit in an index/term or append payload)
        if (self.fp.corrupt_pct != 0 and self.prng.random().uintLessThan(u8, 100) < self.fp.corrupt_pct) {
            switch (out) {
                .append => |*m| {
                    if (m.cmd.len != 0) {
                        const i = self.prng.random().uintLessThan(usize, m.cmd.len);
                        m.cmd[i] ^= 0x01;
                    } else {
                        m.index ^= 1;
                    }
                },
                .ack => |*m| m.index ^= 1,
                .commit => |*m| m.commit_index ^= 1,
                .heartbeat => |*m| m.commit_index ^= 1,
                .request_vote => |*m| m.last_index ^= 1,
                else => {},
            }
        }

        // Delay
        const d = if (self.fp.max_delay == 0) 0 else self.prng.random().uintLessThan(u64, self.fp.max_delay + 1);

        // Duplicate
        const dup = self.fp.dup_pct != 0 and self.prng.random().uintLessThan(u8, 100) < self.fp.dup_pct;

        try self.schedule(self.now + 1 + d, .{ .net_deliver = out });
        self.trace("t={d} NET send kind={s} from={d} to={d}->{d} delay={d}", .{ self.now, msgKindName(out), from, to_decl, to_actual, d });

        if (dup) {
            const copy = try self.cloneMsg(out);
            const d2 = if (self.fp.max_delay == 0) 0 else self.prng.random().uintLessThan(u64, self.fp.max_delay + 1);
            try self.schedule(self.now + 1 + d2, .{ .net_deliver = copy });
        }
    }

    // -----------------------------
    // State machine apply helpers
    // -----------------------------

    fn applyEntryToStore(self: *Sim, node: *Node, entry: *const Entry) void {
        _ = self;
        const cmd = decodeCmd(entry.cmd) catch {
            node.needs_resync = true;
            return;
        };
        switch (cmd) {
            .create_bucket => |b| {
                _ = node.store.createBucket(b) catch {};
            },
            .delete_bucket => |b| {
                _ = node.store.deleteBucket(b) catch {};
            },
            .put_object => |p| {
                _ = node.store.putObject(p.bucket, p.key, p.data) catch {};
            },
            .delete_object => |p| {
                _ = node.store.deleteObject(p.bucket, p.key) catch {};
            },
        }
    }

    fn rebuildStoreFromDisk(self: *Sim, node: *Node) void {
        // Rebuild store from snapshot + committed durable log.
        node.store.deinit();
        node.store = MemStore.init(self.allocator);

        if (node.snapshot.len != 0) {
            const s = decodeStoreSnapshot(node.snapshot, self.allocator) catch {
                node.needs_resync = true;
                return;
            };
            // swap stores
            node.store.deinit();
            node.store = s;
            node.applied_index = node.snapshot_index;
        } else {
            node.applied_index = 0;
        }

        // Apply durable log entries up to commit_index.
        if (node.commit_index < node.snapshot_index) {
            node.needs_resync = true;
            return;
        }
        const max_apply = node.commit_index;
        var idx: u64 = node.snapshot_index + 1;
        while (idx <= max_apply) : (idx += 1) {
            const e_opt = node.getDurableEntry(idx);
            if (e_opt == null) {
                node.needs_resync = true;
                return;
            }
            const e = e_opt.?;
            if (cmdChecksum(e.cmd) != e.checksum) {
                node.needs_resync = true;
                return;
            }
            self.applyEntryToStore(node, e);
            node.applied_index = idx;
        }
    }

    fn nodeApplyCommits(self: *Sim, node: *Node) void {
        if (!node.alive or node.paused) return;
        if (node.needs_resync) return;

        if (node.applied_index < node.snapshot_index) {
            // Volatile state got out of sync; recover by replay.
            self.rebuildStoreFromDisk(node);
            return;
        }

        while (node.applied_index < node.commit_index) {
            const next = node.applied_index + 1;
            if (next <= node.snapshot_index) {
                node.applied_index = next;
                continue;
            }
            const e_opt = node.getDurableEntry(next);
            if (e_opt == null) {
                node.needs_resync = true;
                return;
            }
            const e = e_opt.?;
            if (cmdChecksum(e.cmd) != e.checksum) {
                node.needs_resync = true;
                return;
            }
            self.applyEntryToStore(node, e);
            node.applied_index = next;
        }
    }

    // -----------------------------
    // Disk write + fsync modeling
    // -----------------------------

    fn scheduleFsync(self: *Sim, node_id: u8, index: u64, term: u64, leader: u8) !void {
        const d = if (self.fp.disk_fsync_max_delay == 0) 0 else self.prng.random().uintLessThan(u64, self.fp.disk_fsync_max_delay + 1);
        try self.schedule(self.now + 1 + d, .{ .disk_fsync = .{ .node = node_id, .index = index, .term = term, .leader = leader } });
    }

    fn appendToUnstable(self: *Sim, node: *Node, index: u64, checksum: u64, cmd_bytes_in: []const u8, apply_faults: bool) !bool {
        if (!node.alive or node.paused) return false;

        // Only allow sequential appends.
        const expected = node.lastIndex() + 1;
        if (index != expected) {
            node.needs_resync = true;
            return false;
        }

        // Validate checksum.
        if (cmdChecksum(cmd_bytes_in) != checksum) return false;

        // Disk write faults.
        var bytes = try self.allocator.dupe(u8, cmd_bytes_in);
        errdefer self.allocator.free(bytes);

        if (apply_faults) {
            const rnd = self.prng.random();
            if (self.fp.disk.write_lost_pct != 0 and rnd.uintLessThan(u8, 100) < self.fp.disk.write_lost_pct) {
                self.allocator.free(bytes);
                return false;
            }
            if (self.fp.disk.write_misdirect_pct != 0 and rnd.uintLessThan(u8, 100) < self.fp.disk.write_misdirect_pct) {
                // Treat as a "wrong write" by corrupting the payload; checksum check will fail later.
                if (bytes.len != 0) {
                    const i = rnd.uintLessThan(usize, bytes.len);
                    bytes[i] ^= 0x55;
                }
            }
            if (self.fp.disk.write_corrupt_pct != 0 and rnd.uintLessThan(u8, 100) < self.fp.disk.write_corrupt_pct and bytes.len != 0) {
                const i = rnd.uintLessThan(usize, bytes.len);
                bytes[i] ^= 0x01;
            }
            if (self.fp.disk.write_torn_pct != 0 and rnd.uintLessThan(u8, 100) < self.fp.disk.write_torn_pct and bytes.len > 1) {
                const new_len = rnd.uintLessThan(usize, bytes.len);
                bytes = try self.allocator.realloc(bytes, new_len);
            }
        }

        try node.unstable_log.append(self.allocator, .{ .index = index, .checksum = checksum, .cmd = bytes });
        return true;
    }

    fn fsyncOne(self: *Sim, node_id: u8, index: u64, term: u64, leader_id: u8) !void {
        var node = &self.nodes[node_id];
        if (!node.alive or node.paused) return;

        // Find the matching unstable entry.
        var found: ?usize = null;
        for (node.unstable_log.items, 0..) |e, i| {
            if (e.index == index) {
                found = i;
                break;
            }
        }
        if (found == null) return;

        const i = found.?;
        // Only fsync in-order: enforce that the fsynced entry is the earliest unstable.
        if (i != 0) return;

        // Move entry to durable log.
        var e = node.unstable_log.orderedRemove(0);
        // Durable log must be contiguous.
        const expected = node.durableLastIndex() + 1;
        if (e.index != expected) {
            e.deinit(self.allocator);
            node.needs_resync = true;
            return;
        }
        try node.durable_log.append(self.allocator, e);

        // If this is the leader, mark fsynced for commit gating.
        if (node_id == leader_id and node.role == .leader and node.term == term and node_id == self.leader) {
            if (self.findPending(index)) |p| {
                p.leader_fsynced = true;
            }
            try self.maybeAdvanceCommit();
            return;
        }

        // Followers: ack back to the leader recorded in the append.
        if (node_id != leader_id) {
            try self.send(NetMsg{ .ack = .{ .term = term, .from = node_id, .to = leader_id, .index = index, .ok = true } });
        }
    }

    // -----------------------------
    // Pending/commit helpers
    // -----------------------------

    fn findPending(self: *Sim, index: u64) ?*Pending {
        for (self.pending.items) |*p| if (p.index == index) return p;
        return null;
    }

    fn pendingAppend(self: *Sim, index: u64) !void {
        const bit: u64 = (@as(u64, 1) << @intCast(self.leader));
        try self.pending.append(self.allocator, .{ .index = index, .acks_mask = bit, .leader_fsynced = false, .committed = false });
    }

    fn maybeAdvanceCommit(self: *Sim) !void {
        var leader_node = &self.nodes[self.leader];
        if (!leader_node.alive or leader_node.role != .leader) return;
        if (leader_node.needs_resync) return;

        while (true) {
            const next = leader_node.commit_index + 1;
            const p = self.findPending(next) orelse break;
            if (p.committed) {
                leader_node.commit_index = next;
                continue;
            }
            if (!p.leader_fsynced) break;
            if (self.ackCount(p.acks_mask) < self.majority()) break;

            // Safety: commit only if leader has it durable.
            if (leader_node.durableLastIndex() < next) break;

            p.committed = true;
            leader_node.commit_index = next;

            // Apply incrementally.
            self.nodeApplyCommits(leader_node);
            if (leader_node.needs_resync) {
                // Local corruption detected while applying; step down and let a healthy
                // node take over.
                self.trace("t={d} LEADER apply_corrupt stepdown leader={d}", .{ self.now, leader_node.id });
                self.becomeFollower(leader_node.id, leader_node.term);
                return;
            }

            // Snapshot (optional, only if no pending uncommitted beyond commit).
            try self.maybeSnapshot(leader_node);

            // Send commit to followers.
            var fid: u8 = 0;
            while (fid < self.nodes.len) : (fid += 1) {
                if (fid == self.leader) continue;
                try self.send(NetMsg{ .commit = .{ .term = leader_node.term, .from = self.leader, .to = fid, .commit_index = leader_node.commit_index } });
            }
        }
    }

    fn maybeSnapshot(self: *Sim, node: *Node) !void {
        if (!node.alive) return;
        if (node.needs_resync) return;
        if (node.commit_index <= node.snapshot_index) return;
        if (node.commit_index - node.snapshot_index < self.fp.snapshot_every) return;

        // Only snapshot when there is no unstable data and the durable log contains no uncommitted tail.
        if (node.unstable_log.items.len != 0) return;
        if (node.durableLastIndex() != node.commit_index) return;

        const snap = try encodeStoreSnapshot(self.allocator, &node.store);
        self.allocator.free(@constCast(node.snapshot));
        node.snapshot = snap;
        node.snapshot_index = node.commit_index;

        // Drop all durable log entries (they are now in snapshot).
        for (node.durable_log.items) |*e| e.deinit(self.allocator);
        node.durable_log.clearRetainingCapacity();

        self.trace("t={d} SNAPSHOT node={d} idx={d}", .{ self.now, node.id, node.snapshot_index });
    }

    // -----------------------------
    // Elections
    // -----------------------------

    fn becomeFollower(self: *Sim, node_id: u8, new_term: u64) void {
        var n = &self.nodes[node_id];
        if (new_term > n.term) {
            n.term = new_term;
            n.voted_for = null;
        }
        n.role = .follower;
        n.votes_mask = 0;
        self.resetElectionDeadline(node_id);
    }

    fn startElection(self: *Sim, node_id: u8) !void {
        var n = &self.nodes[node_id];
        if (!n.alive or n.paused) return;
        if (n.needs_resync) return;

        n.role = .candidate;
        n.term += 1;
        n.voted_for = node_id;
        n.votes_mask = (@as(u64, 1) << @intCast(node_id));
        self.resetElectionDeadline(node_id);

        const last_index = n.durableLastIndex();
        const cidx = n.commit_index;

        self.trace("t={d} ELECT start cand={d} term={d} last={d} commit={d}", .{ self.now, node_id, n.term, last_index, cidx });

        var peer: u8 = 0;
        while (peer < self.nodes.len) : (peer += 1) {
            if (peer == node_id) continue;
            try self.send(NetMsg{ .request_vote = .{ .term = n.term, .from = node_id, .to = peer, .last_index = last_index, .commit_index = cidx } });
        }
    }

    fn tryWinElection(self: *Sim, node_id: u8) !void {
        var n = &self.nodes[node_id];
        if (n.role != .candidate) return;
        if (self.ackCount(n.votes_mask) < self.majority()) return;

        // Become leader.
        n.role = .leader;
        self.leader = node_id;
        self.pending.clearRetainingCapacity();

        // Discard any uncommitted tail (keep only committed prefix). This mirrors a safe leader step-up.
        n.truncateTo(self.allocator, n.commit_index);

        // Ensure store matches disk.
        self.rebuildStoreFromDisk(n);

        self.trace("t={d} ELECT win leader={d} term={d} commit={d}", .{ self.now, node_id, n.term, n.commit_index });

        // Announce leadership quickly.
        var peer: u8 = 0;
        while (peer < self.nodes.len) : (peer += 1) {
            if (peer == node_id) continue;
            try self.send(NetMsg{ .heartbeat = .{ .term = n.term, .from = node_id, .to = peer, .commit_index = n.commit_index } });
        }
    }

    fn candidateUpToDate(candidate_last: u64, candidate_commit: u64, voter_last: u64, voter_commit: u64) bool {
        if (candidate_commit > voter_commit) return true;
        if (candidate_commit < voter_commit) return false;
        return candidate_last >= voter_last;
    }

    // -----------------------------
    // Protocol: leader propose
    // -----------------------------

    fn leaderProposeRandom(self: *Sim) !void {
        const leader_node = &self.nodes[self.leader];
        if (!leader_node.alive or leader_node.paused) return;
        if (leader_node.role != .leader) return;

        const rnd = self.prng.random();
        const bucket_id = rnd.uintLessThan(u8, 2);
        const key_id = rnd.uintLessThan(u8, 3);

        var bname_buf: [8]u8 = undefined;
        const bname = std.fmt.bufPrint(&bname_buf, "b{d}", .{bucket_id}) catch "b0";

        var kname_buf: [8]u8 = undefined;
        const kname = std.fmt.bufPrint(&kname_buf, "k{d}", .{key_id}) catch "k0";

        const op = rnd.uintLessThan(u8, 4);
        const cmd_view: CmdView = switch (op) {
            0 => .{ .create_bucket = bname },
            1 => .{ .delete_bucket = bname },
            2 => blk: {
                var data_buf: [8]u8 = undefined;
                const data = std.fmt.bufPrint(&data_buf, "v{d}{d}", .{ bucket_id, key_id }) catch "v";
                break :blk CmdView{ .put_object = .{ .bucket = bname, .key = kname, .data = data } };
            },
            else => .{ .delete_object = .{ .bucket = bname, .key = kname } },
        };

        const cmd_bytes = try encodeCmd(self.allocator, cmd_view);
        errdefer self.allocator.free(cmd_bytes);
        const sum = cmdChecksum(cmd_bytes);

        const index = leader_node.lastIndex() + 1;

        // Append to leader's unstable log (no disk faults on leader in this toy core).
        const ok = try self.appendToUnstable(&self.nodes[self.leader], index, sum, cmd_bytes, false);
        if (!ok) {
            self.allocator.free(cmd_bytes);
            return;
        }

        try self.pendingAppend(index);
        try self.scheduleFsync(self.leader, index, leader_node.term, self.leader);

        // Replicate to followers.
        var follower: u8 = 0;
        while (follower < self.nodes.len) : (follower += 1) {
            if (follower == self.leader) continue;
            const bytes = try self.allocator.dupe(u8, cmd_bytes);
            try self.send(NetMsg{ .append = .{ .term = leader_node.term, .from = self.leader, .to = follower, .index = index, .checksum = sum, .cmd = bytes } });
        }

        // All uses of `cmd_bytes` above duplicate into owned buffers.
        self.allocator.free(cmd_bytes);

        self.trace("t={d} CLIENT propose leader={d} term={d} idx={d}", .{ self.now, self.leader, leader_node.term, index });
    }

    // -----------------------------
    // Resync
    // -----------------------------

    fn resyncFollower(self: *Sim, follower_id: u8) !void {
        const leader_node = &self.nodes[self.leader];
        if (!leader_node.alive or leader_node.role != .leader) return;

        // Encode current committed store state.
        const payload = try encodeStoreSnapshot(self.allocator, &leader_node.store);
        try self.send(NetMsg{ .snapshot = .{ .term = leader_node.term, .from = self.leader, .to = follower_id, .commit_index = leader_node.commit_index, .snapshot_index = leader_node.commit_index, .payload = payload } });
    }

    // -----------------------------
    // Deliver handlers
    // -----------------------------

    fn onDeliver(self: *Sim, msg: NetMsg) !void {
        defer {
            var tmp = msg;
            tmp.deinit(self.allocator);
        }

        switch (msg) {
            .append => |m| {
                var node = &self.nodes[m.to];
                if (!node.alive or node.paused) return;

                // Term handling.
                if (m.term < node.term) return;
                if (m.term > node.term) {
                    node.term = m.term;
                    node.voted_for = null;
                }
                if (node.role != .follower) node.role = .follower;
                self.resetElectionDeadline(node.id);

                if (node.needs_resync) return;

                const ok = try self.appendToUnstable(node, m.index, m.checksum, m.cmd, true);
                if (ok) {
                    try self.scheduleFsync(node.id, m.index, m.term, m.from);
                } else {
                    node.needs_resync = true;
                    // Ask for snapshot from the known leader (declared by from).
                    try self.send(NetMsg{ .resync_req = .{ .term = node.term, .from = node.id, .to = m.from } });
                }
            },
            .ack => |a| {
                const leader_node = &self.nodes[self.leader];
                if (a.to != self.leader) return;
                if (!leader_node.alive or leader_node.role != .leader) return;
                if (a.term != leader_node.term) return;

                var p = self.findPending(a.index) orelse return;
                const bit: u64 = (@as(u64, 1) << @intCast(a.from));
                p.acks_mask |= bit;
                try self.maybeAdvanceCommit();
            },
            .commit => |c| {
                var node = &self.nodes[c.to];
                if (!node.alive or node.paused) return;

                if (c.term < node.term) return;
                if (c.term > node.term) {
                    node.term = c.term;
                    node.voted_for = null;
                }
                node.role = .follower;
                self.resetElectionDeadline(node.id);

                if (c.commit_index > node.durableLastIndex()) {
                    node.needs_resync = true;
                } else if (c.commit_index > node.commit_index) {
                    node.commit_index = c.commit_index;
                }

                self.nodeApplyCommits(node);
                if (node.needs_resync) {
                    try self.send(NetMsg{ .resync_req = .{ .term = node.term, .from = node.id, .to = c.from } });
                }
            },
            .heartbeat => |h| {
                var node = &self.nodes[h.to];
                if (!node.alive or node.paused) return;

                if (h.term < node.term) return;
                if (h.term > node.term) {
                    node.term = h.term;
                    node.voted_for = null;
                }
                if (node.role != .follower) node.role = .follower;
                self.resetElectionDeadline(node.id);

                // Optionally advance commit index if possible.
                if (h.commit_index > node.durableLastIndex()) {
                    node.needs_resync = true;
                    try self.send(NetMsg{ .resync_req = .{ .term = node.term, .from = node.id, .to = h.from } });
                } else if (h.commit_index > node.commit_index) {
                    node.commit_index = h.commit_index;
                    self.nodeApplyCommits(node);
                }
            },
            .resync_req => |r| {
                const leader_node = &self.nodes[self.leader];
                if (!leader_node.alive or leader_node.role != .leader) return;
                if (r.to != self.leader) return;
                if (r.term > leader_node.term) {
                    // Step down if the requester is ahead (rare in this toy core).
                    self.becomeFollower(self.leader, r.term);
                    return;
                }
                try self.resyncFollower(r.from);
            },
            .snapshot => |s| {
                var node = &self.nodes[s.to];
                if (!node.alive) return;

                if (s.term < node.term) return;
                if (s.term > node.term) {
                    node.term = s.term;
                    node.voted_for = null;
                }
                node.role = .follower;
                self.resetElectionDeadline(node.id);

                // Replace snapshot.
                self.allocator.free(@constCast(node.snapshot));
                node.snapshot = try self.allocator.dupe(u8, s.payload);
                node.snapshot_index = s.snapshot_index;

                // Drop logs.
                for (node.durable_log.items) |*e| e.deinit(self.allocator);
                node.durable_log.clearRetainingCapacity();
                node.dropUnstable(self.allocator);

                node.commit_index = s.commit_index;
                node.needs_resync = false;

                // Rebuild store.
                self.rebuildStoreFromDisk(node);

                if (node.needs_resync) {
                    // Ask again.
                    try self.send(NetMsg{ .resync_req = .{ .term = node.term, .from = node.id, .to = s.from } });
                }
            },
            .request_vote => |rv| {
                var node = &self.nodes[rv.to];
                if (!node.alive or node.paused) return;

                if (rv.term < node.term) {
                    try self.send(NetMsg{ .vote = .{ .term = node.term, .from = node.id, .to = rv.from, .granted = false } });
                    return;
                }

                if (rv.term > node.term) {
                    node.term = rv.term;
                    node.voted_for = null;
                    node.role = .follower;
                }

                // A node that has detected local corruption should not participate in elections
                // until it has been resynced.
                if (node.needs_resync) {
                    try self.send(NetMsg{ .vote = .{ .term = node.term, .from = node.id, .to = rv.from, .granted = false } });
                    return;
                }

                const voter_last = node.durableLastIndex();
                const voter_commit = node.commit_index;
                const cand_ok = candidateUpToDate(rv.last_index, rv.commit_index, voter_last, voter_commit);

                var grant = false;
                if (cand_ok) {
                    if (node.voted_for == null or node.voted_for.? == rv.from) {
                        node.voted_for = rv.from;
                        grant = true;
                        self.resetElectionDeadline(node.id);
                    }
                }

                try self.send(NetMsg{ .vote = .{ .term = node.term, .from = node.id, .to = rv.from, .granted = grant } });
            },
            .vote => |v| {
                var node = &self.nodes[v.to];
                if (!node.alive or node.paused) return;

                if (node.role != .candidate) return;
                if (v.term < node.term) return;
                if (v.term > node.term) {
                    self.becomeFollower(node.id, v.term);
                    return;
                }

                if (v.granted) {
                    const bit: u64 = (@as(u64, 1) << @intCast(v.from));
                    node.votes_mask |= bit;
                    try self.tryWinElection(node.id);
                }
            },
        }
    }

    // -----------------------------
    // Fault injection
    // -----------------------------

    fn applyFault(self: *Sim, f: Fault) !void {
        switch (f.kind) {
            .crash => {
                if (self.nodes[f.a].alive) {
                    self.trace("t={d} FAULT crash n{d}", .{ self.now, f.a });
                    var n = &self.nodes[f.a];
                    n.alive = false;
                    n.paused = false;
                    n.needs_resync = false;
                    n.role = .follower;
                    n.votes_mask = 0;
                    // lose unflushed writes
                    n.dropUnstable(self.allocator);
                    // lose volatile state
                    n.store.deinit();
                    n.store = MemStore.init(self.allocator);
                    n.applied_index = 0;
                }
            },
            .recover => {
                if (!self.nodes[f.a].alive) {
                    self.trace("t={d} FAULT recover n{d}", .{ self.now, f.a });
                    var n = &self.nodes[f.a];
                    n.alive = true;
                    n.paused = false;
                    n.needs_resync = false;
                    n.role = .follower;
                    n.votes_mask = 0;
                    self.resetElectionDeadline(n.id);

                    // replay from disk
                    self.rebuildStoreFromDisk(n);

                    // if behind leader, request resync.
                    if (self.nodes[self.leader].alive and self.nodes[self.leader].role == .leader and n.commit_index < self.nodes[self.leader].commit_index) {
                        try self.send(NetMsg{ .resync_req = .{ .term = n.term, .from = n.id, .to = self.leader } });
                    }
                }
            },
            .pause => {
                if (self.nodes[f.a].alive) {
                    self.nodes[f.a].paused = true;
                    self.trace("t={d} FAULT pause n{d}", .{ self.now, f.a });
                }
            },
            .resume_node => {
                if (self.nodes[f.a].alive) {
                    self.nodes[f.a].paused = false;
                    self.resetElectionDeadline(f.a);
                    self.trace("t={d} FAULT resume n{d}", .{ self.now, f.a });
                }
            },
            .partition_on => {
                self.net.setLink(f.a, f.b, false);
                self.net.setLink(f.b, f.a, false);
                self.trace("t={d} FAULT partition_on {d}<->{d}", .{ self.now, f.a, f.b });
            },
            .partition_off => {
                self.net.setLink(f.a, f.b, true);
                self.net.setLink(f.b, f.a, true);
                self.trace("t={d} FAULT partition_off {d}<->{d}", .{ self.now, f.a, f.b });
            },
            .scrub_corrupt => {
                if (self.fp.disk.scrub_corrupt_pct == 0) return;
                const rnd = self.prng.random();
                if (rnd.uintLessThan(u8, 100) >= self.fp.disk.scrub_corrupt_pct) return;

                const n_id = rnd.uintLessThan(u8, self.fp.nodes);
                var n = &self.nodes[n_id];
                if (!n.alive) return;

                // corrupt either a durable log entry or snapshot
                if (n.durable_log.items.len != 0 and rnd.uintLessThan(u8, 2) == 0) {
                    const ei = rnd.uintLessThan(usize, n.durable_log.items.len);
                    var e = &n.durable_log.items[ei];
                    if (e.cmd.len == 0) return;
                    const bi = rnd.uintLessThan(usize, e.cmd.len);
                    e.cmd[bi] ^= 0x01;
                    self.trace("t={d} FAULT scrub_corrupt n{d} idx={d}", .{ self.now, n_id, e.index });
                } else if (n.snapshot.len != 0) {
                    const bi = rnd.uintLessThan(usize, n.snapshot.len);
                    @constCast(n.snapshot)[bi] ^= 0x01;
                    self.trace("t={d} FAULT scrub_corrupt_snapshot n{d} snap={d}", .{ self.now, n_id, n.snapshot_index });
                }

                n.needs_resync = true;
                if (n_id == self.leader and n.role == .leader) {
                    self.trace("t={d} FAULT scrub_corrupt leader_stepdown n{d}", .{ self.now, n_id });
                    self.becomeFollower(n_id, n.term);
                }
            },
        }
    }

    // -----------------------------
    // Tick driver
    // -----------------------------

    fn tick(self: *Sim) !void {
        // If the current leader has detected local corruption, it must step down and
        // resync from a healthy leader.
        const leader_node = &self.nodes[self.leader];
        if (leader_node.alive and leader_node.role == .leader and leader_node.needs_resync) {
            self.trace("t={d} LEADER stepdown needs_resync leader={d} term={d}", .{ self.now, self.leader, leader_node.term });
            self.becomeFollower(self.leader, leader_node.term);
        }

        // 1) Heartbeats.
        if (leader_node.alive and leader_node.role == .leader) {
            if (self.now % self.fp.heartbeat_interval == 0) {
                var peer: u8 = 0;
                while (peer < self.nodes.len) : (peer += 1) {
                    if (peer == self.leader) continue;
                    try self.send(NetMsg{ .heartbeat = .{ .term = leader_node.term, .from = self.leader, .to = peer, .commit_index = leader_node.commit_index } });
                }
            }
        }

        // 2) Drive resync requests.
        var i: u8 = 0;
        while (i < self.nodes.len) : (i += 1) {
            if (i == self.leader) continue;
            if (self.nodes[i].alive and !self.nodes[i].paused and self.nodes[i].needs_resync) {
                if (leader_node.alive and leader_node.role == .leader) {
                    try self.send(NetMsg{ .resync_req = .{ .term = self.nodes[i].term, .from = i, .to = self.leader } });
                }
            }
        }

        // 3) Elections.
        var nid: u8 = 0;
        while (nid < self.nodes.len) : (nid += 1) {
            var n = &self.nodes[nid];
            if (!n.alive or n.paused) continue;
            if (n.needs_resync) continue;
            if (n.role == .leader) continue;

            if (self.now >= n.election_deadline) {
                try self.startElection(nid);
            }
        }

        // 4) If there is no alive leader, let the smallest alive node start an election immediately.
        const l = &self.nodes[self.leader];
        if (!(l.alive and l.role == .leader)) {
            var cand: ?u8 = null;
            var j: u8 = 0;
            while (j < self.nodes.len) : (j += 1) {
                if (self.nodes[j].alive and !self.nodes[j].paused and !self.nodes[j].needs_resync) {
                    cand = j;
                    break;
                }
            }
            if (cand != null) {
                // Force deadline and election.
                self.nodes[cand.?].election_deadline = self.now;
                try self.startElection(cand.?);
            }
        }
    }

    // -----------------------------
    // Oracle
    // -----------------------------

    fn oracle(self: *Sim) bool {
        // 1) At most one alive leader per *term*.
        //
        // In real consensus protocols, temporary multiple leaders across *different* terms
        // can happen during partitions. The safety property is "one leader per term".
        var i: usize = 0;
        while (i < self.nodes.len) : (i += 1) {
            const a = self.nodes[i];
            if (!a.alive) continue;
            if (a.role == .leader) {
                var j: usize = i + 1;
                while (j < self.nodes.len) : (j += 1) {
                    const b = self.nodes[j];
                    if (!b.alive) continue;
                    if (b.role == .leader and b.term == a.term) return false;
                }
            }

            // 2) Basic state-machine invariants.
            if (a.commit_index < a.snapshot_index) return false;
            if (a.commit_index > a.durableLastIndex()) return false;
            if (a.lastIndex() < a.durableLastIndex()) return false;
            if (a.applied_index > a.commit_index) return false;
        }

        return true;
    }

    pub fn run(self: *Sim, ops: u32, max_time: u64, max_events: usize) !SimResult {
        // Schedule client ops
        var o: u32 = 0;
        while (o < ops) : (o += 1) {
            const t = 1 + self.prng.random().uintLessThan(u64, max_time);
            try self.schedule(t, .{ .client_op = {} });
        }

        // Schedule faults deterministically.
        if (self.fp.enable_partition and self.fp.nodes >= 3) {
            try self.schedule(max_time / 3, .{ .fault = .{ .kind = .partition_on, .a = 1, .b = 2 } });
            try self.schedule(max_time / 3 + 10, .{ .fault = .{ .kind = .partition_off, .a = 1, .b = 2 } });
        }
        if (self.fp.enable_crash and self.fp.nodes >= 3) {
            // follower crash
            try self.schedule(max_time / 2, .{ .fault = .{ .kind = .crash, .a = 2 } });
            try self.schedule(max_time / 2 + 20, .{ .fault = .{ .kind = .recover, .a = 2 } });
        }
        if (self.fp.enable_leader_crash and self.fp.nodes >= 3) {
            // leader crash (initial leader is 0)
            try self.schedule(max_time / 4, .{ .fault = .{ .kind = .crash, .a = 0 } });
            try self.schedule(max_time / 4 + 25, .{ .fault = .{ .kind = .recover, .a = 0 } });
        }
        if (self.fp.enable_pause and self.fp.nodes >= 2) {
            try self.schedule(max_time / 5, .{ .fault = .{ .kind = .pause, .a = 1 } });
            try self.schedule(max_time / 5 + 15, .{ .fault = .{ .kind = .resume_node, .a = 1 } });
        }
        // disk scrub
        try self.schedule(max_time / 2 + 5, .{ .fault = .{ .kind = .scrub_corrupt } });

        try self.schedule(1, .{ .tick = {} });

        var processed: usize = 0;
        while (processed < max_events) : (processed += 1) {
            const ev_opt = self.pq.removeOrNull();
            if (ev_opt == null) break;
            const ev = ev_opt.?;
            if (ev.time > max_time) break;

            self.now = ev.time;
            switch (ev.payload) {
                .client_op => try self.leaderProposeRandom(),
                .net_deliver => |m| try self.onDeliver(m),
                .fault => |f| try self.applyFault(f),
                .disk_fsync => |d| try self.fsyncOne(d.node, d.index, d.term, d.leader),
                .tick => {
                    try self.tick();
                    try self.schedule(self.now + 1, .{ .tick = {} });
                },
            }

            if (!self.oracle()) {
                self.trace("t={d} ORACLE_FAIL", .{self.now});
                break;
            }
        }

        // Stabilize: heal partitions, resume everyone, disable probabilistic faults.
        if (self.fp.enable_partition and self.fp.nodes >= 3) {
            self.net.setLink(1, 2, true);
            self.net.setLink(2, 1, true);
        }
        for (self.nodes) |*n| {
            if (n.alive) n.paused = false;
        }

        self.fp.drop_pct = 0;
        self.fp.dup_pct = 0;
        self.fp.corrupt_pct = 0;
        self.fp.misdirect_pct = 0;
        self.fp.max_delay = 0;
        self.fp.disk.write_lost_pct = 0;
        self.fp.disk.write_corrupt_pct = 0;
        self.fp.disk.write_torn_pct = 0;
        self.fp.disk.write_misdirect_pct = 0;
        self.fp.disk.scrub_corrupt_pct = 0;
        self.fp.disk_fsync_max_delay = 0;

        // Drive some extra ticks to elect a leader and drain.
        var extra: u64 = 0;
        while (extra < 60) : (extra += 1) {
            self.now = max_time + extra;
            try self.tick();
            // deliver whatever is queued at this time
            var drained: usize = 0;
            while (drained < 400) : (drained += 1) {
                const ev2 = self.pq.peek() orelse break;
                if (ev2.time > self.now) break;
                _ = self.pq.remove();
                switch (ev2.payload) {
                    .net_deliver => |m| try self.onDeliver(m),
                    .fault => |f| try self.applyFault(f),
                    .disk_fsync => |d| try self.fsyncOne(d.node, d.index, d.term, d.leader),
                    else => {},
                }
            }
        }

        // Force a clean resync to converge.
        var fid: u8 = 0;
        while (fid < self.nodes.len) : (fid += 1) {
            if (fid == self.leader) continue;
            if (self.nodes[fid].alive) {
                try self.send(NetMsg{ .resync_req = .{ .term = self.nodes[fid].term, .from = fid, .to = self.leader } });
            }
        }

        // Drain some more.
        extra = 0;
        while (extra < 40) : (extra += 1) {
            self.now = max_time + 60 + extra;
            try self.tick();
            var drained: usize = 0;
            while (drained < 400) : (drained += 1) {
                const ev2 = self.pq.peek() orelse break;
                if (ev2.time > self.now) break;
                _ = self.pq.remove();
                switch (ev2.payload) {
                    .net_deliver => |m| try self.onDeliver(m),
                    .disk_fsync => |d| try self.fsyncOne(d.node, d.index, d.term, d.leader),
                    else => {},
                }
            }
        }

        // Final settle: keep advancing time until a leader is established and all alive
        // nodes have applied the same committed prefix (or we hit a hard limit).
        var settle: u64 = 0;
        while (settle < 300) : (settle += 1) {
            const leader_node = &self.nodes[self.leader];
            var converged = leader_node.alive and leader_node.role == .leader;
            if (converged) {
                for (self.nodes) |*n| {
                    if (!n.alive) continue;
                    if (n.needs_resync) {
                        converged = false;
                        break;
                    }
                    if (n.commit_index != leader_node.commit_index) {
                        converged = false;
                        break;
                    }
                    if (n.applied_index != n.commit_index) {
                        converged = false;
                        break;
                    }
                }
            }
            if (converged) break;

            self.now += 1;
            try self.tick();

            var drained: usize = 0;
            while (drained < 800) : (drained += 1) {
                const ev3 = self.pq.peek() orelse break;
                if (ev3.time > self.now) break;
                _ = self.pq.remove();
                switch (ev3.payload) {
                    .net_deliver => |m| try self.onDeliver(m),
                    .fault => |f| try self.applyFault(f),
                    .disk_fsync => |d| try self.fsyncOne(d.node, d.index, d.term, d.leader),
                    else => {},
                }
            }
        }

        // Tail-drain any remaining scheduled work (e.g., messages scheduled at now+1 by
        // the last tick). This helps the run finish in a quiescent state without
        // requiring yet another fixed number of ticks.
        var tail: usize = 0;
        while (tail < 10_000) : (tail += 1) {
            const ev4 = self.pq.removeOrNull() orelse break;
            if (ev4.time > self.now) self.now = ev4.time;
            switch (ev4.payload) {
                .net_deliver => |m| try self.onDeliver(m),
                .fault => |f| try self.applyFault(f),
                .disk_fsync => |d| try self.fsyncOne(d.node, d.index, d.term, d.leader),
                else => {},
            }
        }

        var ok = self.oracle();
        if (ok) {
            const leader_node = &self.nodes[self.leader];
            if (!(leader_node.alive and leader_node.role == .leader)) ok = false;

            var d0: [32]u8 = [_]u8{0} ** 32;
            if (ok) {
                d0 = storeDigest(self.allocator, &leader_node.store) catch blk: {
                    ok = false;
                    break :blk [_]u8{0} ** 32;
                };
            }
            if (ok) {
                for (self.nodes) |*n| {
                    if (!n.alive) continue;
                    if (n.commit_index != leader_node.commit_index) {
                        ok = false;
                        break;
                    }
                    if (n.applied_index != n.commit_index) {
                        ok = false;
                        break;
                    }
                    const d = storeDigest(self.allocator, &n.store) catch {
                        ok = false;
                        break;
                    };
                    if (!std.mem.eql(u8, &d, &d0)) {
                        ok = false;
                        break;
                    }
                }
            }
        }

        var digest: [32]u8 = undefined;
        self.hasher.final(&digest);
        return .{ .seed = self.seed, .trace_sha256 = digest, .oracle_ok = ok };
    }
};
