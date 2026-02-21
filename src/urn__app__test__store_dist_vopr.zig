const std = @import("std");

const Store = @import("urn__app__s3api__storage_dist.zig").Store;
const codec = @import("urn__app__dist__codec.zig");

const Sha256 = std.crypto.hash.sha2.Sha256;

const bucket_name = "b";
const heal_key = "__heal";

fn snapDigest(allocator: std.mem.Allocator, memstore: anytype) ![32]u8 {
    const snap = try codec.encodeStoreSnapshot(allocator, memstore);
    defer allocator.free(snap);
    var out: [32]u8 = undefined;
    Sha256.hash(snap, &out, .{});
    return out;
}

fn replicaDigest(allocator: std.mem.Allocator, store: *Store, rid: usize) ![32]u8 {
    return snapDigest(allocator, &store.replicas[rid].store);
}

fn leaderDigest(allocator: std.mem.Allocator, store: *Store) ![32]u8 {
    return replicaDigest(allocator, store, store.leader_id);
}

const RefState = struct {
    allocator: std.mem.Allocator,
    // key -> value
    objs: std.StringHashMapUnmanaged([]u8) = .{},

    fn deinit(self: *RefState) void {
        var it = self.objs.iterator();
        while (it.next()) |e| self.allocator.free(e.value_ptr.*);
        self.objs.deinit(self.allocator);
        self.* = undefined;
    }

    fn put(self: *RefState, key: []const u8, data: []const u8) !void {
        const dup = try self.allocator.dupe(u8, data);
        errdefer self.allocator.free(dup);
        if (self.objs.getPtr(key)) |v| {
            self.allocator.free(v.*);
            v.* = dup;
            return;
        }
        try self.objs.put(self.allocator, key, dup);
    }

    fn del(self: *RefState, key: []const u8) void {
        if (self.objs.fetchRemove(key)) |kv| {
            self.allocator.free(kv.value);
        }
    }

    fn get(self: *RefState, key: []const u8) ?[]const u8 {
        return self.objs.get(key);
    }
};

fn corruptFollowerLastEntry(store: *Store, rid: usize, rng: std.Random) void {
    // Only followers.
    if (rid == store.leader_id) return;
    var r = &store.replicas[rid];
    if (!r.alive) return;
    if (r.entries.items.len == 0) return;

    const last = r.entries.items[r.entries.items.len - 1];
    if (last.cmd_len == 0) return;

    const off = last.cmd_offset + rng.uintLessThan(u64, last.cmd_len);
    var b: [1]u8 = .{0};
    if (r.wal.readPositionalAll(r.io, &b, off) catch null == null) return;
    b[0] ^= 0xFF;
    _ = r.wal.writePositionalAll(r.io, &b, off) catch return;
    _ = r.wal.sync(r.io) catch return;
}

fn checkMatchesRef(store: *Store, ref: *RefState) !void {
    // For all keys in the ref, ensure store returns the same bytes.
    var it = ref.objs.iterator();
    while (it.next()) |e| {
        const got = try store.getObject(bucket_name, e.key_ptr.*);
        try std.testing.expectEqualStrings(e.value_ptr.*, got);
    }

    // For a non-existent key, store must return NoSuchKey.
    if (ref.objs.count() == 0) {
        _ = store.getObject(bucket_name, "missing") catch |err| {
            try std.testing.expectEqual(error.NoSuchKey, err);
            return;
        };
        return error.ExpectedNoSuchKey;
    }
}

fn runScenario(seed: u64) ![32]u8 {
    const io = std.testing.io;
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var before: [32]u8 = undefined;

    // First run: mutate under faults, then stabilize and record leader digest.
    {
        var store = try Store.initAtDir(allocator, io, tmp.dir, .{ .replicas = 3, .leader_id = 0 });
        defer store.deinit();

        var ref: RefState = .{ .allocator = allocator };
        defer ref.deinit();

        // Create the single bucket.
        try store.createBucket(bucket_name);

        var prng = std.Random.DefaultPrng.init(seed);
        const rng = prng.random();

        const keys = [_][]const u8{ "k1", "k2", "k3" };

        var step: usize = 0;
        while (step < 200) : (step += 1) {
            // Fault injection ~15%.
            if (rng.uintLessThan(u32, 100) < 15) {
                const fid = rng.uintLessThan(usize, store.replicas.len);
                const which = rng.uintLessThan(u32, 3);
                switch (which) {
                    0 => store.replicas[fid].alive = false,
                    1 => store.replicas[fid].alive = true,
                    2 => corruptFollowerLastEntry(&store, fid, rng),
                    else => {},
                }
            }

            // Operation.
            const k = keys[rng.uintLessThan(usize, keys.len)];
            const op = rng.uintLessThan(u32, 3);
            switch (op) {
                0 => { // put
                    var buf: [32]u8 = undefined;
                    const n = 1 + rng.uintLessThan(usize, buf.len);
                    for (buf[0..n], 0..) |*b, i| b.* = @intCast((seed + step + i) & 0xFF);

                    store.putObject(bucket_name, k, buf[0..n]) catch |err| switch (err) {
                        error.NoQuorum => {},
                        else => return err,
                    };
                    // Update ref if read-back succeeded (covers the NoQuorum rollback case).
                    const got_opt = store.getObject(bucket_name, k) catch null;
                    if (got_opt) |got| try ref.put(k, got);
                },
                1 => { // delete
                    store.deleteObject(bucket_name, k) catch |err| switch (err) {
                        error.NoQuorum, error.NoSuchKey => {},
                        else => return err,
                    };
                    // If key is gone, update ref.
                    _ = store.getObject(bucket_name, k) catch {
                        ref.del(k);
                        continue;
                    };
                },
                2 => { // get
                    if (ref.get(k)) |exp| {
                        const got = try store.getObject(bucket_name, k);
                        try std.testing.expectEqualStrings(exp, got);
                    } else {
                        _ = store.getObject(bucket_name, k) catch |err| {
                            try std.testing.expectEqual(error.NoSuchKey, err);
                            continue;
                        };
                    }
                },
                else => {},
            }

            // Basic consistency check against ref.
            try checkMatchesRef(&store, &ref);
        }

        // Stabilize: bring followers back, write a heal entry to force resync.
        for (store.replicas) |*r| r.alive = true;

        // Force replication/resync.
        store.putObject(bucket_name, heal_key, "x") catch |err| switch (err) {
            error.NoQuorum => return error.UnexpectedNoQuorumInStabilize,
            else => return err,
        };
        try ref.put(heal_key, "x");

        const ld = try leaderDigest(allocator, &store);
        // Followers should converge to leader after the heal write.
        for (store.replicas, 0..) |*r, rid| {
            if (rid == store.leader_id) continue;
            try std.testing.expect(r.alive);
            try std.testing.expect(!r.needs_resync);
            const d = try replicaDigest(allocator, &store, rid);
            try std.testing.expectEqual(ld, d);
        }

        before = ld;
    }

    // Second run: reopen and ensure leader digest matches.
    {
        var store2 = try Store.initAtDir(allocator, io, tmp.dir, .{ .replicas = 3, .leader_id = 0 });
        defer store2.deinit();

        const after = try leaderDigest(allocator, &store2);
        try std.testing.expectEqual(before, after);
    }

    return before;
}

test "store_dist_vopr: determinism (same seed => same final digest)" {
    const d1 = try runScenario(123);
    const d2 = try runScenario(123);
    try std.testing.expectEqual(d1, d2);
}

test "store_dist_vopr: fault matrix (seeds 1..10) stays safe and converges" {
    var seed: u64 = 1;
    while (seed <= 10) : (seed += 1) {
        _ = try runScenario(seed);
    }
}
