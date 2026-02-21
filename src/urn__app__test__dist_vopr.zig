const std = @import("std");
const vopr = @import("urn__app__dist__vopr_objstore.zig");
const DistStore = @import("urn__app__s3api__storage_dist.zig").Store;
const codec = @import("urn__app__dist__codec.zig");

test "dist/vopr determinism (same seed => same trace hash)" {
    const allocator = std.testing.allocator;

    var fp = vopr.FaultProfile{};
    fp.nodes = 3;
    fp.drop_pct = 5;
    fp.dup_pct = 3;
    fp.max_delay = 5;
    fp.disk.write_corrupt_pct = 2;
    fp.disk.write_torn_pct = 1;
    fp.disk.scrub_corrupt_pct = 2;
    fp.disk_fsync_max_delay = 3;

    var sim1 = try vopr.Sim.init(allocator, 123, fp);
    defer sim1.deinit();
    const r1 = try sim1.run(60, 180, 40000);

    var sim2 = try vopr.Sim.init(allocator, 123, fp);
    defer sim2.deinit();
    const r2 = try sim2.run(60, 180, 40000);

    try std.testing.expect(r1.oracle_ok);
    try std.testing.expect(r2.oracle_ok);
    try std.testing.expect(std.mem.eql(u8, &r1.trace_sha256, &r2.trace_sha256));
}

test "dist/vopr fault matrix (safety + convergence)" {
    const allocator = std.testing.allocator;

    const profiles = [_]vopr.FaultProfile{
        // baseline
        .{ .nodes = 3 },
        // network: drop+delay+dup
        .{ .nodes = 3, .drop_pct = 10, .dup_pct = 5, .max_delay = 10 },
        // network: misdirect+corrupt (should be handled by checksums + resync)
        .{ .nodes = 3, .drop_pct = 5, .corrupt_pct = 2, .misdirect_pct = 2, .max_delay = 8 },
        // disk: follower write corruption / torn writes + scrub
        .{ .nodes = 3, .drop_pct = 5, .max_delay = 6, .disk_fsync_max_delay = 4, .disk = .{ .write_corrupt_pct = 3, .write_torn_pct = 2, .scrub_corrupt_pct = 5 } },
    };

    var seed: u64 = 1;
    while (seed <= 10) : (seed += 1) {
        for (profiles) |fp| {
            var sim = try vopr.Sim.init(allocator, seed, fp);
            defer sim.deinit();
            const r = try sim.run(80, 220, 60000);
            if (!r.oracle_ok) {
                std.debug.print("\nFAIL seed={d} profile={any} trace={x}\n", .{ seed, fp, r.trace_sha256 });
            }
            try std.testing.expect(r.oracle_ok);
        }
    }
}

fn corruptFollowerWal(io: std.Io, rep: anytype, rand: std.Random) void {
    if (rep.wal_size == 0) return;
    const off = rand.uintLessThan(u64, rep.wal_size);
    var b: [1]u8 = undefined;
    const n = rep.wal.readPositionalAll(io, &b, off) catch return;
    if (n != 1) return;
    b[0] ^= 0x5a;
    rep.wal.writePositionalAll(io, &b, off) catch return;
    rep.wal.sync(io) catch return;
}

// vopr-style stress test for the *real* durable distributed store.
//
// We intentionally keep the model small and lean on three invariants:
// 1) operations that return success must be durable,
// 2) after healing, replicas converge to the leader's committed state,
// 3) follower corruption is detected and healed via snapshot resync.
test "dist_store vopr-style: partitions + restarts + corruption converge" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store = try DistStore.initAtDir(allocator, io, tmp.dir, .{ .replicas = 3, .leader_id = 0 });
    defer store.deinit();

    var prng = std.Random.DefaultPrng.init(0x6c7f3a2d_1b4c9e01);
    const rand = prng.random();

    const buckets = [_][]const u8{ "a", "b", "c" };
    const keys = [_][]const u8{ "k1", "k2", "k3", "k4" };

    var step: usize = 0;
    while (step < 120) : (step += 1) {
        // Randomly kill/revive followers.
        if (rand.uintLessThan(u32, 100) < 10) {
            const fid: usize = 1 + rand.uintLessThan(usize, 2); // 1 or 2
            store.replicas[fid].alive = !store.replicas[fid].alive;
        }

        // Randomly corrupt a follower WAL and restart the whole store to surface it.
        if (rand.uintLessThan(u32, 100) < 5) {
            const fid: usize = 1 + rand.uintLessThan(usize, 2);
            corruptFollowerWal(io, &store.replicas[fid], rand);
            store.deinit();
            store = try DistStore.initAtDir(allocator, io, tmp.dir, .{ .replicas = 3, .leader_id = 0 });
        }

        // Random operations.
        const op = rand.uintLessThan(u32, 4);
        const b = buckets[rand.uintLessThan(usize, buckets.len)];
        const k = keys[rand.uintLessThan(usize, keys.len)];

        switch (op) {
            0 => {
                store.createBucket(b) catch |err| switch (err) {
                    error.BucketAlreadyExists, error.NoQuorum => {},
                    else => return err,
                };
            },
            1 => {
                var data: [16]u8 = undefined;
                const n = rand.uintLessThan(usize, data.len + 1);
                var i: usize = 0;
                while (i < n) : (i += 1) data[i] = @intCast(rand.uintLessThan(u8, 255));
                store.putObject(b, k, data[0..n]) catch |err| switch (err) {
                    error.NoSuchBucket, error.NoQuorum => {},
                    else => return err,
                };
            },
            2 => {
                store.deleteObject(b, k) catch |err| switch (err) {
                    error.NoSuchBucket, error.NoSuchKey, error.NoQuorum => {},
                    else => return err,
                };
            },
            3 => {
                store.deleteBucket(b) catch |err| switch (err) {
                    error.NoSuchBucket, error.BucketNotEmpty, error.NoQuorum => {},
                    else => return err,
                };
            },
            else => unreachable,
        }
    }

    // Heal.
    for (store.replicas) |*r| r.alive = true;

    // One more write to force resync of stragglers.
    store.createBucket("final") catch {};

    const leader_digest = try codec.storeDigest(allocator, &store.replicas[0].store);
    for (store.replicas) |*r| {
        const d = try codec.storeDigest(allocator, &r.store);
        try std.testing.expect(std.mem.eql(u8, &leader_digest, &d));
    }
}
