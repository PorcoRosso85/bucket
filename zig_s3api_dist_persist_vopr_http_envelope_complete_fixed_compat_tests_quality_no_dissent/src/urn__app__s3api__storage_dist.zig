const std = @import("std");

const MemStore = @import("urn__app__core__memstore.zig").Store;
const codec = @import("urn__app__dist__codec.zig");

/// A durable, replicated bucket store.
///
/// - External interface: same methods as the in-memory store.
/// - Durability: WAL + commit index persisted per replica.
/// - Replication: leader (replica 0) replicates to N-1 followers and commits
///   when a quorum ACKs.
/// - Resync: if a follower is behind/corrupted, it is snapshotted from the
///   leader's committed state.
///
/// This is intentionally small: the goal is a *development* S3-compatible
/// bucket that is durable and distributed, and whose safety can be stressed
/// via DST (vopr-style) tests.
pub const Store = struct {
    allocator: std.mem.Allocator,
    io: std.Io,

    base_dir: std.Io.Dir,
    owns_base_dir: bool,

    replicas: []Replica,
    leader_id: u8 = 0,

    pub const InitOptions = struct {
        /// Number of replicas. Use 3 for majority quorum.
        replicas: u8 = 3,
        /// Leader id (default 0). For now, leader is static.
        leader_id: u8 = 0,
    };

    pub fn initAtDir(allocator: std.mem.Allocator, io: std.Io, base_dir: std.Io.Dir, opts: InitOptions) !Store {
        if (opts.replicas == 0) return error.BadConfig;
        if (opts.leader_id >= opts.replicas) return error.BadConfig;

        var replicas = try allocator.alloc(Replica, opts.replicas);
        errdefer allocator.free(replicas);

        var i: usize = 0;
        errdefer {
            var j: usize = 0;
            while (j < i) : (j += 1) replicas[j].deinit();
        }

        while (i < replicas.len) : (i += 1) {
            replicas[i] = try Replica.init(allocator, io, base_dir, @intCast(i));
        }

        var s: Store = .{
            .allocator = allocator,
            .io = io,
            .base_dir = base_dir,
            .owns_base_dir = false,
            .replicas = replicas,
            .leader_id = opts.leader_id,
        };

        // Ensure leader pointer is consistent with on-disk commit index.
        try s.ensureLeaderHealthy();
        return s;
    }

    /// Convenience init that opens/creates `base_path` under cwd.
    pub fn init(allocator: std.mem.Allocator, io: std.Io, base_path: []const u8, opts: InitOptions) !Store {
        const cwd = std.Io.Dir.cwd();
        var dir = try cwd.createDirPathOpen(io, base_path, .{});
        errdefer dir.close(io);

        var s = try initAtDir(allocator, io, dir, opts);
        s.owns_base_dir = true;
        return s;
    }

    pub fn deinit(self: *Store) void {
        for (self.replicas) |*r| r.deinit();
        self.allocator.free(self.replicas);
        if (self.owns_base_dir) self.base_dir.close(self.io);
        self.* = undefined;
    }

    fn majority(self: *Store) usize {
        return (self.replicas.len / 2) + 1;
    }

    fn leader(self: *Store) *Replica {
        return &self.replicas[self.leader_id];
    }

    fn ensureLeaderHealthy(self: *Store) !void {
        // In this implementation the leader is static. If the leader is not
        // alive, we cannot make progress (no election/failover).
        if (!self.leader().alive) return error.NoQuorum;
        // If leader has detected corruption, this implementation cannot
        // transparently fail over yet.
        if (self.leader().needs_resync) return error.LeaderCorrupt;
    }

    fn leaderApply(self: *Store, cmd: codec.CmdView) !void {
        try self.ensureLeaderHealthy();

        var leader_rep = self.leader();
        const index: u64 = leader_rep.lastIndex() + 1;

        const cmd_bytes = try codec.encodeCmd(self.allocator, cmd);
        defer self.allocator.free(cmd_bytes);
        const checksum = codec.cmdChecksum(cmd_bytes);

        // Track who successfully appended so we can roll back on NoQuorum.
        var appended = try self.allocator.alloc(bool, self.replicas.len);
        defer self.allocator.free(appended);
        @memset(appended, false);

        // Append to leader (durable).
        _ = try leader_rep.appendEntry(index, checksum, cmd_bytes);
        appended[self.leader_id] = true;

        // Replicate to followers.
        var ack_count: usize = 1; // leader itself

        // Snapshot payload for resync (only used if needed).
        //
        // NOTE: generating a full snapshot is O(data). Keep it lazy so that
        // the common case (healthy quorum) stays O(1) per write.
        var snap_payload_opt: ?[]u8 = null;
        defer if (snap_payload_opt) |p| self.allocator.free(p);

        const getSnapPayload = struct {
            fn f(self_store: *Store, leader_ptr: *Replica, opt: *?[]u8) ![]const u8 {
                if (opt.*) |p| return p;
                const p = try codec.encodeStoreSnapshot(self_store.allocator, &leader_ptr.store);
                opt.* = p;
                return p;
            }
        }.f;

        var fid: usize = 0;
        while (fid < self.replicas.len) : (fid += 1) {
            if (fid == self.leader_id) continue;
            var f = &self.replicas[fid];
            if (!f.alive) continue;

            // First attempt.
            const ok = f.appendEntry(index, checksum, cmd_bytes) catch |err| switch (err) {
                error.NeedsResync, error.BadChecksum => blk2: {
                    // Install leader snapshot and retry append.
                    const snap_payload = getSnapPayload(self, leader_rep, &snap_payload_opt) catch {
                        break :blk2 false;
                    };
                    f.installSnapshot(leader_rep.commit_index, snap_payload) catch {
                        f.alive = false;
                        break :blk2 false;
                    };
                    const retry_ok = f.appendEntry(index, checksum, cmd_bytes) catch {
                        break :blk2 false;
                    };
                    break :blk2 retry_ok;
                },
                else => false,
            };
            if (ok) {
                ack_count += 1;
                appended[fid] = true;
            }
        }

        if (ack_count < self.majority()) {
            // Roll back uncommitted tail so snapshot-based resync remains valid.
            var rid: usize = 0;
            while (rid < self.replicas.len) : (rid += 1) {
                if (!appended[rid]) continue;
                self.replicas[rid].rollbackLast() catch {
                    self.replicas[rid].alive = false;
                };
            }
            return error.NoQuorum;
        }

        // Commit on leader.
        try leader_rep.commitTo(index);

        // Commit on followers best-effort.
        fid = 0;
        while (fid < self.replicas.len) : (fid += 1) {
            if (fid == self.leader_id) continue;
            var f = &self.replicas[fid];
            if (!f.alive) continue;
            f.commitTo(index) catch |err| switch (err) {
                error.NeedsResync, error.BadChecksum, error.CorruptLog => {
                    // Try resync and commit again.
                    const snap_payload = getSnapPayload(self, leader_rep, &snap_payload_opt) catch {
                        f.alive = false;
                        continue;
                    };
                    f.installSnapshot(leader_rep.commit_index, snap_payload) catch {
                        f.alive = false;
                        continue;
                    };
                    f.commitTo(index) catch {
                        f.alive = false;
                        continue;
                    };
                },
                else => {
                    f.alive = false;
                    continue;
                },
            };
        }
    }

    // -----------------------------
    // Store API (used by S3 handler)
    // -----------------------------

    pub fn listBucketNames(self: *Store, allocator: std.mem.Allocator) ![]const []const u8 {
        return self.leader().store.listBucketNames(allocator);
    }

    pub fn createBucket(self: *Store, name: []const u8) !void {
        // Validate against committed state on leader first to keep error
        // semantics stable.
        if (self.leader().store.buckets.contains(name)) return error.BucketAlreadyExists;
        try self.leaderApply(.{ .create_bucket = name });
    }

    pub fn deleteBucket(self: *Store, name: []const u8) !void {
        // Validate using leader state.
        const b = self.leader().store.buckets.getPtr(name) orelse return error.NoSuchBucket;
        if (b.objects.count() != 0) return error.BucketNotEmpty;
        try self.leaderApply(.{ .delete_bucket = name });
    }

    pub fn listObjectKeys(self: *Store, bucket: []const u8, allocator: std.mem.Allocator) ![]const []const u8 {
        return self.leader().store.listObjectKeys(bucket, allocator);
    }

    pub fn putObject(self: *Store, bucket: []const u8, key: []const u8, data: []const u8) !void {
        // Validate bucket exists on leader.
        _ = self.leader().store.buckets.getPtr(bucket) orelse return error.NoSuchBucket;
        try self.leaderApply(.{ .put_object = .{ .bucket = bucket, .key = key, .data = data } });
    }

    pub fn getObject(self: *Store, bucket: []const u8, key: []const u8) ![]const u8 {
        return self.leader().store.getObject(bucket, key);
    }

    pub fn deleteObject(self: *Store, bucket: []const u8, key: []const u8) !void {
        // Validate bucket exists on leader.
        const b = self.leader().store.buckets.getPtr(bucket) orelse return error.NoSuchBucket;
        // S3-like semantics: DeleteObject is idempotent. If the key does not
        // exist, treat it as success (204).
        if (!b.objects.contains(key)) return;
        try self.leaderApply(.{ .delete_object = .{ .bucket = bucket, .key = key } });
    }
};

const EntryMeta = struct {
    index: u64,
    cmd_offset: u64,
    cmd_len: u32,
    checksum: u64,
};

const Replica = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    id: u8,

    dir: std.Io.Dir,
    store: MemStore,

    // Persistent files
    wal: std.Io.File,
    wal_size: u64,

    // In-memory index into WAL
    entries: std.ArrayListUnmanaged(EntryMeta) = .{},

    snapshot_index: u64 = 0,
    commit_index: u64 = 0,
    applied_index: u64 = 0,

    alive: bool = true,
    needs_resync: bool = false,

    fn init(allocator: std.mem.Allocator, io: std.Io, base_dir: std.Io.Dir, id: u8) !Replica {
        var name_buf: [16]u8 = undefined;
        const sub = try std.fmt.bufPrint(&name_buf, "node{d}", .{id});

        var dir = try base_dir.createDirPathOpen(io, sub, .{});
        errdefer dir.close(io);

        // Open or create WAL (no truncate).
        var wal = try dir.createFile(io, "wal.bin", .{ .read = true, .truncate = false });
        errdefer wal.close(io);

        const wal_stat = try wal.stat(io);
        const wal_size: u64 = wal_stat.size;

        var r: Replica = .{
            .allocator = allocator,
            .io = io,
            .id = id,
            .dir = dir,
            .store = MemStore.init(allocator),
            .wal = wal,
            .wal_size = wal_size,
        };

        // Load snapshot + WAL + commit index.
        //
        // If a follower is corrupted, we prefer to mark it as needing resync
        // instead of failing the entire store initialization.
        r.loadSnapshot() catch {
            r.needs_resync = true;
        };
        r.loadCommitIndex() catch {
            r.needs_resync = true;
        };
        r.indexWal() catch {
            r.needs_resync = true;
        };
        r.applyCommitted() catch {
            r.needs_resync = true;
        };
        return r;
    }

    fn deinit(self: *Replica) void {
        self.store.deinit();
        self.entries.deinit(self.allocator);
        self.wal.close(self.io);
        self.dir.close(self.io);
        self.* = undefined;
    }

    fn lastIndex(self: *const Replica) u64 {
        return self.snapshot_index + @as(u64, @intCast(self.entries.items.len));
    }

    fn loadSnapshot(self: *Replica) !void {
        const snap = self.dir.openFile(self.io, "snapshot.bin", .{ .mode = .read_only }) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };
        defer snap.close(self.io);

        const st = try snap.stat(self.io);
        if (st.size < 8) return error.CorruptSnapshot;
        const size = @as(usize, @intCast(st.size));
        const buf = try self.allocator.alloc(u8, size);
        defer self.allocator.free(buf);
        const nread = try snap.readPositionalAll(self.io, buf, 0);
        if (nread != size) return error.CorruptSnapshot;

        const snap_index = std.mem.readInt(u64, buf[0..8], .big);
        const payload = buf[8..];
        const new_store = try codec.decodeStoreSnapshot(payload, self.allocator);
        self.store.deinit();
        self.store = new_store;
        self.snapshot_index = snap_index;
        self.applied_index = snap_index;
    }

    fn loadCommitIndex(self: *Replica) !void {
        const f = self.dir.openFile(self.io, "commit.bin", .{ .mode = .read_only }) catch |err| switch (err) {
            error.FileNotFound => {
                self.commit_index = self.snapshot_index;
                return;
            },
            else => return err,
        };
        defer f.close(self.io);

        const st = try f.stat(self.io);
        if (st.size < 8) return error.CorruptCommit;
        var buf: [8]u8 = undefined;
        const n = try f.readPositionalAll(self.io, &buf, 0);
        if (n != 8) return error.CorruptCommit;
        const cidx = std.mem.readInt(u64, &buf, .big);
        self.commit_index = cidx;
        if (self.commit_index < self.snapshot_index) return error.CorruptCommit;
    }

    fn persistCommitIndex(self: *Replica, cidx: u64) !void {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, cidx, .big);

        // Atomic-ish: write commit.tmp then rename.
        self.dir.writeFile(self.io, .{ .sub_path = "commit.tmp", .data = buf[0..], .flags = .{ .truncate = true } }) catch |err| return err;
        try std.Io.Dir.rename(self.dir, "commit.tmp", self.dir, "commit.bin", self.io);
    }

    fn indexWal(self: *Replica) !void {
        self.entries.clearRetainingCapacity();

        var offset: u64 = 0;
        while (true) {
            if (offset + 20 > self.wal_size) break;

            var hdr: [20]u8 = undefined;
            const n = try self.wal.readPositionalAll(self.io, &hdr, offset);
            if (n != hdr.len) break;
            const index = std.mem.readInt(u64, hdr[0..8], .big);
            const checksum = std.mem.readInt(u64, hdr[8..16], .big);
            const len = std.mem.readInt(u32, hdr[16..20], .big);
            const cmd_off = offset + 20;
            const end = cmd_off + @as(u64, len);
            if (end > self.wal_size) break;

            // Basic structural sanity: indices must be contiguous starting at snapshot_index+1.
            const expected = self.snapshot_index + 1 + @as(u64, @intCast(self.entries.items.len));
            if (index != expected) {
                self.needs_resync = true;
                return;
            }

            try self.entries.append(self.allocator, .{ .index = index, .cmd_offset = cmd_off, .cmd_len = len, .checksum = checksum });
            offset = end;
        }
    }

    fn applyCommitted(self: *Replica) !void {
        if (self.needs_resync) return;
        if (self.commit_index < self.snapshot_index) {
            self.needs_resync = true;
            return;
        }
        const max_idx = self.lastIndex();
        if (self.commit_index > max_idx) {
            // Commit index points beyond log.
            self.needs_resync = true;
            return;
        }

        var idx: u64 = self.applied_index + 1;
        while (idx <= self.commit_index) : (idx += 1) {
            try self.applyEntry(idx);
            self.applied_index = idx;
        }
    }

    fn applyEntry(self: *Replica, index: u64) !void {
        if (index <= self.snapshot_index) return; // already in snapshot
        const pos = @as(usize, @intCast(index - self.snapshot_index - 1));
        if (pos >= self.entries.items.len) return error.CorruptLog;
        const e = self.entries.items[pos];

        const cmd = try self.allocator.alloc(u8, e.cmd_len);
        defer self.allocator.free(cmd);
        const n = try self.wal.readPositionalAll(self.io, cmd, e.cmd_offset);
        if (n != e.cmd_len) return error.CorruptLog;
        if (codec.cmdChecksum(cmd) != e.checksum) return error.BadChecksum;

        const view = try codec.decodeCmd(cmd);
        switch (view) {
            .create_bucket => |b| {
                self.store.createBucket(b) catch |err| switch (err) {
                    error.BucketAlreadyExists => {},
                    else => return err,
                };
            },
            .delete_bucket => |b| {
                self.store.deleteBucket(b) catch |err| return err;
            },
            .put_object => |p| {
                try self.store.putObject(p.bucket, p.key, p.data);
            },
            .delete_object => |p| {
                try self.store.deleteObject(p.bucket, p.key);
            },
        }
    }

    /// Append an entry to this replica's WAL.
    ///
    /// This does not advance commit/apply.
    fn appendEntry(self: *Replica, index: u64, checksum: u64, cmd_bytes: []const u8) !bool {
        if (!self.alive) return false;
        if (self.needs_resync) return error.NeedsResync;

        const expected = self.lastIndex() + 1;
        if (index != expected) return error.NeedsResync;
        if (codec.cmdChecksum(cmd_bytes) != checksum) return error.BadChecksum;

        var hdr: [20]u8 = undefined;
        std.mem.writeInt(u64, hdr[0..8], index, .big);
        std.mem.writeInt(u64, hdr[8..16], checksum, .big);
        std.mem.writeInt(u32, hdr[16..20], @intCast(cmd_bytes.len), .big);

        try self.wal.writePositionalAll(self.io, &hdr, self.wal_size);
        try self.wal.writePositionalAll(self.io, cmd_bytes, self.wal_size + hdr.len);
        self.wal_size += hdr.len + cmd_bytes.len;
        try self.wal.sync(self.io);

        try self.entries.append(self.allocator, .{ .index = index, .cmd_offset = self.wal_size - cmd_bytes.len, .cmd_len = @intCast(cmd_bytes.len), .checksum = checksum });
        return true;
    }

    /// Remove the last (uncommitted) WAL entry.
    ///
    /// Used when a proposal failed to reach quorum.
    fn rollbackLast(self: *Replica) !void {
        if (!self.alive) return;
        if (self.entries.items.len == 0) return;

        const last = self.entries.items[self.entries.items.len - 1];
        // Our WAL record layout is: [index:u64][checksum:u64][len:u32][cmd_bytes...]
        // cmd_offset points to cmd_bytes start.
        const record_start = last.cmd_offset - 20;
        if (record_start > self.wal_size) return error.CorruptLog;
        if (last.index <= self.commit_index) return error.CorruptLog;

        try self.wal.setLength(self.io, record_start);
        self.wal_size = record_start;
        _ = self.entries.pop();
        try self.wal.sync(self.io);
    }

    /// Commit and apply through `index`.
    fn commitTo(self: *Replica, index: u64) !void {
        if (!self.alive) return;
        if (self.needs_resync) return error.NeedsResync;

        if (index <= self.commit_index) return;
        if (index > self.lastIndex()) return error.NeedsResync;

        // Persist commit index before applying. If apply fails, treat it as corruption.
        try self.persistCommitIndex(index);
        self.commit_index = index;
        self.applyCommitted() catch |err| {
            self.needs_resync = true;
            return err;
        };
    }

    /// Replace this replica's state with a leader snapshot of committed state.
    fn installSnapshot(self: *Replica, snap_index: u64, payload: []const u8) !void {
        if (!self.alive) return;

        // Decode first (may fail) before mutating disk.
        const new_store = try codec.decodeStoreSnapshot(payload, self.allocator);
        errdefer {
            // `MemStore.deinit` requires a mutable pointer; free via a copy.
            var tmp_store = new_store;
            tmp_store.deinit();
        }

        // Write snapshot.bin = [snap_index:u64][payload...]
        var tmp = std.ArrayList(u8).empty;
        defer tmp.deinit(self.allocator);
        var hdr: [8]u8 = undefined;
        std.mem.writeInt(u64, &hdr, snap_index, .big);
        try tmp.appendSlice(self.allocator, &hdr);
        try tmp.appendSlice(self.allocator, payload);
        self.dir.writeFile(self.io, .{ .sub_path = "snapshot.tmp", .data = tmp.items, .flags = .{ .truncate = true } }) catch |err| return err;
        try std.Io.Dir.rename(self.dir, "snapshot.tmp", self.dir, "snapshot.bin", self.io);

        // Reset WAL.
        try self.wal.setLength(self.io, 0);
        self.wal_size = 0;
        self.entries.clearRetainingCapacity();

        // Persist commit.
        try self.persistCommitIndex(snap_index);

        // Replace in-memory store.
        self.store.deinit();
        self.store = new_store;

        self.snapshot_index = snap_index;
        self.commit_index = snap_index;
        self.applied_index = snap_index;
        self.needs_resync = false;
    }
};
