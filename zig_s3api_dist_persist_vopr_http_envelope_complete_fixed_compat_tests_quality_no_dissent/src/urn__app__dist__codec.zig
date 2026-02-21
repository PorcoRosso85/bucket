const std = @import("std");
const MemStore = @import("urn__app__core__memstore.zig").Store;

// -----------------------------
// Command encoding/decoding
// -----------------------------

pub const CmdTag = enum(u8) {
    create_bucket = 1,
    delete_bucket = 2,
    put_object = 3,
    delete_object = 4,
};

pub const CmdView = union(CmdTag) {
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

pub fn cmdChecksum(buf: []const u8) u64 {
    // Not cryptographic; used for corruption detection.
    return std.hash.Wyhash.hash(0, buf);
}

// -----------------------------
// Store snapshot encoding
// -----------------------------

// Format:
// [bucket_count:u16]
//   repeat bucket_count:
//     [bucket_len:u16][bucket_bytes]
//     [obj_count:u16]
//       repeat obj_count:
//         [key_len:u16][key_bytes]
//         [data_len:u32][data_bytes]
pub fn encodeStoreSnapshot(allocator: std.mem.Allocator, store: *MemStore) ![]u8 {
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

pub fn decodeStoreSnapshot(payload: []const u8, allocator: std.mem.Allocator) !MemStore {
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

/// Deterministic digest of the current object-store state.
///
/// Used by DST/oracles to assert that replicas converged.
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
