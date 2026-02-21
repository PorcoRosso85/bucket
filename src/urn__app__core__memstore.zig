const std = @import("std");

/// In-memory object-store state.
///
/// This is the *in-process* representation of buckets/objects.
///
/// IMPORTANT:
/// - This type on its own is not a durable store.
/// - Production code should wrap it with a durability layer (WAL/snapshot)
///   and/or a replication layer.
///
/// The S3 handler expects the methods on this struct.
pub const Store = struct {
    allocator: std.mem.Allocator,
    buckets: std.StringHashMap(Bucket),

    pub fn init(allocator: std.mem.Allocator) Store {
        return .{
            .allocator = allocator,
            .buckets = std.StringHashMap(Bucket).init(allocator),
        };
    }

    pub fn deinit(self: *Store) void {
        var it = self.buckets.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
            self.allocator.free(@constCast(entry.key_ptr.*));
        }
        self.buckets.deinit();
    }

    pub fn createBucket(self: *Store, name: []const u8) !void {
        if (self.buckets.contains(name)) return error.BucketAlreadyExists;
        const key = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(key);
        try self.buckets.put(key, Bucket.init(self.allocator));
    }

    pub fn deleteBucket(self: *Store, name: []const u8) !void {
        const b = self.buckets.getPtr(name) orelse return error.NoSuchBucket;
        if (b.objects.count() != 0) return error.BucketNotEmpty;

        const kv = self.buckets.fetchRemove(name) orelse return error.NoSuchBucket;
        var bucket_val = kv.value;
        bucket_val.deinit(self.allocator);
        self.allocator.free(@constCast(kv.key));
    }

    pub fn listBucketNames(self: *Store, allocator: std.mem.Allocator) ![]const []const u8 {
        // Caller owns the outer slice; strings are owned by store.
        var names: std.ArrayList([]const u8) = .empty;
        errdefer names.deinit(allocator);

        var it = self.buckets.iterator();
        while (it.next()) |entry| {
            try names.append(allocator, entry.key_ptr.*);
        }
        return names.toOwnedSlice(allocator);
    }

    pub fn putObject(self: *Store, bucket: []const u8, key: []const u8, data: []const u8) !void {
        var b = self.buckets.getPtr(bucket) orelse return error.NoSuchBucket;
        try b.put(self.allocator, key, data);
    }

    pub fn getObject(self: *Store, bucket: []const u8, key: []const u8) ![]const u8 {
        const b = self.buckets.getPtr(bucket) orelse return error.NoSuchBucket;
        return b.get(key) orelse error.NoSuchKey;
    }

    pub fn deleteObject(self: *Store, bucket: []const u8, key: []const u8) !void {
        var b = self.buckets.getPtr(bucket) orelse return error.NoSuchBucket;
        try b.delete(self.allocator, key);
    }

    pub fn listObjectKeys(self: *Store, bucket: []const u8, allocator: std.mem.Allocator) ![]const []const u8 {
        const b = self.buckets.getPtr(bucket) orelse return error.NoSuchBucket;
        return b.listKeys(allocator);
    }
};

const Bucket = struct {
    objects: std.StringHashMap([]u8),

    pub fn init(allocator: std.mem.Allocator) Bucket {
        return .{ .objects = std.StringHashMap([]u8).init(allocator) };
    }

    pub fn deinit(self: *Bucket, allocator: std.mem.Allocator) void {
        var it = self.objects.iterator();
        while (it.next()) |entry| {
            allocator.free(entry.value_ptr.*);
            allocator.free(@constCast(entry.key_ptr.*));
        }
        self.objects.deinit();
    }

    pub fn put(self: *Bucket, allocator: std.mem.Allocator, key: []const u8, data: []const u8) !void {
        if (self.objects.getEntry(key)) |existing| {
            allocator.free(existing.value_ptr.*);
            const data_copy = try allocator.dupe(u8, data);
            existing.value_ptr.* = data_copy;
            return;
        }
        const key_copy = try allocator.dupe(u8, key);
        errdefer allocator.free(key_copy);
        const data_copy = try allocator.dupe(u8, data);
        errdefer allocator.free(data_copy);
        try self.objects.put(key_copy, data_copy);
    }

    pub fn get(self: *Bucket, key: []const u8) ?[]const u8 {
        const v = self.objects.get(key) orelse return null;
        return v;
    }

    pub fn delete(self: *Bucket, allocator: std.mem.Allocator, key: []const u8) !void {
        const kv = self.objects.fetchRemove(key) orelse return error.NoSuchKey;
        allocator.free(kv.value);
        allocator.free(@constCast(kv.key));
    }

    pub fn listKeys(self: *Bucket, allocator: std.mem.Allocator) ![]const []const u8 {
        var keys: std.ArrayList([]const u8) = .empty;
        errdefer keys.deinit(allocator);

        var it = self.objects.iterator();
        while (it.next()) |entry| {
            try keys.append(allocator, entry.key_ptr.*);
        }
        return keys.toOwnedSlice(allocator);
    }
};
