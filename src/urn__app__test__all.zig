const std = @import("std");
const handler = @import("urn__app__s3api__handler.zig");
const Store = @import("urn__app__s3api__storage_dist.zig").Store;

fn req(method: []const u8, target: []const u8, body: []const u8) handler.Request {
    return .{ .method = method, .target = target, .body = body };
}

test "S3 ListBuckets returns XML root" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store = try Store.initAtDir(allocator, std.testing.io, tmp.dir, .{ .replicas = 3, .leader_id = 0 });
    defer store.deinit();

    const resp = try handler.handle(allocator, &store, req("GET", "/", ""));
    defer resp.deinit(allocator);

    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expect(std.mem.indexOf(u8, resp.body, "ListAllMyBucketsResult") != null);
}

test "S3 CreateBucket then ListBuckets includes bucket name" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store = try Store.initAtDir(allocator, std.testing.io, tmp.dir, .{ .replicas = 3, .leader_id = 0 });
    defer store.deinit();

    const r1 = try handler.handle(allocator, &store, req("PUT", "/mybucket", ""));
    defer r1.deinit(allocator);
    try std.testing.expectEqual(@as(u16, 200), r1.status);

    const r2 = try handler.handle(allocator, &store, req("GET", "/", ""));
    defer r2.deinit(allocator);
    try std.testing.expect(std.mem.indexOf(u8, r2.body, "<Name>mybucket</Name>") != null);
}

test "S3 PutObject then GetObject returns bytes" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store = try Store.initAtDir(allocator, std.testing.io, tmp.dir, .{ .replicas = 3, .leader_id = 0 });
    defer store.deinit();

    const mk = try handler.handle(allocator, &store, req("PUT", "/mybucket", ""));
    defer mk.deinit(allocator);

    const put = try handler.handle(allocator, &store, req("PUT", "/mybucket/hello.txt", "hi"));
    defer put.deinit(allocator);
    try std.testing.expectEqual(@as(u16, 200), put.status);

    const get = try handler.handle(allocator, &store, req("GET", "/mybucket/hello.txt", ""));
    defer get.deinit(allocator);
    try std.testing.expectEqual(@as(u16, 200), get.status);
    try std.testing.expectEqualStrings("hi", get.body);
}

test "S3 ListObjectsV2 includes object key" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store = try Store.initAtDir(allocator, std.testing.io, tmp.dir, .{ .replicas = 3, .leader_id = 0 });
    defer store.deinit();

    const mk = try handler.handle(allocator, &store, req("PUT", "/mybucket", ""));
    defer mk.deinit(allocator);

    const put = try handler.handle(allocator, &store, req("PUT", "/mybucket/hello.txt", "hi"));
    defer put.deinit(allocator);

    const list = try handler.handle(allocator, &store, req("GET", "/mybucket?list-type=2", ""));
    defer list.deinit(allocator);
    try std.testing.expectEqual(@as(u16, 200), list.status);
    try std.testing.expect(std.mem.indexOf(u8, list.body, "<Key>hello.txt</Key>") != null);
}

test "S3 DeleteBucket fails when not empty, then succeeds after object delete" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store = try Store.initAtDir(allocator, std.testing.io, tmp.dir, .{ .replicas = 3, .leader_id = 0 });
    defer store.deinit();

    const mk = try handler.handle(allocator, &store, req("PUT", "/mybucket", ""));
    defer mk.deinit(allocator);

    const put = try handler.handle(allocator, &store, req("PUT", "/mybucket/hello.txt", "hi"));
    defer put.deinit(allocator);

    const del_bucket_fail = try handler.handle(allocator, &store, req("DELETE", "/mybucket", ""));
    defer del_bucket_fail.deinit(allocator);
    try std.testing.expectEqual(@as(u16, 409), del_bucket_fail.status);
    try std.testing.expect(std.mem.indexOf(u8, del_bucket_fail.body, "BucketNotEmpty") != null);

    const del_obj = try handler.handle(allocator, &store, req("DELETE", "/mybucket/hello.txt", ""));
    defer del_obj.deinit(allocator);
    try std.testing.expectEqual(@as(u16, 204), del_obj.status);

    const del_bucket_ok = try handler.handle(allocator, &store, req("DELETE", "/mybucket", ""));
    defer del_bucket_ok.deinit(allocator);
    try std.testing.expectEqual(@as(u16, 204), del_bucket_ok.status);
}

test "S3 Missing bucket returns NoSuchBucket" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store = try Store.initAtDir(allocator, std.testing.io, tmp.dir, .{ .replicas = 3, .leader_id = 0 });
    defer store.deinit();

    const r = try handler.handle(allocator, &store, req("GET", "/missing?list-type=2", ""));
    defer r.deinit(allocator);
    try std.testing.expectEqual(@as(u16, 404), r.status);
    try std.testing.expect(std.mem.indexOf(u8, r.body, "NoSuchBucket") != null);
}

test "S3 Missing key returns NoSuchKey" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store = try Store.initAtDir(allocator, std.testing.io, tmp.dir, .{ .replicas = 3, .leader_id = 0 });
    defer store.deinit();

    const mk = try handler.handle(allocator, &store, req("PUT", "/mybucket", ""));
    defer mk.deinit(allocator);

    const r = try handler.handle(allocator, &store, req("GET", "/mybucket/missing.txt", ""));
    defer r.deinit(allocator);
    try std.testing.expectEqual(@as(u16, 404), r.status);
    try std.testing.expect(std.mem.indexOf(u8, r.body, "NoSuchKey") != null);
}

test "S3 HEAD returns correct content length (body is elided at HTTP layer)" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store = try Store.initAtDir(allocator, std.testing.io, tmp.dir, .{ .replicas = 3, .leader_id = 0 });
    defer store.deinit();

    const mk = try handler.handle(allocator, &store, req("PUT", "/mybucket", ""));
    defer mk.deinit(allocator);
    const put = try handler.handle(allocator, &store, req("PUT", "/mybucket/key", "hello"));
    defer put.deinit(allocator);

    const head = try handler.handle(allocator, &store, req("HEAD", "/mybucket/key", ""));
    defer head.deinit(allocator);
    try std.testing.expectEqual(@as(u16, 200), head.status);
    try std.testing.expectEqual(@as(usize, 5), head.body.len);
}

test "S3 DELETE object is idempotent (missing key => 204)" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store = try Store.initAtDir(allocator, std.testing.io, tmp.dir, .{ .replicas = 3, .leader_id = 0 });
    defer store.deinit();

    const mk = try handler.handle(allocator, &store, req("PUT", "/mybucket", ""));
    defer mk.deinit(allocator);
    const put = try handler.handle(allocator, &store, req("PUT", "/mybucket/key", "hi"));
    defer put.deinit(allocator);

    const del1 = try handler.handle(allocator, &store, req("DELETE", "/mybucket/key", ""));
    defer del1.deinit(allocator);
    try std.testing.expectEqual(@as(u16, 204), del1.status);

    const del2 = try handler.handle(allocator, &store, req("DELETE", "/mybucket/key", ""));
    defer del2.deinit(allocator);
    try std.testing.expectEqual(@as(u16, 204), del2.status);
}

test "S3 percent-decoding is applied to object keys" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store = try Store.initAtDir(allocator, std.testing.io, tmp.dir, .{ .replicas = 3, .leader_id = 0 });
    defer store.deinit();

    const mk = try handler.handle(allocator, &store, req("PUT", "/mybucket", ""));
    defer mk.deinit(allocator);
    const put = try handler.handle(allocator, &store, req("PUT", "/mybucket/space%20key", "x"));
    defer put.deinit(allocator);

    const list = try handler.handle(allocator, &store, req("GET", "/mybucket?list-type=2", ""));
    defer list.deinit(allocator);
    try std.testing.expectEqual(@as(u16, 200), list.status);
    try std.testing.expect(std.mem.indexOf(u8, list.body, "<Key>space key</Key>") != null);
}

test "S3 percent-decoding includes %2F (slash inside key)" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store = try Store.initAtDir(allocator, std.testing.io, tmp.dir, .{ .replicas = 3, .leader_id = 0 });
    defer store.deinit();

    const mk = try handler.handle(allocator, &store, req("PUT", "/mybucket", ""));
    defer mk.deinit(allocator);

    // Store a key that contains an encoded slash.
    const put = try handler.handle(allocator, &store, req("PUT", "/mybucket/a%2Fb", "x"));
    defer put.deinit(allocator);
    try std.testing.expectEqual(@as(u16, 200), put.status);

    // List returns the decoded key.
    const list = try handler.handle(allocator, &store, req("GET", "/mybucket?list-type=2", ""));
    defer list.deinit(allocator);
    try std.testing.expectEqual(@as(u16, 200), list.status);
    try std.testing.expect(std.mem.indexOf(u8, list.body, "<Key>a/b</Key>") != null);

    // GET with encoded path retrieves the same object.
    const get = try handler.handle(allocator, &store, req("GET", "/mybucket/a%2Fb", ""));
    defer get.deinit(allocator);
    try std.testing.expectEqual(@as(u16, 200), get.status);
    try std.testing.expectEqualStrings("x", get.body);
}

fn expectBucketCreateOk(name: []const u8, ok: bool) !void {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store = try Store.initAtDir(allocator, std.testing.io, tmp.dir, .{ .replicas = 3, .leader_id = 0 });
    defer store.deinit();

    const target = try std.fmt.allocPrint(allocator, "/{s}", .{name});
    defer allocator.free(target);

    const r = try handler.handle(allocator, &store, req("PUT", target, ""));
    defer r.deinit(allocator);
    if (ok) {
        try std.testing.expectEqual(@as(u16, 200), r.status);
    } else {
        try std.testing.expectEqual(@as(u16, 400), r.status);
        try std.testing.expect(std.mem.indexOf(u8, r.body, "InvalidBucketName") != null);
    }
}

test "S3 bucket name validation boundary cases" {
    // Length boundaries.
    try expectBucketCreateOk("ab", false); // too short
    try expectBucketCreateOk("abc", true); // min

    var name63: [63]u8 = undefined;
    @memset(&name63, 'a');
    try expectBucketCreateOk(name63[0..], true);

    var name64: [64]u8 = undefined;
    @memset(&name64, 'a');
    try expectBucketCreateOk(name64[0..], false);

    // Character set / shape.
    try expectBucketCreateOk("MyBucket", false); // uppercase
    try expectBucketCreateOk("a_b", false); // underscore
    try expectBucketCreateOk("-abc", false); // must start with alnum
    try expectBucketCreateOk("abc-", false); // must end with alnum
    try expectBucketCreateOk("a..b", false); // no consecutive dots

    // IPv4-like names are rejected to avoid TLS/cert & migration surprises.
    try expectBucketCreateOk("192.168.0.1", false);

    // Some typical valid shapes.
    try expectBucketCreateOk("a-b", true);
    try expectBucketCreateOk("a.b", true);
    try expectBucketCreateOk("abc123", true);
}

test "S3 ListObjectsV2 order is lexicographic" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store = try Store.initAtDir(allocator, std.testing.io, tmp.dir, .{ .replicas = 3, .leader_id = 0 });
    defer store.deinit();

    const mk = try handler.handle(allocator, &store, req("PUT", "/mybucket", ""));
    defer mk.deinit(allocator);
    const put_z = try handler.handle(allocator, &store, req("PUT", "/mybucket/z", "1"));
    defer put_z.deinit(allocator);
    const put_a = try handler.handle(allocator, &store, req("PUT", "/mybucket/a", "2"));
    defer put_a.deinit(allocator);

    const list = try handler.handle(allocator, &store, req("GET", "/mybucket?list-type=2", ""));
    defer list.deinit(allocator);
    const i_a = std.mem.indexOf(u8, list.body, "<Key>a</Key>") orelse return error.TestExpectedEqual;
    const i_z = std.mem.indexOf(u8, list.body, "<Key>z</Key>") orelse return error.TestExpectedEqual;
    try std.testing.expect(i_a < i_z);
}

test "S3 invalid bucket name is rejected" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store = try Store.initAtDir(allocator, std.testing.io, tmp.dir, .{ .replicas = 3, .leader_id = 0 });
    defer store.deinit();

    const bad = try handler.handle(allocator, &store, req("PUT", "/MyBucket", ""));
    defer bad.deinit(allocator);
    try std.testing.expectEqual(@as(u16, 400), bad.status);
    try std.testing.expect(std.mem.indexOf(u8, bad.body, "InvalidBucketName") != null);
}

test "Durability: restart preserves committed state" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var store = try Store.initAtDir(allocator, std.testing.io, tmp.dir, .{ .replicas = 3, .leader_id = 0 });
        defer store.deinit();

        const r1 = try handler.handle(allocator, &store, req("PUT", "/mybucket", ""));
        defer r1.deinit(allocator);
        const r2 = try handler.handle(allocator, &store, req("PUT", "/mybucket/hello.txt", "hi"));
        defer r2.deinit(allocator);
    }

    // Re-open from the same data-dir.
    {
        var store2 = try Store.initAtDir(allocator, std.testing.io, tmp.dir, .{ .replicas = 3, .leader_id = 0 });
        defer store2.deinit();

        const get = try handler.handle(allocator, &store2, req("GET", "/mybucket/hello.txt", ""));
        defer get.deinit(allocator);
        try std.testing.expectEqual(@as(u16, 200), get.status);
        try std.testing.expectEqualStrings("hi", get.body);
    }
}

// Distributed simulation tests (vopr-style)
// Ensure the distributed VOPR tests are compiled and executed as part of
// `zig build test`.
//
// Zig's lazy analysis means a plain unused `const _ = @import(...)` can be
// optimized away; anchoring the import inside a `test` block forces inclusion.
test "dist_vopr (property tests)" {
    _ = @import("urn__app__test__dist_vopr.zig");
}

// HTTP end-to-end smoke tests
test "http_e2e (smoke)" {
    _ = @import("urn__app__test__http_e2e.zig");
}

// vopr-style DST that targets the *production* durable replicated store.
test "store_dist_vopr (property tests)" {
    _ = @import("urn__app__test__store_dist_vopr.zig");
}

// Process-level blackbox tests (spawn the built `s3gw` binary)
test "process_blackbox (s3gw binary)" {
    _ = @import("urn__app__test__process_blackbox.zig");
}
