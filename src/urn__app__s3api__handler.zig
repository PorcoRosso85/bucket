const std = @import("std");
const types = @import("urn__app__s3api__types.zig");
const xml = @import("urn__app__s3api__xml.zig");

pub const Request = struct {
    method: []const u8,
    target: []const u8,
    body: []const u8,
};

const Target = struct {
    path: []const u8,
    query: []const u8,
};

const Route = struct {
    bucket: ?[]const u8,
    key: ?[]const u8,
};

const Decoded = struct {
    bytes: []const u8,
    owned: bool,

    fn deinit(self: Decoded, allocator: std.mem.Allocator) void {
        if (self.owned) allocator.free(@constCast(self.bytes));
    }
};

pub fn handle(allocator: std.mem.Allocator, store: anytype, req: Request) !types.Response {
    const t = splitTarget(req.target);
    const r = parsePath(t.path);

    if (isMethod(req.method, "GET") and r.bucket == null) {
        // GET /
        const names = try store.listBucketNames(allocator);
        defer allocator.free(names);

        // Deterministic/S3-like order (lexicographic).
        std.sort.heap([]const u8, @constCast(names), {}, struct {
            fn lt(_: void, a: []const u8, b: []const u8) bool {
                return std.mem.lessThan(u8, a, b);
            }
        }.lt);

        const body = try xml.listBucketsXml(allocator, names);
        errdefer allocator.free(body);
        return responseWithContentType(allocator, 200, "application/xml", body);
    }

    if (r.bucket) |bucket_raw| {
        const bucket_dec = maybePercentDecode(allocator, bucket_raw) catch {
            return s3Error(allocator, 400, "InvalidRequest", "Invalid percent-encoding in path", t.path);
        };
        defer bucket_dec.deinit(allocator);
        const bucket = bucket_dec.bytes;

        if (!isValidBucketName(bucket)) {
            return s3Error(allocator, 400, "InvalidBucketName", "The specified bucket is not valid", t.path);
        }

        if (r.key == null) {
            // Bucket-level operations
            if (isMethod(req.method, "PUT")) {
                store.createBucket(bucket) catch |err| {
                    return mapStoreError(allocator, err, t.path);
                };
                const body = try allocator.alloc(u8, 0);
                return responseWithContentType(allocator, 200, "application/xml", body);
            }
            if (isMethod(req.method, "DELETE")) {
                store.deleteBucket(bucket) catch |err| {
                    return mapStoreError(allocator, err, t.path);
                };
                const body = try allocator.alloc(u8, 0);
                return responseWithContentType(allocator, 204, "application/xml", body);
            }
            if (isMethod(req.method, "GET") and queryHas(t.query, "list-type", "2")) {
                const keys = store.listObjectKeys(bucket, allocator) catch |err| {
                    return mapStoreError(allocator, err, t.path);
                };
                defer allocator.free(keys);

                // S3-like behavior: return keys in lexicographic order.
                std.sort.heap([]const u8, @constCast(keys), {}, struct {
                    fn lt(_: void, a: []const u8, b: []const u8) bool {
                        return std.mem.lessThan(u8, a, b);
                    }
                }.lt);

                const body = try xml.listObjectsV2Xml(allocator, bucket, keys);
                errdefer allocator.free(body);
                return responseWithContentType(allocator, 200, "application/xml", body);
            }
        } else if (r.key) |key_raw| {
            const key_dec = maybePercentDecode(allocator, key_raw) catch {
                return s3Error(allocator, 400, "InvalidRequest", "Invalid percent-encoding in path", t.path);
            };
            defer key_dec.deinit(allocator);
            const key = key_dec.bytes;

            // Object-level operations
            if (isMethod(req.method, "PUT")) {
                store.putObject(bucket, key, req.body) catch |err| {
                    return mapStoreError(allocator, err, t.path);
                };
                const body = try allocator.alloc(u8, 0);
                return responseWithContentType(allocator, 200, "application/xml", body);
            }
            if (isMethod(req.method, "GET")) {
                const data = store.getObject(bucket, key) catch |err| {
                    return mapStoreError(allocator, err, t.path);
                };
                const body = try allocator.dupe(u8, data);
                errdefer allocator.free(body);
                return responseWithContentType(allocator, 200, "application/octet-stream", body);
            }
            if (isMethod(req.method, "HEAD")) {
                const data = store.getObject(bucket, key) catch |err| {
                    return mapStoreError(allocator, err, t.path);
                };
                // HEAD should return the same headers as GET would, including
                // Content-Length, but without a body.
                //
                // Zig std.http.Server.respond() uses `content.len` to emit
                // `content-length:` and automatically elides the body for HEAD
                // requests. So we return a dummy slice of the right length.
                const body = try allocator.alloc(u8, data.len);
                return responseWithContentType(allocator, 200, "application/octet-stream", body);
            }
            if (isMethod(req.method, "DELETE")) {
                store.deleteObject(bucket, key) catch |err| {
                    return mapStoreError(allocator, err, t.path);
                };
                const body = try allocator.alloc(u8, 0);
                return responseWithContentType(allocator, 204, "application/xml", body);
            }
        }
    }

    return s3Error(allocator, 400, "InvalidRequest", "Unsupported request", t.path);
}

fn responseWithContentType(
    allocator: std.mem.Allocator,
    status: u16,
    content_type: []const u8,
    body: []u8,
) !types.Response {
    var headers: std.ArrayList(types.Header) = .empty;
    errdefer headers.deinit(allocator);

    try headers.append(allocator, try types.header(allocator, "Content-Type", content_type));

    return .{
        .status = status,
        .headers = try headers.toOwnedSlice(allocator),
        .body = body,
    };
}

fn splitTarget(target: []const u8) Target {
    const i = std.mem.indexOfScalar(u8, target, '?') orelse return .{ .path = target, .query = "" };
    return .{ .path = target[0..i], .query = target[i + 1 ..] };
}

fn parsePath(path: []const u8) Route {
    if (std.mem.eql(u8, path, "/") or path.len == 0) return .{ .bucket = null, .key = null };

    var p = path;
    if (p[0] == '/') p = p[1..];
    if (p.len == 0) return .{ .bucket = null, .key = null };

    const slash = std.mem.indexOfScalar(u8, p, '/');
    if (slash) |idx| {
        const bucket = p[0..idx];
        const key = p[idx + 1 ..];
        if (key.len == 0) return .{ .bucket = bucket, .key = null };
        return .{ .bucket = bucket, .key = key };
    }
    return .{ .bucket = p, .key = null };
}

fn isMethod(method: []const u8, expected: []const u8) bool {
    return std.mem.eql(u8, method, expected);
}

fn maybePercentDecode(allocator: std.mem.Allocator, s: []const u8) !Decoded {
    // Fast path: common case has no percent-escapes.
    if (std.mem.indexOfScalar(u8, s, '%') == null) return .{ .bytes = s, .owned = false };

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.ensureTotalCapacity(allocator, s.len);

    var i: usize = 0;
    while (i < s.len) : (i += 1) {
        const c = s[i];
        if (c == '%') {
            if (i + 2 >= s.len) return error.BadRequest;
            const hi = try fromHex(s[i + 1]);
            const lo = try fromHex(s[i + 2]);
            try out.append(allocator, (hi << 4) | lo);
            i += 2;
            continue;
        }
        // NOTE: do not treat '+' specially; that's for query encoding, not path.
        try out.append(allocator, c);
    }

    return .{ .bytes = try out.toOwnedSlice(allocator), .owned = true };
}

fn fromHex(c: u8) !u8 {
    return switch (c) {
        '0'...'9' => c - '0',
        'a'...'f' => 10 + (c - 'a'),
        'A'...'F' => 10 + (c - 'A'),
        else => error.BadRequest,
    };
}

fn isValidBucketName(name: []const u8) bool {
    // A small, S3-aligned subset of the AWS rules.
    // This is meant to prevent "works here but fails on real S3" surprises.
    if (name.len < 3 or name.len > 63) return false;
    // Must start/end with a letter or digit.
    if (!isLowerAlphaNum(name[0])) return false;
    if (!isLowerAlphaNum(name[name.len - 1])) return false;

    var dot_run: usize = 0;
    var all_digits_or_dots = true;

    for (name) |c| {
        if (c >= 'a' and c <= 'z') {
            dot_run = 0;
            all_digits_or_dots = false; // letters break the IP-like pattern
            continue;
        }
        if (c >= '0' and c <= '9') {
            dot_run = 0;
            continue;
        }
        if (c == '-') {
            dot_run = 0;
            all_digits_or_dots = false;
            continue;
        }
        if (c == '.') {
            dot_run += 1;
            if (dot_run >= 2) return false; // no ".."
            continue;
        }
        return false;
    }

    // Disallow IPv4-like bucket names (e.g. 192.168.0.1)
    if (all_digits_or_dots and isIpv4Like(name)) return false;
    return true;
}

fn isLowerAlphaNum(c: u8) bool {
    return (c >= 'a' and c <= 'z') or (c >= '0' and c <= '9');
}

fn isIpv4Like(name: []const u8) bool {
    var count: usize = 0;
    var it = std.mem.splitScalar(u8, name, '.');
    while (it.next()) |p| {
        if (p.len == 0) return false;
        if (count >= 4) return false;
        const n = std.fmt.parseInt(u16, p, 10) catch return false;
        if (n > 255) return false;
        count += 1;
    }
    return count == 4;
}

fn queryHas(query: []const u8, name: []const u8, value: []const u8) bool {
    if (query.len == 0) return false;

    var it = std.mem.splitScalar(u8, query, '&');
    while (it.next()) |pair| {
        const eq = std.mem.indexOfScalar(u8, pair, '=');
        if (eq) |i| {
            const k = pair[0..i];
            const v = pair[i + 1 ..];
            if (std.mem.eql(u8, k, name) and std.mem.eql(u8, v, value)) return true;
        } else {
            if (std.mem.eql(u8, pair, name) and value.len == 0) return true;
        }
    }
    return false;
}

fn mapStoreError(allocator: std.mem.Allocator, err: anyerror, resource: []const u8) !types.Response {
    return switch (err) {
        error.NoSuchBucket => s3Error(allocator, 404, "NoSuchBucket", "The specified bucket does not exist", resource),
        error.NoSuchKey => s3Error(allocator, 404, "NoSuchKey", "The specified key does not exist", resource),
        error.BucketAlreadyExists => s3Error(allocator, 409, "BucketAlreadyOwnedByYou", "The bucket you tried to create already exists", resource),
        error.BucketNotEmpty => s3Error(allocator, 409, "BucketNotEmpty", "The bucket you tried to delete is not empty", resource),
        // Distributed/cluster-oriented errors (if the backing store provides them).
        error.NoQuorum => s3Error(allocator, 503, "ServiceUnavailable", "Not enough replicas/quorum", resource),
        error.NotLeader => s3Error(allocator, 503, "ServiceUnavailable", "Request must be sent to leader", resource),
        else => s3Error(allocator, 500, "InternalError", "Internal error", resource),
    };
}

fn s3Error(
    allocator: std.mem.Allocator,
    status: u16,
    code: []const u8,
    message: []const u8,
    resource: []const u8,
) !types.Response {
    const body = try xml.errorXml(allocator, code, message, resource);
    errdefer allocator.free(body);
    return responseWithContentType(allocator, status, "application/xml", body);
}
