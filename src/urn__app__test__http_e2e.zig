const std = @import("std");

const Store = @import("urn__app__s3api__storage_dist.zig").Store;
const http_server = @import("urn__app__s3api__http_server.zig");

const Resp = struct {
    status: u16,
    content_length: usize,
    body: []u8,

    pub fn deinit(self: *Resp, allocator: std.mem.Allocator) void {
        allocator.free(self.body);
        self.* = undefined;
    }
};

fn parseStatus(head: []const u8) !u16 {
    // "HTTP/1.1 200 OK\r\n" => extract 200
    const sp1 = std.mem.indexOfScalar(u8, head, ' ') orelse return error.BadResponse;
    const rest = head[sp1 + 1 ..];
    const sp2 = std.mem.indexOfScalar(u8, rest, ' ') orelse return error.BadResponse;
    return try std.fmt.parseInt(u16, rest[0..sp2], 10);
}

fn parseContentLength(head: []const u8) !usize {
    var it = std.mem.splitSequence(u8, head, "\r\n");
    _ = it.next(); // status line
    while (it.next()) |line| {
        if (line.len == 0) break;
        // case-insensitive startsWith("content-length:")
        if (line.len >= 15) {
            const prefix = line[0..15];
            if (std.ascii.eqlIgnoreCase(prefix, "content-length:")) {
                const v = std.mem.trim(u8, line[15..], " \t");
                const n64 = try std.fmt.parseInt(u64, v, 10);
                return std.math.cast(usize, n64) orelse return error.BadResponse;
            }
        }
    }
    return 0;
}

fn readResponse(allocator: std.mem.Allocator, r: *std.Io.Reader, read_body: bool) !Resp {
    while (true) {
        const buffered = try r.peekGreedy(1);
        if (std.mem.indexOf(u8, buffered, "\r\n\r\n")) |idx| {
            const head = buffered[0 .. idx + 4];
            const status = try parseStatus(head);
            const content_len = try parseContentLength(head);

            // Consume head.
            r.toss(idx + 4);

            if (!read_body) {
                const body = try allocator.alloc(u8, 0);
                return .{ .status = status, .content_length = content_len, .body = body };
            }

            const body = try allocator.alloc(u8, content_len);
            errdefer allocator.free(body);
            if (content_len > 0) {
                try r.readSliceAll(body);
            }
            return .{ .status = status, .content_length = content_len, .body = body };
        }
        // Need more bytes.
        r.fillMore() catch |err| switch (err) {
            error.EndOfStream => return error.BadResponse,
            else => return err,
        };
    }
}

fn writeAllAndFlush(w: *std.Io.Writer, bytes: []const u8) !void {
    try w.writeAll(bytes);
    try w.flush();
}

fn serverThreadMain(io: std.Io, store: *Store, stream: std.Io.net.Stream) void {
    defer stream.close(io);
    // Drive the same path as production, but on this single accepted stream.
    http_server.serveConnection(store.allocator, store, io, stream) catch {};
}

fn withOneConnection(
    store: *Store,
    io: std.Io,
    comptime f: fn (*std.Io.net.Stream.Reader, *std.Io.net.Stream.Writer) anyerror!void,
) !void {
    const listen_ip = try std.Io.net.IpAddress.parseIp4("127.0.0.1", 0);
    var server = try listen_ip.listen(io, .{ .reuse_address = true });
    defer server.deinit(io);

    const port = server.socket.address.getPort();
    const connect_ip = try std.Io.net.IpAddress.parseIp4("127.0.0.1", port);
    var client_stream = try connect_ip.connect(io, .{ .mode = .stream });
    var client_open = true;
    errdefer if (client_open) client_stream.close(io);

    const server_stream = try server.accept(io);

    var t = try std.Thread.spawn(.{}, serverThreadMain, .{ io, store, server_stream });
    defer t.join();
    // Ensure the connection is closed before waiting for the server thread.
    defer {
        client_open = false;
        client_stream.close(io);
    }

    var read_buf: [64 * 1024]u8 = undefined;
    var write_buf: [16 * 1024]u8 = undefined;
    var r = client_stream.reader(io, &read_buf);
    var w = client_stream.writer(io, &write_buf);

    try f(&r, &w);
}

test "http_e2e: basic S3 subset over HTTP (no panic, correct status/body)" {
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // Dedicated allocator for the durable store + server thread.
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const server_alloc = gpa.allocator();

    var store = try Store.initAtDir(server_alloc, io, tmp.dir, .{ .replicas = 3, .leader_id = 0 });
    defer store.deinit();

    try withOneConnection(&store, io, struct {
        fn run(r: *std.Io.net.Stream.Reader, w: *std.Io.net.Stream.Writer) !void {
            // 1) Create bucket.
            try writeAllAndFlush(&w.interface, "PUT /mybucket HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
            var resp = try readResponse(std.testing.allocator, &r.interface, true);
            defer resp.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u16, 200), resp.status);

            // 2) Put object (body) - this used to panic.
            try writeAllAndFlush(&w.interface, "PUT /mybucket/key1 HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\n\r\nhello");
            var resp2 = try readResponse(std.testing.allocator, &r.interface, true);
            defer resp2.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u16, 200), resp2.status);

            // 3) Get object.
            try writeAllAndFlush(&w.interface, "GET /mybucket/key1 HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
            var resp3 = try readResponse(std.testing.allocator, &r.interface, true);
            defer resp3.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u16, 200), resp3.status);
            try std.testing.expectEqualStrings("hello", resp3.body);

            // 4) HEAD object (no body).
            try writeAllAndFlush(&w.interface, "HEAD /mybucket/key1 HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
            var resp4 = try readResponse(std.testing.allocator, &r.interface, false);
            defer resp4.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u16, 200), resp4.status);
            try std.testing.expectEqual(@as(usize, 5), resp4.content_length);
            try std.testing.expectEqual(@as(usize, 0), resp4.body.len);

            // 5) List objects.
            try writeAllAndFlush(&w.interface, "GET /mybucket?list-type=2 HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
            var resp5 = try readResponse(std.testing.allocator, &r.interface, true);
            defer resp5.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u16, 200), resp5.status);
            try std.testing.expect(std.mem.indexOf(u8, resp5.body, "<Key>key1</Key>") != null);

            // 6) Delete object.
            try writeAllAndFlush(&w.interface, "DELETE /mybucket/key1 HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
            var resp6 = try readResponse(std.testing.allocator, &r.interface, true);
            defer resp6.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u16, 204), resp6.status);

            // 6b) Delete missing object should be idempotent (204).
            try writeAllAndFlush(&w.interface, "DELETE /mybucket/key1 HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
            var resp6b = try readResponse(std.testing.allocator, &r.interface, true);
            defer resp6b.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u16, 204), resp6b.status);

            // 7) Delete bucket.
            try writeAllAndFlush(&w.interface, "DELETE /mybucket HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
            var resp7 = try readResponse(std.testing.allocator, &r.interface, true);
            defer resp7.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u16, 204), resp7.status);
        }
    }.run);
}

test "http_e2e: durability across restart (HTTP)" {
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const server_alloc = gpa.allocator();

    // First run: create bucket+object.
    {
        var store = try Store.initAtDir(server_alloc, io, tmp.dir, .{ .replicas = 3, .leader_id = 0 });
        defer store.deinit();

        try withOneConnection(&store, io, struct {
            fn run(r: *std.Io.net.Stream.Reader, w: *std.Io.net.Stream.Writer) !void {
                // create bucket and object
                try writeAllAndFlush(&w.interface, "PUT /bbb HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
                var resp = try readResponse(std.testing.allocator, &r.interface, true);
                defer resp.deinit(std.testing.allocator);
                try std.testing.expectEqual(@as(u16, 200), resp.status);

                try writeAllAndFlush(&w.interface, "PUT /bbb/k HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\n\r\nhi");
                var resp2 = try readResponse(std.testing.allocator, &r.interface, true);
                defer resp2.deinit(std.testing.allocator);
                try std.testing.expectEqual(@as(u16, 200), resp2.status);
            }
        }.run);
    }

    // Second run: reopen and read back.
    {
        var store2 = try Store.initAtDir(server_alloc, io, tmp.dir, .{ .replicas = 3, .leader_id = 0 });
        defer store2.deinit();

        try withOneConnection(&store2, io, struct {
            fn run(r: *std.Io.net.Stream.Reader, w: *std.Io.net.Stream.Writer) !void {
                try writeAllAndFlush(&w.interface, "GET /bbb/k HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
                var resp = try readResponse(std.testing.allocator, &r.interface, true);
                defer resp.deinit(std.testing.allocator);
                try std.testing.expectEqual(@as(u16, 200), resp.status);
                try std.testing.expectEqualStrings("hi", resp.body);
            }
        }.run);
    }
}

test "http_e2e: errors + limits (Compatibility Envelope)" {
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const server_alloc = gpa.allocator();

    var store = try Store.initAtDir(server_alloc, io, tmp.dir, .{ .replicas = 3, .leader_id = 0 });
    defer store.deinit();

    // 1) NoSuchBucket on object PUT.
    try withOneConnection(&store, io, struct {
        fn run(r: *std.Io.net.Stream.Reader, w: *std.Io.net.Stream.Writer) !void {
            try writeAllAndFlush(&w.interface, "PUT /nope/key HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1\r\n\r\na");
            var resp = try readResponse(std.testing.allocator, &r.interface, true);
            defer resp.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u16, 404), resp.status);
            try std.testing.expect(std.mem.indexOf(u8, resp.body, "<Code>NoSuchBucket</Code>") != null);
        }
    }.run);

    // 2) BucketAlreadyOwnedByYou on duplicate create, BucketNotEmpty on delete.
    try withOneConnection(&store, io, struct {
        fn run(r: *std.Io.net.Stream.Reader, w: *std.Io.net.Stream.Writer) !void {
            // create bucket
            try writeAllAndFlush(&w.interface, "PUT /bbb HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
            var resp1 = try readResponse(std.testing.allocator, &r.interface, true);
            defer resp1.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u16, 200), resp1.status);

            // duplicate create
            try writeAllAndFlush(&w.interface, "PUT /bbb HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
            var resp2 = try readResponse(std.testing.allocator, &r.interface, true);
            defer resp2.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u16, 409), resp2.status);
            try std.testing.expect(std.mem.indexOf(u8, resp2.body, "<Code>BucketAlreadyOwnedByYou</Code>") != null);

            // put object
            try writeAllAndFlush(&w.interface, "PUT /bbb/k HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\n\r\nhi");
            var resp3 = try readResponse(std.testing.allocator, &r.interface, true);
            defer resp3.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u16, 200), resp3.status);

            // delete bucket should fail
            try writeAllAndFlush(&w.interface, "DELETE /bbb HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
            var resp4 = try readResponse(std.testing.allocator, &r.interface, true);
            defer resp4.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u16, 409), resp4.status);
            try std.testing.expect(std.mem.indexOf(u8, resp4.body, "<Code>BucketNotEmpty</Code>") != null);

            // ListBuckets contains the bucket name.
            try writeAllAndFlush(&w.interface, "GET / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
            var resp5 = try readResponse(std.testing.allocator, &r.interface, true);
            defer resp5.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u16, 200), resp5.status);
            try std.testing.expect(std.mem.indexOf(u8, resp5.body, "<Name>bbb</Name>") != null);

            // delete object
            try writeAllAndFlush(&w.interface, "DELETE /bbb/k HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
            var resp6 = try readResponse(std.testing.allocator, &r.interface, true);
            defer resp6.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u16, 204), resp6.status);

            // delete bucket now succeeds
            try writeAllAndFlush(&w.interface, "DELETE /bbb HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
            var resp7 = try readResponse(std.testing.allocator, &r.interface, true);
            defer resp7.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u16, 204), resp7.status);
        }
    }.run);

    // 3) 413 Payload Too Large - server should not panic and should close.
    try withOneConnection(&store, io, struct {
        fn run(r: *std.Io.net.Stream.Reader, w: *std.Io.net.Stream.Writer) !void {
            // content-length = 16MiB + 1
            try writeAllAndFlush(&w.interface, "PUT /bigbkt HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
            var ok = try readResponse(std.testing.allocator, &r.interface, true);
            defer ok.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u16, 200), ok.status);

            try writeAllAndFlush(&w.interface, "PUT /bigbkt/big HTTP/1.1\r\nHost: localhost\r\nContent-Length: 16777217\r\n\r\n");
            var resp = try readResponse(std.testing.allocator, &r.interface, true);
            defer resp.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u16, 413), resp.status);
        }
    }.run);

    // 4) Invalid percent-encoding in the path must be rejected as InvalidRequest.
    // Keep it as an HTTP blackbox regression: we send a raw target containing "%ZZ".
    try withOneConnection(&store, io, struct {
        fn run(r: *std.Io.net.Stream.Reader, w: *std.Io.net.Stream.Writer) !void {
            try writeAllAndFlush(&w.interface, "GET /bbb/bad%ZZkey HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
            var resp = try readResponse(std.testing.allocator, &r.interface, true);
            defer resp.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u16, 400), resp.status);
            try std.testing.expect(std.mem.indexOf(u8, resp.body, "<Code>InvalidRequest</Code>") != null);
        }
    }.run);
}

test "http_e2e: compatibility edges (URL decode, list order, bucket name validation)" {
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const server_alloc = gpa.allocator();

    var store = try Store.initAtDir(server_alloc, io, tmp.dir, .{ .replicas = 3, .leader_id = 0 });
    defer store.deinit();

    try withOneConnection(&store, io, struct {
        fn run(r: *std.Io.net.Stream.Reader, w: *std.Io.net.Stream.Writer) !void {
            // Invalid bucket name should fail early (avoid "works here, fails on S3" surprises).
            try writeAllAndFlush(&w.interface, "PUT /MyBucket HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
            var bad = try readResponse(std.testing.allocator, &r.interface, true);
            defer bad.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u16, 400), bad.status);
            try std.testing.expect(std.mem.indexOf(u8, bad.body, "<Code>InvalidBucketName</Code>") != null);

            // Create a valid bucket.
            try writeAllAndFlush(&w.interface, "PUT /compat HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
            var mk = try readResponse(std.testing.allocator, &r.interface, true);
            defer mk.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u16, 200), mk.status);

            // URL-decoded key: "space key" stored by PUT /space%20key.
            try writeAllAndFlush(&w.interface, "PUT /compat/space%20key HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1\r\n\r\ns");
            var put_space = try readResponse(std.testing.allocator, &r.interface, true);
            defer put_space.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u16, 200), put_space.status);

            // URL-decoded key with an encoded slash: "a/b" stored by PUT /a%2Fb.
            try writeAllAndFlush(&w.interface, "PUT /compat/a%2Fb HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1\r\n\r\n/");
            var put_slash = try readResponse(std.testing.allocator, &r.interface, true);
            defer put_slash.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u16, 200), put_slash.status);

            // Keys that require XML escaping must not break the ListObjectsV2 XML.
            // Store: "weird&<>\"'" via percent-decoding.
            try writeAllAndFlush(&w.interface, "PUT /compat/weird%26%3C%3E%22%27 HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1\r\n\r\nx");
            var put_xml = try readResponse(std.testing.allocator, &r.interface, true);
            defer put_xml.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u16, 200), put_xml.status);

            // Add keys that demonstrate lexicographic ordering.
            try writeAllAndFlush(&w.interface, "PUT /compat/z HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1\r\n\r\nz");
            var put_z = try readResponse(std.testing.allocator, &r.interface, true);
            defer put_z.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u16, 200), put_z.status);

            try writeAllAndFlush(&w.interface, "PUT /compat/a HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1\r\n\r\na");
            var put_a = try readResponse(std.testing.allocator, &r.interface, true);
            defer put_a.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u16, 200), put_a.status);

            // ListObjectsV2 must be sorted lexicographically.
            try writeAllAndFlush(&w.interface, "GET /compat?list-type=2 HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
            var list = try readResponse(std.testing.allocator, &r.interface, true);
            defer list.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u16, 200), list.status);

            const i_a = std.mem.indexOf(u8, list.body, "<Key>a</Key>") orelse return error.BadResponse;
            const i_ab = std.mem.indexOf(u8, list.body, "<Key>a/b</Key>") orelse return error.BadResponse;
            const i_space = std.mem.indexOf(u8, list.body, "<Key>space key</Key>") orelse return error.BadResponse;
            const i_weird = std.mem.indexOf(u8, list.body, "<Key>weird&amp;&lt;&gt;&quot;&apos;</Key>") orelse return error.BadResponse;
            const i_z = std.mem.indexOf(u8, list.body, "<Key>z</Key>") orelse return error.BadResponse;
            try std.testing.expect(i_a < i_ab);
            try std.testing.expect(i_ab < i_space);
            try std.testing.expect(i_space < i_weird);
            try std.testing.expect(i_weird < i_z);

            // Ensure the key was escaped (raw "<" must not appear inside a <Key>...</Key>).
            try std.testing.expect(std.mem.indexOf(u8, list.body, "<Key>weird&<") == null);

            // GET with encoded path should retrieve the decoded key.
            try writeAllAndFlush(&w.interface, "GET /compat/space%20key HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
            var get_space = try readResponse(std.testing.allocator, &r.interface, true);
            defer get_space.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u16, 200), get_space.status);
            try std.testing.expectEqualStrings("s", get_space.body);

            // GET with encoded slash retrieves the decoded key ("a/b").
            try writeAllAndFlush(&w.interface, "GET /compat/a%2Fb HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
            var get_slash = try readResponse(std.testing.allocator, &r.interface, true);
            defer get_slash.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u16, 200), get_slash.status);
            try std.testing.expectEqualStrings("/", get_slash.body);
        }
    }.run);
}
