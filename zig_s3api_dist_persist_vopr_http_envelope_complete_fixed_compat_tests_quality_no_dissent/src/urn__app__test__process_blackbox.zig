const std = @import("std");

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

fn pickFreePort(io: std.Io) !u16 {
    const ip = try std.Io.net.IpAddress.parseIp4("127.0.0.1", 0);
    var server = try ip.listen(io, .{ .reuse_address = true });
    defer server.deinit(io);
    return server.socket.address.getPort();
}

fn connectRetry(io: std.Io, port: u16) !std.Io.net.Stream {
    const ip = try std.Io.net.IpAddress.parseIp4("127.0.0.1", port);

    var attempt: usize = 0;
    while (true) : (attempt += 1) {
        return ip.connect(io, .{ .mode = .stream }) catch |err| {
            if (attempt >= 200) return err;
            // Give the child process time to bind/listen.
            _ = std.Io.sleep(io, std.Io.Duration.fromMilliseconds(1), .awake) catch {};
            continue;
        };
    }
}

test "process_e2e: spawn s3gw and drive it over sockets (multi-connection + slow body)" {
    const io = std.testing.io;
    const allocator = std.testing.allocator;

    // Ensure unique durability dir.
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const data_dir = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path[0..]});
    defer allocator.free(data_dir);

    const port = try pickFreePort(io);

    const exe_path = "zig-out/bin/s3gw";

    var port_buf: [16]u8 = undefined;
    const port_str = try std.fmt.bufPrint(&port_buf, "{d}", .{port});

    const argv = [_][]const u8{
        exe_path,
        "--data-dir",
        data_dir,
        "--replicas",
        "3",
        "--bind",
        "127.0.0.1",
        "--port",
        port_str,
    };

    var child = try std.process.spawn(io, .{
        .argv = &argv,
        .stdin = .ignore,
        .stdout = .pipe,
        .stderr = .pipe,
    });
    defer child.kill(io);

    // Connection 1: create bucket, put object via slow body chunking, get it.
    {
        var stream = try connectRetry(io, port);
        defer stream.close(io);

        var read_buf: [64 * 1024]u8 = undefined;
        var write_buf: [16 * 1024]u8 = undefined;
        var r = stream.reader(io, &read_buf);
        var w = stream.writer(io, &write_buf);

        // Create bucket.
        try writeAllAndFlush(&w.interface, "PUT /bbk HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
        var resp1 = try readResponse(allocator, &r.interface, true);
        defer resp1.deinit(allocator);
        try std.testing.expectEqual(@as(u16, 200), resp1.status);

        // Slow body: send in two chunks. Also tests %2F decoding inside key.
        try writeAllAndFlush(&w.interface, "PUT /bbk/slow%2Fkey HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\n\r\nhe");
        try writeAllAndFlush(&w.interface, "llo");
        var resp2 = try readResponse(allocator, &r.interface, true);
        defer resp2.deinit(allocator);
        try std.testing.expectEqual(@as(u16, 200), resp2.status);

        // Get the object back.
        try writeAllAndFlush(&w.interface, "GET /bbk/slow%2Fkey HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
        var resp3 = try readResponse(allocator, &r.interface, true);
        defer resp3.deinit(allocator);
        try std.testing.expectEqual(@as(u16, 200), resp3.status);
        try std.testing.expectEqualStrings("hello", resp3.body);
    }

    // Connection 2: ensure accept loop is healthy, HEAD content-length, list contains decoded key.
    {
        var stream2 = try connectRetry(io, port);
        defer stream2.close(io);

        var read_buf2: [64 * 1024]u8 = undefined;
        var write_buf2: [16 * 1024]u8 = undefined;
        var r2 = stream2.reader(io, &read_buf2);
        var w2 = stream2.writer(io, &write_buf2);

        try writeAllAndFlush(&w2.interface, "HEAD /bbk/slow%2Fkey HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
        var head = try readResponse(allocator, &r2.interface, false);
        defer head.deinit(allocator);
        try std.testing.expectEqual(@as(u16, 200), head.status);
        try std.testing.expectEqual(@as(usize, 5), head.content_length);

        try writeAllAndFlush(&w2.interface, "GET /bbk?list-type=2 HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
        var list = try readResponse(allocator, &r2.interface, true);
        defer list.deinit(allocator);
        try std.testing.expectEqual(@as(u16, 200), list.status);
        try std.testing.expect(std.mem.indexOf(u8, list.body, "<Key>slow/key</Key>") != null);
    }

    // Connection 3: client abort mid-body should not kill the server accept loop.
    {
        var stream3 = try connectRetry(io, port);
        // Send a truncated request and close.
        var write_buf3: [16 * 1024]u8 = undefined;
        var w3 = stream3.writer(io, &write_buf3);
        _ = w3.interface.writeAll("PUT /bbk/trunc HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\n\r\nhe") catch {};
        _ = w3.interface.flush() catch {};
        stream3.close(io);

        // Give the server a moment to observe EOF.
        _ = std.Io.sleep(io, std.Io.Duration.fromMilliseconds(2), .awake) catch {};

        // New connection should still work.
        var stream4 = try connectRetry(io, port);
        defer stream4.close(io);

        var read_buf4: [64 * 1024]u8 = undefined;
        var write_buf4: [16 * 1024]u8 = undefined;
        var r4 = stream4.reader(io, &read_buf4);
        var w4 = stream4.writer(io, &write_buf4);

        try writeAllAndFlush(&w4.interface, "GET /bbk/slow%2Fkey HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
        var resp = try readResponse(allocator, &r4.interface, true);
        defer resp.deinit(allocator);
        try std.testing.expectEqual(@as(u16, 200), resp.status);
        try std.testing.expectEqualStrings("hello", resp.body);
    }
}
