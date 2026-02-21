const std = @import("std");
const handler = @import("urn__app__s3api__handler.zig");

const http = std.http;

const max_body_bytes: usize = 16 * 1024 * 1024; // 16 MiB (dev-safe default)

pub fn serve(allocator: std.mem.Allocator, store: anytype, io: std.Io, listen_ip: std.Io.net.IpAddress) !void {
    var server = try listen_ip.listen(io, .{ .reuse_address = true });
    defer server.deinit(io);

    var addr_buf: [64]u8 = undefined;
    var w: std.Io.Writer = .fixed(&addr_buf);
    try listen_ip.format(&w);
    std.debug.print("s3gw listening on http://{s}\n", .{addr_buf[0..w.end]});

    while (true) {
        const stream = try server.accept(io);
        defer stream.close(io);

        serveConnection(allocator, store, io, stream) catch {
            // On any connection-level error, just drop the connection.
        };
    }
}

/// Serve HTTP requests on an already-accepted stream.
///
/// This is `pub` so that tests can drive the full HTTP stack end-to-end
/// without needing to run an infinite accept loop.
pub fn serveConnection(allocator: std.mem.Allocator, store: anytype, io: std.Io, stream: std.Io.net.Stream) !void {
    var read_buf: [16 * 1024]u8 = undefined;
    var write_buf: [16 * 1024]u8 = undefined;

    var r = stream.reader(io, &read_buf);
    var w = stream.writer(io, &write_buf);

    var s = http.Server.init(&r.interface, &w.interface);

    while (true) {
        var req = s.receiveHead() catch |err| switch (err) {
            error.HttpConnectionClosing => return,
            error.HttpRequestTruncated => return,
            error.ReadFailed => return,
            error.HttpHeadersOversize => return,
            error.HttpHeadersInvalid => return,
        };

        // NOTE: calling readerExpect* invalidates strings inside req.head.
        const target_copy = try allocator.dupe(u8, req.head.target);
        defer allocator.free(target_copy);

        const method_str = @tagName(req.head.method);

        var body: []u8 = &.{};
        defer if (body.len != 0) allocator.free(body);

        if (req.head.method.requestHasBody()) {
            // NOTE: do not use `readSliceShort` with a buffer larger than the
            // request body. With Zig 0.16-dev's `std.http` content-length
            // stream, that pattern can trigger a runtime-safety panic because
            // the reader transitions to `.ready` and the next read attempts to
            // access the wrong union field.

            // Early reject if client declares too large content-length.
            if (req.head.content_length) |cl| {
                if (cl > max_body_bytes) {
                    try req.respond("", .{ .status = .payload_too_large, .keep_alive = false });
                    return;
                }
            }

            var body_reader_buf: [8 * 1024]u8 = undefined;
            const body_reader = req.readerExpectContinue(&body_reader_buf) catch {
                try req.respond("", .{ .status = .expectation_failed, .keep_alive = false });
                return;
            };

            if (req.head.content_length) |cl| {
                const len_usize = std.math.cast(usize, cl) orelse {
                    try req.respond("", .{ .status = .payload_too_large, .keep_alive = false });
                    return;
                };
                if (len_usize == 0) {
                    // Still transition the underlying reader to `.ready`.
                    _ = body_reader.discardRemaining() catch {};
                } else {
                    body = try allocator.alloc(u8, len_usize);
                    errdefer allocator.free(body);
                    try body_reader.readSliceAll(body);
                    // Ensure the request reader becomes `.ready` so the
                    // connection can be safely kept alive.
                    _ = body_reader.discardRemaining() catch {};
                }
            } else {
                // Chunked (or body-until-close). Keep it bounded.
                body = body_reader.allocRemaining(allocator, .limited(max_body_bytes)) catch |err| switch (err) {
                    error.StreamTooLong => {
                        try req.respond("", .{ .status = .payload_too_large, .keep_alive = false });
                        return;
                    },
                    else => return err,
                };
            }
        }

        const resp = handler.handle(allocator, store, .{ .method = method_str, .target = target_copy, .body = body }) catch {
            try req.respond("", .{ .status = .internal_server_error, .keep_alive = req.head.keep_alive });
            continue;
        };
        defer resp.deinit(allocator);

        var extra_headers: std.ArrayList(http.Header) = .empty;
        defer extra_headers.deinit(allocator);
        for (resp.headers) |h| {
            try extra_headers.append(allocator, .{ .name = h.name, .value = h.value });
        }

        const status_enum: http.Status = @enumFromInt(@as(u10, @intCast(resp.status)));
        try req.respond(resp.body, .{
            .status = status_enum,
            .keep_alive = req.head.keep_alive,
            .extra_headers = extra_headers.items,
        });
    }
}
