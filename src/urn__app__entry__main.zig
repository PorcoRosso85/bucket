const std = @import("std");
const DistStore = @import("urn__app__s3api__storage_dist.zig").Store;
const http_server = @import("urn__app__s3api__http_server.zig");

pub fn main(init: std.process.Init) !void {
    // Zig 0.16+: prefer the process init context (args/env/io/gpa) provided by
    // the runtime over the removed `std.process.argsWithAllocator` API.
    const allocator = init.gpa;

    // CLI (very small):
    //   --replicas N       (default: 3)
    //   --data-dir PATH    (default: .zig-cache/s3bucket)
    //   --bind IP          (default: 127.0.0.1)
    //   --port PORT        (default: 9000)
    //
    // This server always runs a durable replicated bucket (no in-memory mode).
    var replicas: u8 = 3;
    var data_dir: []const u8 = ".zig-cache/s3bucket";
    var bind_ip_str: []const u8 = "127.0.0.1";
    var port: u16 = 9000;
    var args_it = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args_it.deinit();
    _ = args_it.next(); // argv0
    while (args_it.next()) |a| {
        if (std.mem.eql(u8, a, "--replicas")) {
            if (args_it.next()) |v| {
                replicas = @intCast(try std.fmt.parseInt(u8, v, 10));
            } else return error.BadArgs;
        }
        if (std.mem.eql(u8, a, "--data-dir")) {
            if (args_it.next()) |v| {
                data_dir = v;
            } else return error.BadArgs;
        }
        if (std.mem.eql(u8, a, "--bind")) {
            if (args_it.next()) |v| {
                bind_ip_str = v;
            } else return error.BadArgs;
        }
        if (std.mem.eql(u8, a, "--port")) {
            if (args_it.next()) |v| {
                port = @intCast(try std.fmt.parseInt(u16, v, 10));
            } else return error.BadArgs;
        }
    }

    const io = init.io;

    const listen_ip = try std.Io.net.IpAddress.parseIp4(bind_ip_str, port);

    if (!std.mem.eql(u8, bind_ip_str, "127.0.0.1")) {
        std.debug.print(
            "WARNING: binding to {s} without TLS/auth. Prefer 127.0.0.1 for local dev.\n",
            .{bind_ip_str},
        );
    }

    if (replicas < 1) return error.BadArgs;

    var store = try DistStore.init(allocator, io, data_dir, .{ .replicas = replicas, .leader_id = 0 });
    defer store.deinit();
    try http_server.serve(allocator, &store, io, listen_ip);
}
