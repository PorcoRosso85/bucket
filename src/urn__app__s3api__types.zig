const std = @import("std");

pub const Header = struct {
    name: []u8,
    value: []u8,
};

pub const Response = struct {
    status: u16,
    headers: []Header,
    body: []u8,

    pub fn deinit(self: Response, allocator: std.mem.Allocator) void {
        for (self.headers) |h| {
            allocator.free(h.name);
            allocator.free(h.value);
        }
        allocator.free(self.headers);
        allocator.free(self.body);
    }
};

pub fn header(allocator: std.mem.Allocator, name: []const u8, value: []const u8) !Header {
    const name_copy = try allocator.dupe(u8, name);
    errdefer allocator.free(name_copy);
    const value_copy = try allocator.dupe(u8, value);
    errdefer allocator.free(value_copy);
    return .{ .name = name_copy, .value = value_copy };
}
