const std = @import("std");

pub fn listBucketsXml(allocator: std.mem.Allocator, bucket_names: []const []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);

    try out.appendSlice(allocator, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    try out.appendSlice(allocator, "<ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
    try out.appendSlice(allocator, "<Buckets>");

    for (bucket_names) |name| {
        try out.appendSlice(allocator, "<Bucket><Name>");
        try xmlEscapeAppend(&out, allocator, name);
        try out.appendSlice(allocator, "</Name></Bucket>");
    }

    try out.appendSlice(allocator, "</Buckets>");
    try out.appendSlice(allocator, "</ListAllMyBucketsResult>");
    return out.toOwnedSlice(allocator);
}

pub fn listObjectsV2Xml(allocator: std.mem.Allocator, bucket: []const u8, keys: []const []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);

    try out.appendSlice(allocator, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    try out.appendSlice(allocator, "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
    try out.appendSlice(allocator, "<Name>");
    try xmlEscapeAppend(&out, allocator, bucket);
    try out.appendSlice(allocator, "</Name>");
    try out.print(allocator, "<KeyCount>{d}</KeyCount>", .{keys.len});
    try out.appendSlice(allocator, "<MaxKeys>1000</MaxKeys>");
    try out.appendSlice(allocator, "<IsTruncated>false</IsTruncated>");

    for (keys) |key| {
        try out.appendSlice(allocator, "<Contents><Key>");
        try xmlEscapeAppend(&out, allocator, key);
        try out.appendSlice(allocator, "</Key></Contents>");
    }

    try out.appendSlice(allocator, "</ListBucketResult>");
    return out.toOwnedSlice(allocator);
}

pub fn errorXml(allocator: std.mem.Allocator, code: []const u8, message: []const u8, resource: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);

    try out.appendSlice(allocator, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    try out.appendSlice(allocator, "<Error>");
    try out.appendSlice(allocator, "<Code>");
    try xmlEscapeAppend(&out, allocator, code);
    try out.appendSlice(allocator, "</Code>");
    try out.appendSlice(allocator, "<Message>");
    try xmlEscapeAppend(&out, allocator, message);
    try out.appendSlice(allocator, "</Message>");
    try out.appendSlice(allocator, "<Resource>");
    try xmlEscapeAppend(&out, allocator, resource);
    try out.appendSlice(allocator, "</Resource>");
    try out.appendSlice(allocator, "</Error>");

    return out.toOwnedSlice(allocator);
}

fn xmlEscapeAppend(out: *std.ArrayList(u8), allocator: std.mem.Allocator, s: []const u8) !void {
    for (s) |c| {
        switch (c) {
            '&' => try out.appendSlice(allocator, "&amp;"),
            '<' => try out.appendSlice(allocator, "&lt;"),
            '>' => try out.appendSlice(allocator, "&gt;"),
            '"' => try out.appendSlice(allocator, "&quot;"),
            '\'' => try out.appendSlice(allocator, "&apos;"),
            else => try out.append(allocator, c),
        }
    }
}
