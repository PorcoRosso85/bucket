const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const exe_mod = b.createModule(.{
        .root_source_file = b.path("src/urn__app__entry__main.zig"),
        .target = target,
        .optimize = optimize,
    });

    const exe = b.addExecutable(.{
        .name = "s3gw",
        .root_module = exe_mod,
    });
    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }
    const run_step = b.step("run", "Run the S3-compatible HTTP gateway");
    run_step.dependOn(&run_cmd.step);

    const test_mod = b.createModule(.{
        .root_source_file = b.path("src/urn__app__test__all.zig"),
        .target = target,
        .optimize = optimize,
    });

    const tests = b.addTest(.{
        .root_module = test_mod,
    });
    const test_run = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run tests (unit + HTTP E2E + dist simulation)");
    // The test suite includes a process-level blackbox test that spawns the
    // installed `zig-out/bin/s3gw` binary.
    //
    // IMPORTANT: `dependOn(A)` + `dependOn(B)` does not guarantee ordering
    // between A and B. Make the *test run* depend on install so the binary
    // exists before the test process starts.
    test_run.step.dependOn(b.getInstallStep());
    test_step.dependOn(&test_run.step);
}
