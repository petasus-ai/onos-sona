load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

PROTOBUF_VERSION = "3.10.0"
SHA = "33cba8b89be6c81b1461f1c438424f7a1aa4e31998dbe9ed6f8319583daac8c7"

def generate_protobuf():
    http_archive(
        name = "com_google_protobuf",
        urls = ["https://github.com/protocolbuffers/protobuf/archive/v%s.zip" %
                PROTOBUF_VERSION],
        sha256 = SHA,
        strip_prefix = "protobuf-" + PROTOBUF_VERSION,
    )

    # protobuf 3.10.0's protobuf_deps() would otherwise pull zlib 1.2.11, whose
    # zutil.h defines fdopen() to NULL when TARGET_OS_MAC is set. Modern macOS
    # SDKs always define TARGET_OS_MAC, so that macro mangles the system
    # <stdio.h> fdopen declaration and the zlib host tool fails to compile with
    # the current clang. zlib 1.3.1 dropped that legacy hack. Declare zlib here
    # (reusing protobuf's own BUILD file) so protobuf_deps()'s
    # native.existing_rule("zlib") guard keeps this newer version.
    http_archive(
        name = "zlib",
        build_file = "@com_google_protobuf//:third_party/zlib.BUILD",
        sha256 = "9a93b2b7dfdac77ceba5a558a580e74667dd6fede4585b91eefb60f03b72df23",
        strip_prefix = "zlib-1.3.1",
        urls = [
            "https://github.com/madler/zlib/releases/download/v1.3.1/zlib-1.3.1.tar.gz",
            "https://zlib.net/zlib-1.3.1.tar.gz",
        ],
    )
