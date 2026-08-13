workspace(
    name = "org_onosproject_onos",
)

load("//tools/build/bazel:bazel_version.bzl", "check_bazel_version")

check_bazel_version()

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# bazel-skylib pinned to a Bazel 7-compatible release. The old 1.0.2 pin
# registered a unittest toolchain that referenced the @bazel_tools//platforms
# package removed in Bazel 7, breaking toolchain resolution for the whole build.
BAZEL_SKYLIB_VERSION = "1.5.0"

BAZEL_SKYLIB_SHA256 = "cd55a062e763b9349921f0f5db8c3933288dc8ba4f76dd9416aac68acee3cb94"

http_archive(
    name = "bazel_skylib",
    sha256 = BAZEL_SKYLIB_SHA256,
    urls = [
        "https://github.com/bazelbuild/bazel-skylib/releases/download/%s/bazel-skylib-%s.tar.gz" % (BAZEL_SKYLIB_VERSION, BAZEL_SKYLIB_VERSION),
    ],
)

load("@bazel_skylib//:workspace.bzl", "bazel_skylib_workspace")

bazel_skylib_workspace()

# rules_java pinned to the same release Bazel 7.6.1 bundles as
# @rules_java_builtin. Without this pin, protobuf_deps() below installs a 2019
# snapshot of rules_java that predates the toolchains/ package. That breaks the
# Docker build: local_java_repository() (appended there via WORKSPACE-docker)
# generates a @dockerjdk repository whose BUILD file loads
# "@rules_java//toolchains:local_java_repository.bzl". The pin must precede
# protobuf_deps() — the first fetch of a repository during WORKSPACE evaluation
# freezes it, so a later (re)definition would not take effect.
RULES_JAVA_VERSION = "7.6.5"

RULES_JAVA_SHA256 = "8afd053dd2a7b85a4f033584f30a7f1666c5492c56c76e04eec4428bdb2a86cf"

http_archive(
    name = "rules_java",
    sha256 = RULES_JAVA_SHA256,
    urls = [
        "https://github.com/bazelbuild/rules_java/releases/download/%s/rules_java-%s.tar.gz" % (RULES_JAVA_VERSION, RULES_JAVA_VERSION),
    ],
)

load("//tools/build/bazel:local_jar.bzl", "local_atomix", "local_jar", "local_yang_tools")

# Use this to build against locally built arbitrary 3rd party artifacts
#local_jar(
#    name = "atomix",
#    path = "/Users/tom/atomix/core/target/atomix-3.0.8-SNAPSHOT.jar",
#)

# Use this to build against locally built Atomix
#local_atomix(
#    path = "/home/sdn/atomix",
#    version = "3.1.12-SNAPSHOT",
#)

# Use this to build against locally built YANG tools
#local_yang_tools(
#    path = "/Users/andrea/onos-yang-tools",
#    version = "2.6-SNAPSHOT",
#)

load("//tools/build/bazel:generate_workspace.bzl", "generated_maven_jars")

generated_maven_jars()

load("//tools/build/bazel:protobuf_workspace.bzl", "generate_protobuf")

generate_protobuf()

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

load("//tools/build/bazel:grpc_workspace.bzl", "generate_grpc")

generate_grpc()

load("@io_grpc_grpc_java//:repositories.bzl", "grpc_java_repositories")

# We omit as many dependencies as we can and instead import the same via
# deps.json, so they get wrapped properly for Karaf runtime.
grpc_java_repositories(
    omit_bazel_skylib = False,
    omit_com_google_android_annotations = True,
    omit_com_google_api_grpc_google_common_protos = True,
    omit_com_google_auth_google_auth_library_credentials = True,
    omit_com_google_auth_google_auth_library_oauth2_http = True,
    omit_com_google_code_findbugs_jsr305 = True,
    omit_com_google_code_gson = True,
    omit_com_google_errorprone_error_prone_annotations = True,
    omit_com_google_guava = True,
    omit_com_google_guava_failureaccess = True,
    omit_com_google_j2objc_j2objc_annotations = True,
    omit_com_google_protobuf = True,
    omit_com_google_protobuf_javalite = True,
    omit_com_google_truth_truth = True,
    omit_com_squareup_okhttp = True,
    omit_com_squareup_okio = True,
    omit_io_grpc_grpc_proto = True,
    omit_io_netty_buffer = True,
    omit_io_netty_codec = True,
    omit_io_netty_codec_http = True,
    omit_io_netty_codec_http2 = True,
    omit_io_netty_codec_socks = True,
    omit_io_netty_common = True,
    omit_io_netty_handler = True,
    omit_io_netty_handler_proxy = True,
    omit_io_netty_resolver = True,
    omit_io_netty_tcnative_boringssl_static = True,
    omit_io_netty_transport = True,
    omit_io_opencensus_api = True,
    omit_io_opencensus_grpc_metrics = True,
    omit_io_perfmark = True,
    omit_javax_annotation = True,
    omit_junit_junit = True,
    omit_net_zlib = True,
    omit_org_apache_commons_lang3 = True,
    omit_org_codehaus_mojo_animal_sniffer_annotations = True,
)

load("//tools/build/bazel:p4lang_workspace.bzl", "generate_p4lang")

generate_p4lang()

load("//tools/build/bazel:gnmi_workspace.bzl", "generate_gnmi")

generate_gnmi()

load("//tools/build/bazel:gnoi_workspace.bzl", "generate_gnoi")

generate_gnoi()

# For GUI2 build
RULES_NODEJS_VERSION = "2.3.2"

RULES_NODEJS_SHA256 = "b3521b29c7cb0c47a1a735cce7e7e811a4f80d8e3720cf3a1b624533e4bb7cb6"

load("//tools/build/bazel:topo_workspace.bzl", "generate_topo_device")

generate_topo_device()

http_archive(
    name = "build_bazel_rules_nodejs",
    # Bazel 7 removed the @bazel_tools//platforms package; its constraint values
    # now live in @platforms//os and @platforms//cpu. rules_nodejs 2.3.2 still
    # references the old labels in its node toolchain definitions, which breaks
    # toolchain resolution for every build. Rewrite them to the @platforms labels.
    patch_cmds = [
        """
        f=toolchains/node/BUILD.bazel
        sed -e 's|@bazel_tools//platforms:osx|@platforms//os:osx|g' \
            -e 's|@bazel_tools//platforms:linux|@platforms//os:linux|g' \
            -e 's|@bazel_tools//platforms:windows|@platforms//os:windows|g' \
            -e 's|@bazel_tools//platforms:x86_64|@platforms//cpu:x86_64|g' \
            -e 's|@bazel_tools//platforms:aarch64|@platforms//cpu:aarch64|g' \
            -e 's|@bazel_tools//platforms:s390x|@platforms//cpu:s390x|g' \
            "$f" > "$f.tmp" && mv "$f.tmp" "$f"
        """,
        # rules_nodejs 2.3.2 predates Apple Silicon: BUILT_IN_NODE_PLATFORMS has
        # no darwin_arm64, so on an arm64 mac no toolchain satisfies the node
        # toolchain type and every target reaching @npm fails toolchain
        # resolution. The rules already treat any mac as "darwin_amd64" when
        # picking the node binary (os_name() in internal/common/os_name.bzl), so
        # point the arm64 host at the same x86_64 node and let Rosetta 2 run it.
        # Registered below; a no-op on every other host.
        """
        cat >> toolchains/node/BUILD.bazel <<'EOF'

toolchain(
    name = "node_darwin_arm64_toolchain",
    target_compatible_with = [
        "@platforms//os:osx",
        "@platforms//cpu:aarch64",
    ],
    toolchain = "@nodejs_darwin_amd64_config//:toolchain",
    toolchain_type = ":toolchain_type",
)
EOF
        """,
    ],
    sha256 = RULES_NODEJS_SHA256,
    urls = [
        "https://github.com/bazelbuild/rules_nodejs/releases/download/%s/rules_nodejs-%s.tar.gz" % (RULES_NODEJS_VERSION, RULES_NODEJS_VERSION),
    ],
)

# Rules for compiling sass
RULES_SASS_VERSION = "1.25.0"

RULES_SASS_SHA256 = "c78be58f5e0a29a04686b628cf54faaee0094322ae0ac99da5a8a8afca59a647"

http_archive(
    name = "io_bazel_rules_sass",
    sha256 = RULES_SASS_SHA256,
    strip_prefix = "rules_sass-%s" % RULES_SASS_VERSION,
    urls = [
        "https://github.com/bazelbuild/rules_sass/archive/%s.zip" % RULES_SASS_VERSION,
        "https://mirror.bazel.build/github.com/bazelbuild/rules_sass/archive/%s.zip" % RULES_SASS_VERSION,
    ],
)

load("@build_bazel_rules_nodejs//:index.bzl", "node_repositories", "npm_install", "yarn_install")

# Setup the Node repositories. We need a NodeJS version that is more recent than v10.15.0
# because "selenium-webdriver" which is required for "ng e2e" cannot be installed.
node_repositories(
    node_repositories = {
        "10.16.0-linux_arm64": ("node-v10.16.0-linux-arm64.tar.gz", "node-v10.16.0-linux-arm64", "2d84a777318bc95dd2a201ab8d700aea7e20641b3ece0c048399398dc645cbd7"),
        "10.16.0-darwin_amd64": ("node-v10.16.0-darwin-x64.tar.gz", "node-v10.16.0-darwin-x64", "6c009df1b724026d84ae9a838c5b382662e30f6c5563a0995532f2bece39fa9c"),
        "10.16.0-linux_amd64": ("node-v10.16.0-linux-x64.tar.xz", "node-v10.16.0-linux-x64", "1827f5b99084740234de0c506f4dd2202a696ed60f76059696747c34339b9d48"),
        "10.16.0-windows_amd64": ("node-v10.16.0-win-x64.zip", "node-v10.16.0-win-x64", "aa22cb357f0fb54ccbc06b19b60e37eefea5d7dd9940912675d3ed988bf9a059"),
        # Never used to build ONOS, but rules_nodejs declares a repository for
        # every entry in BUILT_IN_NODE_PLATFORMS, and `bazel sync` fetches all
        # of them. Without this entry the fetch fails with
        # "Unknown NodeJS version-host 10.16.0-linux_s390x".
        "10.16.0-linux_s390x": ("node-v10.16.0-linux-s390x.tar.xz", "node-v10.16.0-linux-s390x", "e8202e285a88be9b53bbf50cfae2f08fff2b1ae3597893e4049c9dff3e4b9b14"),
    },
    node_version = "10.16.0",
    # Bazel tries these in order, so a single mirror timing out no longer
    # fails the build; the pinned sha256 above guards integrity on all of
    # them. The bazel mirror lacks the linux-arm64 tarball, hence last.
    node_urls = [
        "https://nodejs.org/dist/v{version}/{filename}",
        "https://mirrors.cloud.tencent.com/nodejs-release/v{version}/{filename}",
        "https://mirror.bazel.build/nodejs.org/dist/v{version}/{filename}",
    ],
)

# node_repositories() registers a toolchain per BUILT_IN_NODE_PLATFORMS, which
# does not include darwin_arm64. Register the one appended by the patch_cmds
# above; it only matches an arm64 mac host.
register_toolchains("@build_bazel_rules_nodejs//toolchains/node:node_darwin_arm64_toolchain")

# TODO give this a name like `gui2_npm` once the @bazel/karma tools can tolerate a name other than `npm`
yarn_install(
    name = "npm",
    package_json = "//web/gui2:package.json",
    use_global_yarn_cache = True,
    yarn_lock = "//web/gui2:yarn.lock",
)

npm_install(
    # Name this npm so that Bazel Label references look like @npm//package
    name = "gui1_npm",
    package_json = "//tools/gui:package.json",
    package_lock_json = "//tools/gui:package-lock.json",
)

# Install any Bazel rules which were extracted earlier by the npm_install rule.
# Versions are set in web/gui2-fw-lib/package.json

RULES_WEBTESTING_VERSION = "0.3.3"

RULES_WEBTESTING_SHA256 = "9bb461d5ef08e850025480bab185fd269242d4e533bca75bfb748001ceb343c3"

http_archive(
    name = "io_bazel_rules_webtesting",
    sha256 = RULES_WEBTESTING_SHA256,
    urls = [
        "https://github.com/bazelbuild/rules_webtesting/releases/download/%s/rules_webtesting.tar.gz" % RULES_WEBTESTING_VERSION,
    ],
)

load("//tools/build/bazel:angular_workspace.bzl", "load_angular")

load_angular()

# buildifier is written in Go and hence needs rules_go to be built.
# See https://github.com/bazelbuild/rules_go for the up to date setup instructions.
RULES_GO_VERSION = "v0.19.8"

RULES_GO_SHA256 = "9976c2572587aa71f81b502cc870ef8058f6de37f5fcfaade6a5996934b4a324"

# Two compatibility fixes for this 2019-era rules_go pin, both load-phase
# breakages that abort the whole build:
#   1. It loads @bazel_skylib//lib:old_sets.bzl, which bazel-skylib 1.0 turned
#      into a fail() stub, so with the skylib 1.5.0 pin above any Go rule (e.g.
#      via @com_github_bazelbuild_buildtools//buildifier, reached by both
#      //:buildifier_check and `bazel sync`) fails to load. The patch drops the
#      load and inlines the one helper rules_go uses, sets.union().
#   2. It predates GO_TOOLCHAIN_LABEL in go/private/common.bzl, which the
#      IntelliJ Bazel plugin's generated aspect (.bazelbsp/modules/go_info.bzl)
#      loads unconditionally, breaking IDE sync. The patch defines it.
# The long-term fix is upgrading rules_go; see rules-go-upgrade-spec.md.
http_archive(
    name = "io_bazel_rules_go",
    patch_args = ["-p1"],
    patches = ["//tools/build/bazel:rules_go_compat.patch"],
    sha256 = RULES_GO_SHA256,
    urls = [
        "https://storage.googleapis.com/bazel-mirror/github.com/bazelbuild/rules_go/releases/download/%s/rules_go-%s.tar.gz" % (RULES_GO_VERSION, RULES_GO_VERSION),
        "https://github.com/bazelbuild/rules_go/releases/download/%s/rules_go-%s.tar.gz" % (RULES_GO_VERSION, RULES_GO_VERSION),
    ],
)

load("@io_bazel_rules_go//go:deps.bzl", "go_rules_dependencies")

go_rules_dependencies()

# NOTE: go_register_toolchains() is intentionally not called. rules_go v0.19.8
# generates a @go_sdk whose toolchains reference the @bazel_tools//platforms
# package that Bazel 7 removed, which breaks toolchain resolution for the whole
# build. Go is only needed by the buildifier dev tooling (not part of the ONOS
# deploy build), so we skip registering the Go toolchains entirely.

GAZELLE_VERSION = "0.18.1"

GAZELLE_SHA256 = "be9296bfd64882e3c08e3283c58fcb461fa6dd3c171764fcc4cf322f60615a9b"

http_archive(
    name = "bazel_gazelle",
    sha256 = GAZELLE_SHA256,
    urls = [
        "https://storage.googleapis.com/bazel-mirror/github.com/bazelbuild/bazel-gazelle/releases/download/%s/bazel-gazelle-%s.tar.gz" % (GAZELLE_VERSION, GAZELLE_VERSION),
        "https://github.com/bazelbuild/bazel-gazelle/releases/download/%s/bazel-gazelle-%s.tar.gz" % (GAZELLE_VERSION, GAZELLE_VERSION),
    ],
)

# NOTE: gazelle_dependencies() is intentionally not called. It declares
# @bazel_gazelle_go_repository_cache, whose fetch needs a Go SDK and therefore
# fails ("gazelle could not find a Go SDK") because go_register_toolchains() is
# skipped above. ONOS has no gazelle targets; the archive itself stays because
# @com_github_bazelbuild_buildtools//BUILD.bazel loads @bazel_gazelle//:def.bzl.

BUILDTOOLS_VERSION = "0.29.0"

BUILDTOOLS_SHA256 = "05eb52437fb250c7591dd6cbcfd1f9b5b61d85d6b20f04b041e0830dd1ab39b3"

http_archive(
    name = "com_github_bazelbuild_buildtools",
    sha256 = BUILDTOOLS_SHA256,
    strip_prefix = "buildtools-" + BUILDTOOLS_VERSION,
    url = "https://github.com/bazelbuild/buildtools/archive/%s.zip" % BUILDTOOLS_VERSION,
)
