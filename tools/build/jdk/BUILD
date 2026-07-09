load("@bazel_tools//tools/jdk:default_java_toolchain.bzl", "default_java_toolchain")

# ONOS is a JDK 11 codebase, but Bazel 7's default Java toolchain runs JavaBuilder
# on the bundled JDK 21. That breaks the build in two ways:
#   1. JDK 21 javac rejects the "-source 7 / -target 7" flags hardcoded by some
#      vendored deps (e.g. protobuf 3.10.0): "Source option 7 is no longer
#      supported. Use 8 or later." (source/target 7 was removed in JDK 20.)
#   2. JDK 21 writes jars with a zip extra-field that the stricter zip validation
#      (CVE backport) later rejects when the OSGi/bnd wrapper reads them:
#      "Invalid CEN header (invalid extra data field size...)".
#
# The natural fix is to run JavaBuilder on the JDK this project targets, but
# Bazel 7.6's JavaBuilder itself requires a newer JDK to run: on JDK 11 it dies
# with NoSuchMethodError on StandardJavaFileManager.getJavaFileObjectsFromPaths
# (the Collection overload was only added in JDK 13). JDK 17 is the sweet spot:
# JavaBuilder runs, javac still accepts -source/-target 7, and it predates the
# JDK 21 zip behavior. Compilation still TARGETS Java 11 bytecode via
# source_version/target_version below. Selected via --java_language_version=11 /
# --tool_java_language_version=11 and given highest priority through
# --extra_toolchains (both set in .bazelrc).
default_java_toolchain(
    name = "onos_jdk11_toolchain",
    source_version = "11",
    target_version = "11",
    java_runtime = "@bazel_tools//tools/jdk:remotejdk_17",
    visibility = ["//visibility:public"],
)

# We use the following rule to package the same JDK used for building and make
# it available for external scripts as their JAVA_HOME, such as for `bazel run
# onos-local`.

# FIXME: @bazel_tools//tools/jdk:current_java_runtime should be fixed upstream
#  to include the missing JDK conf/security files, required by Karaf. The
#  workaround for now is to check in those files here and include them in the
#  produced JDK tar.

genrule(
    name = "current_jdk_tar",
    srcs = [
        "@bazel_tools//tools/jdk:current_java_runtime",
        ":conf-security",
    ],
    outs = ["current_jdk.tar.gz"],
    cmd = "mkdir current_jdk && " +
          "cp -r $(JAVABASE)/* current_jdk/ && " +
          "cp -r tools/build/jdk/* current_jdk/ && " +
          "tar -hczf $(location current_jdk.tar.gz) current_jdk",
    output_to_bindir = True,
    toolchains = ["@bazel_tools//tools/jdk:current_java_runtime"],
    visibility = ["//visibility:public"],
)

filegroup(
    name = "conf-security",
    srcs = glob(["conf/security/**"]),
)
