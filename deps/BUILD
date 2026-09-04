load("//tools/build/bazel:osgi_java_library.bzl", "wrapped_osgi_jar")

# Wrap protobuf and grpc-related JARs in OSGi-compatible ones, since the
# original ones are built with Bazel and NOT imported via mvn.

wrapped_osgi_jar(
    # Differently from the official "grpc-api" package, here we also include
    # "grpc-context" to solve the OSGI split-brain problem:
    # https://github.com/grpc/grpc-java/issues/2727
    # We use patched Bazel BUILD files to package together api and context (see
    # tools/build/bazel/grpc_workspace.bzl). If you need grpc-context as a
    # compile-time dependency, please use this one.
    name = "io_grpc_grpc_api_context",
    generate_pom = True,
    # Name resolver and load balancer providers which are part of the internal
    # package are loaded dynamically, hence this import statement.
    import_packages = "io.grpc.internal,*",
    jar = "@io_grpc_grpc_java//api",
    visibility = ["//visibility:public"],
    deps = [],
)

wrapped_osgi_jar(
    name = "io_grpc_grpc_core_inprocess",
    generate_pom = True,
    jar = "@io_grpc_grpc_java//core:inprocess",
    visibility = ["//visibility:public"],
    deps = [],
)

wrapped_osgi_jar(
    name = "io_grpc_grpc_core_internal",
    generate_pom = True,
    jar = "@io_grpc_grpc_java//core:internal",
    visibility = ["//visibility:public"],
    deps = [],
)

wrapped_osgi_jar(
    name = "io_grpc_grpc_core_util",
    generate_pom = True,
    jar = "@io_grpc_grpc_java//core:util",
    visibility = ["//visibility:public"],
    deps = [],
)

wrapped_osgi_jar(
    name = "io_grpc_grpc_stub",
    generate_pom = True,
    jar = "@io_grpc_grpc_java//stub",
    visibility = ["//visibility:public"],
    deps = [],
)

wrapped_osgi_jar(
    name = "io_grpc_grpc_netty",
    generate_pom = True,
    jar = "@io_grpc_grpc_java//netty",
    visibility = ["//visibility:public"],
    deps = [],
)

wrapped_osgi_jar(
    name = "io_grpc_grpc_auth",
    generate_pom = True,
    jar = "@io_grpc_grpc_java//auth",
    visibility = ["//visibility:public"],
    deps = [],
)

wrapped_osgi_jar(
    name = "io_grpc_grpc_protobuf",
    generate_pom = True,
    jar = "@io_grpc_grpc_java//protobuf",
    visibility = ["//visibility:public"],
    deps = [],
)

wrapped_osgi_jar(
    name = "io_grpc_grpc_protobuf_lite",
    generate_pom = True,
    jar = "@io_grpc_grpc_java//protobuf-lite",
    visibility = ["//visibility:public"],
    deps = [],
)

wrapped_osgi_jar(
    name = "com_google_protobuf_protobuf_java",
    generate_pom = True,
    jar = "@com_google_protobuf//:protobuf_java",
    visibility = ["//visibility:public"],
    deps = [],
)

# Atomix 3.1.12 is the last release of its line and pins Guava to the major it
# was built against (Import-Package com.google.common.*;version="[22.0,23)").
# It is binary compatible with the Guava shipped in deps.json (every Guava
# member it references still exists there), so only the manifest range has to
# change for the bundles to resolve. deps.json's ATOMIX library points at these
# copies; the jar contents and export versions are untouched.
load("//tools/build/bazel:widened_import_jar.bzl", "widened_import_jar")

[widened_import_jar(
    name = "atomix" + suffix,
    jar = "@atomix" + suffix + "//jar",
    ranges = {"com.google.common": "[22.0,34)"},
) for suffix in [
    "",
    "_cluster",
    "_primary_backup",
    "_primitive",
    "_raft",
    "_storage",
    "_utils",
]]

# Dropwizard metrics 4.x never shipped a metrics-ganglia; 3.2.6 is the last one
# and pins com.codahale.metrics to "[3.2,4)". Every metrics-core member it
# references (ScheduledReporter constructors, Snapshot, Metered, ...) still
# exists in 4.2, so only the range has to move.
widened_import_jar(
    name = "metrics_ganglia",
    jar = "@metrics_ganglia//jar",
    ranges = {"com.codahale.metrics": "[3.2,5)"},
)

# metrics-graphite 4.2.x imports com.rabbitmq.client without resolution:=optional
# (it was optional in 3.2.x); the package is only used by GraphiteRabbitMQ, which
# ONOS never instantiates, and amqp-client 5.x would drag Netty 4.2 into Karaf.
widened_import_jar(
    name = "metrics_graphite",
    jar = "@metrics_graphite//jar",
    optional = ["com.rabbitmq.client"],
)
