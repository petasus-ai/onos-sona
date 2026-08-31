"""Detects whether the host is an Apple Silicon mac.

rules_nodejs 2.3.2 identifies every mac as "darwin_amd64" and its pinned
node v10 has no darwin-arm64 build, so the WORKSPACE needs to know the real
host CPU to decide which node tarball to serve into @nodejs_darwin_amd64.
A repository rule is the only place WORKSPACE evaluation can inspect the
host, so this writes the answer into a loadable defs.bzl.
"""

def _node_host_repository_impl(repository_ctx):
    is_darwin_arm64 = False
    if repository_ctx.os.name.lower().startswith("mac"):
        result = repository_ctx.execute(["uname", "-m"])
        is_darwin_arm64 = result.return_code == 0 and result.stdout.strip() == "arm64"
    repository_ctx.file("BUILD.bazel", "")
    repository_ctx.file("defs.bzl", "IS_DARWIN_ARM64 = %r\n" % is_darwin_arm64)

node_host_repository = repository_rule(
    implementation = _node_host_repository_impl,
    local = True,
    doc = "Writes IS_DARWIN_ARM64 for the machine evaluating the WORKSPACE.",
)
