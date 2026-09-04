"""
 Copyright 2026-present Open Networking Foundation

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
"""

def widened_import_jar(name, jar, ranges = {}, optional = [], drop_capabilities = [], exports = [], visibility = ["//visibility:public"]):
    """Copies a third-party OSGi bundle, widening selected Import-Package version ranges.

    Args:
      name: target name; also usable as a java_library dependency.
      jar: label of the original jar (e.g. "@atomix//jar").
      ranges: dict of package prefix -> new OSGi version range, e.g.
              {"com.google.common": "[22.0,34)"}.
      optional: package prefixes whose imports become resolution:=optional
              (their version range is left alone).
      drop_capabilities: capability namespaces (e.g. "osgi.extender") whose
              Require-Capability and Provide-Capability clauses are removed.
      exports: packages added to Export-Package at the bundle's own version.
    """
    args = []
    for prefix, version_range in ranges.items():
        args += [prefix, "'%s'" % version_range]
    for prefix in optional:
        args += [prefix, "optional"]
    for namespace in drop_capabilities:
        args += [namespace, "drop-capability"]
    for package in exports:
        args += [package, "export"]
    native.genrule(
        name = name + "-widen",
        srcs = [jar],
        outs = [name + ".jar"],
        cmd = "$(location //tools/build/bazel:widen_import_range) $(location %s) $@ %s" % (jar, " ".join(args)),
        tools = ["//tools/build/bazel:widen_import_range"],
    )
    native.java_import(
        name = name,
        jars = [name + ".jar"],
        visibility = visibility,
    )
