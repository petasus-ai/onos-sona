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

def widened_import_jar(name, jar, ranges, visibility = ["//visibility:public"]):
    """Copies a third-party OSGi bundle, widening selected Import-Package version ranges.

    Args:
      name: target name; also usable as a java_library dependency.
      jar: label of the original jar (e.g. "@atomix//jar").
      ranges: dict of package prefix -> new OSGi version range, e.g.
              {"com.google.common": "[22.0,34)"}.
    """
    args = []
    for prefix, version_range in ranges.items():
        args += [prefix, "'%s'" % version_range]
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
