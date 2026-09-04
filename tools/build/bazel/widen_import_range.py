#!/usr/bin/env python3
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

# Rewrites the version range of selected Import-Package clauses in a jar's
# OSGi manifest and writes the result as a new jar. Everything else in the jar
# (classes, other manifest headers, export versions) is copied verbatim.
#
# Used for third-party bundles whose manifest pins a dependency to the exact
# major version they were compiled against (bnd's default "[x.y,x+1)" policy)
# even though the bundle is binary compatible with newer releases. Re-wrapping
# such a jar with bnd would also regenerate its Export-Package versions, which
# is what the rest of the build resolves against, so a targeted manifest edit
# is safer.
#
# usage: widen_import_range.py <in.jar> <out.jar> <package-prefix> <range> [<package-prefix> <range> ...]

import re
import sys
import zipfile


def parse_manifest(raw):
    lines = []
    for line in raw.splitlines():
        if line.startswith(" ") and lines:
            lines[-1] += line[1:]
        else:
            lines.append(line)
    return [l for l in lines if l]


def format_manifest(lines):
    out = []
    for line in lines:
        data = line.encode("utf-8")
        chunks = []
        limit = 72
        while len(data) > limit:
            cut = limit
            # never split inside a multi-byte UTF-8 sequence
            while cut > 0 and (data[cut] & 0xC0) == 0x80:
                cut -= 1
            chunks.append(data[:cut])
            data = data[cut:]
            limit = 71
        chunks.append(data)
        out.append(b"\r\n ".join(chunks))
    return b"\r\n".join(out) + b"\r\n\r\n"


def split_clauses(value):
    clauses, current, quoted = [], "", False
    for ch in value:
        if ch == '"':
            quoted = not quoted
        if ch == "," and not quoted:
            clauses.append(current)
            current = ""
        else:
            current += ch
    if current:
        clauses.append(current)
    return clauses


def widen(value, rules):
    clauses = []
    for clause in split_clauses(value):
        pkg = clause.split(";")[0].strip()
        for prefix, new_range in rules:
            if pkg == prefix or pkg.startswith(prefix + "."):
                clause = re.sub(r'version="[^"]*"', 'version="%s"' % new_range, clause)
                break
        clauses.append(clause)
    return ",".join(clauses)


def main(argv):
    src, dst = argv[1], argv[2]
    rules = list(zip(argv[3::2], argv[4::2]))
    with zipfile.ZipFile(src) as zin, zipfile.ZipFile(dst, "w", zipfile.ZIP_DEFLATED) as zout:
        for item in zin.infolist():
            data = zin.read(item.filename)
            if item.filename == "META-INF/MANIFEST.MF":
                lines = parse_manifest(data.decode("utf-8"))
                for i, line in enumerate(lines):
                    if line.startswith("Import-Package:"):
                        lines[i] = "Import-Package: " + widen(line[len("Import-Package:"):].strip(), rules)
                data = format_manifest(lines)
            # keep the entry metadata, but write a fresh, reproducible timestamp
            info = zipfile.ZipInfo(item.filename, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = item.external_attr
            zout.writestr(info, data)


if __name__ == "__main__":
    main(sys.argv)
