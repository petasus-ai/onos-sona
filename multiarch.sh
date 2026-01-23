#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./make-multiarch-manifests.sh [VERSION] [IMAGE_PREFIX]
# Examples:
#   ./make-multiarch-manifests.sh v2.13.2 myregistry.example.com/myproject
# Environment (optional):
#   DELETE_REMOTE=true        # if set, the script will attempt to delete the manifest from the remote registry
#   REG_USER="username"       # registry username (required for DELETE_REMOTE)
#   REG_PASS="password"       # registry password (required for DELETE_REMOTE)
#   INSECURE_REGISTRY=true    # if set, curl uses -k (skip TLS verification) for registry API calls
#
# Notes:
# - This script uses docker CLI `manifest` subcommands.
# - If you prefer buildx/imagetools, I can produce that variant.
# - Remote deletion is best-effort and may not work for all registries; use cautiously.

VERSION="${1:-kubevirt}"
IMAGE_PREFIX="${2:-registry.gitlab.com/sonaproject}"   # e.g. "registry.example.com/project" or leave empty for local images

# normalize prefix (append trailing slash if non-empty and missing)
if [[ -n "${IMAGE_PREFIX}" && "${IMAGE_PREFIX: -1}" != "/" ]]; then
  IMAGE_PREFIX="${IMAGE_PREFIX}/"
fi

components=(
  onos-sona-nightly-docker
)

# Check docker manifest availability
if ! docker manifest --help >/dev/null 2>&1; then
  cat >&2 <<'EOF'

ERROR: `docker manifest` not found in your docker client.
Either install a Docker CLI with manifest support or use buildx imagetools.
If you'd like, I can provide a buildx-based script instead.

EOF
  exit 1
fi

echo "Creating multi-arch manifests (will first remove existing local manifest if present)"
echo "Version: ${VERSION}"
if [[ -n "${IMAGE_PREFIX}" ]]; then
  echo "Image prefix: ${IMAGE_PREFIX}"
else
  echo "Image prefix: (none) — using local/repo names"
fi

for comp in "${components[@]}"; do
  target="${IMAGE_PREFIX}${comp}:${VERSION}"
  img_amd="${IMAGE_PREFIX}${comp}:${VERSION}-amd64"
  img_arm="${IMAGE_PREFIX}${comp}:${VERSION}-arm64"

  echo
  echo "-------------------------------------------"
  echo "Component: ${comp}"
  echo "  target manifest: ${target}"
  echo "  amd64 image:     ${img_amd}"
  echo "  arm64 image:     ${img_arm}"

  # 1) Remove any existing local manifest (safe: ignore errors)
  echo "  removing existing local manifest (if any)..."
  if docker manifest rm "${target}" >/dev/null 2>&1; then
    echo "    removed local manifest: ${target}"
  else
    echo "    no local manifest to remove or removal failed (continuing)..."
  fi

  # 1b) Optional: attempt to remove remote manifest (best-effort)
  if [[ "${DELETE_REMOTE:-false}" == "true" && -n "${IMAGE_PREFIX}" ]]; then
    if [[ -z "${REG_USER:-}" || -z "${REG_PASS:-}" ]]; then
      echo "  DELETE_REMOTE enabled but REG_USER/REG_PASS unset — skipping remote deletion."
    else
      echo "  attempting remote manifest deletion (best-effort)..."
      # Build registry host and repo path
      # IMAGE_PREFIX is like: registry.host[:port]/project/ (we ensured trailing slash)
      registry_host="${IMAGE_PREFIX%%/*}"
      repo_path="${IMAGE_PREFIX#*/}"  # includes trailing slash
      # remove trailing slash from repo_path then append component-version
      repo_path="${repo_path%/}/${comp}-${VERSION}"

      accept_mime="application/vnd.docker.distribution.manifest.v2+json"
      curl_args=("-sS" "--fail" "-I")
      [[ "${INSECURE_REGISTRY:-false}" == "true" ]] && curl_args+=("-k")

      # Fetch manifest headers to obtain Docker-Content-Digest
      set +e
      headers=$(curl "${curl_args[@]}" -u "${REG_USER}:${REG_PASS}" -H "Accept: ${accept_mime}" "https://${registry_host}/v2/${repo_path}/manifests/${VERSION}" 2>/dev/null)
      rc=$?
      set -e
      if [[ $rc -ne 0 || -z "$headers" ]]; then
        echo "    failed to fetch manifest headers for remote repo ${registry_host}/v2/${repo_path}/manifests/${VERSION} — skipping remote delete."
      else
        # parse digest from headers (case-insensitive)
        digest=$(printf '%s\n' "$headers" | awk 'BEGIN{IGNORECASE=1} /Docker-Content-Digest/ {print $2}' | tr -d $'\r')
        if [[ -z "${digest}" ]]; then
          echo "    Docker-Content-Digest header not found — cannot delete by digest. Skipping remote delete."
        else
          echo "    found digest: ${digest}"
          # DELETE by digest
          set +e
          if curl "${curl_args[@]/#/-X}" "DELETE" -u "${REG_USER}:${REG_PASS}" "https://${registry_host}/v2/${repo_path}/manifests/${digest}" >/dev/null 2>&1; then
            echo "    remote manifest deleted (requested)."
          else
            echo "    remote delete request failed (registry may not allow deletion via v2 API)."
          fi
          set -e
        fi
      fi
    fi
  fi

  # 2) (Optional) try to pull source images so manifest create won't fail unexpectedly
  echo "  pulling source images (best-effort)..."
  set +e
  docker pull "${img_amd}" >/dev/null 2>&1
  rc_amd=$?
  docker pull "${img_arm}" >/dev/null 2>&1
  rc_arm=$?
  set -e
  if [[ $rc_amd -ne 0 ]]; then
    echo "    WARNING: failed to pull ${img_amd} (it may not exist or auth required). Continuing..."
  fi
  if [[ $rc_arm -ne 0 ]]; then
    echo "    WARNING: failed to pull ${img_arm} (it may not exist or auth required). Continuing..."
  fi

  # 3) create manifest list
  echo "  creating manifest list..."
  docker manifest create "${target}" "${img_amd}" "${img_arm}"

  # 4) annotate properly
  echo "  annotating manifest entries..."
  docker manifest annotate "${target}" "${img_amd}" --os linux --arch amd64 || true
  docker manifest annotate "${target}" "${img_arm}" --os linux --arch arm64 || true

  # 5) push the manifest list
  echo "  pushing manifest ${target}..."
  docker manifest push "${target}"

  echo "  manifest inspect (summary):"
  docker manifest inspect "${target}" --verbose || true

  echo "  DONE: ${target}"
done

echo
echo "All done. Multi-arch manifests recreated for version ${VERSION}."

