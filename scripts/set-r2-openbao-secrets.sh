#!/usr/bin/env bash
# Store the Cloudflare R2 credentials for the SkyBazaar cold tier in OpenBao.
#
# Writes S3__ACCESS_KEY and S3__SECRET_KEY into the KV-v2 leaf kv/sky/sky-bazaar
# of the secrets-cluster OpenBao by calling dev/ansible's
# scripts/set-openbao-kv-secret.sh, which prompts for each value with hidden
# input (nothing lands in argv or shell history), merges into the leaf with a
# CAS patch (other keys are kept), refuses to overwrite existing keys unless
# --overwrite is given, mints a kv-write token scoped to this leaf and revokes
# it afterwards. Only key names are ever printed.
#
# Non-secret settings (S3__ENDPOINT, S3__BUCKET, S3__REGION, S3__USE_PATH_STYLE,
# HISTORY_ARCHIVE__*) live in git: hypixel/k8s/main/sky/chart/talos.yaml.
#
# Prerequisites (see dev/ansible/scripts/set-openbao-kv-secret.sh --help):
#   - the disk with the eu-cluster-openbao-init-*.json admin credential mounted
#   - the userspace Tailscale proxy on 127.0.0.1:1055 running
#     (from dev/ansible: scripts/manage-local-tailscale.sh start)
#
# Usage: scripts/set-r2-openbao-secrets.sh [--overwrite] [--print]
#            [--ansible-dir DIR] [--leaf MOUNT/PATH]
set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ansible_dir="${ANSIBLE_DIR:-${SCRIPT_DIR}/../../../ansible}"
leaf="kv/sky/sky-bazaar"
overwrite=""
print_only="false"
readonly -a KEYS=(S3__ACCESS_KEY S3__SECRET_KEY)

usage() {
  sed -n '2,/^set -euo/p' "${BASH_SOURCE[0]}" | sed '$d' | sed 's/^# \{0,1\}//'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --overwrite) overwrite="--overwrite"; shift ;;
    --print) print_only="true"; shift ;;
    --ansible-dir) ansible_dir="$2"; shift 2 ;;
    --leaf) leaf="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'Unknown argument: %s\n' "$1" >&2; usage >&2; exit 1 ;;
  esac
done

[[ "${leaf}" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*/[A-Za-z0-9][A-Za-z0-9._/-]*$ && "${leaf}" != *..* ]] \
  || { printf 'Unsafe --leaf: %s\n' "${leaf}" >&2; exit 1; }
ansible_dir="$(cd "${ansible_dir}" 2>/dev/null && pwd)" \
  || { printf 'ansible repo not found; pass --ansible-dir or set ANSIBLE_DIR\n' >&2; exit 1; }
setter="${ansible_dir}/scripts/set-openbao-kv-secret.sh"
[[ -x "${setter}" ]] || { printf 'Missing or not executable: %s\n' "${setter}" >&2; exit 1; }

printf 'OpenBao leaf:   %s\n' "${leaf}"
printf 'Keys to set:    %s\n' "${KEYS[*]}"
printf 'Overwrite:      %s\n' "${overwrite:+yes}${overwrite:-no}"
printf 'Command (run from %s):\n  scripts/set-openbao-kv-secret.sh %s%s %s\n' \
  "${ansible_dir}" "${overwrite:+${overwrite} }" "${leaf}" "${KEYS[*]}"
if [[ "${print_only}" == "true" ]]; then
  exit 0
fi

printf '\nPaste the bucket-scoped R2 token (Object Read & Write on skybazaar-cold) when prompted; input is hidden.\n\n'
cd "${ansible_dir}"
# Each key name without "=" makes the setter prompt for a hidden value.
scripts/set-openbao-kv-secret.sh ${overwrite} "${leaf}" "${KEYS[@]}"

cat <<MSG

Done. sky-bazaar reads OpenBao only at startup: restart the deployment on talos-eu
  kubectl --context talos-eu -n sky rollout restart deployment/sky-bazaar
and check the log line "S3 cold-tier enabled: bucket=skybazaar-cold".
MSG
