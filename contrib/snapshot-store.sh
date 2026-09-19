#!/usr/bin/env bash
#
# chronicle-snapshot-store — a cold, consistent copy of this host's
# chronicle state to object storage (chronicle-hq/02-DESIGN/09-hosted-
# environment.md § the operational floor). One archive per run, checksummed,
# optionally encrypted, uploaded with rclone, and old ones pruned.
#
# Environment (see contrib/systemd/chronicle-snapshot.service):
#   SNAPSHOT_PATHS          space-separated absolute dirs to archive (required)
#   SNAPSHOT_REMOTE         rclone destination, e.g. scaleway:bucket/prefix (required)
#   SNAPSHOT_STOP_UNIT      systemd unit stopped around the archive — a NATS
#                           node's nats-server.service; empty archives hot
#   SNAPSHOT_AGE_RECIPIENT  age public key; when set the archive is encrypted
#                           before upload — mandatory for any dir holding seeds
#   SNAPSHOT_KEEP_DAYS      prune remote archives older than this (default 30)
#   SNAPSHOT_NAME           archive stem (default: the hostname)
#
# Restoring is the drill the design demands, run by hand: fetch the
# archive, verify its checksum, decrypt when encrypted, stop the unit,
# move the live dirs aside, `tar -C / -xzf`, restore ownership, start, and
# run the verification read.
set -euo pipefail

: "${SNAPSHOT_PATHS:?space-separated dirs to archive}"
: "${SNAPSHOT_REMOTE:?rclone destination, e.g. remote:bucket/prefix}"
STOP_UNIT="${SNAPSHOT_STOP_UNIT:-}"
RECIPIENT="${SNAPSHOT_AGE_RECIPIENT:-}"
KEEP_DAYS="${SNAPSHOT_KEEP_DAYS:-30}"
NAME="${SNAPSHOT_NAME:-$(hostname -s)}"

for p in $SNAPSHOT_PATHS; do
  [ -d "$p" ] || { echo "snapshot: $p is not a directory" >&2; exit 1; }
done
command -v rclone >/dev/null || { echo "snapshot: rclone is not installed" >&2; exit 1; }
if [ -n "$RECIPIENT" ]; then
  command -v age >/dev/null || { echo "snapshot: age is not installed but SNAPSHOT_AGE_RECIPIENT is set" >&2; exit 1; }
fi

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
work="$(mktemp -d)"
archive="$work/$NAME-$stamp.tar.gz"
stopped=0

cleanup() {
  if [ "$stopped" = 1 ]; then systemctl start "$STOP_UNIT"; fi
  rm -rf "$work"
}
trap cleanup EXIT

# Relative members so the archive restores with `tar -C /`.
members=()
for p in $SNAPSHOT_PATHS; do members+=("${p#/}"); done

if [ -n "$STOP_UNIT" ]; then
  systemctl stop "$STOP_UNIT"
  stopped=1
fi
tar -C / -czf "$archive" "${members[@]}"
if [ "$stopped" = 1 ]; then
  systemctl start "$STOP_UNIT"
  stopped=0
fi

if [ -n "$RECIPIENT" ]; then
  age -r "$RECIPIENT" -o "$archive.age" "$archive"
  rm -f "$archive"
  archive="$archive.age"
fi
(cd "$work" && sha256sum "$(basename "$archive")" > "$(basename "$archive").sha256")

rclone copy --checksum "$work" "$SNAPSHOT_REMOTE/"
rclone delete --min-age "${KEEP_DAYS}d" "$SNAPSHOT_REMOTE/"

size="$(du -h "$archive" | cut -f1)"
echo "snapshot ok: $(basename "$archive") ($size) → $SNAPSHOT_REMOTE, kept ${KEEP_DAYS}d"
