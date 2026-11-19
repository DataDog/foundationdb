#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"

repeat=1
if [ "${1:-}" = "-n" ]; then
  repeat="$2"
  shift 2
fi
[ $# -ge 1 ] || die "usage: bench.sh [-n repeat] <variant>..."
for v in "$@"; do load_variant "$v"; done
require_all_zones

# All runs reuse the series restore version (runs/series-restore-version, see record_series_version
# in lib.sh), whether it was recorded by an earlier standalone run or by this invocation's first run.
if [ -n "${RESTORE_VERSION:-}" ]; then
  log "series restore version: $RESTORE_VERSION"
else
  log "no series yet: the first run records the latest restorable version for all later runs"
fi
for i in $(seq 1 "$repeat"); do
  for v in "$@"; do
    "$BENCH_DIR/run.sh" "$v" "r$i"
  done
done
column -s, -t <"$BENCH_DIR/runs/results.csv"
