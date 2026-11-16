#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"

[ $# -eq 1 ] || die "usage: create.sh <variant>"
load_variant "$1"
require_all_zones
# Ownership requires positive proof of absence (no pods, no StatefulSet/Deployment) in every zone:
# a discovery failure in any zone refuses instead of being read as "absent".
cluster_absent || die "$FDB_CLUSTER exists in $NS or its absence couldn't be checked in every zone: not touching it"

# From here on this run owns the cluster, including a partially provisioned one: the caller's
# cleanup only deletes a cluster whose ownership marker exists (run.sh).
if [ -n "${CLUSTER_OWNED_MARKER:-}" ]; then touch "$CLUSTER_OWNED_MARKER"; fi
atlas_run "$ATLAS_CREATE_TYPE" "restore-bench-create-$FDB_CLUSTER-$(date +%s)" "$(create_cluster_input)"

# A failed discovery counts as "not ready yet" (pod_count fails), never as a match.
fdb_ready_count() { local n; n="$(pod_count foundationdb ready)" && [ "$n" -eq "$FDB_REPLICAS" ]; }
wait_for "$FDB_REPLICAS fdb pods across ${KUBE_CONTEXT_LIST[*]}" 3600 fdb_ready_count
toolboxes_ready() { local n; n="$(pod_count foundationdb-toolbox ready)" && [ "$n" -eq "$TOOLBOX_REPLICAS" ]; }
wait_for "$TOOLBOX_REPLICAS toolbox pods (one per zonal k8s cluster)" 1800 toolboxes_ready
wait_for "database healthy" 3600 db_healthy

enough_coordinators() {
  status_json | jq -e --argjson n "$EXPECTED_COORDINATORS" '(.client.coordinators.coordinators | length) >= $n' >/dev/null 2>&1
}
wait_until 900 enough_coordinators ||
  log "WARNING: fewer than $EXPECTED_COORDINATORS coordinators (toolbox target for $REDUNDANCY) after 15 min, continuing"

apply_knobs

# The toolbox reconciler may still change process classes or coordinators, each causing a recovery.
last_gen=""
stable=0
cluster_stable() {
  local s gen moving
  s="$(status_json 2>/dev/null)" || return 1
  gen="$(echo "$s" | jq -r '.cluster.generation')"
  moving="$(echo "$s" | jq -r '(.cluster.data.moving_data.in_flight_bytes // 0) + (.cluster.data.moving_data.in_queue_bytes // 0)')"
  if [ "$gen" = "$last_gen" ] && [ "$moving" = 0 ]; then stable=$((stable + 1)); else stable=0; fi
  last_gen="$gen"
  log "stability: generation $gen, moving bytes $moving, stable polls $stable/8"
  [ "$stable" -ge 8 ]
}
wait_for "cluster to stabilize (no recovery, no data movement for 2 min)" 1800 cluster_stable

verify_cluster || die "cluster does not match variant $1 ($REDUNDANCY, $ENGINE)"
ensure_no_backup || log "WARNING: a backup was running on the fresh cluster, aborted"
log "cluster $FDB_CLUSTER ready with variant $1 ($REDUNDANCY, $ENGINE): ${KNOBS[*]:-no knobs}"
