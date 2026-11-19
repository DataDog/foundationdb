#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"

require_all_zones
[ -n "${DELETE_GATES:-}" ] || log "deletion uses the worker's default LOCK_DB gate: Slack prompt up to 15 min, then the database stays locked 2 h before deletion"
atlas_run "$ATLAS_DELETE_TYPE" "restore-bench-delete-$FDB_CLUSTER-$(date +%s)" "$(delete_cluster_input)"

# Done only once absence (pods and workloads) is positively established in every zone.
wait_for "$FDB_CLUSTER pods and workloads to be gone in every zone" 1800 cluster_absent
log "cluster $FDB_CLUSTER deleted"
