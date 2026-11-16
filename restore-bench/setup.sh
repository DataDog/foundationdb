#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"

# Read-only preflight: kubectl and Atlas access, request validation (Atlas --dry-run),
# and, if $FDB_CLUSTER already exists, its capacity and the backup to restore.
BACKUP_SIZE_GIB="${BACKUP_SIZE_GIB:-200}"

log "kubectl contexts: ${KUBE_CONTEXT_LIST[*]} (namespace $NS)"
if [ "${#KUBE_CONTEXT_LIST[@]}" -ne "$EXPECTED_ZONES" ]; then
  log "WARNING: $FDB_CLUSTER spans $EXPECTED_ZONES zonal k8s clusters but ${#KUBE_CONTEXT_LIST[@]} context(s) are configured: create/run/delete will refuse until all are authorized and listed in KUBE_CONTEXTS"
fi
log "kubectl: pods of $FDB_CLUSTER"
existing="$(fdb_pods)" || die "pod discovery failed in at least one zone"
echo "${existing:-none}"

log "atlas: workflow types on $ATLAS_TASK_QUEUE ($ATLAS_CONTEXT)"
types="$(atlas --context "$ATLAS_CONTEXT" workflow list-types --task-queue "$ATLAS_TASK_QUEUE" --output json)"
for t in "$ATLAS_CREATE_TYPE" "$ATLAS_DELETE_TYPE"; do
  echo "$types" | grep -q "$t" || die "workflow type $t not registered on $ATLAS_TASK_QUEUE"
  echo "ok: $t"
done

for kind in create delete; do
  log "atlas: dry-run $kind request"
  if [ "$kind" = create ]; then type="$ATLAS_CREATE_TYPE" input="$(create_cluster_input)"; else type="$ATLAS_DELETE_TYPE" input="$(delete_cluster_input)"; fi
  echo "$input" | jq .
  atlas --context "$ATLAS_CONTEXT" workflow start --dry-run \
    --workflow-type "$type" --task-queue "$ATLAS_TASK_QUEUE" --input "$input"
done

if [ -z "$existing" ]; then
  log "$FDB_CLUSTER does not exist: skipping capacity and backup checks (run with KEEP_CLUSTER=1 to inspect one)"
  exit 0
fi

st="$(status_json)"
log "capacity"
echo "$st" | jq --argjson gib "$BACKUP_SIZE_GIB" --arg r "$REDUNDANCY" '
  .cluster as $c
  | ([$c.processes[].roles[] | select(.role == "storage")]) as $ss
  | ({double: 2, triple: 3, three_data_hall: 3}[$r] // 3) as $rf
  | {
      benchmark_redundancy: $r,
      configuration: $c.configuration,
      fdb_version: ([$c.processes[].version] | unique),
      machines: ([$c.processes[].machine_id] | unique | length),
      storage_servers: ($ss | length),
      kvstore_total_gib: (($ss | map(.kvstore_total_bytes) | add) / 1073741824 | floor),
      kvstore_available_gib: (($ss | map(.kvstore_available_bytes) | add) / 1073741824 | floor),
      needed_gib_with_30pct_headroom: ($gib * $rf * 1.3 | floor)
    }'

tb="$(first_toolbox)"
log "backup status of $FDB_CLUSTER"
toolbox_api "$tb" GET /backup/status | jq -c '{running, state, url}'
log "describe $BACKUP_FOLDER"
toolbox_api "$tb" GET "/backup/describe?backup=$BACKUP_FOLDER" | jq '.'
