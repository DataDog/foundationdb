#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"

[ $# -ge 1 ] || die "usage: run.sh <variant> [label]"
variant="$1"
label="${2:-}"
MAX_RESTORE_HOURS="${MAX_RESTORE_HOURS:-12}"
[ -n "${BACKUP_FOLDER:-}" ] || die "BACKUP_FOLDER not set in bench.conf (see setup.sh)"
load_variant "$variant"

run_dir="$BENCH_DIR/runs/$(date -u +%Y%m%dT%H%M%SZ)-$variant-$REDUNDANCY-$ENGINE${label:+-$label}"
mkdir -p "$run_dir"
exec > >(tee -a "$run_dir/run.log") 2>&1
log "run dir: $run_dir"

require_all_zones
# create.sh creates this marker right before starting the Atlas creation. Without it the cluster
# was not created by this run (e.g. it already existed) and cleanup must not touch it.
export CLUSTER_OWNED_MARKER="$run_dir/cluster-owned"
cleanup() {
  if [ ! -f "$CLUSTER_OWNED_MARKER" ]; then
    log "cleanup: $FDB_CLUSTER was not created by this run, leaving it untouched"
  elif [ "${KEEP_CLUSTER:-0}" = 1 ]; then
    set_rebalance on || true
    resume_toolboxes
    log "KEEP_CLUSTER=1: leaving $FDB_CLUSTER running"
  elif "$BENCH_DIR/delete.sh"; then
    rm -f "$CLUSTER_OWNED_MARKER"
  else
    log "WARNING: deleting $FDB_CLUSTER failed, run delete.sh manually"
  fi
}
trap cleanup EXIT
"$BENCH_DIR/create.sh" "$variant"
set_rebalance "$DD_REBALANCE"
ensure_no_backup || log "WARNING: a backup was running before the restore, aborted"

rs="$(restore_status)" || die "cannot read the restore status before starting"
prev_uid="$(printf '%s\n' "$rs" | jq -r '.uid // empty')"
status_json >"$run_dir/status-start.json" || log "WARNING: could not save the status before the restore"

restore_args=(-F "backup=$BACKUP_FOLDER" -F dry_run=false)
[ -n "${RESTORE_VERSION:-}" ] && restore_args+=(-F "version=$RESTORE_VERSION")
tb="$(first_toolbox)"
# Every toolbox (one per zone) must acknowledge the pause before the restore clears the database.
pause_toolboxes || { resume_toolboxes; die "could not pause every toolbox, restore not started"; }
start_ts="$(date +%s)"
# Never retried: after an error the restore may or may not have started.
if ! resp="$(toolbox_api "$tb" POST /restore/start "${restore_args[@]}")"; then
  printf '%s\n' "$resp" >"$run_dir/restore-start.json"
  resume_toolboxes
  die "restore start returned an error (not retried): ${resp:0:500}"
fi
resume_toolboxes
printf '%s\n' "$resp" >"$run_dir/restore-start.json"
printf '%s\n' "$resp" | jq -e '.success == true' >/dev/null 2>&1 || die "restore start failed: ${resp:0:500}"
# fdbrestore prints the version it restores to, e.g. "Ussing target restore version 53348886043238"
# (sic) when it picks the latest, or the given -v version.
restore_version="$(printf '%s\n' "$resp" | jq -r '.message // ""' | grep -oiE 'restore version [0-9]+' | grep -oE '[0-9]+$' | tail -1 || true)"
restore_version="${restore_version:-${RESTORE_VERSION:-}}"
[ -n "$restore_version" ] || die "could not determine the restore version from: $(printf '%s\n' "$resp" | jq -c .)"
if [ -n "${RESTORE_VERSION:-}" ] && [ "$restore_version" != "$RESTORE_VERSION" ]; then
  die "restore uses version $restore_version, expected RESTORE_VERSION=$RESTORE_VERSION"
fi
log "restore started at version $restore_version"
[ -n "${RESTORE_VERSION:-}" ] || record_series_version "$restore_version"

jq -n \
  --arg variant "$variant" --arg label "$label" --arg backup "$BACKUP_FOLDER" \
  --argjson version "$restore_version" --arg rebalance "$DD_REBALANCE" \
  --arg fdb "$FDB_VERSION" --argjson start "$start_ts" --arg redundancy "$REDUNDANCY" --arg engine "$ENGINE" \
  --args '{variant: $variant, label: $label, redundancy: $redundancy, engine: $engine, backup: $backup, restore_version: $version,
    knobs: $ARGS.positional, rebalance: $rebalance, fdb_version: $fdb, start_ts: $start}' \
  "${KNOBS[@]}" >"$run_dir/meta.json"

echo "ts,state,blocks_finished,blocks_total,blocks_in_progress,bytes_written,apply_version_lag,current_version,last_error" >"$run_dir/restore.csv"
echo "ts,generation,dd_state,partitions,kv_bytes,moving_in_flight,moving_in_queue,moving_written,limited_by,tps_limit,released_tps,worst_ss_queue,worst_ss_durability_lag_s,worst_tlog_queue,bytes_written_hz,ss_input_hz_max,ss_input_hz_median,ss_input_hz_mean" >"$run_dir/cluster.csv"

polls=0
while true; do
  now="$(date +%s)"
  polls=$((polls + 1))
  if [ $((polls % 20)) -eq 0 ]; then
    ensure_no_backup || log "WARNING: a backup started during the restore, aborted"
  fi
  # A failed or malformed sample (transport error, HTTP error, no "state") is logged and skipped:
  # monitoring gaps are tolerated, only a reported "aborted" restore ends the run.
  if rs="$(restore_status)" && line="$(printf '%s\n' "$rs" | jq -r --arg ts "$now" \
    '[$ts, (.state | ascii_downcase), .blocksFinished, .blocksCount, .blocksInProgress, .bytesWritten,
      .applyVersionLag, .currentVersion, .lastError] | @csv' 2>/dev/null)"; then
    printf '%s\n' "$line" >>"$run_dir/restore.csv"
  else
    log "restore status sample skipped"
    rs='{}'
  fi
  if st="$(status_json 2>/dev/null)" && line="$(printf '%s\n' "$st" | jq -r --arg ts "$now" '.cluster as $c
      | ([$c.processes[].roles[] | select(.role == "storage") | .input_bytes.hz // 0] | sort) as $in
      | [$ts, $c.generation, $c.data.state.name,
      $c.data.partitions_count, $c.data.total_kv_size_bytes, $c.data.moving_data.in_flight_bytes,
      $c.data.moving_data.in_queue_bytes, $c.data.moving_data.total_written_bytes,
      $c.qos.performance_limited_by.name, $c.qos.transactions_per_second_limit,
      $c.qos.released_transactions_per_second, $c.qos.worst_queue_bytes_storage_server,
      $c.qos.worst_durability_lag_storage_server.seconds, $c.qos.worst_queue_bytes_log_server,
      $c.workload.bytes.written.hz, ($in | max),
      (if ($in | length) > 0 then $in[($in | length / 2 | floor)] else null end),
      (if ($in | length) > 0 then ($in | add) / ($in | length) else null end)] | @csv' 2>/dev/null)"; then
    printf '%s\n' "$line" >>"$run_dir/cluster.csv"
  else
    log "cluster status sample skipped"
  fi

  uid="$(printf '%s\n' "$rs" | jq -r '.uid // empty' 2>/dev/null || true)"
  state="$(printf '%s\n' "$rs" | jq -r '.state // "" | ascii_downcase' 2>/dev/null || true)"
  if [ -n "$uid" ] && [ "$uid" != "$prev_uid" ]; then
    case "$state" in
      completed) break ;;
      aborted) die "restore aborted: $(printf '%s\n' "$rs" | jq -r '.lastError // ""' 2>/dev/null)" ;;
    esac
  fi
  [ $((now - start_ts)) -lt $((MAX_RESTORE_HOURS * 3600)) ] || die "restore still running after ${MAX_RESTORE_HOURS}h"
  sleep "$POLL_SECONDS"
done

end_ts="$(date +%s)"
log "restore completed in $((end_ts - start_ts))s"
ensure_no_backup || log "WARNING: a backup was running at the end of the restore, aborted"
status_json >"$run_dir/status-end.json" || log "WARNING: could not save the status after the restore"
jq --argjson end "$end_ts" '.end_ts = $end' "$run_dir/meta.json" >"$run_dir/meta.json.tmp"
mv "$run_dir/meta.json.tmp" "$run_dir/meta.json"
"$BENCH_DIR/summarize.sh" "$run_dir"
