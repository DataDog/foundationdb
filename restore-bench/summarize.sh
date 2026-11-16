#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"

d="${1:-}"
[ -f "$d/meta.json" ] || die "usage: summarize.sh <run-dir>"
start="$(jq -r '.start_ts' "$d/meta.json")"
end="$(jq -r '.end_ts // empty' "$d/meta.json")"
[ -n "$end" ] || die "run not completed: $d"

read -r restore_bytes max_apply_lag < <(awk -F, 'NR > 1 { gsub(/"/, ""); bw = $6; if ($7 > ml) ml = $7 } END { print bw + 0, ml + 0 }' "$d/restore.csv")

# Storage write skew: busiest storage server's input rate / mean input rate across storage servers.
# The mean (not the median) keeps samples where traffic is concentrated on a minority of servers
# (median 0, max > 0), which are counted separately as "concentrated". Idle samples (max 0) are skipped.
read -r gen_delta dd_moved part_max part_end ss_queue_max ss_lag_max tlog_queue_max skew_avg skew_max concentrated_pct limited < <(awk -F, '
  NR > 1 {
    gsub(/"/, ""); n++
    if (n == 1) { g0 = $2; wprev = $8 }
    # moving_written resets when data distribution restarts: add the new value instead of a negative delta.
    if ($8 != "") { moved += ($8 >= wprev) ? $8 - wprev : $8; wprev = $8 }
    g1 = $2; pend = $4
    if ($4 > pmax) pmax = $4
    if ($12 > q) q = $12
    if ($13 > lag) lag = $13
    if ($14 > tq) tq = $14
    lim[$9 == "" ? "none" : $9]++
    if ($16 > 0 && $18 > 0) {
      busy++
      r = $16 / $18; rs += r; if (r > rmax) rmax = r
      if ($17 == 0) conc++
    }
  }
  END {
    s = ""
    for (k in lim) s = s (s == "" ? "" : ";") k ":" int(lim[k] * 100 / n) "%"
    printf "%.0f %.0f %.0f %.0f %.0f %s %.0f %.2f %.2f %.0f %s\n", g1 - g0, moved, pmax, pend, q, lag + 0, tq,
      (busy ? rs / busy : 0), rmax, (busy ? conc * 100 / busy : 0), s
  }' "$d/cluster.csv")

# Sum of a Datadog count metric over the run: {"value": <number|null>, "status": "ok|no_data|error"}.
# pup keeps the API's top-level "data": v2 timeseries values are under data.attributes.values
# (pup may also print them flattened under data.values). An empty result is "no_data", not 0.
pup_sum() {
  local out
  # shellcheck disable=SC2086
  if ! out="$(pup $PUP_ARGS metrics query --query "$1" --from "$start" --to "$end" 2>/dev/null)"; then
    echo '{"value": null, "status": "error"}'
    return
  fi
  echo "$out" | jq -c '
    if .status != "success" then {value: null, status: "error"}
    else ((.data.attributes // .data).values) as $v
      | ([($v // [])[] | arrays | .[] | select(. != null)]) as $points
      | if ($v | type) != "array" then {value: null, status: "error"}
        elif ($points | length) == 0 then {value: null, status: "no_data"}
        else {value: ($points | add), status: "ok"} end
    end' 2>/dev/null || echo '{"value": null, "status": "error"}'
}
fetched="$(pup_sum "sum:fdb.vector.storage_metrics.bytes_fetched{$DD_FILTER}.as_count()")"
ingested="$(pup_sum "sum:fdb.vector.storage_metrics.bytes_input{$DD_FILTER}.as_count()")"

jq -n \
  --slurpfile meta "$d/meta.json" \
  --argjson duration "$((end - start))" --argjson restore_bytes "$restore_bytes" \
  --argjson max_apply_lag "$max_apply_lag" --argjson gen_delta "$gen_delta" \
  --argjson dd_moved "$dd_moved" --argjson part_max "$part_max" --argjson part_end "$part_end" \
  --argjson ss_queue_max "$ss_queue_max" --argjson ss_lag_max "$ss_lag_max" \
  --argjson tlog_queue_max "$tlog_queue_max" --arg limited "$limited" \
  --argjson skew_avg "$skew_avg" --argjson skew_max "$skew_max" --argjson concentrated_pct "$concentrated_pct" \
  --argjson fetched "$fetched" --argjson ingested "$ingested" --arg dd_filter "$DD_FILTER" '
  $meta[0] + {
    duration_s: $duration,
    restore_bytes: $restore_bytes,
    throughput_mib_s: (if $duration > 0 then $restore_bytes / $duration / 1048576 | . * 100 | round / 100 else null end),
    max_apply_lag_s: ($max_apply_lag / 1e6),
    recoveries_generation_delta: $gen_delta,
    dd_moved_bytes: $dd_moved,
    dd_moved_per_restored_byte: (if $restore_bytes > 0 then $dd_moved / $restore_bytes | . * 1000 | round / 1000 else null end),
    partitions_max: $part_max,
    partitions_end: $part_end,
    worst_ss_queue_max_bytes: $ss_queue_max,
    worst_ss_durability_lag_max_s: $ss_lag_max,
    worst_tlog_queue_max_bytes: $tlog_queue_max,
    ss_input_skew_avg: $skew_avg,
    ss_input_skew_max: $skew_max,
    ss_input_concentrated_pct: $concentrated_pct,
    limited_by: $limited,
    dd_filter: $dd_filter,
    ss_bytes_fetched: $fetched.value,
    ss_bytes_input: $ingested.value,
    metrics_status: (if $fetched.status == "ok" and $ingested.status == "ok" then "ok" else "fetched:\($fetched.status),input:\($ingested.status)" end),
    fetched_per_input: (if ($fetched.value | type) == "number" and ($ingested.value | type) == "number" and $ingested.value > 0
      then $fetched.value / $ingested.value | . * 1000 | round / 1000 else null end)
  }' >"$d/summary.json"

results="$BENCH_DIR/runs/results.csv"
fields='["run","variant","redundancy","engine","label","restore_version","duration_s","throughput_mib_s","max_apply_lag_s","dd_moved_bytes","dd_moved_per_restored_byte","fetched_per_input","partitions_max","partitions_end","worst_ss_queue_max_bytes","worst_ss_durability_lag_max_s","worst_tlog_queue_max_bytes","ss_input_skew_avg","ss_input_skew_max","ss_input_concentrated_pct","recoveries_generation_delta","limited_by","metrics_status"]'
[ -f "$results" ] || jq -rn --argjson f "$fields" '$f | @csv' >"$results"
jq -r --arg run "$(basename "$d")" --argjson f "$fields" '. + {run: $run} | [$f[] as $k | .[$k]] | @csv' "$d/summary.json" >>"$results"

jq . "$d/summary.json"
log "appended to $results"
