#!/usr/bin/env bash
set -euo pipefail

NS=hmq-test

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FDB_VERSION=7.3.77
FDB_REPLICAS=9
TOOLBOX_PORT=8080
TOOLBOX_CLUSTER_FILE=/etc/foundationdb/fdb.cluster
POLL_SECONDS="${POLL_SECONDS:-15}"
PUP_ARGS="${PUP_ARGS:-}"

if [ -f "$BENCH_DIR/bench.conf" ]; then source "$BENCH_DIR/bench.conf"; fi
: "${FDB_CLUSTER:?FDB_CLUSTER must be set in bench.conf}"
# The foundationdb-worker runs on the prod Atlas cluster; the task queue selects the target datacenter.
ATLAS_CONTEXT="${ATLAS_CONTEXT:-prod}"
DATACENTER="${DATACENTER:-us1.staging.dog}"
KUBE_CLUSTER="${KUBE_CLUSTER:-oddish-a}"
ATLAS_TASK_QUEUE="foundationdb/$DATACENTER"
# The worker expands a zonal k8s cluster (oddish-a) to its zonal set (oddish-a/b/c), spreads
# unsetReplicas evenly across it and deploys one toolbox per k8s cluster
# (foundationdb-worker pkg/config/worker.go GetZonalTargets, create_cluster/deploy_executor.go).
# Every zonal cluster must be reachable to count pods, apply knobs and pause toolboxes.
EXPECTED_ZONES="${EXPECTED_ZONES:-3}"
KUBE_CONTEXTS="${KUBE_CONTEXTS:-oddish-a.us1.staging.dog}"
# Arrays are not exported to child scripts: keep KUBE_CONTEXTS as a string, split it here.
read -r -a KUBE_CONTEXT_LIST <<<"$KUBE_CONTEXTS"
TOOLBOX_REPLICAS="$EXPECTED_ZONES"
# Pods are named <cluster>-unset-<n> and <cluster>-toolbox-<hash>.
RELEASE_PREFIX="$FDB_CLUSTER-"
# Vector metrics carry the pod's cluster and datacenter tags (used by the FoundationDB dashboards).
DD_FILTER="${DD_FILTER:-cluster:$FDB_CLUSTER,datacenter:$DATACENTER}"

# Per-run overrides, e.g. ENGINE=ssd-redwood-1 ./run.sh baseline
REDUNDANCY="${REDUNDANCY:-${CLUSTER_REDUNDANCY:-three_data_hall}}"
ENGINE="${ENGINE:-${CLUSTER_ENGINE:-ssd}}"
# Coordinators: the toolbox reconciler's target for each redundancy (toolbox foundationdb/status.py:95-107).
case "$REDUNDANCY" in
  double) REDUNDANCY_ENUM=REDUNDANCY_MODE_DOUBLE EXPECTED_COORDINATORS=3 ;;
  triple) REDUNDANCY_ENUM=REDUNDANCY_MODE_TRIPLE EXPECTED_COORDINATORS=5 ;;
  three_data_hall) REDUNDANCY_ENUM=REDUNDANCY_MODE_THREE_DATA_HALL EXPECTED_COORDINATORS=9 ;;
  *) printf 'ERROR: REDUNDANCY must be double, triple or three_data_hall (got %s)\n' "$REDUNDANCY" >&2; exit 1 ;;
esac
# status json reports canonical engine names (FDBTypes.cpp:220-244).
case "$ENGINE" in
  ssd) ENGINE_ENUM=STORAGE_ENGINE_SSD EXPECTED_ENGINE=ssd-2 ;;
  ssd-redwood-1) ENGINE_ENUM=STORAGE_ENGINE_REDWOOD EXPECTED_ENGINE=ssd-redwood-1 ;;
  *) printf 'ERROR: ENGINE must be ssd or ssd-redwood-1 (got %s)\n' "$ENGINE" >&2; exit 1 ;;
esac

log() { printf '%s %s\n' "$(date -u +%FT%TZ)" "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

# kubectl restricted to the configured contexts and to namespace $NS.
kcx() {
  local ctx="$1" a ok=0 c
  shift
  for c in "${KUBE_CONTEXT_LIST[@]}"; do [ "$c" = "$ctx" ] && ok=1; done
  [ "$ok" = 1 ] || die "kcx: context $ctx is not in KUBE_CONTEXTS"
  for a in "$@"; do
    [ "$a" = "--" ] && break
    case "$a" in
      --context* | --namespace* | -n | -A | --all-namespaces | --kubeconfig* | --cluster*)
        die "kcx: refusing to override context/namespace ($a)" ;;
    esac
  done
  kubectl --context "$ctx" --namespace "$NS" "$@"
}

# Refuses to operate on a partial view of the cluster (see EXPECTED_ZONES).
require_all_zones() {
  [ "${#KUBE_CONTEXT_LIST[@]}" -eq "$EXPECTED_ZONES" ] ||
    die "KUBE_CONTEXTS lists ${#KUBE_CONTEXT_LIST[@]} context(s) but $FDB_CLUSTER spans $EXPECTED_ZONES zonal k8s clusters: kubectl access to all of them must be authorized and listed in bench.conf"
}

# Discovery functions fail (non-zero) if any zone can't be inspected: an empty output only means
# "nothing found" when the function succeeded. Callers must check the status, e.g.
# `pods="$(fdb_pods)" || die ...`, never `[ -z "$(fdb_pods)" ]`.

# hmq-test hosts other clusters: only consider pods of $FDB_CLUSTER. Prints "<context>/<pod>".
pods_with_container() {
  local ctx ready="${2:-}" json names all=""
  for ctx in "${KUBE_CONTEXT_LIST[@]}"; do
    json="$(kcx "$ctx" get pods -o json)" || { log "pod discovery failed in $ctx"; return 1; }
    names="$(printf '%s\n' "$json" | jq -r --arg c "$1" --arg p "$RELEASE_PREFIX" --arg ctx "$ctx" --arg ready "$ready" '
      .items[]
      | select(.metadata.name | startswith($p))
      | select(any(.spec.containers[]; .name == $c))
      | select($ready == "" or any(.status.conditions[]?; .type == "Ready" and .status == "True"))
      | "\($ctx)/\(.metadata.name)"')" || { log "unreadable pod list from $ctx"; return 1; }
    all="$all$names"$'\n'
  done
  printf '%s' "$all" | grep -v '^$' | sort || true
}

# The cluster's workload resources (StatefulSet <cluster>-unset, Deployment <cluster>-toolbox) in
# every zone, so a deployment scaled to zero still counts as existing. Prints "<context>/<kind>/<name>".
cluster_workloads() {
  local ctx json names all=""
  for ctx in "${KUBE_CONTEXT_LIST[@]}"; do
    json="$(kcx "$ctx" get statefulsets,deployments -o json)" || { log "workload discovery failed in $ctx"; return 1; }
    names="$(printf '%s\n' "$json" | jq -r --arg p "$RELEASE_PREFIX" --arg ctx "$ctx" '
      .items[] | select(.metadata.name | startswith($p)) | "\($ctx)/\(.kind)/\(.metadata.name)"')" ||
      { log "unreadable workload list from $ctx"; return 1; }
    all="$all$names"$'\n'
  done
  printf '%s' "$all" | grep -v '^$' | sort || true
}

# Succeeds only if absence of the cluster (pods and workloads) was positively established in every zone.
cluster_absent() {
  local fdb tb wl
  fdb="$(pods_with_container foundationdb)" || return 1
  tb="$(pods_with_container foundationdb-toolbox)" || return 1
  wl="$(cluster_workloads)" || return 1
  [ -z "$fdb$tb$wl" ]
}

# Number of matching pods; fails if discovery failed.
pod_count() {
  local pods
  pods="$(pods_with_container "$@")" || return 1
  if [ -z "$pods" ]; then echo 0; else printf '%s\n' "$pods" | wc -l | tr -d ' '; fi
}

ready_pods_with_container() { pods_with_container "$1" ready; }
fdb_pods() { pods_with_container foundationdb; }
toolbox_pods() { ready_pods_with_container foundationdb-toolbox; }

first_toolbox() {
  local tbs
  tbs="$(toolbox_pods)" || die "toolbox discovery failed"
  [ -n "$tbs" ] || die "no ready toolbox pod"
  printf '%s\n' "$tbs" | head -1
}

# $1 is "<context>/<pod>". Prints the response body; fails on transport errors and non-2xx HTTP.
toolbox_api() {
  local target="$1" method="$2" path="$3" out code
  shift 3
  out="$(kcx "${target%%/*}" exec "${target#*/}" -c foundationdb-toolbox -- \
    curl -sS -X "$method" -w '\n%{http_code}' "http://localhost:$TOOLBOX_PORT$path" "$@")" || return 1
  code="${out##*$'\n'}"
  out="${out%$'\n'*}"
  printf '%s\n' "$out"
  case "$code" in
    2??) ;;
    *) log "toolbox $method $path on $target returned HTTP $code"; return 1 ;;
  esac
}

fdbcli_exec() {
  local tb
  tb="$(first_toolbox)"
  kcx "${tb%%/*}" exec "${tb#*/}" -c foundationdb-toolbox -- fdbcli -C "$TOOLBOX_CLUSTER_FILE" --timeout 60 --exec "$1"
}

status_json() {
  local s tb
  tb="$(first_toolbox)" || return 1
  s="$(toolbox_api "$tb" GET /status)" || return 1
  printf '%s\n' "$s" | jq -e '.cluster' >/dev/null 2>&1 || { log "unexpected /status output: ${s:0:300}"; return 1; }
  printf '%s\n' "$s"
}

# Prints the restore status JSON; fails on any error or if it has no string "state".
restore_status() {
  local s tb
  tb="$(first_toolbox)" || return 1
  s="$(toolbox_api "$tb" GET /restore/status)" || return 1
  printf '%s\n' "$s" | jq -e '.state | type == "string"' >/dev/null 2>&1 || { log "unexpected /restore/status output: ${s:0:300}"; return 1; }
  printf '%s\n' "$s"
}

# Pauses every toolbox (one per zone) and verifies each acknowledged it; fails otherwise.
pause_toolboxes() {
  local tbs tb resp n
  tbs="$(toolbox_pods)" || { log "toolbox discovery failed"; return 1; }
  n="$(printf '%s\n' "$tbs" | grep -c . || true)"
  [ "$n" -eq "$TOOLBOX_REPLICAS" ] || { log "found $n ready toolboxes, expected $TOOLBOX_REPLICAS"; return 1; }
  for tb in $tbs; do
    resp="$(toolbox_api "$tb" POST /pause)" || return 1
    printf '%s\n' "$resp" | jq -e '.success == true' >/dev/null 2>&1 || { log "pause not acknowledged by $tb: ${resp:0:300}"; return 1; }
  done
}

# Best effort: logs toolboxes that can't be listed or resumed.
resume_toolboxes() {
  local tbs tb resp
  tbs="$(toolbox_pods)" || { log "WARNING: toolbox discovery failed, toolboxes may stay paused"; return 0; }
  for tb in $tbs; do
    resp="$(toolbox_api "$tb" POST /resume)" && printf '%s\n' "$resp" | jq -e '.success == true' >/dev/null 2>&1 ||
      log "WARNING: resume not acknowledged by $tb"
  done
}

set_rebalance() {
  case "$1" in
    on) fdbcli_exec "datadistribution enable rebalance" ;;
    off) fdbcli_exec "datadistribution disable rebalance" ;;
    *) die "set_rebalance: on|off" ;;
  esac
}

# Returns 1 on timeout.
wait_until() {
  local timeout="$1"
  shift
  local deadline=$(($(date +%s) + timeout))
  until "$@"; do
    [ "$(date +%s)" -lt "$deadline" ] || return 1
    sleep "$POLL_SECONDS"
  done
}

wait_for() {
  local desc="$1" timeout="$2"
  shift 2
  wait_until "$timeout" "$@" || die "timeout waiting for $desc"
}

db_available() { status_json | jq -e '.client.database_status.available == true' >/dev/null 2>&1; }
db_healthy() { status_json | jq -e '.client.database_status.available == true and .cluster.data.state.healthy == true' >/dev/null 2>&1; }
# $1 is "<context>/<pod>".
pod_ready() { kcx "${1%%/*}" get pod "${1#*/}" -o json 2>/dev/null | jq -e 'any(.status.conditions[]?; .type == "Ready" and .status == "True")' >/dev/null; }

load_variant() {
  local vf="$BENCH_DIR/variants/$1.env"
  [ -f "$vf" ] || die "unknown variant $1 (see $BENCH_DIR/variants)"
  KNOBS=()
  DD_REBALANCE=on
  source "$vf"
}

managed_knob_names() {
  cat "$BENCH_DIR"/variants/*.env | grep -oE '^[[:space:]]*knob_[a-z0-9_]+' | tr -d ' ' | sort -u
}

# Same shape as the historical-metrics e2e test cluster (foundationdb-worker
# create_cluster/defaults.go DefaultHistoricalMetricsIOCreateTestClusterRequest),
# with $FDB_REPLICAS nodes, version $FDB_VERSION and no backup.
create_cluster_input() {
  jq -nc \
    --arg dc "$DATACENTER" --arg kc "$KUBE_CLUSTER" --arg ns "$NS" --arg name "$FDB_CLUSTER" \
    --argjson replicas "$FDB_REPLICAS" --arg version "$FDB_VERSION" \
    --arg engine "$ENGINE_ENUM" --arg redundancy "$REDUNDANCY_ENUM" --arg channel "${SLACK_CHANNEL:-}" '{
      spec: ({
        target: {datacenter: $dc, kubernetesCluster: $kc, namespace: $ns, resource: $name},
        unsetReplicas: $replicas,
        shared: "historical-metrics",
        team: "historical-metrics-query",
        backup: false,
        config: {version: $version, useDnsDiscovery: true, storageEngine: $engine, redundancyMode: $redundancy, tls: true}
      } + (if $channel == "" then {} else {channel: $channel} end)),
      options: {skipPrs: true, useDynamicDeployment: true}
    }'
}

# DELETE_GATES empty keeps the worker default (DELETION_GATE_TYPE_LOCK_DB: Slack prompt up to
# 15 min, then the database locked for 2 hours before deletion, delete_cluster/deletion_gate.go).
delete_cluster_input() {
  jq -nc --arg dc "$DATACENTER" --arg kc "$KUBE_CLUSTER" --arg ns "$NS" --arg name "$FDB_CLUSTER" --arg gates "${DELETE_GATES:-}" '{
    spec: {
      target: {datacenter: $dc, kubernetesCluster: $kc, namespace: $ns, resource: $name},
      shared: "historical-metrics",
      team: "historical-metrics-query",
      skipPrs: true
    },
    options: ({cleanBackup: false, skipDeletionPrompt: true, useDynamicDeployment: true}
      + (if $gates == "" then {} else {gates: ($gates | split(" "))} end))
  }'
}

ATLAS_CREATE_TYPE=create_cluster.CreateClusterService_CreateCluster
ATLAS_DELETE_TYPE=delete_cluster.DeleteClusterService_DeleteCluster

# Starts an Atlas workflow and blocks until it ends. `atlas workflow watch --output jsonl`
# prints progress to stderr and one final JSON record to stdout, whose status is the Temporal
# enum string (atlas pkg/cmd/workflow/watch.go stateToWatchJSON): "Completed" on success.
atlas_run() {
  local type="$1" id="$2" input="$3" final status
  log "atlas: starting $type ($id)"
  atlas --context "$ATLAS_CONTEXT" workflow start \
    --workflow-type "$type" --task-queue "$ATLAS_TASK_QUEUE" --workflow-id "$id" --input "$input"
  final="$(atlas --context "$ATLAS_CONTEXT" workflow watch --workflow-id "$id" --output jsonl)" ||
    die "atlas workflow watch failed for $id: ${final:0:500}"
  status="$(echo "$final" | tail -1 | jq -r '.status // empty' 2>/dev/null || true)"
  [ "$status" = Completed ] || die "atlas workflow $id ended with status '${status:-unknown}': ${final:0:500}"
  log "atlas: $type ($id) completed"
}

# Adds the variant's knobs to the [fdbserver] section of the generated foundationdb.conf
# (in the class ConfigMap's init.sh) in every zonal k8s cluster, then restarts the fdb pods
# one by one. The StatefulSet uses updateStrategy OnDelete, so pods only pick it up when recreated.
apply_knobs() {
  local cm="$FDB_CLUSTER-unset" kv block init new pod ctx
  [ ${#KNOBS[@]} -gt 0 ] || return 0
  require_all_zones
  block=""
  for kv in "${KNOBS[@]}"; do block="$block$kv"$'\n'; done
  for ctx in "${KUBE_CONTEXT_LIST[@]}"; do
    init="$(kcx "$ctx" get configmap "$cm" -o json | jq -r '.data["init.sh"]')"
    echo "$init" | grep -q '^\[fdbserver\]$' || die "no [fdbserver] section in configmap $cm ($ctx)"
    # The block goes through the environment: BSD awk rejects newlines in -v values.
    new="$(echo "$init" | KNOB_BLOCK="$block" awk '
      /^# BEGIN restore-bench/ { skip = 1; next }
      /^# END restore-bench/ { skip = 0; next }
      skip { next }
      { print }
      /^\[fdbserver\]$/ { printf "# BEGIN restore-bench\n%s# END restore-bench\n", ENVIRON["KNOB_BLOCK"] }')"
    log "patching configmap $cm in $ctx with: ${KNOBS[*]}"
    kcx "$ctx" get configmap "$cm" -o json | jq --arg s "$new" '.data["init.sh"] = $s' | kcx "$ctx" replace -f -
  done
  local pods
  pods="$(fdb_pods)" || die "fdb pod discovery failed, knobs not applied to running pods"
  [ "$(printf '%s\n' "$pods" | grep -c .)" -eq "$FDB_REPLICAS" ] || die "expected $FDB_REPLICAS fdb pods, found: $pods"
  for pod in $pods; do
    log "restarting $pod"
    kcx "${pod%%/*}" delete pod "${pod#*/}" --wait=true >/dev/null
    wait_for "$pod ready" 1800 pod_ready "$pod"
    wait_for "database available" 1800 db_available
  done
}

# Checks the loaded variant's knobs (and only those), the version, process count, redundancy and engine.
verify_cluster() {
  local status name expected kv
  status="$(status_json)"
  for name in $(managed_knob_names); do
    expected=""
    for kv in "${KNOBS[@]}"; do
      [ "${kv%%=*}" = "$name" ] && expected="${kv#*=}"
    done
    if [ -n "$expected" ]; then
      echo "$status" | jq -e --arg a "--$name=$expected" \
        '[.cluster.processes[].command_line | contains($a)] | all' >/dev/null || { log "knob $name=$expected missing"; return 1; }
    else
      echo "$status" | jq -e --arg a "--$name=" \
        '[.cluster.processes[].command_line | contains($a)] | any | not' >/dev/null || { log "unexpected knob $name"; return 1; }
    fi
  done
  echo "$status" | jq -e --arg v "$FDB_VERSION" '[.cluster.processes[].version] | map(. == $v) | all' >/dev/null || { log "version is not $FDB_VERSION"; return 1; }
  echo "$status" | jq -e --argjson n "$FDB_REPLICAS" '[.cluster.processes[].machine_id] | unique | length == $n' >/dev/null || { log "not $FDB_REPLICAS machines"; return 1; }
  echo "$status" | jq -e --arg r "$REDUNDANCY" '.cluster.configuration.redundancy_mode == $r' >/dev/null || { log "redundancy is not $REDUNDANCY"; return 1; }
  echo "$status" | jq -e --arg e "$EXPECTED_ENGINE" '.cluster.configuration.storage_engine == $e' >/dev/null || { log "engine is not $EXPECTED_ENGINE"; return 1; }
}

# Returns 1 if a backup was running (and aborts it): backups are useless during benchmarks.
# Never fails the caller on a lookup error (e.g. kubectl credentials or VPN blip mid-restore).
ensure_no_backup() {
  local tb st
  if ! tb="$(first_toolbox 2>/dev/null)" || ! st="$(toolbox_api "$tb" GET /backup/status 2>/dev/null)"; then
    log "backup check skipped: toolbox unreachable"
    return 0
  fi
  if printf '%s\n' "$st" | jq -e '.running == true' >/dev/null 2>&1; then
    log "aborting running backup: $(printf '%s\n' "$st" | jq -c '{state, url}')"
    toolbox_api "$tb" POST /backup/abort >/dev/null || log "WARNING: backup abort request failed"
    return 1
  fi
}

# Restore version shared by every run of a comparison series (baseline, variants, Redwood...):
# the first run without RESTORE_VERSION records the version it restored here, and later
# invocations load it from bench.conf. Delete the file to start a new series. The backup keeps
# expiring old data (historical-metrics flavor: expire_days 7), so a series must finish while
# its version is still restorable.
SERIES_VERSION_FILE="$BENCH_DIR/runs/series-restore-version"
record_series_version() {
  [ -f "$SERIES_VERSION_FILE" ] && return 0
  mkdir -p "$(dirname "$SERIES_VERSION_FILE")"
  printf '%s\n' "$1" >"$SERIES_VERSION_FILE"
  log "new series: restore version $1 recorded in $SERIES_VERSION_FILE"
}
