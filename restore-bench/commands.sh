# shellcheck shell=bash
# Restore benchmark: commands to run by hand, in 4 steps covering all clusters at once. Not meant to be
# executed as a whole.
#
# Cluster shape: FDB 7.3.77, Redwood, 27 nodes (9 per zone, 3 processes each = 81 processes),
# three_data_hall_fallback, 6 tlogs, 6 commit proxies, one class per process: per zone, pods 0-4 run 3 storage
# processes, pods 5-6 run log + commit_proxy + stateless, pods 7-8 run 3 stateless processes. Totals: 45 storage,
# 6 log, 6 commit_proxy, 24 stateless (grv proxies, resolvers, master, cluster controller, ratekeeper, DD).
#
# Where each setting comes from:
# - k8s-resources per-cluster values files: version, knobs (variant knobs + storage queue target 1.5e9 and
#   hard limit 2.25e9).
# - CreateCluster request (step 1): 27 nodes (unsetReplicas), Redwood (storageEngine).
# - Runtime (step 2): the toolbox reconciler is switched to dry-run (it would otherwise revert
#   three_data_hall_fallback to three_data_hall and re-assign process classes), then fdbcli configures
#   three_data_hall_fallback + logs=6 commit_proxies=6 and sets each process's class (setclass).
#   The creation workflow itself needs the reconciler active (it waits for 9 coordinators), so the
#   cluster is created in three_data_hall and switched afterwards. A toolbox restart resets dry-run.
#
# Prerequisites:
# - The fdb-test-bench-* values files and clusters.bzl entries are merged in k8s-resources: creation deploys
#   from them (no dynamic deployment) and skips its own k8s-resources PR (skipPrs).
# - kubectl access to oddish-a/b/c.us1.staging.dog, namespace hmq-test. The worker spreads each cluster over
#   the zonal set (9 fdb pods and 1 toolbox per zone).
# - Node capacity: 7 clusters x 27 = 189 fdb pods on the historical-metrics shared nodes, 63 per zone.
# - atlas logged in; jq installed.
#
# Notes:
# - The 7 restores run at the same time: they share the backup bucket's read bandwidth and possibly the
#   historical-metrics shared nodes, so compare variants knowing they ran under the same contention.
# - Each restore uses the latest restorable version (no version given): the difference between runs is
#   negligible. The /restore/start response prints the version used ("target restore version ...").
# - Workflow ids end with -1: bump the suffix to rerun.
# - fdbcli runs in the toolbox container, which has the cluster file and the TLS settings.
# - The restore needs an empty database: the oddish-a toolbox pauses itself on /restore/start, the oddish-b
#   and oddish-c toolboxes are paused by hand before the start and resumed after the restore (step 3b).
# - Deletion uses the worker's default LOCK_DB gate: Slack prompt (up to 15 min), then the database is locked
#   2 h before deletion. Add "gates": ["DELETION_GATE_TYPE_NOOP"] to the delete options to skip it.
# - Deletion also skips its k8s-resources PR (skipPrs): the clusters' config stays in k8s-resources and must be
#   removed by hand once the benchmark is done.
# - Restore progress isn't in Datadog: the /restore/status commands are the only progress view. Datadog
#   metrics for a run (staging org, between the noted start and end, unix timestamps), e.g.:
#   pup metrics query --from <START> --to <END> \
#     --query 'sum:fdb.vector.storage_metrics.bytes_fetched{cluster:fdb-test-bench-baseline}.as_count()'

# ==================== 1. Creation ====================
# Start the 7 creation workflows, then watch them until they complete.

atlas --context prod workflow start --task-queue foundationdb/us1.staging.dog \
  --workflow-type create_cluster.CreateClusterService_CreateCluster \
  --workflow-id restore-bench-create-fdb-test-bench-baseline-1 \
  --input '{
    "spec": {
      "target": {"datacenter": "us1.staging.dog", "kubernetesCluster": "oddish", "namespace": "hmq-test",
                 "resource": "fdb-test-bench-baseline"},
      "unsetReplicas": 27, "shared": "historical-metrics", "team": "historical-metrics-query",
      "backup": false, "channel": "hm-io-cnab",
      "config": {"version": "7.3.77", "useDnsDiscovery": true, "storageEngine": "STORAGE_ENGINE_REDWOOD",
                 "redundancyMode": "REDUNDANCY_MODE_THREE_DATA_HALL", "tls": true}
    },
    "options": {"skipPrs": true}
  }'

atlas --context prod workflow start --task-queue foundationdb/us1.staging.dog \
  --workflow-type create_cluster.CreateClusterService_CreateCluster \
  --workflow-id restore-bench-create-fdb-test-bench-merge-delay-1 \
  --input '{
    "spec": {
      "target": {"datacenter": "us1.staging.dog", "kubernetesCluster": "oddish", "namespace": "hmq-test",
                 "resource": "fdb-test-bench-merge-delay"},
      "unsetReplicas": 27, "shared": "historical-metrics", "team": "historical-metrics-query",
      "backup": false, "channel": "hm-io-cnab",
      "config": {"version": "7.3.77", "useDnsDiscovery": true, "storageEngine": "STORAGE_ENGINE_REDWOOD",
                 "redundancyMode": "REDUNDANCY_MODE_THREE_DATA_HALL", "tls": true}
    },
    "options": {"skipPrs": true}
  }'

atlas --context prod workflow start --task-queue foundationdb/us1.staging.dog \
  --workflow-type create_cluster.CreateClusterService_CreateCluster \
  --workflow-id restore-bench-create-fdb-test-bench-write-split-1 \
  --input '{
    "spec": {
      "target": {"datacenter": "us1.staging.dog", "kubernetesCluster": "oddish", "namespace": "hmq-test",
                 "resource": "fdb-test-bench-write-split"},
      "unsetReplicas": 27, "shared": "historical-metrics", "team": "historical-metrics-query",
      "backup": false, "channel": "hm-io-cnab",
      "config": {"version": "7.3.77", "useDnsDiscovery": true, "storageEngine": "STORAGE_ENGINE_REDWOOD",
                 "redundancyMode": "REDUNDANCY_MODE_THREE_DATA_HALL", "tls": true}
    },
    "options": {"skipPrs": true}
  }'

atlas --context prod workflow start --task-queue foundationdb/us1.staging.dog \
  --workflow-type create_cluster.CreateClusterService_CreateCluster \
  --workflow-id restore-bench-create-fdb-test-bench-write-split-aggressive-1 \
  --input '{
    "spec": {
      "target": {"datacenter": "us1.staging.dog", "kubernetesCluster": "oddish", "namespace": "hmq-test",
                 "resource": "fdb-test-bench-write-split-aggressive"},
      "unsetReplicas": 27, "shared": "historical-metrics", "team": "historical-metrics-query",
      "backup": false, "channel": "hm-io-cnab",
      "config": {"version": "7.3.77", "useDnsDiscovery": true, "storageEngine": "STORAGE_ENGINE_REDWOOD",
                 "redundancyMode": "REDUNDANCY_MODE_THREE_DATA_HALL", "tls": true}
    },
    "options": {"skipPrs": true}
  }'

atlas --context prod workflow start --task-queue foundationdb/us1.staging.dog \
  --workflow-type create_cluster.CreateClusterService_CreateCluster \
  --workflow-id restore-bench-create-fdb-test-bench-metrics-interval-1 \
  --input '{
    "spec": {
      "target": {"datacenter": "us1.staging.dog", "kubernetesCluster": "oddish", "namespace": "hmq-test",
                 "resource": "fdb-test-bench-metrics-interval"},
      "unsetReplicas": 27, "shared": "historical-metrics", "team": "historical-metrics-query",
      "backup": false, "channel": "hm-io-cnab",
      "config": {"version": "7.3.77", "useDnsDiscovery": true, "storageEngine": "STORAGE_ENGINE_REDWOOD",
                 "redundancyMode": "REDUNDANCY_MODE_THREE_DATA_HALL", "tls": true}
    },
    "options": {"skipPrs": true}
  }'

atlas --context prod workflow start --task-queue foundationdb/us1.staging.dog \
  --workflow-type create_cluster.CreateClusterService_CreateCluster \
  --workflow-id restore-bench-create-fdb-test-bench-rebalance-off-1 \
  --input '{
    "spec": {
      "target": {"datacenter": "us1.staging.dog", "kubernetesCluster": "oddish", "namespace": "hmq-test",
                 "resource": "fdb-test-bench-rebalance-off"},
      "unsetReplicas": 27, "shared": "historical-metrics", "team": "historical-metrics-query",
      "backup": false, "channel": "hm-io-cnab",
      "config": {"version": "7.3.77", "useDnsDiscovery": true, "storageEngine": "STORAGE_ENGINE_REDWOOD",
                 "redundancyMode": "REDUNDANCY_MODE_THREE_DATA_HALL", "tls": true}
    },
    "options": {"skipPrs": true}
  }'

atlas --context prod workflow start --task-queue foundationdb/us1.staging.dog \
  --workflow-type create_cluster.CreateClusterService_CreateCluster \
  --workflow-id restore-bench-create-fdb-test-bench-minimize-data-movement-1 \
  --input '{
    "spec": {
      "target": {"datacenter": "us1.staging.dog", "kubernetesCluster": "oddish", "namespace": "hmq-test",
                 "resource": "fdb-test-bench-minimize-data-movement"},
      "unsetReplicas": 27, "shared": "historical-metrics", "team": "historical-metrics-query",
      "backup": false, "channel": "hm-io-cnab",
      "config": {"version": "7.3.77", "useDnsDiscovery": true, "storageEngine": "STORAGE_ENGINE_REDWOOD",
                 "redundancyMode": "REDUNDANCY_MODE_THREE_DATA_HALL", "tls": true}
    },
    "options": {"skipPrs": true}
  }'

atlas --context prod workflow watch --workflow-id restore-bench-create-fdb-test-bench-baseline-1
atlas --context prod workflow watch --workflow-id restore-bench-create-fdb-test-bench-merge-delay-1
atlas --context prod workflow watch --workflow-id restore-bench-create-fdb-test-bench-write-split-1
atlas --context prod workflow watch --workflow-id restore-bench-create-fdb-test-bench-write-split-aggressive-1
atlas --context prod workflow watch --workflow-id restore-bench-create-fdb-test-bench-metrics-interval-1
atlas --context prod workflow watch --workflow-id restore-bench-create-fdb-test-bench-rebalance-off-1
atlas --context prod workflow watch --workflow-id restore-bench-create-fdb-test-bench-minimize-data-movement-1

# ==================== 2. Config update ====================
# Per cluster:
# 1. reconciler dry-run on the 3 toolboxes;
# 2. three_data_hall_fallback, 6 tlogs, 6 commit proxies;
# 3. one class per process (setclass), built from the status: per zone (data hall), pods sorted by name;
# 4. checks: configuration, class counts (expect commit_proxy 6, log 6, stateless 24, storage 45), knobs (each
#    should print 81 = 27 pods x 3 processes);
# 5. rebalance on/off.

# ----- fdb-test-bench-baseline: rebalance enable -----
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-baseline \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  fdbcli -C /etc/foundationdb/fdb.cluster --timeout 60 --exec "status minimal"
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-baseline \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/reconciler/config -F dryrun=true
kubectl --context oddish-b.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-b.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-baseline \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/reconciler/config -F dryrun=true
kubectl --context oddish-c.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-c.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-baseline \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/reconciler/config -F dryrun=true
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-baseline \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  fdbcli -C /etc/foundationdb/fdb.cluster --timeout 120 \
  --exec "configure three_data_hall_fallback logs=6 commit_proxies=6"
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-baseline \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq -r '[.cluster.processes[] | {a: .address, m: .locality.machineid, h: .locality.data_hall}]
      | group_by(.h) | map(group_by(.m) | sort_by(.[0].m) | to_entries | map(.key as $i | .value | sort_by(.a)
        | to_entries | map(.key as $p | "setclass \(.value.a) \(if $i < 5 then "storage" elif $i < 7
        then ["log", "commit_proxy", "stateless"][$p] else "stateless" end)")))
      | flatten | join("; ")' \
  | kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec -i \
    "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
      -l app=foundationdb-toolbox,cluster=fdb-test-bench-baseline \
      -o jsonpath='{.items[0].metadata.name}')" \
    -c foundationdb-toolbox -- \
    sh -c 'fdbcli -C /etc/foundationdb/fdb.cluster --timeout 300 --exec "$(cat)"'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-baseline \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq -c '{config: (.cluster.configuration | {redundancy_mode, storage_engine, logs, commit_proxies}),
      classes: ([.cluster.processes[].class_type] | group_by(.) | map({(.[0]): length}) | add)}'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-baseline \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq '[.cluster.processes[].command_line
      | select(contains("--knob_target_bytes_per_storage_server=1500000000"))] | length'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-baseline \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq '[.cluster.processes[].command_line
      | select(contains("--knob_storage_hard_limit_bytes=2250000000"))] | length'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-baseline \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  fdbcli -C /etc/foundationdb/fdb.cluster --timeout 60 --exec "datadistribution enable rebalance"

# ----- fdb-test-bench-merge-delay: rebalance enable -----
# knob: knob_dd_merge_coalesce_delay=86400
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-merge-delay \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  fdbcli -C /etc/foundationdb/fdb.cluster --timeout 60 --exec "status minimal"
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-merge-delay \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/reconciler/config -F dryrun=true
kubectl --context oddish-b.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-b.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-merge-delay \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/reconciler/config -F dryrun=true
kubectl --context oddish-c.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-c.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-merge-delay \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/reconciler/config -F dryrun=true
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-merge-delay \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  fdbcli -C /etc/foundationdb/fdb.cluster --timeout 120 \
  --exec "configure three_data_hall_fallback logs=6 commit_proxies=6"
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-merge-delay \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq -r '[.cluster.processes[] | {a: .address, m: .locality.machineid, h: .locality.data_hall}]
      | group_by(.h) | map(group_by(.m) | sort_by(.[0].m) | to_entries | map(.key as $i | .value | sort_by(.a)
        | to_entries | map(.key as $p | "setclass \(.value.a) \(if $i < 5 then "storage" elif $i < 7
        then ["log", "commit_proxy", "stateless"][$p] else "stateless" end)")))
      | flatten | join("; ")' \
  | kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec -i \
    "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
      -l app=foundationdb-toolbox,cluster=fdb-test-bench-merge-delay \
      -o jsonpath='{.items[0].metadata.name}')" \
    -c foundationdb-toolbox -- \
    sh -c 'fdbcli -C /etc/foundationdb/fdb.cluster --timeout 300 --exec "$(cat)"'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-merge-delay \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq -c '{config: (.cluster.configuration | {redundancy_mode, storage_engine, logs, commit_proxies}),
      classes: ([.cluster.processes[].class_type] | group_by(.) | map({(.[0]): length}) | add)}'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-merge-delay \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq '[.cluster.processes[].command_line
      | select(contains("--knob_target_bytes_per_storage_server=1500000000"))] | length'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-merge-delay \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq '[.cluster.processes[].command_line
      | select(contains("--knob_storage_hard_limit_bytes=2250000000"))] | length'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-merge-delay \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq '[.cluster.processes[].command_line
      | select(contains("--knob_dd_merge_coalesce_delay=86400"))] | length'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-merge-delay \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  fdbcli -C /etc/foundationdb/fdb.cluster --timeout 60 --exec "datadistribution enable rebalance"

# ----- fdb-test-bench-write-split: rebalance enable -----
# knob: knob_shard_max_bytes_per_ksec=10000000000
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  fdbcli -C /etc/foundationdb/fdb.cluster --timeout 60 --exec "status minimal"
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/reconciler/config -F dryrun=true
kubectl --context oddish-b.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-b.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/reconciler/config -F dryrun=true
kubectl --context oddish-c.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-c.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/reconciler/config -F dryrun=true
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  fdbcli -C /etc/foundationdb/fdb.cluster --timeout 120 \
  --exec "configure three_data_hall_fallback logs=6 commit_proxies=6"
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq -r '[.cluster.processes[] | {a: .address, m: .locality.machineid, h: .locality.data_hall}]
      | group_by(.h) | map(group_by(.m) | sort_by(.[0].m) | to_entries | map(.key as $i | .value | sort_by(.a)
        | to_entries | map(.key as $p | "setclass \(.value.a) \(if $i < 5 then "storage" elif $i < 7
        then ["log", "commit_proxy", "stateless"][$p] else "stateless" end)")))
      | flatten | join("; ")' \
  | kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec -i \
    "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
      -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split \
      -o jsonpath='{.items[0].metadata.name}')" \
    -c foundationdb-toolbox -- \
    sh -c 'fdbcli -C /etc/foundationdb/fdb.cluster --timeout 300 --exec "$(cat)"'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq -c '{config: (.cluster.configuration | {redundancy_mode, storage_engine, logs, commit_proxies}),
      classes: ([.cluster.processes[].class_type] | group_by(.) | map({(.[0]): length}) | add)}'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq '[.cluster.processes[].command_line
      | select(contains("--knob_target_bytes_per_storage_server=1500000000"))] | length'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq '[.cluster.processes[].command_line
      | select(contains("--knob_storage_hard_limit_bytes=2250000000"))] | length'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq '[.cluster.processes[].command_line
      | select(contains("--knob_shard_max_bytes_per_ksec=10000000000"))] | length'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  fdbcli -C /etc/foundationdb/fdb.cluster --timeout 60 --exec "datadistribution enable rebalance"

# ----- fdb-test-bench-write-split-aggressive: rebalance enable -----
# knob: knob_shard_max_bytes_per_ksec=250000000
# knob: knob_shard_split_bytes_per_ksec=100000000
# knob: knob_shard_min_bytes_per_ksec=25000000
# knob: knob_enable_write_based_shard_split=true
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split-aggressive \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  fdbcli -C /etc/foundationdb/fdb.cluster --timeout 60 --exec "status minimal"
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split-aggressive \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/reconciler/config -F dryrun=true
kubectl --context oddish-b.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-b.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split-aggressive \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/reconciler/config -F dryrun=true
kubectl --context oddish-c.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-c.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split-aggressive \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/reconciler/config -F dryrun=true
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split-aggressive \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  fdbcli -C /etc/foundationdb/fdb.cluster --timeout 120 \
  --exec "configure three_data_hall_fallback logs=6 commit_proxies=6"
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split-aggressive \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq -r '[.cluster.processes[] | {a: .address, m: .locality.machineid, h: .locality.data_hall}]
      | group_by(.h) | map(group_by(.m) | sort_by(.[0].m) | to_entries | map(.key as $i | .value | sort_by(.a)
        | to_entries | map(.key as $p | "setclass \(.value.a) \(if $i < 5 then "storage" elif $i < 7
        then ["log", "commit_proxy", "stateless"][$p] else "stateless" end)")))
      | flatten | join("; ")' \
  | kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec -i \
    "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
      -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split-aggressive \
      -o jsonpath='{.items[0].metadata.name}')" \
    -c foundationdb-toolbox -- \
    sh -c 'fdbcli -C /etc/foundationdb/fdb.cluster --timeout 300 --exec "$(cat)"'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split-aggressive \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq -c '{config: (.cluster.configuration | {redundancy_mode, storage_engine, logs, commit_proxies}),
      classes: ([.cluster.processes[].class_type] | group_by(.) | map({(.[0]): length}) | add)}'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split-aggressive \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq '[.cluster.processes[].command_line
      | select(contains("--knob_target_bytes_per_storage_server=1500000000"))] | length'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split-aggressive \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq '[.cluster.processes[].command_line
      | select(contains("--knob_storage_hard_limit_bytes=2250000000"))] | length'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split-aggressive \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq '[.cluster.processes[].command_line
      | select(contains("--knob_shard_max_bytes_per_ksec=250000000"))] | length'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split-aggressive \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq '[.cluster.processes[].command_line
      | select(contains("--knob_shard_split_bytes_per_ksec=100000000"))] | length'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split-aggressive \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq '[.cluster.processes[].command_line
      | select(contains("--knob_shard_min_bytes_per_ksec=25000000"))] | length'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split-aggressive \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq '[.cluster.processes[].command_line
      | select(contains("--knob_enable_write_based_shard_split=true"))] | length'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split-aggressive \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  fdbcli -C /etc/foundationdb/fdb.cluster --timeout 60 --exec "datadistribution enable rebalance"

# ----- fdb-test-bench-metrics-interval: rebalance enable -----
# knob: knob_storage_metrics_average_interval=30
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-metrics-interval \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  fdbcli -C /etc/foundationdb/fdb.cluster --timeout 60 --exec "status minimal"
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-metrics-interval \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/reconciler/config -F dryrun=true
kubectl --context oddish-b.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-b.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-metrics-interval \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/reconciler/config -F dryrun=true
kubectl --context oddish-c.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-c.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-metrics-interval \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/reconciler/config -F dryrun=true
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-metrics-interval \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  fdbcli -C /etc/foundationdb/fdb.cluster --timeout 120 \
  --exec "configure three_data_hall_fallback logs=6 commit_proxies=6"
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-metrics-interval \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq -r '[.cluster.processes[] | {a: .address, m: .locality.machineid, h: .locality.data_hall}]
      | group_by(.h) | map(group_by(.m) | sort_by(.[0].m) | to_entries | map(.key as $i | .value | sort_by(.a)
        | to_entries | map(.key as $p | "setclass \(.value.a) \(if $i < 5 then "storage" elif $i < 7
        then ["log", "commit_proxy", "stateless"][$p] else "stateless" end)")))
      | flatten | join("; ")' \
  | kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec -i \
    "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
      -l app=foundationdb-toolbox,cluster=fdb-test-bench-metrics-interval \
      -o jsonpath='{.items[0].metadata.name}')" \
    -c foundationdb-toolbox -- \
    sh -c 'fdbcli -C /etc/foundationdb/fdb.cluster --timeout 300 --exec "$(cat)"'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-metrics-interval \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq -c '{config: (.cluster.configuration | {redundancy_mode, storage_engine, logs, commit_proxies}),
      classes: ([.cluster.processes[].class_type] | group_by(.) | map({(.[0]): length}) | add)}'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-metrics-interval \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq '[.cluster.processes[].command_line
      | select(contains("--knob_target_bytes_per_storage_server=1500000000"))] | length'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-metrics-interval \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq '[.cluster.processes[].command_line
      | select(contains("--knob_storage_hard_limit_bytes=2250000000"))] | length'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-metrics-interval \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq '[.cluster.processes[].command_line
      | select(contains("--knob_storage_metrics_average_interval=30"))] | length'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-metrics-interval \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  fdbcli -C /etc/foundationdb/fdb.cluster --timeout 60 --exec "datadistribution enable rebalance"

# ----- fdb-test-bench-rebalance-off: rebalance disable -----
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-rebalance-off \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  fdbcli -C /etc/foundationdb/fdb.cluster --timeout 60 --exec "status minimal"
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-rebalance-off \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/reconciler/config -F dryrun=true
kubectl --context oddish-b.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-b.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-rebalance-off \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/reconciler/config -F dryrun=true
kubectl --context oddish-c.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-c.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-rebalance-off \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/reconciler/config -F dryrun=true
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-rebalance-off \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  fdbcli -C /etc/foundationdb/fdb.cluster --timeout 120 \
  --exec "configure three_data_hall_fallback logs=6 commit_proxies=6"
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-rebalance-off \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq -r '[.cluster.processes[] | {a: .address, m: .locality.machineid, h: .locality.data_hall}]
      | group_by(.h) | map(group_by(.m) | sort_by(.[0].m) | to_entries | map(.key as $i | .value | sort_by(.a)
        | to_entries | map(.key as $p | "setclass \(.value.a) \(if $i < 5 then "storage" elif $i < 7
        then ["log", "commit_proxy", "stateless"][$p] else "stateless" end)")))
      | flatten | join("; ")' \
  | kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec -i \
    "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
      -l app=foundationdb-toolbox,cluster=fdb-test-bench-rebalance-off \
      -o jsonpath='{.items[0].metadata.name}')" \
    -c foundationdb-toolbox -- \
    sh -c 'fdbcli -C /etc/foundationdb/fdb.cluster --timeout 300 --exec "$(cat)"'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-rebalance-off \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq -c '{config: (.cluster.configuration | {redundancy_mode, storage_engine, logs, commit_proxies}),
      classes: ([.cluster.processes[].class_type] | group_by(.) | map({(.[0]): length}) | add)}'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-rebalance-off \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq '[.cluster.processes[].command_line
      | select(contains("--knob_target_bytes_per_storage_server=1500000000"))] | length'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-rebalance-off \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq '[.cluster.processes[].command_line
      | select(contains("--knob_storage_hard_limit_bytes=2250000000"))] | length'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-rebalance-off \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  fdbcli -C /etc/foundationdb/fdb.cluster --timeout 60 --exec "datadistribution disable rebalance"

# ----- fdb-test-bench-minimize-data-movement: rebalance disable -----
# knob: knob_dd_merge_coalesce_delay=86400
# knob: knob_shard_max_bytes_per_ksec=10000000000
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-minimize-data-movement \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  fdbcli -C /etc/foundationdb/fdb.cluster --timeout 60 --exec "status minimal"
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-minimize-data-movement \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/reconciler/config -F dryrun=true
kubectl --context oddish-b.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-b.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-minimize-data-movement \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/reconciler/config -F dryrun=true
kubectl --context oddish-c.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-c.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-minimize-data-movement \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/reconciler/config -F dryrun=true
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-minimize-data-movement \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  fdbcli -C /etc/foundationdb/fdb.cluster --timeout 120 \
  --exec "configure three_data_hall_fallback logs=6 commit_proxies=6"
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-minimize-data-movement \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq -r '[.cluster.processes[] | {a: .address, m: .locality.machineid, h: .locality.data_hall}]
      | group_by(.h) | map(group_by(.m) | sort_by(.[0].m) | to_entries | map(.key as $i | .value | sort_by(.a)
        | to_entries | map(.key as $p | "setclass \(.value.a) \(if $i < 5 then "storage" elif $i < 7
        then ["log", "commit_proxy", "stateless"][$p] else "stateless" end)")))
      | flatten | join("; ")' \
  | kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec -i \
    "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
      -l app=foundationdb-toolbox,cluster=fdb-test-bench-minimize-data-movement \
      -o jsonpath='{.items[0].metadata.name}')" \
    -c foundationdb-toolbox -- \
    sh -c 'fdbcli -C /etc/foundationdb/fdb.cluster --timeout 300 --exec "$(cat)"'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-minimize-data-movement \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq -c '{config: (.cluster.configuration | {redundancy_mode, storage_engine, logs, commit_proxies}),
      classes: ([.cluster.processes[].class_type] | group_by(.) | map({(.[0]): length}) | add)}'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-minimize-data-movement \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq '[.cluster.processes[].command_line
      | select(contains("--knob_target_bytes_per_storage_server=1500000000"))] | length'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-minimize-data-movement \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq '[.cluster.processes[].command_line
      | select(contains("--knob_storage_hard_limit_bytes=2250000000"))] | length'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-minimize-data-movement \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq '[.cluster.processes[].command_line
      | select(contains("--knob_dd_merge_coalesce_delay=86400"))] | length'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-minimize-data-movement \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/status \
  | jq '[.cluster.processes[].command_line
      | select(contains("--knob_shard_max_bytes_per_ksec=10000000000"))] | length'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-minimize-data-movement \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  fdbcli -C /etc/foundationdb/fdb.cluster --timeout 60 --exec "datadistribution disable rebalance"

# ==================== 3. Restore ====================
# Per cluster: pause the oddish-b and oddish-c toolboxes (they stay paused until 3b), start the restore
# from the oddish-a toolbox (it resumes itself after the start). Note the start time, then poll the status
# of every cluster until all are "completed", noting each cluster's end time.
date -u +%FT%TZ   # restores start

# fdb-test-bench-baseline
kubectl --context oddish-b.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-b.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-baseline \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/pause
kubectl --context oddish-c.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-c.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-baseline \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/pause
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-baseline \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/restore/start -F backup=foundationdb-hsi-default-2025-03-v0 -F dry_run=false

# fdb-test-bench-merge-delay
kubectl --context oddish-b.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-b.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-merge-delay \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/pause
kubectl --context oddish-c.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-c.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-merge-delay \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/pause
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-merge-delay \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/restore/start -F backup=foundationdb-hsi-default-2025-03-v0 -F dry_run=false

# fdb-test-bench-write-split
kubectl --context oddish-b.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-b.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/pause
kubectl --context oddish-c.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-c.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/pause
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/restore/start -F backup=foundationdb-hsi-default-2025-03-v0 -F dry_run=false

# fdb-test-bench-write-split-aggressive
kubectl --context oddish-b.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-b.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split-aggressive \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/pause
kubectl --context oddish-c.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-c.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split-aggressive \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/pause
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split-aggressive \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/restore/start -F backup=foundationdb-hsi-default-2025-03-v0 -F dry_run=false

# fdb-test-bench-metrics-interval
kubectl --context oddish-b.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-b.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-metrics-interval \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/pause
kubectl --context oddish-c.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-c.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-metrics-interval \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/pause
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-metrics-interval \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/restore/start -F backup=foundationdb-hsi-default-2025-03-v0 -F dry_run=false

# fdb-test-bench-rebalance-off
kubectl --context oddish-b.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-b.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-rebalance-off \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/pause
kubectl --context oddish-c.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-c.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-rebalance-off \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/pause
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-rebalance-off \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/restore/start -F backup=foundationdb-hsi-default-2025-03-v0 -F dry_run=false

# fdb-test-bench-minimize-data-movement
kubectl --context oddish-b.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-b.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-minimize-data-movement \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/pause
kubectl --context oddish-c.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-c.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-minimize-data-movement \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/pause
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-minimize-data-movement \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/restore/start -F backup=foundationdb-hsi-default-2025-03-v0 -F dry_run=false

# Status of every cluster (repeat until all are "completed")
date -u +%FT%TZ
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-baseline \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/restore/status \
  | jq -c '{cluster: "fdb-test-bench-baseline", state, blocksFinished, blocksCount,
      bytesWritten, applyVersionLag}'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-merge-delay \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/restore/status \
  | jq -c '{cluster: "fdb-test-bench-merge-delay", state, blocksFinished, blocksCount,
      bytesWritten, applyVersionLag}'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/restore/status \
  | jq -c '{cluster: "fdb-test-bench-write-split", state, blocksFinished, blocksCount,
      bytesWritten, applyVersionLag}'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split-aggressive \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/restore/status \
  | jq -c '{cluster: "fdb-test-bench-write-split-aggressive", state, blocksFinished, blocksCount,
      bytesWritten, applyVersionLag}'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-metrics-interval \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/restore/status \
  | jq -c '{cluster: "fdb-test-bench-metrics-interval", state, blocksFinished, blocksCount,
      bytesWritten, applyVersionLag}'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-rebalance-off \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/restore/status \
  | jq -c '{cluster: "fdb-test-bench-rebalance-off", state, blocksFinished, blocksCount,
      bytesWritten, applyVersionLag}'
kubectl --context oddish-a.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-a.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-minimize-data-movement \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS localhost:8080/restore/status \
  | jq -c '{cluster: "fdb-test-bench-minimize-data-movement", state, blocksFinished, blocksCount,
      bytesWritten, applyVersionLag}'

# ==================== 3b. After the restore ====================
# Once every cluster is "completed": resume the oddish-b and oddish-c toolboxes.

# fdb-test-bench-baseline
kubectl --context oddish-b.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-b.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-baseline \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/resume
kubectl --context oddish-c.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-c.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-baseline \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/resume

# fdb-test-bench-merge-delay
kubectl --context oddish-b.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-b.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-merge-delay \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/resume
kubectl --context oddish-c.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-c.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-merge-delay \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/resume

# fdb-test-bench-write-split
kubectl --context oddish-b.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-b.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/resume
kubectl --context oddish-c.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-c.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/resume

# fdb-test-bench-write-split-aggressive
kubectl --context oddish-b.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-b.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split-aggressive \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/resume
kubectl --context oddish-c.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-c.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-write-split-aggressive \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/resume

# fdb-test-bench-metrics-interval
kubectl --context oddish-b.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-b.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-metrics-interval \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/resume
kubectl --context oddish-c.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-c.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-metrics-interval \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/resume

# fdb-test-bench-rebalance-off
kubectl --context oddish-b.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-b.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-rebalance-off \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/resume
kubectl --context oddish-c.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-c.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-rebalance-off \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/resume

# fdb-test-bench-minimize-data-movement
kubectl --context oddish-b.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-b.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-minimize-data-movement \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/resume
kubectl --context oddish-c.us1.staging.dog --namespace hmq-test exec \
  "$(kubectl --context oddish-c.us1.staging.dog --namespace hmq-test get pods \
    -l app=foundationdb-toolbox,cluster=fdb-test-bench-minimize-data-movement \
    -o jsonpath='{.items[0].metadata.name}')" \
  -c foundationdb-toolbox -- \
  curl -sS -X POST localhost:8080/resume

# ==================== 4. Deletion ====================
# Start the 7 deletion workflows, then watch them until they complete.

atlas --context prod workflow start --task-queue foundationdb/us1.staging.dog \
  --workflow-type delete_cluster.DeleteClusterService_DeleteCluster \
  --workflow-id restore-bench-delete-fdb-test-bench-baseline-1 \
  --input '{
    "spec": {
      "target": {"datacenter": "us1.staging.dog", "kubernetesCluster": "oddish", "namespace": "hmq-test",
                 "resource": "fdb-test-bench-baseline"},
      "shared": "historical-metrics", "team": "historical-metrics-query", "skipPrs": true
    },
    "options": {"cleanBackup": false, "skipDeletionPrompt": true}
  }'

atlas --context prod workflow start --task-queue foundationdb/us1.staging.dog \
  --workflow-type delete_cluster.DeleteClusterService_DeleteCluster \
  --workflow-id restore-bench-delete-fdb-test-bench-merge-delay-1 \
  --input '{
    "spec": {
      "target": {"datacenter": "us1.staging.dog", "kubernetesCluster": "oddish", "namespace": "hmq-test",
                 "resource": "fdb-test-bench-merge-delay"},
      "shared": "historical-metrics", "team": "historical-metrics-query", "skipPrs": true
    },
    "options": {"cleanBackup": false, "skipDeletionPrompt": true}
  }'

atlas --context prod workflow start --task-queue foundationdb/us1.staging.dog \
  --workflow-type delete_cluster.DeleteClusterService_DeleteCluster \
  --workflow-id restore-bench-delete-fdb-test-bench-write-split-1 \
  --input '{
    "spec": {
      "target": {"datacenter": "us1.staging.dog", "kubernetesCluster": "oddish", "namespace": "hmq-test",
                 "resource": "fdb-test-bench-write-split"},
      "shared": "historical-metrics", "team": "historical-metrics-query", "skipPrs": true
    },
    "options": {"cleanBackup": false, "skipDeletionPrompt": true}
  }'

atlas --context prod workflow start --task-queue foundationdb/us1.staging.dog \
  --workflow-type delete_cluster.DeleteClusterService_DeleteCluster \
  --workflow-id restore-bench-delete-fdb-test-bench-write-split-aggressive-1 \
  --input '{
    "spec": {
      "target": {"datacenter": "us1.staging.dog", "kubernetesCluster": "oddish", "namespace": "hmq-test",
                 "resource": "fdb-test-bench-write-split-aggressive"},
      "shared": "historical-metrics", "team": "historical-metrics-query", "skipPrs": true
    },
    "options": {"cleanBackup": false, "skipDeletionPrompt": true}
  }'

atlas --context prod workflow start --task-queue foundationdb/us1.staging.dog \
  --workflow-type delete_cluster.DeleteClusterService_DeleteCluster \
  --workflow-id restore-bench-delete-fdb-test-bench-metrics-interval-1 \
  --input '{
    "spec": {
      "target": {"datacenter": "us1.staging.dog", "kubernetesCluster": "oddish", "namespace": "hmq-test",
                 "resource": "fdb-test-bench-metrics-interval"},
      "shared": "historical-metrics", "team": "historical-metrics-query", "skipPrs": true
    },
    "options": {"cleanBackup": false, "skipDeletionPrompt": true}
  }'

atlas --context prod workflow start --task-queue foundationdb/us1.staging.dog \
  --workflow-type delete_cluster.DeleteClusterService_DeleteCluster \
  --workflow-id restore-bench-delete-fdb-test-bench-rebalance-off-1 \
  --input '{
    "spec": {
      "target": {"datacenter": "us1.staging.dog", "kubernetesCluster": "oddish", "namespace": "hmq-test",
                 "resource": "fdb-test-bench-rebalance-off"},
      "shared": "historical-metrics", "team": "historical-metrics-query", "skipPrs": true
    },
    "options": {"cleanBackup": false, "skipDeletionPrompt": true}
  }'

atlas --context prod workflow start --task-queue foundationdb/us1.staging.dog \
  --workflow-type delete_cluster.DeleteClusterService_DeleteCluster \
  --workflow-id restore-bench-delete-fdb-test-bench-minimize-data-movement-1 \
  --input '{
    "spec": {
      "target": {"datacenter": "us1.staging.dog", "kubernetesCluster": "oddish", "namespace": "hmq-test",
                 "resource": "fdb-test-bench-minimize-data-movement"},
      "shared": "historical-metrics", "team": "historical-metrics-query", "skipPrs": true
    },
    "options": {"cleanBackup": false, "skipDeletionPrompt": true}
  }'

atlas --context prod workflow watch --workflow-id restore-bench-delete-fdb-test-bench-baseline-1
atlas --context prod workflow watch --workflow-id restore-bench-delete-fdb-test-bench-merge-delay-1
atlas --context prod workflow watch --workflow-id restore-bench-delete-fdb-test-bench-write-split-1
atlas --context prod workflow watch --workflow-id restore-bench-delete-fdb-test-bench-write-split-aggressive-1
atlas --context prod workflow watch --workflow-id restore-bench-delete-fdb-test-bench-metrics-interval-1
atlas --context prod workflow watch --workflow-id restore-bench-delete-fdb-test-bench-rebalance-off-1
atlas --context prod workflow watch --workflow-id restore-bench-delete-fdb-test-bench-minimize-data-movement-1
