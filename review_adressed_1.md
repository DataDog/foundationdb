# Review 1 addressed

Response to `codex_review_1.md`. All eight findings were accepted. R3 and R8 also need a user decision, which is recorded as a blocking open decision in the notes rather than taken here.

Nothing was run against a cluster, Atlas or Datadog. Validation used `bash -n`, ShellCheck, and a mocked sandbox (a copy of `restore-bench` in `/tmp` with stub `kubectl`, `atlas` and `pup` on `PATH`, deleted afterwards). `~/dd/k8s-resources` was not touched.

## R1 — Cleanup can delete a pre-existing cluster: fixed

- `run.sh` exports `CLUSTER_OWNED_MARKER=runs/<dir>/cluster-owned`.
- `create.sh` refuses if any `<FDB_CLUSTER>` fdb or toolbox pod exists in any zone. Only after that check does it create the marker, right before starting the Atlas creation. A failed or partial provisioning is therefore still owned and cleaned up.
- `cleanup` (the EXIT trap) does nothing when the marker is absent: no delete, no rebalance change, no toolbox resume. When the marker is present, it deletes the cluster, or with `KEEP_CLUSTER=1` re-enables rebalance and resumes the toolboxes. It removes the marker after a successful delete.

Validation (mocked):
- Pre-existing pods in all 3 zones: `create.sh` fails with "already has pods … not touching it", cleanup logs "not created by this run, leaving it untouched". 0 Atlas calls, 0 kubectl exec/replace/delete calls.
- Marker present: cleanup runs `delete.sh` (one `DeleteCluster` workflow), and the marker is removed.
- Create workflow ending `Failed` (partial provisioning): cleanup still starts `DeleteCluster`.

## R2 — Atlas successful completion is rejected: fixed

`atlas_run` now:
- requires `atlas workflow watch --output jsonl` to exit 0;
- parses the last stdout line as JSON and requires `.status == "Completed"`;
- reports any other status, or unparsable output, as a clean failure (`status 'unknown'`) instead of letting `set -e` abort on a `jq` error.

I confirmed against the local Atlas source (`pkg/cmd/workflow/watch.go`): `stateToWatchJSON` writes `state.Status.String()`, progress goes to stderr, and a single final record goes to stdout.

Validation (mocked `atlas`, exit code of `atlas_run`):

| Watch output | CLI exit code | Result |
|---|---|---|
| `{"status":"Completed"}` | 0 | accepted (0) |
| `{"status":"Failed"}` | 0 | rejected (1) |
| `{"status":"Terminated"}` | 0 | rejected (1) |
| `{"status":"COMPLETED"}` | 0 | rejected (1) |
| non-JSON output | 0 | rejected (1) |
| `{"status":"Completed"}` | 1 | rejected (1) |

## R3 — Kubernetes topology doesn't match the worker: fixed in code, blocked on authorization

I confirmed in the worker source:
- `GetZonalTargets` expands `oddish-a` to the zonal set;
- `deploy_executor.go` uses `zonalUnsetReplicas = minUnsetReplicas / nbK8sCluster` and `toolboxReplicas = ToolboxReplicasPerK8sCluster` (one per k8s cluster);
- delete also resolves the zonal targets.

Changes:
- **Multiple contexts:** `kc` is replaced by `kcx <context>`, which refuses contexts not listed in `KUBE_CONTEXTS` (`bench.conf`, one per zonal k8s cluster) and any namespace override.
- **Pod tracking:** pods are tracked as `<context>/<pod>`. Pod counting, toolbox selection, toolbox pause/resume, `apply_knobs` (ConfigMap patched in every zone, all 9 pods restarted one by one) and pod deletion checks all span every configured context.
- **Counts:** `TOOLBOX_REPLICAS` equals `EXPECTED_ZONES` (3). `create.sh` waits for 9 fdb pods and 3 toolboxes across zones.
- **No silent broadening:** `KUBE_CONTEXTS` defaults to `oddish-a.us1.staging.dog` only. `require_all_zones` makes `create.sh`, `run.sh`, `delete.sh`, `bench.sh` and `apply_knobs` refuse while fewer than 3 contexts are listed, with a message saying access to all zonal clusters must be authorized. `setup.sh` only warns.
- **Notes:** the handoff lists this as blocking open decision 1 (the user must authorize `oddish-b.us1.staging.dog` and `oddish-c.us1.staging.dog`, namespace `hmq-test`). The environment, knob, script and create-step sections describe the zonal topology.
- **Related fix found while testing:** `lib.sh` had turned `KUBE_CONTEXTS` into a bash array, and arrays aren't exported, so child scripts only saw the first context. `KUBE_CONTEXTS` now stays an exported string, split into the internal `KUBE_CONTEXT_LIST`. `bench.conf` also no longer overrides values coming from the environment (`KUBE_CONTEXTS`, `DELETE_GATES`, `RESTORE_VERSION`).

Validation (mocked):
- With one context: `create.sh` refuses before any kubectl or Atlas call (0 calls). `kcx` refuses an unlisted context.
- With three contexts: `fdb_pods` returns one pod per zone and filters out another cluster's pod.
- `apply_knobs` for `minimize-data-movement` patches the ConfigMap in all 3 zones (block inserted under `[fdbserver]`) and deletes exactly the 3 stub fdb pods, never the other cluster's.

## R4 — Datadog metric extraction silently returns zero: fixed

- `pup_sum` now returns `{"value", "status"}`:
  - `error` when `pup` exits non-zero, `.status != "success"`, or the values aren't an array;
  - `no_data` when the result has no points;
  - `ok` with the sum otherwise, including an observed 0.
- Values are read from `data.attributes.values` (v2 timeseries) or `data.values`. The latter is the flattened shape `pup metrics query` actually printed in an earlier session (`{"data":{"times":[],"values":[],…},"status":"success"}`), so both are accepted.
- `summary.json` and `results.csv` record `metrics_status`. The fetched ÷ ingested ratio is only computed when both queries are `ok`.

Validation (mocked `pup`):

| Response | Result |
|---|---|
| `data.attributes.values` `[[100,200],[50,null]]` | 350, `ok` |
| flattened `data.values` `[[10,20]]` | 30, `ok` |
| empty values | null, `no_data` |
| `[[0,0]]` | 0, `ok` |
| `status: error` | null, `error` |
| `pup` exit 1 | null, `error` |

## R5 — Datadog queries include unrelated clusters: fixed, tags unverified remotely

- `DD_FILTER` defaults to `cluster:<FDB_CLUSTER>,datacenter:<DATACENTER>`, computed after `bench.conf` is loaded. The FoundationDB dashboards filter `fdb.vector.*` metrics on the `cluster` and `datacenter` template variables.
- The filter is recorded in `summary.json`.
- Whether this new cluster's Vector metrics carry these exact tags wasn't checked (no remote queries). It's listed under "Unverified assumptions", and `metrics_status: no_data` will flag a mismatch rather than reporting 0.

## R6 — Runs don't pin the restore workload: fixed

- **`run.sh`:**
  - parses the numeric version fdbrestore reports in the toolbox's `/restore/start` response ("… target restore version N");
  - fails if it can't, or if it differs from a set `RESTORE_VERSION`;
  - records it as a number in `meta.json` and in a new `restore_version` column in `results.csv`;
  - writes and reads the response with `printf '%s\n'`, so it doesn't depend on the shell's `echo`.
- **`bench.sh`:** with `RESTORE_VERSION` empty, the first run resolves the latest version, and it's exported for all later runs of the series ("series pinned to restore version N").
- **Notes:** the staging backup is continuous, not immutable, and the historical-metrics flavor sets `expire_days: 7`. A series must finish while its pinned version is still restorable.

Validation:
- On the real `/restore/start` response saved by a devkube debug run (`runs/20261113T121650Z-baseline-double-ssd-debug3/restore-start.json`), the parser returns `53348886043238`, and `.success` parses.
- `bench.sh`'s pinning extraction returns that value from a stub `meta.json`.

## R7 — Hotspot calculation hides the strongest imbalance: fixed

- `run.sh` also records the mean storage input rate (new `cluster.csv` column `ss_input_hz_mean`).
- `summarize.sh` now computes per-sample skew as max ÷ mean over busy samples (max > 0), skipping fully idle samples.
- It adds `ss_input_concentrated_pct`: the share of busy samples whose median is 0, meaning traffic sits on a minority of storage servers.
- The hotspot check in the notes uses both values.

Validation: synthetic `cluster.csv` with one idle sample, two samples at max 10,000,000 / median 0 / mean 3,333,333, and one at max 4,000,000 / median 2,000,000 / mean 2,000,000. Result: `ss_input_skew_avg` 2.67, `ss_input_skew_max` 3.00, `ss_input_concentrated_pct` 67, instead of 0 / 0 before.

## R8 — Deletion still prompts and waits despite skipDeletionPrompt: documented, decision left to the user

I confirmed in `delete_cluster/deletion_gate.go`: with no `gates`, `NewDeletionGate` uses `DELETION_GATE_TYPE_LOCK_DB`, which runs a Slack prompt with a 15-minute timeout and then locks the database for 2 hours. `DELETION_GATE_TYPE_NOOP` bypasses it, and `skipDeletionPrompt` controls a different prompt.

Changes:
- `DELETE_GATES` in `bench.conf`, empty by default (the worker default applies). It's passed as `options.gates` when set.
- `delete.sh` logs the expected prompt and 2-hour lock when the default gate applies.
- The notes list the gate as blocking open decision 2. `NOOP` is only to be set with the user's explicit approval, so no control is bypassed silently.

Validation: `delete_cluster_input` has no `gates` by default, and `"gates":["DELETION_GATE_TYPE_NOOP"]` with `DELETE_GATES=DELETION_GATE_TYPE_NOOP`.

## Other validation

- `bash -n` passes for every script, and ShellCheck (`-S warning`, ignoring SC2034/SC1090, which come from the sourced-library pattern) reports nothing.
- `backup-restore-speedup-notes.md` is updated:
  - handoff: open decisions (zones, deletion gate), unverified assumptions, this review loop, working rules with `kcx`;
  - benchmark sections: zonal topology, ownership, gates, pinned restore version, metrics scoping and statuses, skew definition.

## Still requires the user before any run

1. Authorize kubectl on `oddish-b.us1.staging.dog` and `oddish-c.us1.staging.dog` (namespace `hmq-test`) and list them in `KUBE_CONTEXTS`.
2. Choose the deletion gate policy (`LOCK_DB` default vs `NOOP`).
3. The earlier open decisions: knobs via ConfigMap, cluster name, `skipPrs` / `useDynamicDeployment`, Slack channel, restore through the toolbox API.
