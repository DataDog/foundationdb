# Benchmark review 1

Verdict: changes are not ready to run the staging benchmarks. Address the findings below, then write `review_adressed_1.md` (spelling intentional) in the repository root with the disposition of each finding and the validation performed. The reviewer will read that response, inspect the updated code, and write `codex_review_2.md`. Repeat with matching incremented counters until a review reports no remaining findings.

Scope: `backup-restore-speedup-notes.md`, all new `restore-bench` scripts and variants, and the local implementations of the toolbox, FoundationDB worker, Atlas CLI, and metric response models. Review only: no benchmarks, kubectl commands, Atlas workflows, or remote metrics queries were run. Syntax checks and in-memory reproductions were used. The reviewer has not modified implementation files.

## R1 — [P1] Cleanup can delete a pre-existing cluster

Location: `restore-bench/run.sh:16-26`.

The EXIT trap is installed before invoking `create.sh`, and its default action unconditionally invokes `delete.sh`. If `create.sh` refuses because the named cluster already has pods, the EXIT trap deletes that existing cluster. The KEEP_CLUSTER branch can also change the existing cluster's rebalance setting and resume its toolboxes even though this run never acquired it.

Track ownership before allowing cleanup to mutate or delete a cluster. Preserve cleanup for resources actually created by this run, including partial provisioning failures; merely moving the trap after all of `create.sh` succeeds would leave failures during provisioning unhandled.

## R2 — [P1] Atlas successful completion is rejected

Location: `restore-bench/lib.sh:193-200`.

`atlas_run` searches for uppercase `COMPLETED`. The current Atlas CLI JSONL final record uses `"status":"Completed"`. A successful create is therefore treated as a failure and triggers cleanup. The delete wrapper also reports failure after successful deletion.

Parse and validate the structured final status rather than searching the entire output. Preserve the CLI exit-code check.

Evidence: local Atlas implementation `~/dd/dd-source/domains/atlas/apps/devtools/atlas/pkg/cmd/workflow/watch.go`, `stateToWatchJSON`, assigns `state.Status.String()`; the installed Temporal Go enum implementation returns `Completed`. An in-memory mock returning `{"status":"Completed","workflow_id":"review"}` reproduced the rejection. JSONL progress goes to stderr and the final record goes to stdout; the CLI does wait for completion.

## R3 — [P1] The assumed Kubernetes topology does not match the worker

Locations: `restore-bench/create.sh:10-15`; `restore-bench/lib.sh:4-10`, pod discovery, toolbox pause/resume, and `apply_knobs`.

The worker expands `oddish-a` to the zonal Oddish cluster set (`oddish-a`, `oddish-b`, `oddish-c`) and distributes `unsetReplicas: 9` across those zones. Thus only three of the nine unset pods are in the kubectl context used by these scripts. Waiting for nine ready pods in `oddish-a` will time out. The worker also deploys one toolbox per Kubernetes cluster, rather than three in `oddish-a`.

Fixing the count alone is insufficient: ConfigMap knob patches, process restarts, toolbox pauses, and discovery currently reach only `oddish-a`. The other zones would retain default knobs, and unpaused remote toolboxes could race database clearing. `verify_cluster` checks all database processes and would reject the partially applied variant.

The notes explicitly restrict kubectl access to `oddish-a` / `hmq-test`; resolving this requires an agreed topology/scope. Do not silently broaden that authorization or claim that the Atlas target confines lifecycle operations to one zone. Update the notes consistently with the eventual decision.

Evidence: worker source under `~/dd/dd-source/domains/platform-automation/apps/foundationdb-worker/`: `pkg/config/worker.go` (`GetZonalCluster`, `GetZonalTargets`), `worker/create_cluster/create_multi_clusters.go` (manifest generation per zonal target), and `worker/create_cluster/deploy_executor.go` (`deployConfigureScale`, `zonalUnsetReplicas = minUnsetReplicas / nbK8sCluster`, one toolbox per Kubernetes cluster). Delete also resolves zonal targets.

## R4 — [P2] Datadog metric extraction silently returns zero

Location: `restore-bench/summarize.sh:32-38`.

The jq expression reads `.data.values`, whereas the v2 timeseries payload stores values under `.data.attributes.values`. The installed Pup documentation says an existing top-level API `data` key is preserved in its output envelope. Optional traversal followed by `add // 0` hides the schema mismatch: successful responses produce zero fetched/input byte totals and a null ratio.

Parse the actual command response shape and distinguish unavailable or malformed metric data from an observed zero. Validate with a representative nonzero v2 response, an empty result, and a failed query.

Evidence: `pup metrics query --help` specifies the v2 API; installed Pup README section "AI Agents" documents the envelope; locally cached Datadog API client models `TimeseriesResponse` and `TimeseriesResponseAttributes` define `attributes.values`. A fixture shaped as `{"status":"success","data":{"attributes":{"values":[[100,200]]},"type":"timeseries_response"}}` returns zero with the existing parser.

## R5 — [P2] Datadog queries include unrelated clusters

Locations: `restore-bench/lib.sh:14`; `restore-bench/summarize.sh:37-38`.

The default query filter is only `kube_namespace:hmq-test`, and the notes explicitly say that namespace hosts other clusters. Once R4 is corrected, the copying-overhead ratio would still include their storage traffic. Scope both queries to the benchmark cluster and datacenter using the actual emitted tags. Initialize the default after the relevant configuration has been loaded. Remote tag availability has not been verified by this review.

## R6 — [P2] Runs do not pin the restore workload

Locations: `restore-bench/bench.conf:7`; `restore-bench/run.sh:33-34,46`.

An empty `RESTORE_VERSION` makes every run independently restore the latest available version. Different variants can therefore use different snapshots, data contents, and mutation-log replay amounts. Recording only `latest` in metadata cannot establish equal workloads after the fact.

Resolve one numeric restore version for a comparison series, reuse it for all compared runs, and record the actual numeric version. If the configured backup is immutable, establish and record that fact rather than assuming it. Avoid selecting a fresh "latest" independently for each variant.

## R7 — [P2] Hotspot calculation hides the strongest imbalance

Location: `restore-bench/summarize.sh:24,29`; storage input collection in `restore-bench/run.sh:72-80`.

The skew calculation skips every sample whose median storage input is zero. On a fresh nine-node cluster, three busy replicas and six idle storage servers yield a zero median despite a severe hotspot. If all samples have this shape, both reported skew values are zero. That can incorrectly suppress the planned aggressive-splitting experiment.

Represent max-positive/median-zero samples explicitly as hotspots, or use another documented measure that preserves them. Distinguish an entirely idle cluster from traffic concentrated on a minority of storage servers. A local synthetic CSV with max input 10,000,000 and median input zero reproduced the erroneous zero skew.

## R8 — [P2] Deletion still prompts and waits despite skipDeletionPrompt

Location: `restore-bench/lib.sh:177-186`, delete request options.

The request sets `skipDeletionPrompt: true` but leaves `gates` unspecified. The worker defaults to the LOCK_DB gate, which offers a staging Slack prompt for up to 15 minutes and, if not skipped there, locks the database for two hours before continuing. `skipDeletionPrompt` controls a different rollout prompt. Therefore unattended per-run teardown still involves a prompt and a long wait, contrary to the apparent benchmark lifecycle expectation.

Decide and document the intended gate behavior for this disposable benchmark cluster. If a different gate policy is selected, keep it within the user's agreed lifecycle settings rather than silently bypassing controls.

Evidence: `worker/delete_cluster/deletion_gate.go`, `NewDeletionGate` and `lockDbGate.Run`, and `worker/delete_cluster/executor.go` in the local FoundationDB worker source.

## Validation and boundaries

- `bash -n` passed for all benchmark scripts.
- ShellCheck reported dynamic-source/standalone unused-variable warnings, without demonstrating a separate functional blocker.
- Local fixtures reproduced R2, R4, and R7.
- Historical files under `restore-bench/runs/` are incomplete devkube debug runs, not staging validation.
- Do not run kubectl or workflows, modify `~/dd/k8s-resources`, or launch benchmarks as part of addressing this review without the authorization required by the notes. Implementation fixes and local mocked validation can proceed independently of those operations.
