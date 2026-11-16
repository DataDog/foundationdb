# Review 2 addressed

Response to `codex_review_2.md`. All three findings were accepted and fixed.

Nothing was run against a cluster, Atlas or Datadog. Validation used `bash -n`, ShellCheck, and a mocked sandbox: a copy of `restore-bench` in `/tmp` with stub `kubectl` and `atlas` on `PATH`. The `kubectl` stub can fail per zone, return a scaled-to-zero workload, and serve toolbox HTTP responses with a chosen status code. The sandbox was deleted afterwards. `~/dd/k8s-resources` was not touched.

## R2.1 — Failed discovery still permits claiming and deleting an existing cluster: fixed

**Discovery is now fail-closed (`lib.sh`).**
- `pods_with_container` queries each zone into a variable and returns non-zero, logging the zone, as soon as one zone's `kubectl get` or the `jq` parsing fails. A failure in the first, middle or last zone can no longer be hidden by a later zone's success, and there's no loop piped into `sort` anymore.
- New `cluster_workloads` lists the cluster's StatefulSets and Deployments (`<FDB_CLUSTER>-…`) in every zone, with the same fail-closed behaviour.
- New `cluster_absent` succeeds only when pods (fdb and toolbox) and workloads are positively absent in every zone. A scaled-to-zero deployment therefore counts as existing.
- New `pod_count` fails when discovery fails, instead of reporting 0.
- `first_toolbox` checks the discovery status before using the result.
- The library documents the rule: check the status (`x="$(fdb_pods)" || die`), never `[ -z "$(fdb_pods)" ]`.

**Callers updated:**
- **`create.sh`:** writes the ownership marker and starts creation only if `cluster_absent` succeeds; otherwise it refuses without touching anything. The readiness waits use `pod_count`, so a failed discovery means "not ready yet", never a match.
- **`delete.sh`:** waits for `cluster_absent`, so deletion completes only once absence is proven in every zone.
- **`pause_toolboxes`:** fails if discovery fails or fewer than 3 toolboxes are found. `resume_toolboxes` stays best effort but logs a discovery failure.
- **`apply_knobs`:** refuses to restart pods if fdb discovery fails or doesn't return exactly 9 pods.
- **`setup.sh`:** refuses explicitly when discovery fails.

A grep confirms no `[ -z "$(…)" ]` discovery check, no `for … in $(discovery)` and no `| wc -l` count on discovery remains.

Validation (mocked, 3 configured contexts):

| Case | Result |
|---|---|
| `create.sh`, discovery failing only in oddish-a / oddish-b / oddish-c | exit 1 "absence couldn't be checked in every zone" each time; no marker; 0 Atlas calls |
| `create.sh`, scaled-to-zero `<cluster>-unset` StatefulSet and no pods | refused; no marker; 0 Atlas calls |
| Only another cluster's workload present | `cluster_absent` succeeds |
| `fdb_pods` / `pod_count` / `cluster_absent` with each zone failing in turn | discovery fails, count fails, "not proven absent" |
| Delete completion with oddish-b failing | still waiting |
| `pause_toolboxes` with oddish-c failing | refused |

## R2.2 — The documented baseline-then-variants procedure compares different restore versions: fixed

**A series mechanism shared across invocations:**
- `run.sh` calls `record_series_version` after the restore starts. When `RESTORE_VERSION` was empty, this writes the numeric version actually restored to `runs/series-restore-version`, but only if the file doesn't exist yet.
- `bench.conf` sets `RESTORE_VERSION="${RESTORE_VERSION:-$(cat runs/series-restore-version)}"`. Every later invocation (`run.sh`, `bench.sh`, Redwood runs) therefore restores the same version, and `run.sh` already fails if fdbrestore reports a different one.
- An explicit `RESTORE_VERSION=…` in the environment wins. Deleting the file starts a new series.
- `bench.sh` no longer has its own pinning code: it logs whether it reuses a series version or starts one.

**Notes:**
- Handoff next steps and the Plan now say: start a series (no file), `run.sh baseline` records the version, `bench.sh` and the Redwood runs reuse it automatically, and check `restore_version` in `results.csv`.
- The Redwood section says to stay in the same series.
- The 7-day retention constraint (`expire_days: 7`) stays visible under unverified assumptions and in the benchmark section.

Validation (separate `bash` invocations, as separate script runs would be):

1. Before any run, `RESTORE_VERSION` is empty.
2. Invocation A, a standalone baseline, records 53348886043238 (the version resolved from the real devkube `restore-start.json`).
3. Invocation B, a separate `bench.sh`, loads 53348886043238.
4. Invocation C, a Redwood run with `ENGINE=ssd-redwood-1`, loads 53348886043238.
5. A later run doesn't overwrite the file.
6. `RESTORE_VERSION=111` overrides it.
7. After deleting the file, the version is empty again (new series).

## R2.3 — Toolbox HTTP error responses can abort and delete a running benchmark: fixed

**`lib.sh`:**
- **`toolbox_api`** uses `curl -w '\n%{http_code}'`, prints the body, and returns non-zero on a transport error or a non-2xx status (logging method, path, pod and code).
- **`status_json`** returns non-zero (instead of exiting) unless the response is 2xx JSON with `.cluster`.
- **`restore_status`** returns non-zero unless the response is 2xx JSON with a string `.state`.
- **`pause_toolboxes`** requires `success: true` from each of the 3 toolboxes. `resume_toolboxes` checks it too, but only warns.
- **`ensure_no_backup`** logs a failed abort request.

**`run.sh`:**
- **Each poll** validates and formats the restore sample inside one guarded condition. A failed or malformed sample is logged ("restore status sample skipped") and skipped, and the same goes for the cluster status sample. `uid`/`state` extraction can't abort. Only a reported `aborted` restore (or the time limit) ends the run.
- **Before the start:** reading the restore status and pausing all toolboxes are checked explicitly. If the pause fails, the toolboxes are resumed and the restore isn't started.
- **`/restore/start`** is called once: on a transport or HTTP error the response is saved, the toolboxes are resumed and the run fails with "not retried". It's never retried after an ambiguous outcome.
- **Status snapshots** before and after the restore are best effort and no longer fatal.

Validation (mocked):

| Case | Result |
|---|---|
| `restore_status`: 200 `{"state":"Running",…}` | accepted |
| `restore_status`: 500 JSON error, 502 HTML, 200 JSON without `state`, 200 non-JSON | rejected |
| `pause_toolboxes`: 200 `success:true` | paused (3 toolboxes) |
| `pause_toolboxes`: 500, 200 `success:false`, 502 HTML | refused |
| `status_json`: 503 | rejected |
| `status_json`: 200 with `.cluster` | accepted |
| Polling loop (extracted from `run.sh`): transport failure, non-JSON, JSON error without state, then `completed` | loop ends normally after 5 samples, 2 recorded; EXIT trap (cleanup) not triggered during the transient failures |
| Polling loop: `running`, transport failure, then `aborted` | exits 1 with "restore aborted: boom"; the EXIT trap runs as expected |

## Other validation

- `bash -n` passes for every script, and ShellCheck (`-S warning`, ignoring SC2034/SC1090 from the sourced-library pattern) reports nothing.
- `backup-restore-speedup-notes.md`: the handoff, benchmark sections and Plan describe fail-closed discovery, the HTTP error handling, and the series restore version.

## Still requires the user before any run

Unchanged from `review_adressed_1.md`:
1. kubectl authorization for `oddish-b.us1.staging.dog` and `oddish-c.us1.staging.dog` (namespace `hmq-test`), listed in `KUBE_CONTEXTS`;
2. the deletion gate policy;
3. the other open decisions in the notes' handoff section.
