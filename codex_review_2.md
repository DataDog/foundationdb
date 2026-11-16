# Benchmark review 2

Read `review_adressed_1.md` and re-reviewed the updated scripts, configuration, and notes. Most first-round fixes are correct, but the findings below remain. Address them and write `review_adressed_2.md`; the next review will be `codex_review_3.md`.

No real kubectl, Atlas workflow, Datadog query, or benchmark was run. The reviewer changed only review documents. Validation consisted of local source inspection, Bash syntax/ShellCheck checks, and in-memory Bash/jq fixtures.

## First-round disposition

- R1: ownership marker prevents deletion when a successful pod lookup actually finds existing pods. Incomplete: discovery failures can still result in claiming and deleting an existing cluster (R2.1 below).
- R2: fixed. Current Atlas JSONL status `Completed` is parsed correctly, and the watch exit code is checked.
- R3: zonal iteration and aggregate counts are implemented. Keeping the default context list limited to the authorized zone, refusing lifecycle operations until all zones are configured, and recording the pending user decision is appropriate. Discovery error propagation still needs R2.1.
- R4: fixed for representative numeric v2 and flattened responses. Local fixtures returned the expected sum of 300 for both shapes, and the code distinguishes no data from an observed zero.
- R5: fixed in the configured default filter; actual emitted tags remain a stated remote-validation assumption.
- R6: numeric version extraction and pinning within one `bench.sh` invocation work. The saved real toolbox response resolves to `53348886043238`. The documented cross-invocation procedure remains incomplete (R2.2).
- R7: fixed. Max/mean input skew and the explicit median-zero concentration measure preserve minority-server hotspots.
- R8: the deletion gate behavior and pending policy decision are now explicit; the default has not silently been bypassed.

## R2.1 — [P1] Failed discovery still permits claiming and deleting an existing cluster

Locations: `restore-bench/create.sh:7-11`; `restore-bench/lib.sh:78-87`; similar deletion completion check in `restore-bench/delete.sh`.

The new ownership guard embeds both discovery calls inside `[ -z "$(fdb_pods)$(pods_with_container foundationdb-toolbox)" ]`. Bash tests the resulting text; it does not propagate either command substitution's exit code. If kubectl credentials/VPN fail and discovery emits nothing, the test succeeds, `cluster-owned` is written, and creation proceeds. When creation/watch fails, cleanup can delete the cluster whose absence was never established.

There is a second, independent masking layer: `pods_with_container` runs a `for` loop on the left of a pipeline. A failed first or middle zone is followed by another iteration, so the loop's exit status is that of its last iteration. A successful final-zone query makes the whole discovery return success even though an earlier zone could not be inspected. `set -e` does not reliably save this when callers use conditional tests or command substitutions. This also allows incomplete toolbox lists to be treated as complete and can falsely satisfy deletion completion checks.

In-memory reproductions against the current functions:

```bash
# No kubectl executed: both functions are shell stubs.
fdb_pods() { return 1; }
pods_with_container() { return 1; }
if [ -z "$(fdb_pods)$(pods_with_container foundationdb-toolbox)" ]; then
  echo 'BUG: failed discovery passes the ownership guard'
fi
```

With the real `pods_with_container`, a stub `kcx` returning an error only for `oddish-b` and `{"items":[]}` for `oddish-a/c` also makes `out="$(fdb_pods)"` return success.

Explicitly propagate failures from every configured zone and separately check the status of discovery assignments before interpreting an empty result. Refuse to mark ownership or launch creation unless absence was positively established everywhere. Apply the same distinction to deletion completion and toolbox pause/discovery paths. Include failures in the first, middle, and last zones in mocked validation. Also avoid considering an existing scaled-to-zero benchmark deployment to be absent merely because it currently has no pods: ownership checks should cover the cluster's existing workload resources, not just running pods.

## R2.2 — [P2] The documented baseline-then-variants procedure still compares different restore versions

Locations: `backup-restore-speedup-notes.md:33-38,390-394`; `restore-bench/bench.sh:19-25`.

`bench.sh` correctly pins later runs to its own first run. However, the actual prescribed workflow is still:

1. Run `run.sh baseline` and inspect its hotspot measurements.
2. Run `bench.sh` with the other variants.
3. Later run Redwood baseline and the best variant.

The standalone baseline does not export its resolved version to the user's shell or persist a shared series selection. Consequently, the subsequent `bench.sh` with empty `RESTORE_VERSION` selects a new latest version, and later standalone Redwood runs can do so again. The default documented comparison is therefore still not controlled, even though each individual summary records its version.

Make the documented procedure concretely reuse the first baseline's numeric version for all compared invocations (including Redwood), or provide a series mechanism that does so. A minimal fix can be an explicit command loading/exporting `RESTORE_VERSION` from the chosen baseline's `meta.json`, used consistently in the handoff and Plan. Keep the restorable-retention constraint visible. Validate a standalone baseline followed by a separate variants invocation, not just two iterations inside one `bench.sh`.

## R2.3 — [P2] Toolbox HTTP error responses can abort and delete an otherwise running benchmark

Locations: `restore-bench/lib.sh:102-105,121-130`; `restore-bench/run.sh:78-84`.

The HTTP helper uses `curl -sS`, so HTTP 4xx/5xx responses still return shell success. The polling loop then unconditionally parses `.state | ascii_downcase` under `set -e`. A transient JSON error such as `{"success":false,"message":"temporary error"}` has no state and makes jq exit 5; an HTML gateway error does the same. Rather than logging a missing sample and retrying, `run.sh` exits and its trap tears down the ongoing restore. This conflicts with the documented tolerance for monitoring gaps and the intent of the surrounding conditional.

The same helper treats an HTTP error from `/pause` or `/resume` as success and discards the response, so the required pause before clearing the database is not actually verified.

Have the helper expose non-success HTTP status, and validate restore-status JSON/state inside a guarded branch so a bad read sample follows the intended polling-error path. Check mutation endpoint responses as appropriate; do not automatically retry `/restore/start` after an ambiguous outcome. Mock at least non-2xx JSON, non-JSON, and valid status responses, and confirm that a transient read failure does not invoke cleanup while an actual reported aborted restore still fails.

## Remaining operational decisions

The first response correctly leaves zone authorization, deletion-gate policy, and the other staging approvals pending. Those decisions do not require running external operations to address this review, and this review does not authorize them.
