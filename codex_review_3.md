# Benchmark review 3

Verdict: **no remaining code-review findings**. The review loop is complete; no `review_adressed_3.md` is required.

Read `review_adressed_2.md` and inspected the current benchmark scripts, configuration, shared series-version handling, and updated notes. The findings from reviews 1 and 2 are addressed within the scope of local review.

## Independent validation

- Bash syntax checks pass for every benchmark script.
- ShellCheck passes at warning severity with SC2034/SC1090 excluded for the sourced-library pattern.
- Mocked failures in each of the first, middle, and last zones make `fdb_pods`, `pod_count`, and `cluster_absent` fail. A failed discovery can no longer establish ownership.
- A scaled-to-zero benchmark StatefulSet prevents a claim of absence; a successfully inspected empty cluster is accepted as absent.
- The real HTTP/status helper code, with a stubbed transport, accepts valid 200 restore-status JSON and rejects non-2xx JSON/HTML, missing state, and invalid JSON.
- The actual polling loop, extracted without changes and run with local stubs, skips transport failures and malformed samples, then completes on the new restore UID's completed state. A reported aborted restore still fails with its error message.
- A recorded numeric series version is reloaded in a separate shell invocation; an explicit numeric version overrides the stored value.
- The first-round fixes for Atlas completion parsing, metric extraction/scoping, hotspot measurement, and documented deletion-gate behavior remain intact.

## Scope and operational status

This is a local code review, not validation of an actual staging restore. No real kubectl, Atlas workflow, Datadog query, or benchmark was run. The reviewer changed only `codex_review_1.md`, `codex_review_2.md`, and this review document; local fixture files were temporary and removed. `~/dd/k8s-resources` was not modified.

The user decisions already listed in `backup-restore-speedup-notes.md` remain pending, including access to the other Oddish zones and the deletion-gate policy. Actual Vector tags and backup-version retention still require the documented environment checks. The default configuration continues to refuse lifecycle operations with only one zone configured. Closing the code review does not authorize staging operations or resolve those decisions.
