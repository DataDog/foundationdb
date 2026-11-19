# Speeding up FDB 7.3 backup restores: avoiding repartitioning churn

Notes from an investigation on FDB **7.3.77** (black box: knobs only, no code changes; upgrade possible, no downgrade).
Code references are to `release-7.3` unless stated otherwise (a clone may exist at `/tmp/fdb-src/fdb73`; re-clone `apple/foundationdb` branch `release-7.3` if not).

## Handoff: current status (2026-11-16)

Read this first if you are picking the work up.

### Where things stand

- **Current approach (supersedes the scripts below):** `restore-bench/commands.sh`, a command sheet run by hand, with every value written out (no variables). It has 4 steps, each covering the 7 `fdb-test-bench-<variant>` clusters at once:
  1. **Creation:** Atlas `CreateCluster` for each cluster (`skipPrs: true`, no dynamic deployment, `kubernetesCluster: oddish`, 27 nodes (9 per zone, 81 processes), `three_data_hall`, Redwood, 7.3.77, no backup), then watch them.
  2. **Config update:** per cluster:
     - reconciler dry-run (`POST /reconciler/config -F dryrun=true`) on the 3 toolboxes. Otherwise autotune reverts `three_data_hall_fallback` and re-assigns classes. This is in memory only, so a toolbox restart resets it;
     - `configure three_data_hall_fallback logs=6 commit_proxies=6`;
     - one class per process via `setclass`, built from the status with jq. Per zone, pods sorted by machineid: pods 0-4 are 3× storage, pods 5-6 are log + commit_proxy + stateless, pods 7-8 are 3× stateless. That gives 45 storage, 6 log, 6 commit_proxy, 24 stateless;
     - checks: configuration, class counts, each knob present on 81 processes;
     - `datadistribution enable/disable rebalance`.

     Creation stays in `three_data_hall` because the creation workflow waits for the reconciler and 9 coordinators.
  3. **Restore:** per cluster, pause the oddish-b/c toolboxes, then start the restore (latest restorable version) through the oddish-a toolbox, which resumes itself after the start. Then poll `/restore/status` for all clusters. The 7 restores run concurrently and share the backup bucket's bandwidth and possibly nodes.
     - **3b. After the restore:** once all are `completed`, resume the oddish-b/c toolboxes.
  4. **Deletion:** Atlas `DeleteCluster` for each cluster (no dynamic deployment, `spec.skipPrs: true`: no `k8s-resources` PR, so the clusters' config must be removed by hand after the benchmark), then watch them.

  Every values file also sets `knob_target_bytes_per_storage_server: "1500000000"` and `knob_storage_hard_limit_bytes: "2250000000"`. These were added on 2026-11-19 and are uncommitted in `k8s-resources` (the user commits them). Knobs come from the user's committed per-cluster values files in `k8s-resources` (`k8s/foundationdb/values/clusters/fdb-test-bench-*._._.us1.staging.dog.yaml` and the `clusters.bzl` entries, branch `baptiste.merliot/backup-restore-bench-clusters`). They must be merged before creation, since creation deploys from them. The `run.sh`/`create.sh`/… scripts and the review loop below describe the earlier, fully scripted approach.

- **Analysis done:** sections below (why restores repartition, Options A/B, metrics). Option A is benchmarked as the `minimize-data-movement` variant.
- **Benchmark scripts written** in `~/dd/foundationdb/restore-bench/` for a real staging cluster (`oddish-a.us1.staging.dog`, namespace `hmq-test`), created and deleted per run with Atlas workflows. See "Benchmark setup" below.
- **Nothing has run on staging yet.** The only runs so far were on devkube, abandoned because it was too far from a real setup (see "Lessons from the devkube debug runs"). `runs/` holds those partial debug runs; they are not comparable with staging results.
- `~/dd/k8s-resources` was reset by the user and **must not be modified** anymore.
- A local code review of the scripts was completed in 3 rounds (`codex_review_1..3.md`, responses `review_adressed_1..2.md` in `~/dd/foundationdb/`). Round 3 found nothing left. It was a review only: nothing has been validated on staging. If you change the scripts, read those files first to avoid reintroducing fixed issues (fail-closed discovery, ownership marker, Atlas `Completed` status, HTTP error handling, series restore version).

### Open decisions (ask the user before running)

1. **Zonal topology (blocking):** the worker spreads the cluster over the zonal set `oddish-a/b/c` (3 fdb pods and 1 toolbox in each). The scripts need kubectl access to all three to count pods, apply knobs, pause toolboxes and restart pods. Only `oddish-a.us1.staging.dog` is authorized so far, so `create.sh`/`run.sh`/`delete.sh`/`bench.sh` refuse to run until the user authorizes `oddish-b.us1.staging.dog` and `oddish-c.us1.staging.dog` (namespace `hmq-test`) and they are added to `KUBE_CONTEXTS` in `bench.conf`.
2. **Deletion gate:** by default the worker's `LOCK_DB` gate applies (Slack prompt up to 15 min, then the database stays locked 2 h before deletion), so each run's teardown takes up to ~2h15 or needs someone to answer the prompt. `DELETE_GATES=DELETION_GATE_TYPE_NOOP` would skip it; only set it with the user's explicit approval.
3. **Knobs** are applied by editing the cluster's ConfigMap `<FDB_CLUSTER>-unset` in every zonal k8s cluster and deleting the fdb pods one at a time (`apply_knobs` in `lib.sh`), because `CreateCluster` has no knob field and `k8s-resources` is off limits. These are kubectl writes in `hmq-test` on our own cluster only. Not yet approved.
4. **Cluster name** `foundationdb-hmq-restore-bench` (`bench.conf`). Not yet confirmed.
5. **`skipPrs: true` and `useDynamicDeployment: true`** in the create/delete requests: not confirmed for a new cluster in `hmq-test`.
6. **Slack:** the workflows post to `event-platform-topology-automation-ops` by default; `SLACK_CHANNEL` in `bench.conf` can redirect them. Not decided.
7. **Restore and monitoring** go through the toolbox HTTP API via `kubectl exec` (toolbox RPCs in foundationdb-worker are private activities). Not confirmed.

### Unverified assumptions

- Vector metrics carry `cluster:<FDB_CLUSTER>` and `datacenter:us1.staging.dog` tags (`DD_FILTER`, used by `summarize.sh` for the fetched ÷ ingested ratio). The FoundationDB dashboards filter Vector metrics on these tags, but this cluster's tags haven't been checked. `metrics_status` in the summary says whether the queries returned data.
- The series restore version stays restorable for the whole series: the historical-metrics flavor sets `expire_days: 7`, so all compared runs (including Redwood) must happen within that window, or a new series must be started.

### Next steps

1. Get answers to the open decisions above.
2. `./setup.sh`: read-only preflight (kubectl access, Atlas workflow types, `--dry-run` of both requests).
3. Start a series: make sure `restore-bench/runs/series-restore-version` doesn't exist (delete it to start a new one).
4. `./run.sh baseline` (creates the cluster, restores, summarizes, deletes it). It records the version it restored in `runs/series-restore-version`. Then analyse `runs/<dir>/summary.json`, `restore.csv`, `cluster.csv` with the user, including the hotspot check.
5. `./bench.sh` with the other variants (possibly skipping `write-split-aggressive`), then Redwood runs (`ENGINE=ssd-redwood-1 ./run.sh baseline …`, then the best variant). Every invocation loads the same version from `runs/series-restore-version` automatically; check `restore_version` in `results.csv`.

### Working rules from the user

- Never run `kubectl` without explicit approval; print the command first. In scripts, always `kubectl`, never `k`. Only use authorized contexts (so far `oddish-a.us1.staging.dog`) and namespace `hmq-test`; every call goes through `kcx()`, which refuses contexts not listed in `KUBE_CONTEXTS`.
- Atlas: `atlas --context prod` with `--task-queue foundationdb/us1.staging.dog` (the worker runs on the prod Atlas cluster).
- Don't modify `~/dd/k8s-resources`. Don't edit files of a running script (bash reads scripts incrementally); `lib.sh` is safe because it's sourced at start.
- Ask before deviating from the plan; keep this file up to date when the setup changes.

## Problem

During a restore, the target cluster grows from empty, which triggers shard splits/merges/moves.
Data gets copied between storage servers while the restore is writing, slowing it down.

Question: can the number of partitions be preset from an estimate, so repartitioning mostly doesn't happen?

## TL;DR

- FDB 7.3 has no "number of partitions" setting. Shard boundaries come from size and write-bandwidth thresholds.
- **7.3 options:**
  - **Option A (recommended first):** knobs that stop split/merge churn during the restore, plus `datadistribution disable rebalance`.
  - **Option B:** pin boundaries in advance with `fdbcli rangeconfig`. This pins boundaries but doesn't place shards on teams.
- **Upgrade:** BulkLoad in 7.4 (experimental, fdbcli only) and `fdbrestore --mode bulkload` in 8.0 ingest SSTs directly into storage servers, already partitioned and placed. It needs backups taken with BulkDump.

## How data distribution reacts to a restore (7.3)

### Restore write pattern

- Backup snapshots each source shard with its own task, started at a random version within the snapshot interval (`fdbclient/FileBackupAgent.actor.cpp:2562`).
- Restore replays range files in version order. Each 1MB block is a separate task run in parallel by the agents (`design/restore_v1.md`).
- Result: the restore fills a contiguous chunk of keys (about one source shard, up to ~500MB) in a burst, never writes there again, and does this at random points in the key space.

### Movement sources

| Source | Trigger | Impact during restore |
|---|---|---|
| **Write split** | Shard over 20MB with more than `SHARD_MAX_BYTES_PER_KSEC` = 1e9 bytes per 1000s = **1 MB/s** (`DDShardTracker.actor.cpp:43`, `StorageMetrics.actor.cpp:273`). Split into pieces of at most `SHARD_SPLIT_BYTES_PER_KSEC` (250 KB/s), each at least `MIN_SHARD_BYTES` (10MB). All pieces but one are moved (`executeShardSplit`, l.494–521). | Every burst crosses 1 MB/s. Write rates are averaged over 120s (`STORAGE_METRICS_AVERAGE_INTERVAL`), so the burst is usually over before the pieces move. Mostly wasted moves. |
| **Merge** | Shard smaller than maxShardSize/`SHARD_BYTES_RATIO` (4) (`getShardSizeBounds`, l.101), with less than 100 KB/s (`SHARD_MIN_BYTES_PER_KSEC`), for longer than `DD_MERGE_COALESCE_DELAY` = 300s (`HasBeenTrueFor`, l.431). The merged range is moved to one team. | Chunks go cold right after their burst, so they become merge candidates. The threshold rises (10MB → 125MB) as maxShardSize grows. Moves happen during the restore, and merged shards spanning unrestored ranges can be split and moved again. |
| **Size split** | Shard larger than `getMaxShardSize(dbSize)` (l.121): `min((MIN_SHARD_BYTES + sqrt(dbSize) * SHARD_BYTES_PER_SQRT_BYTES) * SHARD_BYTES_RATIO, MAX_SHARD_BYTES)`, which grows from ~40MB to 500MB (reached at ~6.5TB). | Unavoidable on an empty cluster without pre-split boundaries. |
| **Rebalance** | Background disk/read rebalancer (`DDRelocationQueue.actor.cpp`). | Extra moves. Data arriving at random points balances out on its own anyway. |

Note: `design/AI-generated` / `data-distributor-internals.md` gives the `*_PER_KSEC` knobs in the wrong units (it says 1 GB/s). They are bytes per 1000 seconds.

## Option A: knobs plus disabling rebalance (recommended)

### Knobs

| Knob | Default | During restore | Effect |
|---|---|---|---|
| `DD_MERGE_COALESCE_DELAY` | 300 | **86400** (longer than the restore takes) | No merges during the restore; they happen afterwards. Only read by the data distribution shard tracker. |
| `SHARD_MAX_BYTES_PER_KSEC` | 1e9 (1 MB/s) | **1e10 (10 MB/s)**; 1e12 effectively disables write splits | Bursts stop triggering splits. Must stay above 2× `SHARD_SPLIT_BYTES_PER_KSEC` (250e6). |
| `ALLOW_LARGE_SHARD` / `MAX_LARGE_SHARD_BYTES` | false / 1e9 | **leave at default** | See below. |

Runtime switch: `fdbcli> datadistribution disable rebalance` turns off disk and read rebalancing. Split pieces still go to the least-loaded teams. This setting is stored in the database and **survives restarts**, so it has to be re-enabled by hand.

### Why not `ALLOW_LARGE_SHARD`

When enabled, maxShardSize = max(formula, `MAX_LARGE_SHARD_BYTES`), so it's fixed from the first byte. It's only read in `getMaxShardSize` in 7.3. Rough model, assuming roughly even arrival:

- **Fixed max shard size M:** each split moves about M/2, and growing to D bytes takes about D/(0.75·M) splits, so roughly **⅔·D** moved, whatever M is.
- **Default formula (M grows like √D):** early splits are small and cheap, roughly **⅓·D** moved below ~6.5TB. Above that both behave the same (500MB cap).
- The default formula leaves more, smaller shards at the end. With merges deferred, that clean-up happens after the restore.
- A fixed 500MB also slows the start: everything goes to one team until 500MB has been written.

So keep the default formula for restore speed. `ALLOW_LARGE_SHARD` only helps if total movement including after the restore, or shard count, matters more.

### Why the knobs and not only `disable rebalance`

`datadistribution disable rebalance` sets `rebalanceDDIgnoreKey`. That key is only checked by the background rebalancer (`getSkipRebalanceValue`, `DDRelocationQueue.actor.cpp:2287`). Splits and merges come from `DDShardTracker.actor.cpp`, which never reads it.

`datadistribution off` stops everything, including size splits. Everything would stay on the initial team: ratekeeper throttles on that team, its disks can fill, and data distribution stops re-replicating after storage server failures.

| Lever | Stops |
|---|---|
| `disable rebalance` | rebalance moves |
| `DD_MERGE_COALESCE_DELAY` | merges |
| `SHARD_MAX_BYTES_PER_KSEC` | write splits |
| (nothing on 7.3 without Option B) | size splits, which are needed anyway |

### Applying the knobs

- They're server knobs read when a process starts. Data distribution is a stateless role that can run on any process, so set them on **all fdbserver processes**.
- `foundationdb.conf`, under `[fdbserver]`:
  ```
  knob_dd_merge_coalesce_delay = 86400
  knob_shard_max_bytes_per_ksec = 10000000000
  ```
- k8s operator: put the same values in `customParameters`.
- Don't use 7.3 `setknob`. It depends on the configuration database, which `feature-status.md` lists as having data-consistency issues and as since deleted.

### Procedure

1. Target cluster is empty and idle. Apply the knobs and restart all processes (`fdbcli kill; kill all`, or an operator bounce).
2. Check: `status json` → `cluster.processes.*.command_line` contains the `--knob_...` flags.
3. `fdbcli> datadistribution disable rebalance`
4. `fdbrestore start ...` Don't change knobs until it finishes (that means a restart, which restarts data distribution).
5. When `fdbrestore status` shows completed: revert the knobs, restart, then `datadistribution enable rebalance`.
6. Expect a merge wave starting about 5 minutes later. It runs at low priority (`PRIORITY_MERGE_SHARD` = 340).

### Monitoring and A/B test

Run the same restore with defaults and with the knobs, and compare:

- Wall-clock time.
- `status json` → `cluster.data.moving_data.total_written_bytes`, `in_flight_bytes`, `in_queue_bytes`.
- `cluster.qos.performance_limited_by`. If it shows `storage_server_write_queue_size`, check for a hot team.
- Trace events (the `foundationdb:fdbdd` skill can query them from Datadog logs):
  - `MovingData`: per-priority counts (`PrioritySplitShard`, `PriorityMergeShard`, `PriorityRebalance...`).
  - `RelocateShardStartSplit`: `Bandwidth=High` means a write split, otherwise a size split.
  - `RelocateShardMergeMetrics`: merges.
  - `MaxShardSize`: current threshold and DB size estimate.

Expected with the knobs: `RelocateShardMergeMetrics` disappears and `RelocateShardStartSplit` events with `Bandwidth=High` almost disappear.

Cheapest first step: run one restore with only `disable rebalance`, then check the `MovingData` counts. If split and merge counts dominate, the knobs are worth the restart.

### Risks

- **Hot team.** With write splits off, a burst lands on a single team. Ratekeeper throttles on the worst storage server. If storage queues on a few servers become the limit, lower the threshold (3–5 MB/s).
- **Forgetting to revert.** These settings are wrong for normal traffic, and the rebalance setting stays in the database until re-enabled.
- **More shards during the restore** means more data distribution tracking work (normally fine at tens of thousands of shards).
- **Unavoidable moves still compete with restore writes.** `DD_MOVE_KEYS_PARALLELISM` (15), `RELOCATION_PARALLELISM_PER_SOURCE_SERVER` (2), `FETCH_KEYS_PARALLELISM_BYTES` (4e6) and `FETCH_BLOCK_BYTES` (2e6) speed them up but add storage server load. Only change them if moves turn out to be the bottleneck.

## Option B: pin shard boundaries in advance with `rangeconfig` (7.3)

- `fdbcli rangeconfig set <begin> <end> [replication N] [teamID N]` exists in 7.3.
- When the config changes, data distribution restarts (`DataDistribution.actor.cpp:981`, `dd_config_changed`).
- On restart it splits shards at every rangeconfig boundary (`DataDistribution.actor.cpp:713–745`, `trackInitialShards`).
- Merges never cross a boundary (`shardForward/BackwardMergeFeasible`, `DDShardTracker.actor.cpp:937/949`).

### Recipe

1. Capture the source cluster's shard boundaries (locality `get_boundary_keys` in the bindings) and store them next to each backup. Thin them to roughly final size ÷ target shard size.
2. Pin them with alternating `teamID 0` / `teamID 1`:
   - Adjacent identical ranges are coalesced (`KeyBackedRangeMap.actor.h:159`).
   - `teamID` has no effect anywhere in 7.3 fdbserver (zero uses), so it works as a pure marker.
3. Restore.
4. Unpin with `rangeconfig set "" \xff default`.

### Caveats

- **Boundaries only, not placement.** On a fresh cluster every pinned range still sits on the initial team.
  - Untested idea: `exclude` the initial team's storage servers. Data distribution should then move each pinned (empty) range separately and almost for free. Then `include` the servers back.
- **Restoring into a cluster that already holds similar data is the easy case.** Pin its current boundaries, run `clearrange`, then restore. Both boundaries and placement are kept.
- **Each `rangeconfig` command restarts data distribution**, so keep the number of ranges reasonable. Ranges that grow too big still split by size.
- The DD RPC `distributorSplitRange` exists but isn't wired up in 7.3 or main.

## Other restore speedups

- **Find the bottleneck first:** `cluster.qos.performance_limited_by`, to tell storage limits apart from agent/S3 limits.
- **More `backup_agent` processes.** Client knobs on the agents: `BLOBSTORE_CONCURRENT_REQUESTS`, `BLOBSTORE_REQUESTS_PER_SECOND` (200), `BLOBSTORE_READ_AHEAD_BLOCKS` (0), `BLOBSTORE_CONCURRENT_READS_PER_FILE` (3). Restore dispatch: `RESTORE_DISPATCH_BATCH_SIZE` (30000), `RESTORE_WRITE_TX_SIZE` (256KB).
- **Mutation-log replay is serial** (applied in version order through the commit proxy, see the aLog section of `restore_v1.md`). Shorter snapshot intervals mean less replay. Use `-k` to restore only the key ranges needed.
- **Fewer copies during the restore.** If the topology allows it, restore with `usable_regions=1` (and possibly lower redundancy), then raise it afterwards. Data distribution copies the extra replicas, which is cheaper than going through the transaction system. Durability is reduced during the restore window.

## Upgrade path

- **7.4:** BulkDump/BulkLoad (`fdbcli bulkdump` / `bulkload`). Storage servers ingest SSTs through data distribution, bypassing the transaction system. Experimental; for idle/empty clusters.
- **8.0:** integrated into backup/restore: `fdbbackup start --mode rangefile|bulkdump|both` and `fdbrestore start --mode bulkload` (`release-8.0` `fdbbackup/backup.cpp`).
  - Requires `knob_shard_encode_location_metadata=1` and `knob_enable_read_lock_on_range=1` on all processes, plus a full storage wiggle.
  - Restore can target a non-empty cluster (BulkLoad clears each shard first).
- **Catch:** only backups taken with BulkDump enabled can be restored this way. The source clusters have to be upgraded and producing those backups first.

## Design docs worth reading

- `design/data-distributor-internals.md`: §3.7 shard size bounds, §6 split/merge (watch the wrong knob units).
- `design/restore_v1.md`: task bucket, dispatch batches, aLog, why log replay is serial.
- `design/bulkload-restore-integration.md`: the 8.0 BulkLoad restore path.
- `design/backup_v2_partitioned_logs.md`, `design/feature-status.md`: maturity of backup v2 and BulkLoad.

## Metrics for restore benchmarks

Sources reviewed:
- FDB 7.3 source and https://apple.github.io/foundationdb/monitored-metrics.html
- The two Datadog dashboards below
- The toolbox (`~/dd/datastore-toolboxes/foundationdb-toolbox/`)
- The Vector config (`~/dd/k8s-resources/k8s/foundationdb/templates/config/_vector_metrics.tpl`)

### Where metrics come from

| Pipeline | Prefix | Source | Tags | Notes |
|---|---|---|---|---|
| Toolbox | `dd.foundationdb.*` | status json every 10s (`agent/extractor.py`); `fdbbackup status`/`describe` every 60s | `cluster`, `datacenter`, `team`, `kube_namespace`; role metrics also `fdb_role`, `machine_id`, `port`, `fdb_class` | Role metrics are distributions with a `.histogram` suffix: aggregate with max/p99. |
| Vector | `fdb.vector.<event>.<field>` | each fdbserver's JSON trace files, sidecar (`_vector_metrics.tpl`) | `machine`, `ip`, `port`, `roles` (e.g. `DD_RK`); cluster from pod tags | Counters are `rate * Elapsed` → use `.as_rate()`/`.as_count()`. |

### Dashboards

**FoundationDB** (`u4m-e4w-wfc`, toolbox metrics; filters `cluster`, `team`, `datacenter`, `kube_namespace`)
- Data: `diff(moving_data.total_written_bytes)`, in-flight/queued bytes, `partitions_count`, average partition size, `total_kv_size_bytes`.
- Ratekeeper: released transactions and the TPS limit.
- Worst storage and tlog queue, storage durability and data lag.
- Workload bytes written; per-process CPU, run-loop busy, disk busy, network.
- Backup: blob requests succeeded/failed, and OOM kills of the `foundationdb-backuprestore` container (where the restore agents run).

**FoundationDB Deep Dive** (`trb-u7q-x36`, Vector metrics; filters `datacenter`, `cluster`, which defaults to `foundationdb-estore-shared-datadog-cf7e`)
- Limit Reason (`rk_update.reason`); transactions vs limit.
- Storage server queue and durability lag (limiting and worst); tlog queue.
- Moving Ranges Per Reason (all `moving_data.priority_*`, including split/merge/rebalance).
- Moving-data write rate (`bytes_written_average_rate`); "Where it lands" (`storage_metrics.bytes_fetched` per machine).
- Proxy mutation bytes; storage commit latency histograms.
- CPU busy per role via `network_metrics.priority_starved_below1`.

Not on either dashboard: restore progress, throughput and apply lag; the bytes-fetched / bytes-ingested ratio; a marker for recoveries during a run.

### Benchmark scorecard

Replace `$c` with the cluster tag and compare over the restore window.

| Number | Query | Notes |
|---|---|---|
| **Wall-clock time and throughput** | none yet | See the gaps below |
| **Logical progress** | `max:dd.foundationdb.cluster.data.total_kv_size_bytes{$c}` | Stand-in for % done if the final size is known |
| **Restore write rate** | `sum:fdb.vector.proxy_metrics.mutation_bytes{$c}.as_rate()` | Includes log apply |
| **Bytes moved by data distribution** | `diff(avg:dd.foundationdb.cluster.data.moving_data.total_written_bytes{$c})`, summed over the run | Resets when data distribution restarts |
| **Copying overhead from data movement** | `sum:fdb.vector.storage_metrics.bytes_fetched{$c}.as_count()` ÷ `sum:fdb.vector.storage_metrics.bytes_input{$c}.as_count()` | Should drop with the Option A knobs |
| **Splits, merges and rebalance moves** | `max:fdb.vector.moving_data.priority_{split_shard,merge_shard,rebalance_underutilized_team,rebalance_overutilized_team}{$c}.rollup(max,60)` plus `dd.foundationdb.cluster.data.partitions_count` | Counts of moves queued or in flight, not events |
| **Throttling** | `max:fdb.vector.rk_update.reason{$c}.rollup(max,60)`; `rk_update.tps_limit` vs `rk_update.released_tps` | `RkUpdate` is logged with 10% probability (`Ratekeeper.actor.cpp:1337`), so use 1-minute rollups. `reason` is an enum index. |
| **Storage pressure** | `max:fdb.vector.rk_update.worst_storage_server_queue{$c}`, `rk_update.worst_storage_server_durability_lag{$c}` | Lag is in versions (÷1e6 = seconds) |
| **Tlog pressure** | `max:fdb.vector.rk_update.worst_t_log_queue{$c}` | |
| **CPU busy by role** | `max:fdb.vector.network_metrics.priority_starved_below1{$c AND roles:*ss*}.as_rate()` (same for `*tl*`, `*cp*`) | The Deep Dive's saturation signal |
| **Storage disk** | `fdb.vector.histogram.storage_server.storage_commit_latency.p99`, `fdb.vector.process_metrics.disk_idle_seconds` | |
| **Restore agents** | `kubernetes.cpu.usage.total{kube_container_name:foundationdb-backuprestore,$c}`, pod network rx, `dd.foundationdb.cluster.layers.backup.blob_recent_io.requests_*` | No counter for S3 bytes read (`blob_stats.bytes_sent` counts uploads only) |
| **Is the run valid?** | `diff(max:dd.foundationdb.cluster.generation{$c})` | Any recovery during the run breaks the data-movement totals |

### Restore progress (FDB side, not in Datadog today)

`fdbrestore status` reads the restore config keys, not status json. The same numbers are in the `FileRestoreProgress` trace event (`FileBackupAgent.actor.cpp:433`):

- `FileBlocksFinished` / `FileBlocksTotal`: % complete; rate = throughput.
- `FileBlocksInProgress`: parallelism the agents actually achieve.
- `BytesWritten`: logical bytes committed by restore tasks (before replication).
- `ApplyLag`: mutation-log apply backlog, in versions. Above 300e6 versions (300s), the dispatcher stops starting new batches (`FileBackupAgent.actor.cpp:4401`). This is the key stall signal.
- `CurrentVersion`, `State`.
- `FileRestoreDispatch` events: `Decision=apply_still_behind` counts apply stalls; `restore_complete` marks the end.

### Gaps and fixes

1. **No restore metrics are emitted.** The toolbox parses `fdbrestore status` (`foundationdb/restore.py:135-206`) but only serves it at `GET /restore/status` (`web/routes/restore.py:89`).
   - Fix: add a `RestoreExtractor` emitting `blocks_finished`, `blocks_total`, `blocks_in_progress`, `bytes_written`, `apply_version_lag` and `state`.
   - Until then, the benchmark script polls that endpoint or `fdbrestore status` every 15s.
2. **Toolbox doesn't emit the ratekeeper limiting reason** (`cluster.qos.performance_limited_by`) or the worst/limiting durability and data lags. Vector's `rk_update.reason` covers it, but sparsely.
3. **No split/merge event counts.** `RelocateShardStartSplit` and `RelocateShardMergeMetrics` reach Datadog Logs but are rate-limited (`suppressFor`), so log counts undercount. Use the `priority_*` gauges, `partitions_count` and `total_written_bytes` instead.
4. **`DDTrackerStats` and `MaxShardSize` are dropped** from metrics and logs (`values.yaml:122`). `partitions_count` covers the shard count; max shard size can be computed from `total_kv_size_bytes` with the formula.
5. **No backup/restore trace events go through Vector**, and agent trace files aren't parsed.

### Gotchas

- `moving_data.total_written_bytes` / `MovingData.BytesWritten` reset when data distribution restarts.
- `moving_data.in_flight_bytes` / `in_queue_bytes` are estimates (count × average shard size).
- Versions vs seconds: `ApplyLag` and `*VersionLag` fields are in versions, about 1e6 per second.
- Status json `.hz` values are smoothed rates; `.counter` values are cumulative.
- The official doc spells the durability lag path `cluster.process...`; the real path is `processes`.

### Next steps

1. Add a `RestoreExtractor` to the toolbox.
2. Build a "Restore Benchmark" dashboard or notebook from the scorecard.

## Benchmark setup (staging, oddish-a / hmq-test)

Status: scripts rewritten for staging; nothing has been run yet. An earlier devkube setup was abandoned: too far from a real cluster (shared `c5d.2xlarge` nodes, EBS volumes, 8Gi pod memory causing OOM kills, manual bootstrap).

### Environment

- **Zonal topology:** `kubernetesCluster: oddish-a` in the Atlas request is expanded by the worker to the zonal set `oddish-a/b/c`. The 9 unset pods are spread evenly (3 per zonal k8s cluster) and each zonal cluster gets one toolbox (foundationdb-worker `pkg/config/worker.go` `GetZonalTargets`, `create_cluster/deploy_executor.go`: `zonalUnsetReplicas = minUnsetReplicas / nbK8sCluster`, one toolbox per k8s cluster). Delete also resolves the zonal targets.
- Kube contexts and namespace: `KUBE_CONTEXTS` in `bench.conf` (one per zonal k8s cluster, `EXPECTED_ZONES=3`) and `hmq-test`. Every `kubectl` call goes through `kcx <context>` in `lib.sh`, which refuses contexts not listed in `KUBE_CONTEXTS` and any namespace override. Only `oddish-a.us1.staging.dog` is listed so far (see open decision 1): until all three are, `create.sh`, `run.sh`, `delete.sh` and `bench.sh` refuse to run (`require_all_zones`). `hmq-test` hosts other clusters, so pod lookups only consider pods named `<FDB_CLUSTER>-…`; pods are tracked as `<context>/<pod>`.
- Cluster lifecycle through Atlas workflows (foundationdb-worker, `~/dd/dd-source/domains/platform-automation/apps/foundationdb-worker/worker/`), with `atlas --context prod` (the worker runs on the prod Atlas cluster) and task queue `foundationdb/us1.staging.dog` (selects the target datacenter):
  - create: `create_cluster.CreateClusterService_CreateCluster`
  - delete: `delete_cluster.DeleteClusterService_DeleteCluster`
- `k8s-resources` is not modified.
- Cluster: `FDB_CLUSTER=foundationdb-hmq-restore-bench` (`bench.conf`), 9 nodes, version 7.3.77, `three_data_hall`, `ssd`, TLS, DNS discovery, backup disabled. Same shape as the historical-metrics e2e test cluster (`create_cluster/defaults.go`, `DefaultHistoricalMetricsIOCreateTestClusterRequest`: shared `historical-metrics`, team `historical-metrics-query`, namespace `hmq-test`), with 9 replicas instead of 48 and no backup. Created with `skipPrs: true` and `useDynamicDeployment: true`.
- Toolboxes: one per zonal k8s cluster, so 3; `create.sh` waits for exactly 3 ready toolboxes.
- Redundancy: `three_data_hall` (`CLUSTER_REDUNDANCY`); `double` and `triple` remain possible per run with `REDUNDANCY=…`. The toolbox reconciler targets 9 coordinators for `three_data_hall` (3 for `double`, 5 for `triple`).
- Storage engine: `ssd`, i.e. ssd-2 / SQLite (`CLUSTER_ENGINE`). Override per run with `ENGINE=ssd-redwood-1`.
- Run directories (`…-<variant>-<redundancy>-<engine>-<label>`), `meta.json` and `results.csv` record both.
- Backup source: cluster `foundationdb-hsi-default-2025-03-v0`, bucket `dd-fdb-historical-metrics-backup-us1-staging-dog` (the historical-metrics flavor's bucket), ~200GiB. `BACKUP_FOLDER=foundationdb-hsi-default-2025-03-v0`; the toolbox turns it into `blobstore://foundationdb-historical-metrics@127.0.0.1:9000/<folder>?bucket=<bucket>&secure_connection=0` (`foundationdb_toolbox/k8s/settings.py:62-99`).
- **Restore version, pinned per series:** comparisons must restore the same snapshot and replay the same mutation logs, and the staging backup is continuous (not immutable). `run.sh` records the numeric version it actually restores (parsed from the fdbrestore output in the toolbox response, e.g. "Ussing target restore version 53348886043238" (sic)) in `meta.json` and `results.csv`, and fails if it differs from a set `RESTORE_VERSION`. A **series** shares one version across invocations: the first run with no `RESTORE_VERSION` writes the version it restored to `runs/series-restore-version` (`record_series_version` in `lib.sh`), and `bench.conf` loads that file into `RESTORE_VERSION` for every later invocation (`run.sh`, `bench.sh`, Redwood runs). An explicit `RESTORE_VERSION=…` in the environment wins; deleting the file starts a new series. The historical-metrics flavor expires backups after 7 days, so a series must finish before that version stops being restorable.
- **Discovery is fail-closed:** pod, workload and toolbox discovery query every zone and fail if any zone can't be inspected (`pods_with_container`, `cluster_workloads`, `cluster_absent`, `pod_count` in `lib.sh`). An empty result only means "nothing found" when discovery succeeded. Ownership requires positive proof of absence (no pods and no `<FDB_CLUSTER>-…` StatefulSet/Deployment, so a scaled-to-zero deployment counts as existing). Deletion completes only once absence is proven in every zone. Pausing requires all 3 toolboxes to be found and to acknowledge.
- **Toolbox HTTP calls** (`toolbox_api`) fail on transport errors and non-2xx responses. `restore_status` also requires JSON with a string `state`, `status_json` requires `.cluster`, and `/pause` must answer `success: true`. During the restore, a failed or malformed sample is logged and skipped; only a reported `aborted` restore ends the run. `/restore/start` is never retried after an error, because the restore may or may not have started.
- Datadog queries in `summarize.sh` are scoped with `DD_FILTER=cluster:<FDB_CLUSTER>,datacenter:<DATACENTER>` (set after `bench.conf` is loaded), so other clusters in `hmq-test` don't count.
- Restore and monitoring go through the toolbox HTTP API (`kubectl exec … curl localhost:8080`): the toolbox RPCs in foundationdb-worker are private activities, not workflows the Atlas CLI can start.
- Datadog: staging org (`pup` is logged into it).

### Knobs

`CreateCluster` has no field for fdbserver knobs (chart overrides are limited to `version` and `cpu_arch`, see `create_cluster/CHART_OVERRIDES.md`), and `k8s-resources` must not be modified. So `apply_knobs` in `lib.sh`, only for variants with knobs:
1. in each zonal k8s cluster, adds the knobs in a `# BEGIN/END restore-bench` block right under `[fdbserver]` in the class ConfigMap `<FDB_CLUSTER>-unset` (`init.sh`, which generates `foundationdb.conf` at pod start), via `kubectl replace`;
2. deletes the 9 fdb pods (all zones) one at a time, waiting for each to be ready and the database to be available. The StatefulSet uses `updateStrategy: OnDelete`, so pods only pick up the new configuration when recreated.

The ConfigMap edit is overwritten by the next deploy of that cluster, which is fine since every run creates and deletes its own cluster. `verify_cluster` then checks each process's `command_line`.

### Scripts (`~/dd/foundationdb/restore-bench/`)

| Script | Role |
|---|---|
| `lib.sh` | Shared helpers: `kcx`, `require_all_zones`, pod discovery across zones, toolbox HTTP calls, `fdbcli`, status json, Atlas request builders and `atlas_run`, `apply_knobs`, cluster checks, backup guard. `atlas_run` starts the workflow, blocks on `atlas workflow watch --output jsonl` (progress on stderr, one final JSON record on stdout) and requires a zero exit code and `"status": "Completed"` (the Temporal enum string, atlas `pkg/cmd/workflow/watch.go` `stateToWatchJSON`). |
| `bench.conf` | `FDB_CLUSTER`, Atlas context, datacenter, k8s cluster, `KUBE_CONTEXTS`, `DELETE_GATES`, optional `SLACK_CHANNEL` for the workflows' notifications, `BACKUP_FOLDER`, optional `RESTORE_VERSION`, `BACKUP_SIZE_GIB`, `CLUSTER_REDUNDANCY=three_data_hall`, `CLUSTER_ENGINE=ssd`. `KUBE_CONTEXTS`, `DELETE_GATES` and `RESTORE_VERSION` can be overridden from the environment. |
| `setup.sh` | Read-only preflight: configured contexts vs the 3 zonal clusters, kubectl access to `hmq-test`, the Atlas workflow types, and Atlas `--dry-run` validation of the create and delete requests. If the cluster already exists, also its capacity, backup status and the backup description. |
| `create.sh <variant>` | Creates the cluster with Atlas and gets it ready (steps below). Refuses unless the cluster's absence (pods and workloads) is proven in every zone, and never touches an existing one. |
| `delete.sh` | Deletes the cluster with Atlas (`cleanBackup: false`, `skipDeletionPrompt: true`, `gates` from `DELETE_GATES`, default = worker's `LOCK_DB` gate) and waits until absence is proven in every zone. `skipDeletionPrompt` only controls a rollout prompt, not the deletion gate. |
| `run.sh <variant> [label]` | `create.sh`, set rebalance, start the restore through the toolbox, poll every 15s into `restore.csv` and `cluster.csv` until completed, `summarize.sh`, then `delete.sh`. **Ownership:** `create.sh` writes `runs/<dir>/cluster-owned` right before starting the Atlas creation; cleanup (on success or failure, including partial provisioning) only deletes or touches the cluster if that marker exists, so a pre-existing cluster is left alone. `KEEP_CLUSTER=1` keeps an owned cluster (rebalance re-enabled, toolboxes resumed). |
| `summarize.sh <run-dir>` | Writes `summary.json` and appends a line to `runs/results.csv`. |
| `bench.sh [-n N] <variant>...` | Runs variants interleaved N times with the series restore version, then prints the results table. |
| `variants/*.env` | One file per variant: `KNOBS=(...)` and `DD_REBALANCE=on|off`. |

### Cluster per run (`create.sh`)

1. Atlas `CreateCluster` with the request above; wait for the workflow to complete.
2. Wait for 9 ready fdb pods and 3 ready toolboxes across the zonal k8s clusters, then a healthy database.
3. Wait for the toolbox's coordinator target (9 for `three_data_hall`; warns after 15 min).
4. `apply_knobs` for variants with knobs.
5. Wait until the cluster is stable: same generation and no data movement for 8 polls (2 min). The toolbox reconciler can still change process classes or coordinators on a fresh cluster, and each change is a recovery.
6. Check that only the variant's knobs are present on every process, the version is 7.3.77, there are 9 machines, and the redundancy and storage engine are the expected ones (status json reports `ssd` as `ssd-2`).
7. Abort any running backup (none expected: the cluster is created with backup disabled).

### No backups during benchmarks

The cluster is created with `backup: false`. The scripts still check `GET /backup/status` and abort any running backup after creation, before the restore, every 20 polls, and at the end, logging a warning each time. The check never stops the run on a lookup error.

### Variants

| Variant | Knob changes | Rebalance | What it isolates |
|---|---|---|---|
| `baseline` | none | on | Reference: default 7.3 behaviour |
| `merge-delay` | `DD_MERGE_COALESCE_DELAY=86400` | on | Deferring merges |
| `write-split` | `SHARD_MAX_BYTES_PER_KSEC=1e10` (10 MB/s) | on | Fewer write splits |
| `write-split-aggressive` | `SHARD_MAX_BYTES_PER_KSEC=2.5e8`, `SHARD_SPLIT_BYTES_PER_KSEC=1e8`, `SHARD_MIN_BYTES_PER_KSEC=2.5e7`, `ENABLE_WRITE_BASED_SHARD_SPLIT=true` | on | The opposite bet: split hot ranges sooner and finer to spread a write hotspot |
| `metrics-interval` | `STORAGE_METRICS_AVERAGE_INTERVAL=30` (default 120) | on | Faster reaction of data distribution to bursts |
| `rebalance-off` | none | off | Disabling disk/read rebalancing |
| `minimize-data-movement` | merge delay + `SHARD_MAX_BYTES_PER_KSEC=1e10` | off | Option A: all three combined |

Notes on the less obvious ones:
- **`write-split-aggressive`:** `SHARD_SPLIT_BYTES_PER_KSEC` must stay under half of `SHARD_MAX_BYTES_PER_KSEC`, and `SHARD_MIN_BYTES_PER_KSEC` well below the split size, or new pieces merge straight back (which also means fewer merges overall). `ENABLE_WRITE_BASED_SHARD_SPLIT` lets shards under 20MB (2 × `MIN_SHARD_BYTES`) split by write traffic (`StorageMetrics.actor.cpp:273`); without it, small hot shards on a fresh cluster never split.
- **`metrics-interval`:** setting the interval on the command line also recomputes `STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS`, because FDB re-runs knob initialization after applying command-line knobs and skips only explicitly set ones (`flow/Knobs.cpp:503`, `fdbserver.actor.cpp:1984-1987`). Rates stay consistent but noisier. Shards also turn "low bandwidth" sooner after a burst, so merges start sooner. Read rebalancing is paced by the same knob (`DDRelocationQueue.actor.cpp:1179,1950`) and runs more often.

### What each run records

- `restore.csv`: restore state, blocks finished/total/in progress, bytes written, apply version lag.
- `cluster.csv`: generation, data distribution state, shard count, logical size, moving data (in flight, queued, total written), ratekeeper limit reason, TPS limit and released TPS, worst storage queue and durability lag, worst tlog queue, write rate, and max/median/mean storage `input_bytes.hz`.
- `summary.json` / `runs/results.csv`: numeric restore version, duration, MiB/s, max apply lag, bytes moved by data distribution and per restored byte (added up across counter resets after recoveries), fetched ÷ ingested ratio from Vector, max/final shard count, worst storage queue and lag, worst tlog queue, storage write skew, recoveries, % of time per ratekeeper limit reason, and `metrics_status`.
- **Datadog metrics** (`pup_sum` in `summarize.sh`): each query yields a value and a status. `ok` is a real sum, possibly 0; `no_data` means the query matched nothing (e.g. wrong tags); `error` means the command failed or the payload was malformed. The ratio is only computed when both are `ok`. Values are read from `data.attributes.values` (v2 timeseries) or `data.values` (the flattened shape pup printed in an earlier session).
- **Storage write skew:** per sample, busiest storage server's input ÷ mean input across storage servers; `ss_input_skew_avg`/`_max` over busy samples (max > 0; idle samples skipped). The mean is used instead of the median because a few busy servers among idle ones give a median of 0. `ss_input_concentrated_pct` is the share of busy samples where the median is 0, i.e. traffic on a minority of storage servers.

### Hotspot check

The baseline decides whether `write-split-aggressive` is worth running:
- **Hotspot:** high `ss_input_skew_*` or `ss_input_concentrated_pct`, and ratekeeper often limited by `storage_server_write_queue_size`.
- **No hotspot:** skew close to 1, low concentrated share, and other limit reasons; aggressive splitting would mostly add data movement.

### Lessons from the devkube debug runs

- Data distribution moved more than was restored early on (12.8GB moved for 11.1GB of logical data after ~27 min), so the data-movement overhead is real.
- Write skew between storage servers was high at the start (3–6×) and dropped as the shard count grew.
- VPN drops interrupt kubectl for minutes at a time: keep the laptop on a stable connection, and the run tolerates gaps in samples.

### Plan

1. `setup.sh` (read-only preflight, including Atlas dry-runs).
2. Start a new series (no `runs/series-restore-version`), then `run.sh baseline` and analyse it. It records the series version.
3. `bench.sh` with the other variants, possibly skipping `write-split-aggressive` depending on the hotspot check. It reuses the series version.

### Later: Redwood storage engine

All runs above use `ssd` (ssd-2, SQLite). Test Redwood later with `ENGINE=ssd-redwood-1`, at least for baseline and the best variant, in the same series (same `runs/series-restore-version`, within the 7-day backup retention) so the restored data is identical. Redwood's write path is very different from SQLite's, so the best data-distribution settings may differ.
