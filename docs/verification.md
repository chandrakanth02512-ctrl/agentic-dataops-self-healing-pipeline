# Phase 1 verification

Local evidence was collected in Codex Cloud on 7 November 2026. The first actual
Fabric batch was verified on 8 November 2026. Source data is entirely fictional;
these small-batch results are not a production throughput benchmark.

## Current cloud-deployment follow-up

**PASS for the Phase 1 Fabric notebook path:** initial ingestion, incremental
CDC, the unchanged idempotency replay, additive schema evolution, and intentional
failure after Silver followed by controlled recovery. The published
`agentic-dataops 0.1.0` wheel executed these bounded checks in
`AgenticDataOps_Phase1_Run`, explicitly attached to `AgenticDataOps_Environment`
and `AgenticDataOps_Lakehouse` in `Fabric Lakehouse Platform`. Runtime 2.0 reported
Spark `4.1.1.5.5.20260929.238183312` and Python `3.13.11`. Delta Python distribution
version metadata was unavailable; native Delta reads, writes and CDC MERGE succeeded.

### Initial batch: `001_initial`

- Notebook ID: `7dbe4ee8-eb8a-4aa8-81e9-612272d041e7`.
- Fabric run ID: `7c0f15b0-77dc-468f-b6ab-ee49b97b11c4`; status `Completed`.
- Application/audit run ID: `0116022f-29ac-4542-93f3-09139108c779`.
- Total Fabric duration: **252.940 s**; pipeline call including checkpoint: **53.097 s**.
- The audit report records **47.904 s**, measured before its final checkpoint write.

| Initial-batch metric | Expected | Persisted actual |
| --- | --- | --- |
| Input orders | 10 | 10 |
| Valid events | 6 | 6 |
| Rejected orders | 4 | 4 |
| Active orders | 3 | 3 |
| Gold revenue | 1758.00 | 1758.00 |

Both target directories were absent before writing, with no redirecting shortcuts.
The original three CSV files were uploaded unchanged to
`/lakehouse/default/Files/dataops/source/001_initial`. The output root is
`/lakehouse/default/Files/dataops/lakehouse`. Both resolve to the dedicated
Lakehouse; distributed Spark used their equivalent explicit OneLake ABFS paths.

A fresh Spark SQL session read all eight committed Delta paths after the pipeline
returned. Bronze contains 10 orders, 3 customers and 3 products; Silver contains
6 events and 4 current orders, including one tombstone; Quarantine contains 4
rows; Gold contains 3 rows. `audit/runs` contains exactly one `success` row at
Delta version 1, matching the returned batch report. All reconciliation checks
passed. Independent read-only OneLake checks after notebook completion confirmed
the same commit versions and final checkpoint statistics.

[Full Fabric evidence, original checksums, batch report and reconciliation](evidence/fabric-001-initial.json).
That evidence records the initial run only and remains byte-for-byte unchanged.
No recovery attempt, managed table registration, workspace settings change,
Enterprise Sales access or PR merge was performed. The existing preflight
notebook and published wheel were preserved.

### Incremental CDC: `002_changes`

The existing execution notebook was reused with one execution-cell update; its
attachments and result-return cell were preserved. Before submission, read-only
OneLake reads confirmed the initial success checkpoint, Gold revenue `1758.00`,
and all initial Delta versions. No unfinished batch, active workspace Spark
session, pending project execution, prior target submission, or existing
`002_changes` source/data was found. The three uploaded CSV files matched the
repository bytes and SHA-256 checksums; reference snapshots matched batch 001.

- Fabric run ID: `d63390d6-b479-4238-9c86-d9b7eef004a9`; status `Completed`.
- Application/audit run ID: `097b3035-5afe-412e-af5b-bb86ae61b4a9`.
- Total Fabric duration: **293.545 s**; pipeline call including checkpoint: **57.654 s**.
- Audit report duration: **53.250 s**, measured before its final checkpoint write.
- After interruption, saved state and Fabric job history confirmed the existing
  submission. No second execution, upload, or notebook update was issued.

| CDC batch metric | Expected | Persisted actual |
| --- | --- | --- |
| Input orders | 8 | 8 |
| Valid events | 5 | 5 |
| Rejected orders | 3 | 3 |
| Active orders | 3 | 3 |
| Gold revenue | 3117.00 | 3117.00 |

CDC verification passed: `O008` was inserted with quantity 2, `O001` updated to
quantity 3, and `O002` became a tombstone. The late `O001` event remained in the
ledger without rolling back current state. The late `O003` event did not revive
its existing tombstone. Repeated `O007` was quarantined once as `duplicate_event`
and retained one ledger event. The other rejections were `invalid_quantity` and
`unknown_product`. Exact current state contains five orders: three active and
two tombstones. Native Delta MERGE was confirmed in transaction history.

| Persisted output | Cumulative rows | Delta version before → after |
| --- | --- | --- |
| `bronze/orders` | 18 | 0 → 1 |
| `bronze/customers` | 6 | 0 → 1 |
| `bronze/products` | 6 | 0 → 1 |
| `silver/order_events` | 11 | 0 → 1 |
| `silver/current_orders` | 5 | 0 → 1 |
| `quarantine/orders` | 7 | 0 → 1 |
| `gold/daily_sales` | 3 | 0 → 1 |
| `audit/runs` | 2 | 1 → 3 |

All reconciliation checks passed. There is exactly one success checkpoint for
each approved batch; the initial checkpoint is unchanged. Native Spark verified
the initial batch partitions and all initial time-travel snapshots. Independent
OneLake reads confirmed audit/Gold values, logical row counts, commit versions,
and unchanged original data-file checksums. For current Silver, Fabric used a
deletion vector to mask two superseded rows; the independent reader accounts for
that cardinality, while native Spark verifies the exact current-row selection.

[Full CDC evidence, batch report, checksums, CDC checks and retention proof](evidence/fabric-002-changes.json).
No execution errors or metric discrepancies occurred during the CDC stage. Batch
003 and recovery were verified separately below. CDC source path:
`/lakehouse/default/Files/dataops/source/002_changes`; output root remains
`/lakehouse/default/Files/dataops/lakehouse` in `AgenticDataOps_Lakehouse`.

### Idempotency replay: `002_changes`

The exact completed batch ID and unchanged source CSV bytes were replayed once
using the existing published package and attached resources. Before submission,
both checkpoints were successful, the audit contained two rows, no active or
unfinished writer was present, and Gold revenue was `3117.00`. All eight live
table versions, row counts and retained output-file hashes were recorded.

- Fabric replay run ID: `16e2ccdb-3231-403f-89be-aa005c22b565`; status `Completed`.
- Pipeline result: **`already_processed`**.
- Total Fabric duration: **222.955 s**; replay pipeline call: **2.452 s**.
- Returned application run ID: `097b3035-5afe-412e-af5b-bb86ae61b4a9`, the original
  CDC run. Its returned duration of `53.250 s` belongs to that original run;
  it is not the replay invocation time or a new audit attempt.

| Output | Rows before → after | Delta version before → after |
| --- | --- | --- |
| `bronze/orders` | 18 → 18 | 1 → 1 |
| `bronze/customers` | 6 → 6 | 1 → 1 |
| `bronze/products` | 6 → 6 | 1 → 1 |
| `silver/order_events` | 11 → 11 | 1 → 1 |
| `silver/current_orders` | 5 → 5 | 1 → 1 |
| `quarantine/orders` | 7 → 7 | 1 → 1 |
| `gold/daily_sales` | 3 → 3 | 1 → 1 |
| `audit/runs` | 2 → 2 | 3 → 3 |

Native Spark compared current row fingerprints and every retained historical
Delta snapshot before and after replay. All were unchanged. Independent fresh
OneLake reads compared all 76 retained files, including data, transaction logs,
and deletion vectors: names, sizes and SHA-256 checksums were identical. Source
CSV checksums also remained unchanged. There was no duplicate audit attempt or
additional checkpoint; both original success rows remained intact. Gold stayed
at `3117.00`, with three active orders.

The original CDC evidence is preserved in
[fabric-002-changes.json](evidence/fabric-002-changes.json); replay evidence is
appended under `idempotency_replay`. The initial evidence file is unchanged.
No source upload, package change, automatic resubmission, schema-evolution batch,
or deliberate failure/recovery test occurred during replay verification.

### Schema evolution and controlled recovery: `003_schema_addition`

**PASS.** The exact repository fixture bytes were uploaded once. After an
interruption, saved state and fresh reads confirmed the upload and notebook update
had completed and no execution had been submitted. The historical update-operation
URL returned 404; the saved success record and exact live notebook definition
confirmed its outcome. No upload or update was repeated.

Before execution, all 76 retained files matched the verified replay baseline,
both prior success checkpoints were intact, Gold was `3117.00`, and no active
writer or prior 003 submission existed. The existing notebook kept its explicit
Environment/Lakehouse attachments and used the unchanged v0.1.0 wheel, Runtime 2.0,
and the same small single-executor configuration. Its unrelated cell was preserved.

- Intentional failure: `0f1b904b-da68-4be4-a417-76ecd481e702`; Fabric status **`Failed`**;
  duration **269.490 s**.
- Expected application exception: `RuntimeError: Injected failure after silver`.
  Bronze, quarantine and Silver writes persisted; Gold remained at `3117.00`.
  Audit retained one failed 003 attempt and no 003 success checkpoint.
  Fabric returned a generic statement-failure session-cancellation message;
  the durable failed audit record established the exact application cause.
- Recovery: `b2c908ea-7f34-4f73-933f-958a9119b462`; Fabric status **`Completed`**;
  duration **334.219 s**; pipeline call
  **51.875 s**.
- Recovery used the same batch ID and source checksums, without failure injection,
  only after the failed job terminated, independent output checks passed, and
  fresh checks confirmed no active writer or pending submission.

| Recovery metric | Expected | Actual |
| --- | ---: | ---: |
| Input | 1 | 1 |
| Valid | 1 | 1 |
| Rejected | 0 | 0 |
| Active orders | 4 | 4 |
| Gold revenue | 3357.00 | 3357.00 |
| Successful batch checkpoints | 3 | 3 |

The following transitions are baseline → failed attempt → recovered state:

| Output | Logical rows | Delta versions |
| --- | --- | --- |
| `bronze/orders` | 18 → 19 → 19 | 1 → 2 → 3 |
| `bronze/customers` | 6 → 9 → 9 | 1 → 2 → 3 |
| `bronze/products` | 6 → 9 → 9 | 1 → 2 → 3 |
| `silver/order_events` | 11 → 12 → 12 | 1 → 2 → 2 |
| `silver/current_orders` | 5 → 6 → 6 | 1 → 2 → 2 |
| `quarantine/orders` | 7 → 7 → 7 | 1 → 2 → 2 |
| `gold/daily_sales` | 3 → 3 → 4 | 1 → 1 → 2 |
| `audit/runs` | 2 → 3 → 4 | 3 → 5 → 7 |

`O011` appears exactly once in current Silver with quantity `2`. Its
`sales_channel = web` field survives in Bronze orders, Silver events and Silver
current state. Event keys and current-state business keys remain unique. Audit
contains four attempts: the original two successes, one expected 003 failure,
and exactly one 003 success. Previous audit records are unchanged.

A new native Spark SQL session read the committed recovery outputs. Version-pinned
reads before and after recovery compared current rows and all pre-existing
historical snapshots, including the partial failed state. Independent OneLake
reads reconstructed Delta logs, schemas and logical row counts; read audit and
Gold records; compared every retained filename, size and SHA-256; and rechecked
all nine source files against repository bytes. All earlier retained files and
historical snapshots were preserved. The independent reader accounts for deletion
vector cardinalities; native Spark verifies exact rows in partially masked files.

Complete retained manifests contain 76 files at baseline, 103 after failure and
129 after recovery. Recovery left one additional empty, unreferenced Parquet file
in each Silver directory. Independent reads confirmed both contain zero rows;
neither belongs to the active Delta snapshot. Both were preserved without cleanup.
Silver versions and logical row fingerprints stayed unchanged during recovery.

This demonstrates **per-table Delta commits and controlled recovery**, not a
cross-table atomic transaction. Partial writes existed after the intentional
failure. One writer per output root remains mandatory; the audit guard is not a
distributed lock. Readers must wait for a success checkpoint and no pending batch.

[Machine-readable failure/recovery evidence, full manifests, schemas and audit records](evidence/fabric-003-schema-recovery.json).
Six relevant existing local tests passed again (0 failures/errors/skips): schema
and CDC, partial failure after Silver and recovery, and runtime profile checks.
No package installation, wheel publication, prior Fabric batch rerun, workspace-wide
change, unrelated data access, or automatic retry was performed.

## Local runtime compatibility evidence

Both local compatibility profiles completed all **41 tests** (0 failures,
0 errors, 0 skips):

| Local profile | Python / Spark / Delta | Result | Duration |
| --- | --- | --- | --- |
| Original | 3.12.14 / 4.0.5 / 4.0.0 | 41 passed | 342.90 s |
| Runtime 2.0 compatibility | 3.13.5 / 4.1.1 / 4.2.0 | 41 passed | 333.96 s |

The count includes the previous 31 cases plus 10 connection-safety/runtime-guard
cases. A first Python 3.13 run had 24 failures because workers used Python 3.12
from PATH; the local interpreter selection was corrected and the complete suite
rerun successfully. No failures were skipped. Authentication tests use synthetic
responses; they do not establish live token delivery, cloud access or execution.

The base wheel has no unconditional dependencies and includes the Gold SQL, so
installing application code will not pull a conflicting Spark/Delta version into
Fabric. Repeatable compatibility setup and package dependency checks passed.
The workflow now runs both full profiles on PR updates and main pushes. The live
PR checks are authoritative for the newest commit; older run links below refer
to the earlier implementation, not this follow-up.

- [Current local runtime evidence](evidence/runtime-compatibility.json)
- [Historical cloud blocker, now resolved](evidence/fabric-blocker.json)
- [Current PR checks](https://github.com/chandrakanth02512-ctrl/agentic-dataops-self-healing-pipeline/pull/1/checks)

The earlier acceptance results and demo metrics below remain historical evidence
for the unchanged pipeline business logic. They are not Fabric results.

## Earlier local checks

`python -m pytest -q --junitxml=build/test-results.xml` completed with **31 passed,
0 failed, 0 errors, 0 skipped in 332.05 seconds**. Coverage was not measured.
The original five tests remain and pass. The machine used Python 3.12.14, Java
21.0.13, PySpark 4.0.5, Delta 4.0.0 and pytest 9.2.0.

| Suite | Executed cases | What it exercises |
| --- | ---: | --- |
| `test_transforms.py` | 4 | Original validation, CDC, revenue and renamed-column checks |
| `test_pipeline.py` | 1 | Original Parquet end-to-end run and repeat |
| `test_contracts.py` | 14 | Nulls, invalid types/precision/status/operation, duplicate dimensions, tied events and additions |
| `test_delta_pipeline.py` | 12 | Real Delta MERGE, deterministic ties, late events, duplicate deliveries, schema changes, empty/all-invalid batches, restart and failure recovery |

The Delta tests read actual Delta tables and transaction history. Failure tests
interrupt after Bronze, Silver and Gold; they also simulate unavailable Gold
storage and a failed checkpoint write. They assert the absence of a success
checkpoint and recover by replaying the same batch. No tests are disabled to get
a passing result.

Other completed checks: repeated `scripts/setup_environment.sh`, Maven jar
checksum verification, `pip check`, wheel build with packaged SQL, the documented
`inspect_lakehouse.py`, and `git diff --check`.

Machine-readable evidence:

- [Local test summary](evidence/local-tests.json)
- [Demo reports](evidence/local-demo.json)
- [Separate-process recovery](evidence/process-recovery.json)
- [GitHub Actions conclusions](evidence/github-actions.json)

The latter injected a failure after Gold in one Python/Spark process (exit 1),
then recovered in a new process (exit 0). The event ledger remained at 6 rows and
revenue at 1758.00, demonstrating persisted checkpoint recovery.

## Observed demo results

`python scripts/demo.py --output build/verified-demo` completed all assertions.
A separate process read back current orders, quarantine, Gold and audit tables.

| Batch | Input | Accepted | Quarantined | Active orders | Revenue | Recorded duration |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 001 | 10 | 6 | 4 | 3 | 1758.00 | 23.997 s |
| 002 | 8 | 5 | 3 | 3 | 3117.00 | 28.416 s |
| 003 recovery | 1 | 1 | 0 | 4 | 3357.00 | 25.788 s |

These durations cover individual attempts until just before the checkpoint write;
they exclude Spark startup/teardown and were measured under concurrent test load.
They are not a benchmark. A repeat returns the **original report** with
`already_processed`; its duration is not a measurement of the no-op call.

The final demo has 19 Bronze order rows, 12 Silver events, 6 current orders
(including 2 tombstones), 7 quarantined rows, 4 active orders and revenue 3357.00.

## GitHub Actions

The workflow installs the pinned dependencies, verifies Delta jars, runs the full
suite, builds a wheel, checks the packaged SQL and uploads JUnit results. It runs
on main pushes and PR updates; the current workflow tests both local runtime profiles. Both remote runs for implementation commit `4cfcf6365c303d3caca3d2c5fe6ef1b695a99915`
completed successfully: the [branch run](https://github.com/chandrakanth02512-ctrl/agentic-dataops-self-healing-pipeline/actions/runs/42340972656)
took 3m 19s and the [PR run](https://github.com/chandrakanth02512-ctrl/agentic-dataops-self-healing-pipeline/actions/runs/42340978337)
took 4m 00s. Conclusions were read from the public GitHub run pages. The exact
31-test count above comes from the local JUnit report; remote log-level counts
were not independently extracted. This evidence-only documentation update does
not alter application code or tests. Later commits have their own GitHub checks.

- [Existing PR #1](https://github.com/chandrakanth02512-ctrl/agentic-dataops-self-healing-pipeline/pull/1)
- [Workflow runs](https://github.com/chandrakanth02512-ctrl/agentic-dataops-self-healing-pipeline/actions/workflows/tests.yml)

At the time of the earlier CI capture, GitHub API access was denied by the environment's outbound policy.
That historical network blocker was resolved before the Fabric deployment work.
The existing PR remains open; no duplicate PR or merge was created.

## Definition of done

| Requirement | Evidence/status |
| --- | --- |
| Bronze, Silver, quarantine and Gold | Verified locally with real Delta and the original Parquet demo |
| Incremental ingestion and CDC MERGE | Verified; only the submitted source batch is ingested |
| Idempotent reruns | Exact same-ID replay verified in Fabric; alternate-ID delivery tested locally |
| Invalid rows isolated with reasons | Verified, including original raw values |
| Predictable schema failures and additions | Additive schema evolution verified in Fabric; required-field failures verified locally |
| Failure/recovery and durable checkpoints | Intentional Silver failure and a separate recovery execution verified in Fabric |
| SQL and PySpark together | Verified by Gold revenue and packaged-resource checks |
| Local tests | Both current runtime profiles passed 41 tests; earlier 31-test evidence is historical |
| GitHub Actions | Both implementation-commit runs passed; linked evidence above |
| Reproducible documentation/demo | Commands executed locally; recorded counts above |
| Fabric | Initial ingestion, CDC, unchanged replay, additive schema and controlled recovery verified through the project notebook |

## Fabric execution scope and limits

Authentication, the published application package, batches `001_initial` and
`002_changes`, one unchanged replay, and the single failure/recovery pair for
`003_schema_addition` are verified in the existing FTL4 trial capacity. These
meet the bounded Phase 1 acceptance criteria through `AgenticDataOps_Phase1_Run`.
Further execution requires a new approval. No capacity, billing, role, environment
library or workspace-wide runtime setting was changed.

The alternate `fabric/run_batch.py` entry point is still unsupported with this
unchanged wheel: `validate_fabric_runtime` requests `delta-spark` distribution
metadata, which the managed session lacks. Local inspection reproduced
`PackageNotFoundError` before pipeline execution. Guessing the Delta version or
relaxing the guard would weaken validation, and changing local source would not
repair the published v0.1.0 wheel. The guard remains unchanged and fails closed.
This is an alternate-entry-point limitation, not a claim that it was executed or
verified. The verified notebook uses Fabric's native session directly. A future
entry-point change needs a trustworthy native-version check and separate approval
for any new wheel publication; no runtime package override is an acceptable fix.

The implementation intentionally requires one writer, immutable reference
snapshots and source files no larger than 64 MiB. Gold is rebuilt from current
state; there is no cross-table serving transaction, distributed lock, managed
Fabric table registration or production scale benchmark. These limits and the
reader/recovery protocol are documented in [architecture.md](architecture.md).
