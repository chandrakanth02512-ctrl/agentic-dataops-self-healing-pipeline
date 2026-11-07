# Phase 1 verification

Local evidence was collected in Codex Cloud on 7 November 2026. Source data is
entirely fictional. No Fabric run or production throughput is claimed.

## Local checks

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
on branch pushes and PR updates. Both remote runs for implementation commit `4cfcf6365c303d3caca3d2c5fe6ef1b695a99915`
completed successfully: the [branch run](https://github.com/chandrakanth02512-ctrl/agentic-dataops-self-healing-pipeline/actions/runs/42340972656)
took 3m 19s and the [PR run](https://github.com/chandrakanth02512-ctrl/agentic-dataops-self-healing-pipeline/actions/runs/42340978337)
took 4m 00s. Conclusions were read from the public GitHub run pages. The exact
31-test count above comes from the local JUnit report; remote log-level counts
were not independently extracted. This evidence-only documentation update does
not alter application code or tests. Later commits have their own GitHub checks.

- [Existing PR #1](https://github.com/chandrakanth02512-ctrl/agentic-dataops-self-healing-pipeline/pull/1)
- [Workflow runs](https://github.com/chandrakanth02512-ctrl/agentic-dataops-self-healing-pipeline/actions/workflows/tests.yml)

GitHub API access is currently denied by the environment's outbound policy.
`api.github.com` has been added to the environment draft for review/save. Git
transport and public GitHub run pages remain accessible, so commits can be pushed
and run conclusions inspected there. PR title/body edits via the API require the
network change to be applied. No duplicate PR or merge is needed.

## Definition of done

| Requirement | Evidence/status |
| --- | --- |
| Bronze, Silver, quarantine and Gold | Verified locally with real Delta and the original Parquet demo |
| Incremental ingestion and CDC MERGE | Verified; only the submitted source batch is ingested |
| Idempotent reruns | Verified for same IDs and events delivered under new IDs |
| Invalid rows isolated with reasons | Verified, including original raw values |
| Predictable schema failures and additions | Verified with fixtures and real Delta evolution |
| Failure/recovery and durable checkpoints | Verified, including a new Python/Spark process |
| SQL and PySpark together | Verified by Gold revenue and packaged-resource checks |
| Local tests | 31 passed; no skips |
| GitHub Actions | Both implementation-commit runs passed; linked evidence above |
| Reproducible documentation/demo | Commands executed locally; recorded counts above |
| Fabric | Explicitly blocked, not deployed or executed; see below |

## Fabric blocker and limits

No approved cloud identity/connection, Fabric token binding, workspace ID or
lakehouse ID is configured. Capacity, permissions and the workspace runtime have
not been verified. The minimum next action is to connect an approved identity and
identify an existing workspace/Lakehouse with usable capacity and native Spark
4.0.x / Delta 4.0.x. See [the deployment guide](fabric.md). No paid service or
billing change was made.

Phase 1's cloud clause permits an explicit Fabric blocker; it does not permit
claiming completed cloud execution. All local implementation clauses and GitHub Actions checks for the pushed
implementation are verified. Phase 1 meets the stated definition of done with
Fabric explicitly blocked, as that definition allows; cloud execution itself
remains incomplete.

The implementation intentionally requires one writer, immutable reference
snapshots and source files no larger than 64 MiB. Gold is rebuilt from current
state; there is no cross-table serving transaction, distributed lock, managed
Fabric table registration or production scale benchmark. These limits and the
reader/recovery protocol are documented in [architecture.md](architecture.md).
