# Microsoft Fabric deployment preparation

**Status: prepared, not deployed or verified in Fabric.** The Codex environment
has no configured outbound cloud identity, Fabric token binding, workspace ID,
lakehouse ID or capacity information. The identity manifest also has no
connections. Local Delta evidence does not establish OneLake access or Fabric
execution. No capacity, service or billing change has been provisioned.

Minimum user action for a live run: provide an existing Fabric workspace backed
by usable capacity, a Lakehouse, and an approved identity with permission to run
Spark and read/write its OneLake files. Connect the identity through environment
settings, not chat or GitHub. Supply the non-secret workspace/lakehouse identifiers
and confirm the available Spark runtime. Capacity and permissions have not been
verified; this is not a claim that the workspace does or does not have capacity.
Do not create paid capacity as part of these instructions without approval.

## Runtime and artifacts

The tested local matrix is Python 3.12, Java 21, PySpark 4.0.5 and Delta 4.0.0.
Delta 4.0's published Python dependency and Maven POM target Spark 4.0 / Scala 2.13.
The Fabric job explicitly requires native Spark 4.0.x and Delta 4.0.x. A workspace
that offers only Spark 3.x is a runtime blocker for this build; do not replace
Fabric's bundled Spark/Delta packages with pip to make the check pass. A separate
compatibility change and full test run would be required.

Build the wheel locally:

```bash
python -m pip wheel --no-deps --no-build-isolation . -w build/dist
```

Attach the wheel to an approved Fabric Environment compatible with its native
runtime (install application code without resolving/replacing Spark or Delta).
The wheel includes the SQL resource. Add `fabric/run_batch.py` as the Spark Job
Definition entry point, or call `run_pipeline` from a Python notebook cell using
the Fabric-provided `spark` object.

## Files and execution

1. Attach the intended Lakehouse to the job/notebook.
2. Upload `data/batches/001_initial`, `002_changes`, and `003_schema_addition` under
   `Files/dataops/source`, keeping each batch folder and its CSV bytes intact.
3. Copy `config/fabric.example.json` into an accessible local/mounted job file.
   It uses the attached default Lakehouse mount. For Spark Jobs without that
   mount, set absolute OneLake ABFSS paths using the workspace's actual IDs:
   `abfss://<workspace-id>@onelake.dfs.fabric.microsoft.com/<lakehouse-id>/Files/dataops/source`
   and the corresponding `Files/dataops/lakehouse` output. Ensure the JSON config
   itself is available at the local `--config` path.
4. Configure maximum concurrency 1 per output path. Submit the entry point with
   `--config <config-path> --batch-id 001_initial`; then 002 and 003 in order.
5. Use the approved Fabric/Entra/OneLake connections already supplied by the
   workspace. API automation normally requires network destinations including
   `api.fabric.microsoft.com`, `login.microsoftonline.com` and
   `onelake.dfs.fabric.microsoft.com`; add only destinations needed for the actual
   approved connection. No secrets belong in the example JSON or repository.

The pipeline writes **Delta paths in OneLake Files**. It does not register managed
Lakehouse tables or promise SQL analytics endpoint discovery. Query with Spark
using `spark.read.format('delta').load(path)`; managed table registration can be a
separate deployment decision. The Spark SQL Gold query runs in the same job.

## Evidence required before calling deployment verified

Record the actual runtime versions, workspace/lakehouse identifiers (omit private
identifiers from public reports as appropriate), job run IDs, timestamps, job
status, and the audit reports. Read the persisted Delta tables back in a separate
Fabric session and check the demo counts/revenue. Repeat a committed batch, then
run the documented failure/recovery flow with the notebook callable:

```python
from agentic_dataops.pipeline import run_pipeline
run_pipeline(spark, source_batch, output_root, "delta", "003_schema_addition", fail_after="silver")
# The call above intentionally raises; recover in a later invocation:
run_pipeline(spark, source_batch, output_root, "delta", "003_schema_addition")
```

Save a sanitized evidence report to `docs/verification.md` only after those steps
actually execute. A successful local test, uploaded wheel or submitted cloud job
alone does not establish a completed Fabric run.
