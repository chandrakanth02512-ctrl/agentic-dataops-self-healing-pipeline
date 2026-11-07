# Microsoft Fabric deployment preparation

**Status: cloud deployment blocked; no authenticated Fabric execution.** The user
identified existing resources: workspace **Fabric Lakehouse Platform**, Lakehouse
**AgenticDataOps_Lakehouse**, Environment **AgenticDataOps_Environment**, and Entra
application **AgenticDataOps_Codex**. The user reports Fabric API access and a
Contributor grant. These permissions have not yet been verified from Codex.

All six required settings are absent from the actual Codex process and environment
bindings. The three required Microsoft hosts are denied by the applied outbound
policy. The requirements and domain additions are saved in the environment draft.
Follow [the exact secure connection steps](fabric-connection.md). No token, client
secret, cloud write, paid resource or workspace-wide setting change is involved
in the current local preparation. Never modify EnterpriseSalesLakehouse.

## Runtime and artifacts

Microsoft's [Runtime 2.0 documentation](https://learn.microsoft.com/en-us/fabric/data-engineering/runtime-2-0)
confirms Spark 4.1, Delta 4.2, Python 3.13, Java 21 and Scala 2.13. The runtime guard
requires those Spark/Delta/Python minor versions. It uses Fabric's native session;
it does not install, override or reconfigure the workspace's runtime.

Two separate local dependency profiles preserve the existing developer workflow:

| Profile | Python | PySpark | Delta | Lockfile |
| --- | --- | --- | --- | --- |
| Existing local | 3.12 | 4.0.5 | 4.0.0 | requirements-dev.lock |
| Runtime 2.0 compatibility (local) | 3.13 | 4.1.1 | 4.2.0 | requirements-fabric.lock |

Delta 4.2.0's published metadata restricts PySpark to `>=4.0.1,<=4.1.1`; do not
substitute the latest 4.1 patch without reviewing compatibility. Local jar setup
includes the Delta 4.2 kernel jars and verifies published Maven checksums. Local
Spark workers use their driver's virtualenv interpreter. This setup is never
called inside Fabric.

In Codex, Python 3.13 and uv are available. Prepare and test the isolated profile:

```bash
bash scripts/setup_fabric_compat.sh
build/fabric-venv/bin/python -m pytest -q --junitxml=build/fabric-runtime-tests.xml
```

The standard `scripts/setup_environment.sh` and `.venv` keep the old profile.
GitHub Actions tests both profiles. These are open-source local compatibility
checks, not proof of Fabric's managed/native engine behavior or cloud execution.

The base application wheel now has **no Spark or Delta dependency requirements**;
local dependencies live in optional extras and lockfiles. Build it with:

```bash
.venv/bin/python -m pip wheel --no-deps --no-build-isolation . -w build/dist
```

After authentication and resource verification, attach this application wheel to
the existing AgenticDataOps_Environment using supported Fabric library deployment,
without installing any local-runtime extras or replacing built-in packages.
The wheel includes the SQL resource. Add `fabric/run_batch.py` as the Spark Job
Definition entry point, or call `run_pipeline` from a Python notebook cell using
the Fabric-provided `spark` object. Environment publication, runtime, capacity,
job creation and attachment all remain unverified until authenticated API reads
and the actual run. Do not change workspace-wide settings as a workaround.

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
