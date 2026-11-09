# Reproduce the local Delta demo

Run from the repository root after the README installation steps. Choose a new
output directory; no command below deletes existing user data.

## Automated demo

```bash
python scripts/demo.py --output build/demo
python scripts/inspect_lakehouse.py --output build/demo
```

The demo asserts the expected revenue, repeats batch 002, fails batch 003 after
Silver, checks the absence of a success checkpoint, and recovers it. Measured
reports are saved to `build/demo/demo-results.json`; generated data stays ignored.

## Individual CLI steps (separate Spark processes)

```bash
dataops-pipeline --input data/batches/001_initial --output build/manual-demo --format delta --batch-id 001_initial
python scripts/inspect_lakehouse.py --output build/manual-demo
dataops-pipeline --input data/batches/002_changes --output build/manual-demo --format delta --batch-id 002_changes
dataops-pipeline --input data/batches/002_changes --output build/manual-demo --format delta --batch-id 002_changes
```

Expected: batch 001 has 10 input, 6 valid and 4 rejected rows, 3 active orders and
revenue 1758.00. Batch 002 has 8 input, 5 valid and 3 rejected rows, 3 active orders
and revenue 3117.00. Repeating batch 002 returns `already_processed`. O001 has
quantity 3; O002 and O003 are tombstones, even though a late O003 update arrived.

Run the following failure command on its own. It is **expected to exit nonzero**;
inspect the error rather than hiding its exit code with `|| true`.

```bash
dataops-pipeline --input data/batches/003_schema_addition --output build/manual-demo --format delta --batch-id 003_schema_addition --fail-after silver
```

Silver now includes O011 with `sales_channel=web`, but the batch has no success
checkpoint and Gold still represents batch 002. Recover in a new process:

```bash
dataops-pipeline --input data/batches/003_schema_addition --output build/manual-demo --format delta --batch-id 003_schema_addition
python scripts/inspect_lakehouse.py --output build/manual-demo
```

Expected final state: 4 active orders, 6 current rows including 2 tombstones,
12 accepted events, 7 quarantined rows, 19 Bronze order rows and revenue 3357.00.
Audit records the failed and recovered attempts separately.

The incompatible fixture deliberately renames `customer_id`. Use a separate
output so that this permanently invalid demo source does not block your main demo:

```bash
dataops-pipeline --input data/batches/004_missing_column --output build/schema-error-demo --format delta --batch-id bad-schema
```

It must exit nonzero with `missing required columns: customer_id`, retain a failed
audit attempt, and create no successful checkpoint. Do not guess a rename or edit
an immutable registered file to force this batch through.
