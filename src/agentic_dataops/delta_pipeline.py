"""Incremental, single-writer Delta batches with replay after partial writes.

The final success row in audit/runs is also the batch checkpoint. Delta commits
are atomic per table, not across the whole pipeline. Until that row exists,
readers must treat the output as work in progress and replay the same batch.
"""

import hashlib
import json
import logging
import re
import time
from datetime import datetime, timezone
from uuid import uuid4

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from agentic_dataops.transforms import (
    CUSTOMER_COLUMNS, PRODUCT_COLUMNS, ORDER_COLUMNS,
    current_orders, daily_sales, validate_orders, validate_reference,
)

log = logging.getLogger(__name__)

AUDIT_SCHEMA = """
run_id string, batch_id string, input_fingerprint string, reference_fingerprint string,
started_at string, completed_at string, status string, failure_reason string,
source_rows long, valid_events long, quarantined_rows long,
orders_before long, active_before long, events_before long,
orders_after long, active_orders long, events_after long,
gold_rows long, gold_order_count long, gold_revenue string,
max_event_time string, duration_seconds double
"""


def table_exists(spark, path):
    return DeltaTable.isDeltaTable(spark, path)


def write_audit(spark, path, report):
    data = spark.createDataFrame([report], schema=AUDIT_SCHEMA)
    if not table_exists(spark, path):
        data.write.format("delta").mode("errorifexists").save(path)
    else:
        (DeltaTable.forPath(spark, path).alias("target")
         .merge(data.alias("source"), "target.run_id = source.run_id")
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())


def batch_fingerprints(spark, input_root):
    """Hash the three immutable CSV files on executors, without logging payloads.

    This demo contract limits each source CSV to 64 MiB. binaryFile materializes
    one file per executor row; larger production sources need an upstream signed
    manifest and partitioned ingestion, not an unbounded binaryFile read.
    """
    fingerprints = {}
    for name in ("customers", "products", "orders"):
        files = spark.read.format("binaryFile").load(f"{input_root}/{name}.csv")
        sizes = files.select("length").collect()
        if len(sizes) != 1 or sizes[0].length > 64 * 1024 * 1024:
            raise ValueError(f"{name}.csv must be one file no larger than 64 MiB")
        fingerprints[name] = files.select(F.sha2("content", 256).alias("digest")).first().digest
    digest = lambda value: hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()
    references = {key: fingerprints[key] for key in ("customers", "products")}
    return digest(fingerprints), digest(references)


def write_batch(data, path, batch_id):
    """Replace only this batch partition, preserving even identical raw rows."""
    (data.withColumn("batch_id", F.lit(batch_id)).write.format("delta")
     .mode("overwrite").option("replaceWhere", f"batch_id = '{batch_id}'")
     .option("mergeSchema", "true").partitionBy("batch_id").save(path))


def merge_events(spark, events, path):
    if not table_exists(spark, path):
        events.write.format("delta").mode("errorifexists").save(path)
    else:
        (DeltaTable.forPath(spark, path).alias("target")
         .merge(events.alias("source"), "target.event_key = source.event_key")
         .withSchemaEvolution().whenNotMatchedInsertAll().execute())


def merge_current(spark, events, path):
    latest = current_orders(events).withColumn(
        "operation_rank",
        F.when(F.col("operation") == "D", 3).when(F.col("operation") == "U", 2).otherwise(1),
    )
    if not table_exists(spark, path):
        latest.write.format("delta").mode("errorifexists").save(path)
        return
    # An omitted optional field belongs to this new full-row event as null,
    # rather than retaining a value from the previously winning event.
    for field in spark.read.format("delta").load(path).schema:
        if field.name not in latest.columns:
            latest = latest.withColumn(field.name, F.lit(None).cast(field.dataType))
    newer = """
        source.event_time > target.event_time OR
        (source.event_time = target.event_time AND source.operation_rank > target.operation_rank) OR
        (source.event_time = target.event_time AND source.operation_rank = target.operation_rank
         AND source.event_key > target.event_key)
    """
    (DeltaTable.forPath(spark, path).alias("target")
     .merge(latest.alias("source"), "target.order_id = source.order_id")
     .withSchemaEvolution().whenMatchedUpdateAll(condition=newer)
     .whenNotMatchedInsertAll().execute())


def state_counts(spark, root):
    current_path, events_path = f"{root}/silver/current_orders", f"{root}/silver/order_events"
    orders = active = events = 0
    if table_exists(spark, current_path):
        current = spark.read.format("delta").load(current_path)
        orders = current.count()
        active = current.filter(~F.col("is_deleted")).count()
    if table_exists(spark, events_path):
        events = spark.read.format("delta").load(events_path).count()
    return orders, active, events


def run_delta_batch(spark, input_root, output_root, batch_id, fail_after=None):
    from agentic_dataops.pipeline import read_csv

    if not batch_id or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,99}", batch_id):
        raise ValueError("Delta requires a batch_id of 1-100 letters, digits, dots, underscores or hyphens")
    if fail_after not in (None, "bronze", "silver", "gold"):
        raise ValueError("fail_after must be bronze, silver or gold")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    root, source = output_root.rstrip("/"), input_root.rstrip("/")
    audit_path = f"{root}/audit/runs"
    started = time.monotonic()
    # Null means not measured yet, especially when a run stops mid-write.
    report = {field.strip().split()[0]: None for field in AUDIT_SCHEMA.split(",")}
    report.update(
        run_id=str(uuid4()), batch_id=batch_id, input_fingerprint="", reference_fingerprint="",
        started_at=datetime.now(timezone.utc).isoformat(), completed_at="", status="running",
        failure_reason="", gold_revenue=None, max_event_time="", duration_seconds=0.0,
    )
    cached = []
    registered = False
    try:
        fingerprint, reference_fingerprint = batch_fingerprints(spark, source)
        report.update(input_fingerprint=fingerprint, reference_fingerprint=reference_fingerprint)
        if table_exists(spark, audit_path):
            audit = spark.read.format("delta").load(audit_path)
            prior = audit.filter(F.col("batch_id") == batch_id).collect()
            if any(row.input_fingerprint != fingerprint for row in prior):
                raise ValueError(f"Batch {batch_id} was already registered with different source bytes")
            success = next((row for row in prior if row.status == "success"), None)
            if success:
                log.info("Batch %s already committed; no data writes", batch_id)
                return dict(success.asDict(), status="already_processed")
            committed = audit.filter("status = 'success'").select("batch_id")
            pending = audit.select("batch_id").distinct().join(committed, "batch_id", "left_anti")
            if pending.filter(F.col("batch_id") != batch_id).limit(1).count():
                raise RuntimeError("Recover the unfinished batch before submitting another batch")
            if audit.filter(F.col("reference_fingerprint") != reference_fingerprint).limit(1).count():
                raise ValueError("Reference snapshots changed; Phase 1 requires immutable customers/products")
        write_audit(spark, audit_path, report)
        registered = True
        log.info("Batch %s run %s started", batch_id, report["run_id"])

        customers = read_csv(spark, f"{source}/customers.csv", CUSTOMER_COLUMNS, "customers")
        products = read_csv(spark, f"{source}/products.csv", PRODUCT_COLUMNS, "products")
        orders = read_csv(spark, f"{source}/orders.csv", ORDER_COLUMNS, "orders").cache()
        cached.append(orders)
        report["source_rows"] = orders.count()
        validate_reference(customers, CUSTOMER_COLUMNS, "customers")
        validate_reference(products, PRODUCT_COLUMNS, "products")
        before = state_counts(spark, root)
        report.update(orders_before=before[0], active_before=before[1], events_before=before[2])

        for name, data in (("orders", orders), ("customers", customers), ("products", products)):
            bronze = (data.withColumn("loaded_at", F.lit(report["started_at"]))
                      .withColumn("source_file", F.lit(f"{source}/{name}.csv")))
            write_batch(bronze, f"{root}/bronze/{name}", batch_id)
        if fail_after == "bronze":
            raise RuntimeError("Injected failure after bronze")

        valid, rejected = validate_orders(orders, customers, products)
        events_path = f"{root}/silver/order_events"
        if table_exists(spark, events_path):
            # Own pending events remain eligible on recovery; other deliveries do not.
            known = (spark.read.format("delta").load(events_path)
                     .filter(F.col("batch_id") != batch_id).select("event_key"))
            repeated = valid.join(known, "event_key", "left_semi").withColumn(
                "rejection_reason", F.lit("duplicate_event"))
            rejected = rejected.unionByName(repeated, allowMissingColumns=True)
            valid = valid.join(known, "event_key", "left_anti")
        valid, rejected = valid.cache(), rejected.cache()
        cached.extend([valid, rejected])
        report["valid_events"], report["quarantined_rows"] = valid.count(), rejected.count()
        if report["source_rows"] != report["valid_events"] + report["quarantined_rows"]:
            raise RuntimeError("Input and validation row counts do not reconcile")
        write_batch(rejected, f"{root}/quarantine/orders", batch_id)
        events = valid.withColumn("batch_id", F.lit(batch_id))
        merge_events(spark, events, events_path)
        merge_current(spark, events, f"{root}/silver/current_orders")
        if fail_after == "silver":
            raise RuntimeError("Injected failure after silver")

        current = spark.read.format("delta").load(f"{root}/silver/current_orders")
        gold = daily_sales(current, products).cache()
        cached.append(gold)
        after = state_counts(spark, root)
        report.update(orders_after=after[0], active_orders=after[1], events_after=after[2])
        report["max_event_time"] = str(current.agg(F.max("event_time")).first()[0] or "")
        totals = gold.agg(F.sum("order_count"), F.sum("revenue")).first()
        report.update(gold_rows=gold.count(), gold_order_count=totals[0] or 0,
                      gold_revenue=str(totals[1] or "0.00"))
        if report["gold_order_count"] != report["active_orders"]:
            raise RuntimeError("Gold order count does not reconcile with active Silver orders")
        expected_revenue = current.filter(~F.col("is_deleted")).agg(
            F.sum(F.col("quantity") * F.col("unit_price"))).first()[0]
        if totals[1] != expected_revenue:
            raise RuntimeError("Gold revenue does not reconcile with Silver")
        if current.groupBy("order_id").count().filter("count > 1").limit(1).count():
            raise RuntimeError("Silver business keys are not unique")
        for table, expected in (("bronze/orders", report["source_rows"]),
                                ("silver/order_events", report["valid_events"]),
                                ("quarantine/orders", report["quarantined_rows"])):
            persisted = (spark.read.format("delta").load(f"{root}/{table}")
                         .filter(F.col("batch_id") == batch_id).count())
            if persisted != expected:
                raise RuntimeError(f"Persisted {table} count does not reconcile for {batch_id}")
        gold.write.format("delta").mode("overwrite").save(f"{root}/gold/daily_sales")
        if fail_after == "gold":
            raise RuntimeError("Injected failure after gold")

        report.update(status="success", completed_at=datetime.now(timezone.utc).isoformat(),
                      duration_seconds=round(time.monotonic() - started, 3))
        # This single Delta transaction publishes the report AND checkpoint.
        write_audit(spark, audit_path, report)
        log.info("Batch completed: %s", json.dumps(report, sort_keys=True))
        return report
    except Exception as error:
        report.update(status="failed", completed_at=datetime.now(timezone.utc).isoformat(),
                      duration_seconds=round(time.monotonic() - started, 3),
                      failure_reason=f"{type(error).__name__}: {str(error)[:2000]}")
        log.exception("Batch %s failed; no success checkpoint was issued", batch_id)
        log.error("Failed batch report: %s", json.dumps(report, sort_keys=True))
        if registered:
            try:
                write_audit(spark, audit_path, report)
            except Exception:
                log.exception("Failure audit write also failed; use driver logs and replay the batch")
        raise
    finally:
        for data in cached:
            data.unpersist()
