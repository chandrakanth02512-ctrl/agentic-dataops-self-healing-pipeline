"""Read source files and write Bronze, Silver, Gold, and quarantine tables."""

import argparse
import logging
from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from agentic_dataops.transforms import (
    CUSTOMER_COLUMNS,
    ORDER_COLUMNS,
    PRODUCT_COLUMNS,
    current_orders,
    daily_sales,
    require_columns,
    validate_orders,
)


log = logging.getLogger(__name__)


def read_csv(spark: SparkSession, path: str, columns: list[str], name: str):
    data = spark.read.option("header", True).option("inferSchema", False).csv(path)
    require_columns(data, columns, name)
    return data.select(*columns)


def save_table(data, output_root: str, table_name: str, file_format: str) -> None:
    path = f"{output_root.rstrip('/')}/{table_name}"
    data.write.format(file_format).mode("overwrite").save(path)
    log.info("Saved %s to %s", table_name, path)


def run_pipeline(
    spark: SparkSession, input_root: str, output_root: str, file_format: str = "parquet"
) -> dict:
    """Process all supplied order events into a repeatable current-state snapshot.

    Local runs use Parquet. Fabric runs can use Delta, which is available in
    Fabric Spark. This first version replays the source events on each run.
    """
    if file_format not in {"parquet", "delta"}:
        raise ValueError("file_format must be 'parquet' or 'delta'")

    run_id = str(uuid4())
    input_root = input_root.rstrip("/")
    customers = read_csv(spark, f"{input_root}/customers.csv", CUSTOMER_COLUMNS, "customers")
    products = read_csv(spark, f"{input_root}/products.csv", PRODUCT_COLUMNS, "products")
    raw_orders = read_csv(spark, f"{input_root}/orders.csv", ORDER_COLUMNS, "orders")

    bronze_orders = raw_orders.withColumn("loaded_at", F.current_timestamp()).withColumn(
        "source_file", F.lit("orders.csv")
    )
    valid_events, quarantined = validate_orders(raw_orders, customers, products)
    current = current_orders(valid_events)
    gold = daily_sales(current, products)

    # Count before writing to make the run report useful for troubleshooting.
    input_count = raw_orders.count()
    valid_count = valid_events.count()
    rejected_count = quarantined.count()
    active_count = current.filter(~F.col("is_deleted")).count()
    if input_count != valid_count + rejected_count:
        raise RuntimeError("Input and validation row counts do not reconcile")

    save_table(bronze_orders, output_root, "bronze/orders", file_format)
    save_table(customers, output_root, "bronze/customers", file_format)
    save_table(products, output_root, "bronze/products", file_format)
    save_table(valid_events, output_root, "silver/order_events", file_format)
    save_table(current, output_root, "silver/current_orders", file_format)
    save_table(quarantined, output_root, "quarantine/orders", file_format)
    save_table(gold, output_root, "gold/daily_sales", file_format)

    report = {
        "run_id": run_id,
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "source_rows": input_count,
        "valid_events": valid_count,
        "quarantined_rows": rejected_count,
        "active_orders": active_count,
        "status": "success",
    }
    save_path = f"{output_root.rstrip('/')}/audit/runs"
    spark.createDataFrame([report]).write.format(file_format).mode("append").save(save_path)
    log.info("Pipeline completed: %s", report)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Process e-commerce orders with Spark")
    parser.add_argument("--input", default="data/sample", help="Source data folder")
    parser.add_argument("--output", default="build/lakehouse", help="Output data folder")
    parser.add_argument("--format", choices=["parquet", "delta"], default="parquet")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    spark = SparkSession.builder.appName("AgenticDataOps").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        run_pipeline(spark, args.input, args.output, args.format)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
