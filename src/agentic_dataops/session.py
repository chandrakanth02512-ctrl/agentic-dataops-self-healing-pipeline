"""Local Spark setup. Fabric callers pass their existing Spark session."""

import os
from pathlib import Path

from pyspark.sql import SparkSession


def local_spark(delta: bool = False) -> SparkSession:
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("AgenticDataOps")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.session.locale", "en-US")
    )
    if delta:
        jar_dir = Path(os.environ.get("DATAOPS_DELTA_JARS", ".cache/delta")).resolve()
        jars = [jar_dir / f"{name}-4.0.0.jar" for name in ("delta-spark_2.13", "delta-storage")]
        if not all(path.is_file() for path in jars):
            raise RuntimeError("Run python scripts/setup_local_delta.py from the checkout first")
        builder = (
            builder.config("spark.jars", ",".join(str(path) for path in jars))
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.databricks.delta.snapshotPartitions", "2")
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
