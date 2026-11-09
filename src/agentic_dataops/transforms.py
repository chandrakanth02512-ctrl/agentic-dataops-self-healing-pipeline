"""The data cleaning and reporting steps used by our order pipeline."""

from importlib.resources import files

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


CUSTOMER_COLUMNS = ["customer_id", "customer_name", "region"]
PRODUCT_COLUMNS = ["product_id", "product_name", "category"]
ORDER_COLUMNS = [
    "order_id",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "order_status",
    "order_date",
    "operation",
    "event_time",
]


def require_columns(data: DataFrame, expected: list[str], source: str) -> None:
    """Fail early if a source changes its column names."""
    missing = [column for column in expected if column not in data.columns]
    if missing:
        raise ValueError(f"{source} is missing required columns: {', '.join(missing)}")


def validate_reference(data: DataFrame, columns: list[str], source: str) -> None:
    """Bad reference snapshots fail the batch instead of multiplying orders."""
    require_columns(data, columns, source)
    missing_value = F.lit(False)
    for column in columns:
        missing_value = missing_value | F.col(column).isNull() | (F.trim(column) == "")
    if data.filter(missing_value).limit(1).count():
        raise ValueError(f"{source} contains null or empty required values")
    if data.groupBy(columns[0]).count().filter("count > 1").limit(1).count():
        raise ValueError(f"{source} contains duplicate {columns[0]}")


def validate_orders(
    raw_orders: DataFrame, customers: DataFrame, products: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """Separate valid order events from records that need investigation."""
    require_columns(raw_orders, ORDER_COLUMNS, "orders")
    require_columns(customers, CUSTOMER_COLUMNS, "customers")
    require_columns(products, PRODUCT_COLUMNS, "products")

    orders = raw_orders.withColumn(
        "raw_record", F.to_json(F.struct(*[F.col(c) for c in raw_orders.columns]), {"ignoreNullFields": "false"})
    )
    orders = (
        orders.withColumn("quantity_format_ok", F.col("quantity").cast("string").rlike(r"^[0-9]+$"))
        .withColumn("price_format_ok", F.col("unit_price").cast("string").rlike(r"^[0-9]+(\.[0-9]{1,2})?$"))
        .withColumn("quantity", F.expr("try_cast(quantity AS INT)"))
        .withColumn("unit_price", F.expr("try_cast(unit_price AS DECIMAL(12,2))"))
        .withColumn("order_date", F.expr("try_cast(order_date AS DATE)"))
        .withColumn("event_time", F.expr("try_cast(event_time AS TIMESTAMP)"))
    )

    customer_ids = customers.select("customer_id").distinct().withColumn(
        "customer_found", F.lit(True)
    )
    product_ids = products.select("product_id").distinct().withColumn(
        "product_found", F.lit(True)
    )
    orders = orders.join(customer_ids, "customer_id", "left").join(
        product_ids, "product_id", "left"
    )

    # Each rejected row has one clear reason. We can add more rules later.
    orders = orders.withColumn(
        "rejection_reason",
        F.when(F.col("order_id").isNull() | (F.trim("order_id") == ""), "missing_order_id")
        .when(F.col("customer_id").isNull() | (F.trim("customer_id") == ""), "missing_customer_id")
        .when(F.col("customer_found").isNull(), "unknown_customer")
        .when(F.col("product_id").isNull() | (F.trim("product_id") == ""), "missing_product_id")
        .when(F.col("product_found").isNull(), "unknown_product")
        .when(~F.col("quantity_format_ok") | F.col("quantity").isNull() | (F.col("quantity") <= 0), "invalid_quantity")
        .when(~F.col("price_format_ok") | F.col("unit_price").isNull() | (F.col("unit_price") <= 0), "invalid_price")
        .when(F.col("order_date").isNull(), "invalid_order_date")
        .when(F.col("event_time").isNull(), "invalid_event_time")
        .when(
            F.col("order_status").isNull()
            | ~F.col("order_status").isin("Placed", "Updated", "Shipped", "Delivered", "Cancelled"),
            "invalid_order_status",
        )
        .when(
            F.col("operation").isNull() | ~F.col("operation").isin("I", "U", "D"),
            "invalid_operation",
        ),
    )

    # Duplicate delivery of the same event should not count as another order.
    # JSON preserves boundaries and nulls. Optional columns do not change identity.
    event_json = F.to_json(F.struct(*ORDER_COLUMNS), {"ignoreNullFields": "false"})
    orders = orders.withColumn("event_key", F.sha2(event_json, 256))
    # Distinct events at the same time use the same total order in every batch.
    # Exact duplicates keep the lexically first raw payload (including additions).
    duplicate_window = Window.partitionBy("event_key").orderBy("raw_record")
    orders = orders.withColumn("duplicate_number", F.row_number().over(duplicate_window))
    orders = orders.withColumn(
        "rejection_reason",
        F.when(
            F.col("rejection_reason").isNull() & (F.col("duplicate_number") > 1),
            "duplicate_event",
        ).otherwise(F.col("rejection_reason")),
    )

    helper_columns = ["customer_found", "product_found", "duplicate_number", "quantity_format_ok", "price_format_ok"]
    valid = orders.filter(F.col("rejection_reason").isNull()).drop(
        "rejection_reason", *helper_columns
    )
    rejected = orders.filter(F.col("rejection_reason").isNotNull()).drop(*helper_columns)
    return valid, rejected


def current_orders(valid_events: DataFrame) -> DataFrame:
    """Keep the most recent event for each order, including delete markers."""
    operation_priority = (
        F.when(F.col("operation") == "D", 3)
        .when(F.col("operation") == "U", 2)
        .otherwise(1)
    )
    recent_first = Window.partitionBy("order_id").orderBy(
        F.col("event_time").desc(), operation_priority.desc(), F.col("event_key").desc()
    )
    return (
        valid_events.withColumn("event_number", F.row_number().over(recent_first))
        .filter(F.col("event_number") == 1)
        .drop("event_number")
        .withColumn("is_deleted", F.col("operation") == "D")
    )


def daily_sales(current: DataFrame, products: DataFrame) -> DataFrame:
    """Run the packaged Spark SQL against the PySpark current-state result."""
    current.createOrReplaceTempView("pipeline_current_orders")
    products.createOrReplaceTempView("pipeline_products")
    sql = files("agentic_dataops").joinpath("sql/daily_sales.sql").read_text()
    return current.sparkSession.sql(sql)
