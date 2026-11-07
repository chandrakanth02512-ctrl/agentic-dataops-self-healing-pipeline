"""The data cleaning and reporting steps used by our order pipeline."""

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


def validate_orders(
    raw_orders: DataFrame, customers: DataFrame, products: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """Separate valid order events from records that need investigation."""
    require_columns(raw_orders, ORDER_COLUMNS, "orders")
    require_columns(customers, CUSTOMER_COLUMNS, "customers")
    require_columns(products, PRODUCT_COLUMNS, "products")

    orders = raw_orders.select(*ORDER_COLUMNS)
    orders = (
        orders.withColumn("quantity", F.expr("try_cast(quantity AS INT)"))
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
        .when(F.col("customer_id").isNull(), "missing_customer_id")
        .when(F.col("customer_found").isNull(), "unknown_customer")
        .when(F.col("product_id").isNull(), "missing_product_id")
        .when(F.col("product_found").isNull(), "unknown_product")
        .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "invalid_quantity")
        .when(F.col("unit_price").isNull() | (F.col("unit_price") <= 0), "invalid_price")
        .when(F.col("order_date").isNull(), "invalid_order_date")
        .when(F.col("event_time").isNull(), "invalid_event_time")
        .when(
            F.col("operation").isNull() | ~F.col("operation").isin("I", "U", "D"),
            "invalid_operation",
        ),
    )

    # Duplicate delivery of the same event should not count as another order.
    event_columns = [F.coalesce(F.col(name).cast("string"), F.lit("<null>")) for name in ORDER_COLUMNS]
    orders = orders.withColumn("event_key", F.sha2(F.concat_ws("|", *event_columns), 256))
    # Two events for the same order and timestamp are ambiguous. Keep one
    # consistently and send the other to quarantine.
    duplicate_window = Window.partitionBy("order_id", "event_time").orderBy("event_key")
    orders = orders.withColumn("duplicate_number", F.row_number().over(duplicate_window))
    orders = orders.withColumn(
        "rejection_reason",
        F.when(
            F.col("rejection_reason").isNull() & (F.col("duplicate_number") > 1),
            "duplicate_event",
        ).otherwise(F.col("rejection_reason")),
    )

    helper_columns = ["customer_found", "product_found", "duplicate_number"]
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
        .drop("event_number", "event_key")
        .withColumn("is_deleted", F.col("operation") == "D")
    )


def daily_sales(current: DataFrame, products: DataFrame) -> DataFrame:
    """Create a simple Gold report from active orders."""
    active = current.filter(~F.col("is_deleted"))
    enriched = active.join(products.select("product_id", "category"), "product_id", "left")
    enriched = enriched.withColumn("line_total", F.col("quantity") * F.col("unit_price"))
    return (
        enriched.groupBy("order_date", "category")
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("line_total").cast("decimal(16,2)").alias("revenue"),
        )
        .orderBy("order_date", "category")
    )
