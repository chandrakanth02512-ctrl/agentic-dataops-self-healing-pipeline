from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from agentic_dataops.pipeline import read_csv
from agentic_dataops.transforms import (
    CUSTOMER_COLUMNS,
    ORDER_COLUMNS,
    PRODUCT_COLUMNS,
    current_orders,
    daily_sales,
    validate_orders,
)


SAMPLE_DATA = Path(__file__).resolve().parents[1] / "data" / "sample"


def sample_tables(spark):
    customers = read_csv(spark, str(SAMPLE_DATA / "customers.csv"), CUSTOMER_COLUMNS, "customers")
    products = read_csv(spark, str(SAMPLE_DATA / "products.csv"), PRODUCT_COLUMNS, "products")
    orders = read_csv(spark, str(SAMPLE_DATA / "orders.csv"), ORDER_COLUMNS, "orders")
    return customers, products, orders


def test_invalid_orders_are_quarantined(spark):
    customers, products, orders = sample_tables(spark)
    valid, rejected = validate_orders(orders, customers, products)

    assert orders.count() == 10
    assert valid.count() == 6
    assert rejected.count() == 4
    assert {row.rejection_reason for row in rejected.select("rejection_reason").collect()} == {
        "unknown_customer", "invalid_quantity", "invalid_price", "duplicate_event"
    }


def test_cdc_events_update_and_delete_orders(spark):
    customers, products, orders = sample_tables(spark)
    valid, _ = validate_orders(orders, customers, products)
    current = current_orders(valid)

    assert current.count() == 4
    assert current.filter(~F.col("is_deleted")).count() == 3
    updated = current.filter(F.col("order_id") == "O001").first()
    deleted = current.filter(F.col("order_id") == "O003").first()
    assert updated.quantity == 2
    assert updated.operation == "U"
    assert deleted.is_deleted is True


def test_gold_revenue_excludes_deleted_and_invalid_orders(spark):
    customers, products, orders = sample_tables(spark)
    valid, _ = validate_orders(orders, customers, products)
    gold = daily_sales(current_orders(valid), products)

    rows = gold.collect()
    assert sum(row.order_count for row in rows) == 3
    assert sum((row.revenue for row in rows), Decimal("0.00")) == Decimal("1758.00")


def test_renamed_source_column_fails_with_clear_error(spark):
    customers, products, orders = sample_tables(spark)
    changed_source = orders.withColumnRenamed("customer_id", "client_id")

    with pytest.raises(ValueError, match="missing required columns: customer_id"):
        validate_orders(changed_source, customers, products)
