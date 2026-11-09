import pytest

from agentic_dataops.pipeline import read_csv
from agentic_dataops.transforms import (
    CUSTOMER_COLUMNS, ORDER_COLUMNS, PRODUCT_COLUMNS, current_orders,
    validate_orders, validate_reference,
)
from test_transforms import sample_tables


@pytest.mark.parametrize(('column', 'value', 'reason'), [
    ('order_id', None, 'missing_order_id'),
    ('customer_id', '', 'missing_customer_id'),
    ('product_id', None, 'missing_product_id'),
    ('quantity', '1.5', 'invalid_quantity'),
    ('unit_price', 'not-a-price', 'invalid_price'),
    ('unit_price', '1.234', 'invalid_price'),
    ('order_date', 'not-a-date', 'invalid_order_date'),
    ('event_time', 'not-a-time', 'invalid_event_time'),
    ('operation', 'X', 'invalid_operation'),
    ('order_status', 'Unknown', 'invalid_order_status'),
])
def test_invalid_fields_keep_original_payload(spark, column, value, reason):
    customers, products, orders = sample_tables(spark)
    record = orders.first().asDict()
    record[column] = value
    raw = spark.createDataFrame([record], orders.schema)
    valid, rejected = validate_orders(raw, customers, products)
    assert valid.count() == 0
    row = rejected.first()
    assert row.rejection_reason == reason
    assert column in row.raw_record
    if value:
        assert value in row.raw_record


@pytest.mark.parametrize('source', ['customers', 'products'])
def test_duplicate_reference_keys_fail_clearly(spark, source):
    customers, products, _ = sample_tables(spark)
    data, columns = (customers, CUSTOMER_COLUMNS) if source == 'customers' else (products, PRODUCT_COLUMNS)
    with pytest.raises(ValueError, match='duplicate'):
        validate_reference(data.unionByName(data.limit(1)), columns, source)


def test_equal_time_updates_use_content_key_not_row_order(spark):
    customers, products, orders = sample_tables(spark)
    row = orders.first().asDict()
    row['operation'] = 'U'; row['order_status'] = 'Updated'
    other = dict(row, quantity='9')
    data = spark.createDataFrame([row, other], orders.schema)
    valid, rejected = validate_orders(data, customers, products)
    assert valid.count() == 2 and rejected.count() == 0
    expected = max(r.event_key for r in valid.select('event_key').collect())
    assert current_orders(valid.repartition(2)).first().event_key == expected


def test_optional_columns_survive_and_reserved_columns_fail(spark, tmp_path):
    customers, products, orders = sample_tables(spark)
    from pyspark.sql import functions as F
    valid, _ = validate_orders(orders.withColumn('sales_channel', F.lit('web')), customers, products)
    assert valid.first().sales_channel == 'web'
    path = tmp_path / 'reserved.csv'
    path.write_text(','.join(ORDER_COLUMNS + ['batch_id']) + '\n')
    with pytest.raises(ValueError, match='reserved column: batch_id'):
        read_csv(spark, str(path), ORDER_COLUMNS, 'orders')
