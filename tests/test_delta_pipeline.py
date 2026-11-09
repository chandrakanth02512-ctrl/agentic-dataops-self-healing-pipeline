"""Integration tests use real local Delta tables and real MERGE transactions."""

import csv
import shutil
from pathlib import Path

import pytest
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from agentic_dataops import delta_pipeline
from agentic_dataops.pipeline import run_pipeline
from agentic_dataops.transforms import ORDER_COLUMNS

BATCHES = Path(__file__).resolve().parents[1] / 'data' / 'batches'


def read(spark, root, table):
    return spark.read.format('delta').load(f'{root}/{table}')


def run(spark, root, name='001_initial', batch_id=None, **options):
    return run_pipeline(spark, str(BATCHES / name), str(root), 'delta', batch_id or name, **options)


def make_batch(tmp_path, name, rows):
    folder = tmp_path / name
    folder.mkdir()
    for reference in ('customers.csv', 'products.csv'):
        shutil.copyfile(BATCHES / '001_initial' / reference, folder / reference)
    with (folder / 'orders.csv').open('w') as handle:
        writer = csv.writer(handle)
        writer.writerow(ORDER_COLUMNS)
        writer.writerows(rows)
    return str(folder)


def test_incremental_merge_replay_late_events_and_schema_evolution(spark, tmp_path):
    root = tmp_path / 'lake'
    first = run(spark, root)
    assert (first['source_rows'], first['valid_events'], first['quarantined_rows']) == (10, 6, 4)
    assert first['gold_revenue'] == '1758.00'
    assert first['orders_before'] == 0
    assert first['orders_after'] == 4

    # A different process/session can recover checkpoints from storage.
    restarted = spark.newSession()
    repeat = run(restarted, root)
    assert repeat['status'] == 'already_processed'
    assert read(spark, root, 'audit/runs').count() == 1
    assert read(spark, root, 'bronze/orders').count() == 10

    second = run(spark, root, '002_changes')
    assert (second['source_rows'], second['valid_events'], second['quarantined_rows']) == (8, 5, 3)
    assert (second['orders_before'], second['orders_after'], second['active_orders']) == (4, 5, 3)
    assert (second['events_before'], second['events_after']) == (6, 11)
    assert second['gold_revenue'] == '3117.00'
    current = {row.order_id: row for row in read(spark, root, 'silver/current_orders').collect()}
    assert current['O001'].quantity == 3
    assert current['O002'].is_deleted
    assert current['O003'].is_deleted  # A late update cannot resurrect the tombstone.
    assert current['O008'].quantity == 2
    assert read(spark, root, 'bronze/orders').count() == 18
    reasons = read(spark, root, 'quarantine/orders').filter("batch_id = '002_changes'")
    assert {row.rejection_reason for row in reasons.collect()} == {
        'duplicate_event', 'invalid_quantity', 'unknown_product'}
    assert 'many' in reasons.filter("order_id = 'O009'").first().raw_record
    history = DeltaTable.forPath(spark, f'{root}/silver/current_orders').history()
    assert history.filter("operation = 'MERGE'").count() >= 1

    third = run(spark, root, '003_schema_addition')
    assert third['gold_revenue'] == '3357.00'
    assert third['active_orders'] == 4
    for table in ('bronze/orders', 'silver/order_events', 'silver/current_orders'):
        assert read(spark, root, table).filter("order_id = 'O011'").first().sales_channel == 'web'
    # Replay must not rewrite Gold or any business table.
    version = DeltaTable.forPath(spark, f'{root}/gold/daily_sales').history(1).first().version
    assert run(spark, root, '003_schema_addition')['status'] == 'already_processed'
    assert DeltaTable.forPath(spark, f'{root}/gold/daily_sales').history(1).first().version == version
    assert read(spark, root, 'silver/order_events').count() == 12
    delivered_again = run(spark, root, batch_id='same-events-new-delivery')
    assert delivered_again['valid_events'] == 0
    assert delivered_again['quarantined_rows'] == 10
    assert delivered_again['gold_revenue'] == '3357.00'
    assert read(spark, root, 'silver/order_events').count() == 12


@pytest.mark.parametrize('stage', ['bronze', 'silver', 'gold'])
def test_partial_failure_replays_without_double_counting(spark, tmp_path, stage):
    root = tmp_path / 'lake'
    with pytest.raises(RuntimeError, match=f'Injected failure after {stage}'):
        run(spark, root, fail_after=stage)
    audit = read(spark, root, 'audit/runs')
    assert audit.filter("status = 'success'").count() == 0
    assert stage in audit.first().failure_reason
    if stage in ('bronze', 'silver'):
        assert audit.first().gold_order_count is None
    with pytest.raises(RuntimeError, match='Recover the unfinished batch'):
        run(spark, root, '002_changes')
    recovered = run(spark.newSession(), root)
    assert recovered['status'] == 'success'
    assert recovered['gold_revenue'] == '1758.00'
    assert (recovered['valid_events'], recovered['quarantined_rows']) == (6, 4)
    assert read(spark, root, 'bronze/orders').count() == 10
    assert read(spark, root, 'silver/order_events').count() == 6
    assert read(spark, root, 'quarantine/orders').count() == 4
    assert {r.status for r in read(spark, root, 'audit/runs').collect()} == {'failed', 'success'}


def test_essential_gold_write_failure_does_not_commit(spark, tmp_path, monkeypatch):
    root = tmp_path / 'lake'
    from pyspark.sql.readwriter import DataFrameWriter
    original = DataFrameWriter.save

    def fail_gold(self, path=None, *args, **kwargs):
        if path and path.endswith('/gold/daily_sales'):
            raise OSError('simulated storage outage')
        return original(self, path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(DataFrameWriter, 'save', fail_gold)
        with pytest.raises(OSError, match='storage outage'):
            run(spark, root)
    assert read(spark, root, 'audit/runs').first().status == 'failed'
    assert run(spark, root)['gold_revenue'] == '1758.00'


def test_checkpoint_write_failure_and_restart(spark, tmp_path, monkeypatch):
    root = tmp_path / 'lake'
    original = delta_pipeline.write_audit

    def fail_commit(session, path, report):
        if report['status'] == 'success':
            raise OSError('checkpoint unavailable')
        return original(session, path, report)

    with monkeypatch.context() as patch:
        patch.setattr(delta_pipeline, 'write_audit', fail_commit)
        with pytest.raises(OSError, match='checkpoint unavailable'):
            run(spark, root)
    assert read(spark, root, 'audit/runs').filter("status = 'success'").count() == 0
    assert run(spark.newSession(), root)['status'] == 'success'
    assert read(spark, root, 'silver/order_events').count() == 6


def test_renamed_column_fails_without_success_checkpoint(spark, tmp_path):
    root = tmp_path / 'lake'
    with pytest.raises(ValueError, match='missing required columns: customer_id'):
        run(spark, root, '004_missing_column')
    audit = read(spark, root, 'audit/runs').first()
    assert audit.status == 'failed'
    assert 'customer_id' in audit.failure_reason
    assert not delta_pipeline.table_exists(spark, f'{root}/silver/current_orders')


def test_batch_identity_cannot_be_reused_for_changed_data(spark, tmp_path):
    root = tmp_path / 'lake'
    run(spark, root)
    with pytest.raises(ValueError, match='different source bytes'):
        run(spark, root, '002_changes', batch_id='001_initial')
    assert read(spark, root, 'audit/runs').count() == 1


def test_tied_events_have_same_winner_across_batch_boundaries(spark, tmp_path):
    insert = ['T001', 'C001', 'P1001', '1', '10.00', 'Placed', '2026-11-01', 'I', '2026-11-01T10:00:00']
    update = insert.copy(); update[3] = '2'; update[5] = 'Updated'; update[7] = 'U'
    delete = insert.copy(); delete[5] = 'Cancelled'; delete[7] = 'D'
    combined = make_batch(tmp_path, 'combined', [delete, insert, update, delete])
    first = make_batch(tmp_path, 'delete', [delete])
    late = make_batch(tmp_path, 'older', [update, insert])
    all_root, split_root = str(tmp_path / 'all'), str(tmp_path / 'split')
    report = run_pipeline(spark, combined, all_root, 'delta', 'all')
    assert report['valid_events'] == 3 and report['quarantined_rows'] == 1
    run_pipeline(spark, first, split_root, 'delta', 'delete')
    run_pipeline(spark, late, split_root, 'delta', 'older')
    all_row = read(spark, all_root, 'silver/current_orders').first()
    split_row = read(spark, split_root, 'silver/current_orders').first()
    assert all_row.event_key == split_row.event_key
    assert all_row.is_deleted and split_row.is_deleted
    assert read(spark, split_root, 'gold/daily_sales').count() == 0


def test_equal_rank_merge_and_removed_optional_field(spark, tmp_path):
    from test_transforms import sample_tables
    from agentic_dataops.transforms import validate_orders
    customers, products, orders = sample_tables(spark)
    row = orders.first().asDict()
    row.update(operation='U', order_status='Updated')
    raw = spark.createDataFrame([row, dict(row, quantity='9')], orders.schema)
    valid, _ = validate_orders(raw, customers, products)
    keys = sorted(r.event_key for r in valid.collect())
    path = str(tmp_path / 'current')
    smaller = valid.filter(F.col('event_key') == keys[0]).withColumn('sales_channel', F.lit('web'))
    larger = valid.filter(F.col('event_key') == keys[1])
    delta_pipeline.merge_current(spark, smaller, path)
    delta_pipeline.merge_current(spark, larger, path)
    delta_pipeline.merge_current(spark, smaller, path)
    winner = spark.read.format('delta').load(path).first()
    assert winner.event_key == keys[1]
    assert winner.sales_channel is None


@pytest.mark.parametrize('empty', [True, False])
def test_empty_or_all_invalid_batch_reconciles(spark, tmp_path, empty):
    rows = [] if empty else [['BAD', 'C001', 'P1001', '0', '10.00', 'Placed', '2026-11-01', 'I', '2026-11-01T10:00:00']]
    source = make_batch(tmp_path, 'source', rows)
    report = run_pipeline(spark, source, str(tmp_path / 'lake'), 'delta', 'empty-or-bad')
    assert report['status'] == 'success'
    assert report['source_rows'] == report['quarantined_rows'] == len(rows)
    assert report['valid_events'] == report['active_orders'] == report['gold_order_count'] == 0
    assert report['gold_revenue'] == '0.00'
