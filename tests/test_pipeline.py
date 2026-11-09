from pathlib import Path

from agentic_dataops.pipeline import run_pipeline


SAMPLE_DATA = Path(__file__).resolve().parents[1] / "data" / "sample"


def test_complete_pipeline_and_safe_repeat_run(spark, tmp_path):
    destination = str(tmp_path / "lakehouse")
    first = run_pipeline(spark, str(SAMPLE_DATA), destination)
    second = run_pipeline(spark, str(SAMPLE_DATA), destination)

    assert first["source_rows"] == second["source_rows"] == 10
    assert first["valid_events"] == second["valid_events"] == 6
    assert first["quarantined_rows"] == second["quarantined_rows"] == 4
    assert first["active_orders"] == second["active_orders"] == 3

    assert spark.read.parquet(f"{destination}/bronze/orders").count() == 10
    assert spark.read.parquet(f"{destination}/silver/current_orders").count() == 4
    assert spark.read.parquet(f"{destination}/quarantine/orders").count() == 4
    assert spark.read.parquet(f"{destination}/gold/daily_sales").count() == 3
    assert spark.read.parquet(f"{destination}/audit/runs").count() == 2
