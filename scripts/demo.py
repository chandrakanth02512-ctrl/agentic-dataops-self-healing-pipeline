"""Reproduce the batch, replay, failure and recovery sequence with assertions."""

import argparse
import json
from pathlib import Path

from agentic_dataops.pipeline import run_pipeline
from agentic_dataops.session import local_spark


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', default='build/demo')
    args = parser.parse_args()
    output = Path(args.output)
    if output.exists():
        raise SystemExit('Choose a new --output directory so the demo includes real first-time writes')
    spark = local_spark(delta=True)
    reports = []
    try:
        def run(batch, **options):
            report = run_pipeline(spark, f'data/batches/{batch}', str(output), 'delta', batch, **options)
            reports.append(report)
            return report

        first = run('001_initial')
        assert first['gold_revenue'] == '1758.00'
        second = run('002_changes')
        assert second['gold_revenue'] == '3117.00'
        assert run('002_changes')['status'] == 'already_processed'
        try:
            run('003_schema_addition', fail_after='silver')
        except RuntimeError as error:
            if str(error) != 'Injected failure after silver':
                raise
        else:
            raise AssertionError('Failure injection did not fail')
        audit = spark.read.format('delta').load(f'{output}/audit/runs')
        assert audit.filter("batch_id = '003_schema_addition' AND status = 'success'").count() == 0
        recovered = run('003_schema_addition')
        assert recovered['gold_revenue'] == '3357.00'
        assert recovered['active_orders'] == 4
        spark.read.format('delta').load(f'{output}/gold/daily_sales').show()
        (output / 'demo-results.json').write_text(json.dumps(reports, indent=2) + '\n')
        print(json.dumps(reports, indent=2))
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
