"""Inspect the local demo with the same Delta-enabled session used by the CLI."""

import argparse
from agentic_dataops.session import local_spark

parser = argparse.ArgumentParser()
parser.add_argument('--output', default='build/demo')
args = parser.parse_args()
spark = local_spark(delta=True)
try:
    for name in ['silver/current_orders', 'quarantine/orders', 'gold/daily_sales', 'audit/runs']:
        print(f'\n{name}')
        spark.read.format('delta').load(f'{args.output}/{name}').show(truncate=False)
finally:
    spark.stop()
