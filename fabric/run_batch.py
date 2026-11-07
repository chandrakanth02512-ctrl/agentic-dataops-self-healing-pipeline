"""Fabric Spark job entry point; uses the workspace's native Spark and Delta."""

import argparse
import json
from importlib.metadata import version

from pyspark.sql import SparkSession

from agentic_dataops.pipeline import run_pipeline


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True, help='Non-secret JSON configuration path')
    parser.add_argument('--batch-id', required=True)
    args = parser.parse_args()
    with open(args.config) as handle:
        config = json.load(handle)
    spark = SparkSession.builder.getOrCreate()
    if not spark.version.startswith('4.0.') or not version('delta-spark').startswith('4.0.'):
        raise RuntimeError('This build requires native Spark 4.0.x and Delta 4.0.x; select a compatible Fabric runtime')
    source = config['input_root'].rstrip('/') + '/' + args.batch_id
    report = run_pipeline(spark, source, config['output_root'], 'delta', args.batch_id)
    print(json.dumps(report, sort_keys=True))
    # Fabric owns the session lifecycle.


if __name__ == '__main__':
    main()
