"""Fabric Spark job entry point; uses the workspace's native Spark and Delta."""

import argparse
import json

from pyspark.sql import SparkSession

from agentic_dataops.pipeline import run_pipeline
from agentic_dataops.fabric_runtime import validate_fabric_runtime


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True, help='Non-secret JSON configuration path')
    parser.add_argument('--batch-id', required=True)
    args = parser.parse_args()
    with open(args.config) as handle:
        config = json.load(handle)
    spark = SparkSession.builder.getOrCreate()
    runtime = validate_fabric_runtime(spark)
    print(json.dumps({'runtime': runtime}, sort_keys=True))
    source = config['input_root'].rstrip('/') + '/' + args.batch_id
    report = run_pipeline(spark, source, config['output_root'], 'delta', args.batch_id)
    print(json.dumps(report, sort_keys=True))
    # Fabric owns the session lifecycle.


if __name__ == '__main__':
    main()
