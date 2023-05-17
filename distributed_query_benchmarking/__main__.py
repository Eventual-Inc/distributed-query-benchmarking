import argparse
import os
import pathlib

from distributed_query_benchmarking.daft_queries import tpch
from distributed_query_benchmarking.common import Config
from distributed_query_benchmarking.ray_job_runner import run_on_ray


def run_benchmarking(config: Config):
    if config.framework == "daft":
        # TODO: hardcoded for now
        tpch_qnum = 1
        ray_job_params = tpch.construct_ray_job(config, tpch_qnum)
        run_on_ray(config, ray_job_params)
    else:
        raise NotImplementedError(f"Frameowkr not implemented: {config.framework}")


def main():
    parser = argparse.ArgumentParser(prog="dqb")
    parser.add_argument(
        "framework",
        choices=["daft"],
        help="Framework to run benchmarks",
    )
    parser.add_argument(
        "--s3-parquet-url",
        help="Path to Parquet files in AWS S3",
        default="s3://eventual-dev-benchmarking-fixtures/uncompressed/tpch-dbgen/1000_0/512/parquet/",
    )
    parser.add_argument(
        "--results-bucket",
        help="Path to prefix in S3 to dump results",
        default="s3://eventual-dev-benchmarking-results/distributed-query-benchmarking/",
    )
    parser.add_argument(
        "--ray-address",
        default="ray://localhost:10001",
        help="Address to the Ray cluster",
    )

    args = parser.parse_args()
    config = Config.from_args(args)

    run_benchmarking(config)


if __name__ == "__main__":
    main()
