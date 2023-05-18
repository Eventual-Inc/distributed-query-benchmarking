import argparse

from distributed_query_benchmarking.daft_queries import tpch as daft_tpch
from distributed_query_benchmarking.modin_queries import tpch as modin_tpch
from distributed_query_benchmarking.dask_queries import tpch as dask_tpch
from distributed_query_benchmarking.common import Config
from distributed_query_benchmarking.ray_job_runner import run_on_ray


def run_benchmarking(config: Config):
    # TODO: hardcoded for now but should be scheduled by a harness that understands which TPC-H questions to run
    tpch_qnum = 1
    if config.framework == "daft":
        ray_job_params = daft_tpch.construct_ray_job(config, tpch_qnum)
        run_on_ray(config, ray_job_params)
    elif config.framework == "modin":
        ray_job_params = modin_tpch.construct_ray_job(config, tpch_qnum)
        run_on_ray(config, ray_job_params)
    elif config.framework == "dask-on-ray":
        ray_job_params = dask_tpch.construct_ray_job(config, tpch_qnum)
        run_on_ray(config, ray_job_params)
    else:
        raise NotImplementedError(f"Framework not implemented: {config.framework}")


def main():
    parser = argparse.ArgumentParser(prog="dqb")
    parser.add_argument(
        "framework",
        choices=["daft", "modin", "dask-on-ray"],
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
