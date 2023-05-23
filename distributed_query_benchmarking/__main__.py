import argparse

from distributed_query_benchmarking.daft_queries import tpch as daft_tpch
from distributed_query_benchmarking.modin_queries import tpch as modin_tpch
from distributed_query_benchmarking.dask_queries import tpch as dask_tpch
from distributed_query_benchmarking.spark_queries import tpch as spark_tpch
from distributed_query_benchmarking.common import Config
from distributed_query_benchmarking.ray_job_runner import run_on_ray


def run_benchmarking(config: Config):
    for tpch_qnum in config.questions:
        for attempt in config.num_attempts:
            print(f"========== Starting benchmarks for Q{tpch_qnum}, attempt {attempt} ==========\n")
            if config.framework == "daft":
                ray_job_params = daft_tpch.construct_ray_job(config, tpch_qnum)
                run_on_ray(config, ray_job_params)
            elif config.framework == "daft-local-build":
                ray_job_params = daft_tpch.construct_ray_job_local_daft_build(config, tpch_qnum)
                run_on_ray(config, ray_job_params)
            elif config.framework == "modin":
                ray_job_params = modin_tpch.construct_ray_job(config, tpch_qnum)
                run_on_ray(config, ray_job_params)
            elif config.framework == "dask-on-ray":
                ray_job_params = dask_tpch.construct_ray_job(config, tpch_qnum)
                run_on_ray(config, ray_job_params)
            elif config.framework == "dask":
                dask_tpch.run_on_dask(config, tpch_qnum)
            elif config.framework == "spark-on-ray":
                ray_job_params = spark_tpch.construct_ray_job(config, tpch_qnum)
                run_on_ray(config, ray_job_params)
            else:
                raise NotImplementedError(f"Framework not implemented: {config.framework}")
            print(f"========== Finished benchmarks for Q{tpch_qnum}, attempt {attempt} ==========\n")


def main():
    parser = argparse.ArgumentParser(prog="dqb")
    parser.add_argument(
        "framework",
        choices=["daft", "daft-local-build", "modin", "dask-on-ray", "spark-on-ray", "dask"],
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
    parser.add_argument(
        "--dask-address",
        default="localhost:8786",
        help="Address to the Dask cluster",
    )
    parser.add_argument(
        "--num-attempts",
        default=2,
        type=int,
        help="Number of attempts per benchmark",
    )
    parser.add_argument(
        "--questions",
        default=list(range(1, 8)),
        nargs="+",
        type=int,
        help="Questions to run as a list of integers (defaults to Q1-7): `--questions 1 2 3 7`",
    )
    parser.add_argument(
        "--timeout_s",
        default=3600,
        type=int,
        help="Number of seconds to wait before timing out each attempt",
    )

    args = parser.parse_args()
    config = Config.from_args(args)

    run_benchmarking(config)


if __name__ == "__main__":
    main()
