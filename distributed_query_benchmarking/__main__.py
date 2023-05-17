import argparse
import dataclasses
import os
import pathlib

from distributed_query_benchmarking.daft_queries import tpch


@dataclasses.dataclass
class Config:
    framework: str
    s3_parquet_url: str
    results_bucket: str
    ray_address: str
    cluster_config: str

    @classmethod
    def from_args(cls, args):
        return cls(
            framework=args.framework,
            s3_parquet_url=args.s3_parquet_url,
            results_bucket=args.results_bucket,
            ray_address=args.ray_address,
            cluster_config=args.cluster_config,
        )


def run_benchmarking(args):
    if args.framework == "daft":
        tpch.run_on_ray(args.s3_parquet_url, args.ray_address)
    else:
        raise NotImplementedError(f"Frameowkr not implemented: {args.framework}")


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
    parser.add_argument(
        "--cluster-config",
        choices=[f"benchmarks/{fname}" for fname in os.listdir(pathlib.Path(os.path.dirname(__file__)) / ".." / "benchmarks")],
        required=True,
        help="Cluster config file for the infrastructure used to run this benchmark - must be one of the files found in `benchmarks/`",
    )

    args = parser.parse_args()
    run_benchmarking(args)


if __name__ == "__main__":
    main()
