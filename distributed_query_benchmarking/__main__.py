import argparse

from distributed_query_benchmarking.daft_queries import tpch


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
        required=True,
    )
    parser.add_argument(
        "--ray-address",
        default="ray://localhost:10001",
        help="Address to the Ray cluster",
    )

    args = parser.parse_args()
    run_benchmarking(args)


if __name__ == "__main__":
    main()
