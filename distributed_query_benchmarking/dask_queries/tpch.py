import os
import pathlib
import argparse
import uuid

from distributed_query_benchmarking.common import Config, metrics


current_dir = pathlib.Path(os.path.dirname(__file__))
PATH_TO_TPCH_ENTRYPOINT = pathlib.Path(__file__)
DASK_VERSION = "2022.10.1"


def construct_ray_job(config: Config, tpch_qnum: int) -> dict:
    working_dir = (current_dir / ".." / "..").resolve()
    return dict(
        submission_id=f"dask-tpch-q{tpch_qnum}-{str(uuid.uuid4())[:4]}",
        entrypoint=f"python {str(PATH_TO_TPCH_ENTRYPOINT.relative_to(working_dir))} --s3-parquet-url {config.s3_parquet_url} --question-number {tpch_qnum}",
        runtime_env={
            "working_dir": str(working_dir),
            "pip": [f"dask[dataframe]=={DASK_VERSION}", "pandas<2.0.0", "s3fs"],
        },
    )


###
# Job entrypoint
###


def run_tpch_question(s3_url: str, q_num: int):
    """Entrypoint for job that runs in the Ray cluster"""

    from ray.util.dask import enable_dask_on_ray
    import dask.dataframe as dd
    from distributed_query_benchmarking.dask_queries import queries

    import ray
    ray.init(address="auto")
    enable_dask_on_ray()

    def get_df(tbl: str) -> dd.DataFrame:
        return dd.read_parquet(os.path.join(s3_url, tbl))

    print(f"Job starting for TPC-H q{q_num}...")
    query = getattr(queries, f"q{q_num}")

    with metrics() as m:
        result = query(get_df)
    print(f"Q{q_num} computation took: {m.walltime_s}s")
    print(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3-parquet-url", help="Path to TPC-H data stored in AWS S3 as Parquet files", required=True)
    parser.add_argument("--question-number", help="Question number to run", required=True)
    args = parser.parse_args()
    run_tpch_question(args.s3_parquet_url, args.question_number)