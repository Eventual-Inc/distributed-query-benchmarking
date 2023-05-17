import os
import pathlib
import argparse
import uuid

from distributed_query_benchmarking.common import Config, metrics


current_dir = pathlib.Path(os.path.dirname(__file__))
PATH_TO_TPCH_ENTRYPOINT = pathlib.Path(__file__)
DAFT_VERSION = "0.1.0"


def construct_ray_job(config: Config, tpch_qnum: int) -> dict:
    working_dir = (current_dir / ".." / "..").resolve()
    return dict(
        submission_id=f"daft-tpch-q{tpch_qnum}-{str(uuid.uuid4())[:4]}",
        entrypoint=f"python {str(PATH_TO_TPCH_ENTRYPOINT.relative_to(working_dir))} --s3-parquet-url {config.s3_parquet_url} --question-number {tpch_qnum}",
        runtime_env={
            "working_dir": str(working_dir),
            "pip": [f"getdaft[aws,ray]=={DAFT_VERSION}"]
        },
    )



###
# Job entrypoint
###


def run_tpch_question(s3_url: str, q_num: int):
    """Entrypoint for job that runs in the Ray cluster"""

    import daft
    from distributed_query_benchmarking.daft_queries import queries

    def get_df(tbl: str) -> daft.DataFrame:
        daft_minor_version = int(DAFT_VERSION.split(".")[1])
        if daft_minor_version == 0:
            return daft.DataFrame.read_parquet(os.path.join(s3_url, tbl))
        else:
            return daft.read_parquet(os.path.join(s3_url, tbl))
    
    daft.context.set_runner_ray(address="auto")

    print(f"Job starting for TPC-H q{q_num}...")
    query = getattr(queries, f"q{q_num}")

    with metrics() as m:
        df = query(get_df)
    print(f"Q{q_num} df construction took: {m.walltime_s}s")
    print(f"Retrieved dataframe:\n{df}")

    with metrics() as m:
        df.collect()
    print(f"Q{q_num} collect took: {m.walltime_s}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3-parquet-url", help="Path to TPC-H data stored in AWS S3 as Parquet files", required=True)
    parser.add_argument("--question-number", help="Question number to run", required=True)
    args = parser.parse_args()
    run_tpch_question(args.s3_parquet_url, args.question_number)
