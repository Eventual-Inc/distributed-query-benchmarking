import time
import os
import pathlib
import argparse
import uuid

from ray.job_submission import JobSubmissionClient


current_dir = pathlib.Path(os.path.dirname(__file__))
PATH_TO_TPCH_ENTRYPOINT = pathlib.Path(__file__)
DAFT_VERSION = "0.1.0"


def run_on_ray(s3_parquet_url: str, ray_address: str):
    """Submits a job to run in the Ray cluster"""
    print("Submitting Daft benchmarking job to Ray cluster...")

    working_dir = (current_dir / ".." / "..").resolve()
    print(f"\tWorking directory:\t{working_dir}")
    print(f"\tRay address:\t\t{ray_address}")
    print(f"\tDaft version:\t\t{DAFT_VERSION}")

    QUESTIONS = [i for i in range(1, 11)]

    for qnum in QUESTIONS:
        client = JobSubmissionClient(address=ray_address)
        job_id = client.submit_job(
            submission_id=f"daft-tpch-q{qnum}-{str(uuid.uuid4())[:4]}",
            entrypoint=f"python {str(PATH_TO_TPCH_ENTRYPOINT.relative_to(working_dir))} --s3-parquet-url {s3_parquet_url} --question-number {qnum}",
            runtime_env={
                "working_dir": str(working_dir),
                "pip": [f"getdaft[aws,ray]=={DAFT_VERSION}"]
            },
        )
        print(f"Submitted job: {job_id}")

        status = client.get_job_status(job_id)
        while not status.is_terminal():
            print(f"Still waiting on {job_id} with status: {status}")
            time.sleep(10)
            status = client.get_job_status(job_id)
        
        job_info = client.get_job_info(job_id)
        print(f"Job completed in {(job_info.end_time - job_info.start_time) / 1000}s:")
        print(client.get_job_logs(job_id))



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
    df = query(get_df)

    import time
    start = time.time()
    df.collect()
    print(f"Q{q_num}: {time.time() - start}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3-parquet-url", help="Path to TPC-H data stored in AWS S3 as Parquet files", required=True)
    parser.add_argument("--question-number", help="Question number to run", required=True)
    args = parser.parse_args()
    run_tpch_question(args.s3_parquet_url, args.question_number)
