import os
import pathlib

from distributed_query_benchmarking.common import Config, metrics
from distributed_query_benchmarking.ray_job_runner import ray_entrypoint, ray_job_params


current_dir = pathlib.Path(os.path.dirname(__file__))
PATH_TO_TPCH_ENTRYPOINT = pathlib.Path(__file__)
DAFT_VERSION = "0.1.1"


def construct_ray_job(config: Config, tpch_qnum: int) -> dict:
    working_dir = (current_dir / ".." / "..").resolve()
    return ray_job_params(
        config=config,
        tpch_qnum=tpch_qnum,
        working_dir=working_dir,
        entrypoint=PATH_TO_TPCH_ENTRYPOINT,
        runtime_env_pip=[f"getdaft[aws,ray]=={DAFT_VERSION}", "pyarrow==7.0.0"],
    )


def construct_ray_job_local_daft_build(config: Config, tpch_qnum: int) -> dict:
    import daft

    working_dir = (current_dir / ".." / "..").resolve()
    return ray_job_params(
        config=config,
        tpch_qnum=tpch_qnum,
        working_dir=working_dir,
        entrypoint=PATH_TO_TPCH_ENTRYPOINT,
        runtime_env_pip=[
            "pyarrow==12.0.0",
            "fsspec[http]",
            "loguru",
            "tabulate >= 0.9.0",
            "psutil",
        ],
        runtime_env_py_modules=[daft],
    )


###
# Job entrypoint
###


def run_tpch_question(s3_url: str, q_num: int, num_attempts: int):
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

    for attempt in range(num_attempts):
        print(f"--- Attempt {attempt} ---")

        with metrics() as overall_metrics:
            query = getattr(queries, f"q{q_num}")

            with metrics() as get_df_metrics:
                df = query(get_df)
            print(f"Q{q_num} df construction took: {get_df_metrics.walltime_s}s")
            print(f"Retrieved dataframe:\n{df}")
            df.collect()
            print(df.to_pandas())

        print(f"--- Attempt {attempt} walltime: {overall_metrics.walltime_s}s ---")


if __name__ == "__main__":
    args = ray_entrypoint()
    run_tpch_question(args.s3_parquet_url, args.question_number, args.num_attempts)
