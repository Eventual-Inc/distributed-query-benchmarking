import os
import pathlib

from distributed_query_benchmarking.common import Config, metrics
from distributed_query_benchmarking.ray_job_runner import ray_entrypoint, ray_job_params


current_dir = pathlib.Path(os.path.dirname(__file__))
PATH_TO_TPCH_ENTRYPOINT = pathlib.Path(__file__)
DAFT_VERSION = "0.2.1+dev0013.b4467ad6"
DAFT_MICROPARTITIONS_FEATURE_FLAG = "1"


def construct_ray_job(config: Config, tpch_qnum: int) -> dict:
    working_dir = (current_dir / ".." / "..").resolve()
    return ray_job_params(
        config=config,
        tpch_qnum=tpch_qnum,
        working_dir=working_dir,
        entrypoint=PATH_TO_TPCH_ENTRYPOINT,
        # runtime_env_pip=[f"getdaft[aws,ray]=={DAFT_VERSION}"],
        runtime_env_pip=[f"getdaft[aws,ray]=={DAFT_VERSION}", "--pre", "--extra-index-url https://pypi.anaconda.org/daft-nightly/simple"],
        runtime_env_env_vars={"DAFT_MICROPARTITIONS": DAFT_MICROPARTITIONS_FEATURE_FLAG},
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
            "s3fs",
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


def run_tpch_question(s3_url: str, q_num: int):
    """Entrypoint for job that runs in the Ray cluster"""

    import daft
    from distributed_query_benchmarking.daft_queries import queries

    def get_df(tbl: str) -> daft.DataFrame:
        return daft.read_parquet(
            os.path.join(s3_url, tbl),
            use_native_downloader=True,
            io_config=daft.io.IOConfig(s3=daft.io.S3Config(max_connections=8, num_tries=20)),
        )
    
    daft.context.set_runner_ray(address="auto")

    with metrics() as overall_metrics:
        query = getattr(queries, f"q{q_num}")

        with metrics() as get_df_metrics:
            df = query(get_df)
        print(f"Q{q_num} df construction took: {get_df_metrics.walltime_s}s")
        print(f"Retrieved dataframe:\n{df}")
        df.collect()
        print(df.to_pandas())

    print(f"--- walltime: {overall_metrics.walltime_s}s ---")


if __name__ == "__main__":
    args = ray_entrypoint()
    run_tpch_question(args.s3_parquet_url, args.question_number)
