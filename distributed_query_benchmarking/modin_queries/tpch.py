import os
import pathlib
import argparse
import uuid

from distributed_query_benchmarking.common import Config, metrics
from distributed_query_benchmarking.ray_job_runner import ray_entrypoint, ray_job_params


current_dir = pathlib.Path(os.path.dirname(__file__))
PATH_TO_TPCH_ENTRYPOINT = pathlib.Path(__file__)
MODIN_VERSION = "0.20.1"


def construct_ray_job(config: Config, tpch_qnum: int) -> dict:
    working_dir = (current_dir / ".." / "..").resolve()
    return ray_job_params(
        config=config,
        tpch_qnum=tpch_qnum,
        working_dir=working_dir,
        entrypoint=PATH_TO_TPCH_ENTRYPOINT,
        runtime_env_pip=[f"modin[ray,aws]=={MODIN_VERSION}", "s3fs", "pandas"],
        # runtime_env_env_vars={"__MODIN_AUTOIMPORT_PANDAS__": "1"},
    )



###
# Job entrypoint
###


def run_tpch_question(s3_url: str, q_num: int):
    """Entrypoint for job that runs in the Ray cluster"""

    import pandas
    import modin.pandas as pd
    from distributed_query_benchmarking.modin_queries import queries

    import ray
    ray.init(address="auto")

    def get_df(tbl: str) -> pd.DataFrame:
        return pd.read_parquet(os.path.join(s3_url, tbl))

    query = getattr(queries, f"q{q_num}")

    with metrics() as m:
        result = query(get_df)
        print(result)
    print(f"--- walltime: {m.walltime_s}s ---")


if __name__ == "__main__":
    args = ray_entrypoint()
    run_tpch_question(args.s3_parquet_url, args.question_number)
