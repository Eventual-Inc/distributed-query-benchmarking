import os
import pathlib
import contextlib
import signal

from distributed_query_benchmarking.common import Config, metrics
from distributed_query_benchmarking.ray_job_runner import ray_entrypoint, ray_job_params


current_dir = pathlib.Path(os.path.dirname(__file__))
PATH_TO_TPCH_ENTRYPOINT = pathlib.Path(__file__)
DASK_VERSION = "2022.10.1"


def _handle_timeout(signum, frame):
    raise TimeoutError("Timed out!")

@contextlib.contextmanager
def timeout(timeout_s: int):
    signal.signal(signal.SIGALRM, _handle_timeout)
    signal.alarm(timeout_s)
    yield
    signal.alarm(0)


def run_dask_dataframe(s3_url: str, q_num: int, timeout_s: int):
    import dask.dataframe as dd
    from distributed_query_benchmarking.dask_queries import queries

    def get_df(tbl: str) -> dd.DataFrame:
        return dd.read_parquet(os.path.join(s3_url, tbl))

    query = getattr(queries, f"q{q_num}")

    with timeout(timeout_s), metrics() as m:
        try:
            result = query(get_df)
            print(result)
        except TimeoutError:
            print(f"--- timed out after {timeout_s}s ---")
            return
    print(f"--- walltime: {m.walltime_s}s ---")


def run_on_dask(config: Config, tpch_qnum: int) -> None:
    from dask.distributed import Client

    # Unused, but gets registered as the default client on invocation
    client = Client(config.dask_address)

    run_dask_dataframe(config.s3_parquet_url, tpch_qnum, config.timeout_s)



def construct_ray_job(config: Config, tpch_qnum: int) -> dict:
    working_dir = (current_dir / ".." / "..").resolve()
    return ray_job_params(
        config=config,
        tpch_qnum=tpch_qnum,
        working_dir=working_dir,
        entrypoint=PATH_TO_TPCH_ENTRYPOINT,
        runtime_env_pip=[f"dask[dataframe]=={DASK_VERSION}", "pandas<2.0.0", "s3fs"],
    )


###
# Ray entrypoint
###    


if __name__ == "__main__":
    args = ray_entrypoint()

    from ray.util.dask import enable_dask_on_ray

    import ray
    ray.init(address="auto")
    enable_dask_on_ray()

    run_dask_dataframe(args.s3_parquet_url, args.question_number, 0)

