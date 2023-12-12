from __future__ import annotations

import time
import json
import logging
import argparse
import pathlib
import uuid
import asyncio

from distributed_query_benchmarking.common import Config
from ray.job_submission import JobSubmissionClient

from typing import Any


async def print_logs(logs):
    async for lines in logs:
        print(lines, end="")


async def wait_on_job(logs, timeout_s):
    await asyncio.wait_for(print_logs(logs), timeout=timeout_s)


def run_on_ray(config: Config, job_params: dict):
    """Submits a job to run in the Ray cluster"""

    print("Submitting benchmarking job to Ray cluster...")
    print("Parameters:")
    print(job_params)

    client = JobSubmissionClient(address=config.ray_address)
    job_id = client.submit_job(**job_params)
    print(f"Submitted job: {job_id}")

    try:
        asyncio.run(wait_on_job(client.tail_job_logs(job_id), config.timeout_s))
    except asyncio.TimeoutError:
        print(f"Job timed out after {config.timeout_s}s! Stopping job now...")
        client.stop_job(job_id)
        time.sleep(16)

    status = client.get_job_status(job_id)
    assert status.is_terminal(), "Job should have terminated"
    job_info = client.get_job_info(job_id)
    print(f"Job completed with {status}")


def ray_entrypoint():
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3-parquet-url", help="Path to TPC-H data stored in AWS S3 as Parquet files", required=True)
    parser.add_argument("--question-number", help="Question number to run", required=True)
    parser.add_argument("--daft-version", help="Version of Daft running", required=False)
    args = parser.parse_args()
    return args


def ray_job_params(
    config: Config,
    tpch_qnum: int,
    working_dir: pathlib.Path,
    entrypoint: pathlib.Path,
    runtime_env_pip: list[str],
    runtime_env_env_vars: dict[str, str] = {},
    runtime_env_py_modules: list[Any] | None = None,
) -> dict:
    if config.daft_wheel_uri is None:
        daft_version = config.daft_pypi_version
    else:
        from wheel_filename import parse_wheel_filename, InvalidFilenameError

        try:
            pwf = parse_wheel_filename(config.daft_wheel_uri)
            daft_version = pwf.version
        except InvalidFilenameError:
            logging.getLogger().warning(
                "Failed to parse wheel URI's version and unable to do version validation; this suggests that the URI "
                "does not follow the PEP-427 wheel URI spec: https://peps.python.org/pep-0427/#file-name-convention, "
                f"wheel URI = {config.daft_wheel_uri}"
            )
            daft_version = None

    entrypoint = f"python {str(entrypoint.relative_to(working_dir))} --s3-parquet-url {config.s3_parquet_url} --question-number {tpch_qnum}"
    if daft_version is not None:
        entrypoint += f" --daft-version {daft_version}"

    return dict(
        submission_id=f"{config.framework}-tpch-q{tpch_qnum}-{str(uuid.uuid4())[:4]}",
        entrypoint=entrypoint,
        runtime_env={
            "working_dir": str(working_dir),
            "pip": runtime_env_pip,
            "env_vars": runtime_env_env_vars,
            "py_modules": runtime_env_py_modules,
        },
    )
