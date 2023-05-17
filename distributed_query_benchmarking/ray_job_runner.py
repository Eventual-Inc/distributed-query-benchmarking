import time

from distributed_query_benchmarking.common import Config
from ray.job_submission import JobSubmissionClient


def run_on_ray(config: Config, job_params: dict):
    """Submits a job to run in the Ray cluster"""

    print("Submitting Daft benchmarking job to Ray cluster...")
    print("Parameters:")
    print(job_params)

    client = JobSubmissionClient(address=config.ray_address)
    job_id = client.submit_job(**job_params)
    print(f"Submitted job: {job_id}")

    status = client.get_job_status(job_id)
    while not status.is_terminal():
        print(f"Job {job_id} status: {status}")
        status = client.get_job_status(job_id)
        time.sleep(10)

    job_info = client.get_job_info(job_id)
    print(f"Job completed in {(job_info.end_time - job_info.start_time) / 1000}s:")
    print(f"All job logs:\n{client.get_job_logs(job_id)}")
