import os
import pathlib
import argparse
import uuid

from distributed_query_benchmarking.common import Config, metrics


current_dir = pathlib.Path(os.path.dirname(__file__))
PATH_TO_TPCH_ENTRYPOINT = pathlib.Path(__file__)
PYSPARK_VERSION = "3.3.1"
RAYDP_VERSION = "1.5.0"


def construct_ray_job(config: Config, tpch_qnum: int) -> dict:
    working_dir = (current_dir / ".." / "..").resolve()
    return dict(
        submission_id=f"spark-tpch-q{tpch_qnum}-{str(uuid.uuid4())[:4]}",
        entrypoint=f"python {str(PATH_TO_TPCH_ENTRYPOINT.relative_to(working_dir))} --s3-parquet-url {config.s3_parquet_url} --question-number {tpch_qnum}",
        runtime_env={
            "working_dir": str(working_dir),
            "pip": [f"pyspark=={PYSPARK_VERSION}", f"raydp=={RAYDP_VERSION}"],
        },
    )



###
# Job entrypoint
###


def run_tpch_question(s3_url: str, q_num: int):
    """Entrypoint for job that runs in the Ray cluster"""

    from distributed_query_benchmarking.spark_queries import queries

    import ray
    import raydp
    ray.init(address="auto")

    spark = raydp.init_spark(
        app_name=f"tpch-q{q_num}",
        # TODO(jay): calculate from available resources
        executor_cores=4,
        num_executors=16,
        executor_memory='7GB',
        # configs = {
            # Set Spark master to run on head node and consume no resources
            # 'spark.ray.raydp_spark_master.actor.resource.CPU': 0,
            # 'spark.ray.raydp_spark_master.actor.resource.spark_master': 1,  # Force Spark driver related actor run on headnode
            # TODO: try enabling to speed up shuffling
            # "spark.shuffle.service.enabled": "true"
        # }
    )

    def load_table(tbl: str):
        df = spark.read.parquet(os.path.join(s3_url, tbl))
        df.createOrReplaceTempView(tbl)

    print(f"Job starting for TPC-H q{q_num}...")
    query = getattr(queries, f"q{q_num}")

    with metrics() as m:
        result = query(spark, load_table)
        print(result.to_pandas())
    print(f"Q{q_num} computation took: {m.walltime_s}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3-parquet-url", help="Path to TPC-H data stored in AWS S3 as Parquet files", required=True)
    parser.add_argument("--question-number", help="Question number to run", required=True)
    args = parser.parse_args()
    run_tpch_question(args.s3_parquet_url, args.question_number)
