import argparse

from distributed_query_benchmarking import daft as daft_benchmarking

def main():
    parser = argparse.ArgumentParser(prog="dqb")
    parser.add_argument(
        "action",
        choices=["setup", "teardown", "run"],
        help="Select the action to take",
    )
    parser.add_argument(
        "candidate",
        choices=["daft"],
        help="Choose which candidate framework to benchmark",
    )
    args = parser.parse_args()
    job = (args.candidate, args.action)
    if job == ("daft", "setup"):
        daft_benchmarking.setup()
    elif job == ("daft", "run"):
        daft_benchmarking.run()
    elif job == ("daft", "teardown"):
        daft_benchmarking.teardown()
    else:
        raise NotImplementedError(f"Unsupported job: {job}")
