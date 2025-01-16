import os
from datetime import datetime
import subprocess

QUESTIONS = range(1, 23)

def parse_logs_to_csv(run_dir):
    """Parse all logs and timing data into a single CSV file"""
    import json
    import csv
    import glob

    results = []

    # Get all timing JSON files
    timing_files = glob.glob(f"{run_dir}/timings/q*.json")

    for timing_file in timing_files:
        with open(timing_file) as f:
            timing_data = json.load(f)

        query_num = timing_data['query']
        results.append({
            'query': query_num,
            'start_time': timing_data['start_time'],
            'end_time': timing_data['end_time'],
            'duration_seconds': timing_data['duration_seconds'],
            'exit_code': timing_data['exit_code'],
        })

    # Write results to CSV
    csv_file = f"{run_dir}/results.csv"
    with open(csv_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['query', 'start_time', 'end_time',
                                             'duration_seconds', 'exit_code'])
        writer.writeheader()
        writer.writerows(results)

    print(f"\nResults written to: {csv_file}")

def main():
    # Create a timestamped run folder
    run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = f"runs/{run_timestamp}"
    os.makedirs(run_dir, exist_ok=True)

    # Create logs and timings directories within the run folder
    os.makedirs(f"{run_dir}/logs", exist_ok=True)
    os.makedirs(f"{run_dir}/timings", exist_ok=True)

    print(f"\nRun outputs will be stored in: {run_dir}")
    print(f"- Logs will be written to: {run_dir}/logs/")
    print(f"- Timing data will be written to: {run_dir}/timings/\n")

    for q in QUESTIONS:
        print(f"Running spark-submit for Q{q}")
        start_time = datetime.now()

        with open(f"{run_dir}/logs/q{q}.log", "w") as logfile, \
             open(f"{run_dir}/logs/q{q}.err", "w") as errfile:
            exit_code = subprocess.call(
                'spark-submit '
                '--master spark://$(eval hostname):7077 '
                '--packages org.apache.hadoop:hadoop-aws:3.3.4 '
                '--conf "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider" '
                '--executor-memory 122g '
                f'tpch_main.py {q}',
                stdout=logfile,
                stderr=errfile,
                shell=True,
            )

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Create timing JSON for this query
        timing_data = {
            "query": q,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "exit_code": exit_code
        }

        # Write timing data to JSON file
        import json
        with open(f"{run_dir}/timings/q{q}.json", "w") as timing_file:
            json.dump(timing_data, timing_file, indent=2)

        print(f"Q{q} completed in {duration:.2f} seconds with exit code {exit_code}")

    # Parse all logs into CSV at the end
    parse_logs_to_csv(run_dir)

if __name__ == "__main__":
    main()
