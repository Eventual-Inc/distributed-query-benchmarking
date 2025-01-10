import os
from datetime import datetime
import subprocess

QUESTIONS = [
    1,
    3,
    7,
    8,
    9,
    13,
    19,
    24,
    25,
    26,
    28,
    29,
    31,
    32,
    33,
    37,
    38,
    42,
    43,
    48,
    50,
    52,
    55,
    60,
    69,
    73,
    81,
    82,
    85,
    88,
    91,
    92,
    95,
    97,
  ]

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
            subprocess.call(
                'spark-submit '
                '--master spark://$(eval hostname):7077 '
                '--packages org.apache.hadoop:hadoop-aws:3.3.4 '
                '--conf "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider" '
                '--executor-memory 122g '
                f'tpcds_main.py {q}',
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
            "duration_seconds": duration
        }
        
        # Write timing data to JSON file
        import json
        with open(f"{run_dir}/timings/q{q}.json", "w") as timing_file:
            json.dump(timing_data, timing_file, indent=2)
            
        print(f"Q{q} completed in {duration:.2f} seconds")

if __name__ == "__main__":
    main()
