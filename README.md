# distributed-query-benchmarking
Benchmarking of distributed query engines

## Overview

Each benchmark comprises:

1. Setup script: setting up the compute required to run the benchmark using AWS EC2 machines
2. Application script: runs the benchmarks and uses utilities from `distributed-query-benchmarking` to report benchmarking results

## Running the Benchmarks

The scripts in this library were run using Python 3.10.9

1. Create a virtual environment: `python -m venv venv`
2. Activate your virtual environment: `source venv/bin/activate`
3. Install the repository: `pip install .`

You will now have access to the commandline entrypoint `dqb`, which is used to run various operations:

### Generating data

TPC-H data was generated using the utilities [found in the open-sourced Daft repository](https://github.com/Eventual-Inc/Daft/blob/main/benchmarking/tpch/pipelined_data_generation.py).

The output data is stored in AWS S3, in the following folder structure:

```
s3://your-bucket/path/
  customer/
    *.parquet
  lineitem/
    *.parquet
  nation/
    *.parquet
  orders/
    *.parquet
  part/
    *.parquet
  partsupp/
    *.parquet
  region/
    *.parquet
  supplier/
    *.parquet
```

### Running benchmarks

Run `dqb tpch-benchmarks daft` to run the TPC-H benchmarks for Daft.
