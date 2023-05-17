# distributed-query-benchmarking
Benchmarking of distributed query engines

## Overview

Each benchmark result is a JSON file comprising:

```json
{
    "name": "tpch[SF=1000,N=512]:q1",
    "framework": "daft-0.1.1",
    "attempt_walltimes": [100.0, 10.0, 10.0]
}
```

1. `name`: the name of the benchmark being run
2. `framework`: the framework that is being used to run the benchmark
3. `attempt_walltimes`: the walltimes of each attempt at the benchmark (this is a list, which allows us to provision warmup runs)

## Running the Benchmarks

The scripts in this library were run using Python 3.10.9

1. Create a virtual environment: `python -m venv venv`
2. Activate your virtual environment: `source venv/bin/activate`
3. Install the dependencies required by this repository: `pip install -r requirements.txt`

### Generating Data

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

Run `python distributed_query_benchmarking/__main__.py` to run the TPC-H benchmarks.

1. `--s3-parquet-url` points to the S3 path prefix that contains the parquet file generated from the *Generating Data* step
2. `--ray-address` is an address to the Ray cluster to use (if applicable to the current benchmark being run), defaults to `ray://localhost:10001`
