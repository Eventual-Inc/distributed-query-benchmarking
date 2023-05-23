# distributed-query-benchmarking
Benchmarking of distributed query engines

## Overview

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

To run the benchmarks, you will need:

1. A running cluster with appropriate permissions to read data from AWS S3
2. Data in AWS S3 in the aforementioned format

You can run the script like so:

```
python distributed_query_benchmarking/__main__.py \
    {daft, dask, modin, spark} \
    --s3-parquet-url <URL to AWS S3 prefix containing generated data>
    --ray-address <HTTP URL to the Ray dashboard of your Ray cluster if running on a Ray-backed framework (e.g. http://localhost:8265)>
    --dask-address <URL to the Dask cluster if running on a Dask-backed framework (e.g. localhost:8787)>
    --num-attempts <Number of attempts per question>
    --timeout <Amount of time in seconds to wait until timing out for each attempt>
    --questions <Space-separated list of questions to run (e.g. 1 2 3 5 6 10)>
```
