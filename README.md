# distributed-query-benchmarking
Benchmarking of distributed query engines

## Reader's Notes

Thanks for checking out our benchmarking repository! We built this repository because many of our users were asking us for a comparison of Daft and other similar frameworks. 

Benchmarking is notoriously difficult to present in an unbiased way.

To that end, you (the reader!) should be very careful when interpreting the results presented in this repository:

1. These results are only valid for the code that is checked into the repository, on the versions/builds that were specifically used in this repository and in the environments that the code was ran on
2. The workloads, code and infrastructure used for the Daft benchmarks are likely much more optimized than that of other frameworks
2. Any small differences in your use-case will cause performance to be different from what has been observed in this benchmark. This can be as obvious as the size of your dataset/number of files/type of machines, to something as subtle as the size of the row-groups in your Parquet files or newline delimiter in your CSVs. Always do your own homework, on your own workloads, if deciding between frameworks!

That being said, developers of Daft need to understand how Daft performs relative to similar frameworks, on workloads that we think are representative of a typical Daft workload. Thus, we do need to put benchmarks out there, but at the same time acknowledge that these benchmarks are biased.

Where possible, we will attempt to detail in as much detail as possible how we ran these workloads. We would love to collaborate with users who are more familiar with these systems to correctly run them in a way that is more representative of how these systems are usually run. If you see something misrepresented/missing/wrong, please open an Issue/PR.


## Benchmarking Setup

### Infrastructure

Our benchmarks were run in an AWS cloud account, on an EKS Kubernetes cluster. PySpark benchmarks were run from the AWS EMR service instead of on the Kubernetes cluster.

1. Instance type: each machine is an `i3.2xlarge` machine type (8 cores, 60G of memory), which comes with an NVME SSD drive is used as scratch space. To simplify the setup, all "head/driver/scheduler nodes" (nodes that do not perform work but instead are only responsible for coordination) are also just launched on the same instance type.
2. Networking: we launch clusters in the same AWS Region (us-west-2) and Availability Zone
3. Data: The data is kept an AWS S3 bucket as Parquet files. Files are split into `N` partitions, depending on the benchmark being tested. Reading these files from AWS S3 is part of the benchmarking as well as it is representative of real-world applications.
4. Kubernetes: For ease of deployment and easier reproducibility, we use Kubernetes (specifically, EKS on Kubernetes v1.22). We include kubernetes configs which were applied to start the necessary clusters in the `cluster_setup/` folder. This is how we run the Ray, Dask and Spark clusters used in this benchmark.
5. AWS EMR: Hosting a Spark cluster on kubernetes was surprisingly challenging. Instead, we opted to use a managed Spark service on AWS EMR for benchmarking PySpark.

### Code

The code for benchmarking Dask, Modin and Spark are heavily adapted from the [Polars TPC-H benchmarks](https://github.com/pola-rs/tpch) and the [XOrbits TPC-H benchmarks](https://github.com/xprobe-inc/benchmarks).

We acknowledge that this code may not be the most optimal or idiomatic code for non-Daft use-cases, and encourage readers to submit their contributions to make them better!

## Running the Benchmarks

The scripts in this library were run using Python 3.10.9

1. Create a virtual environment: `python -m venv venv`
2. Activate your virtual environment: `source venv/bin/activate`
3. Install the dependencies required by this repository: `pip install -r requirements.txt`

You can run the benchmarking script like so:

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

## Generating Data

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

Each subfolder contains `N` number of parquet files, where `N` is the number of partitions in the generated dataset.
