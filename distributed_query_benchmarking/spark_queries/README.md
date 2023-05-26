# Spark Benchmarks

Unfortunately, we were unable to get Spark running in our Kubernetes cluster. Instead, our benchmarks are run in AWS EMR, which is a managed offering by AWS for running Spark.

Note that AWS EMR runs an Amazon distribution of Spark, which has been tuned by Amazon to be faster than Open-Sourced Spark. Your mileage may vary if hosting and running your own tuned/optimized Spark cluster.

## Running Benchmarks

### Starting an EMR cluster

See `emr.sh` for how we start an EMR cluster. Using the AWS CLI.

Key points to note:

1. We are using the same instance types (`i3.2xlarge`) as the rest of our benchmarks
2. We ensure that all nodes are scheduled in the same AZ for colocation
3. We run on the `emr-6.10.0` distribution, which runs Spark on version `3.3.1`
4. We run a custom bootstrap script (`bootstrap-emr-pip-install.sh`) to install Python 3.8, Pandas, and PyArrow==12.0.0 which are requirements of our Python script

## Running jobs on the EMR cluster

See `emr.sh` for how we start a step in EMR to run our script.

Logs for this step can be retrieved from the configured logging S3 bucket for the cluster at step completion.
