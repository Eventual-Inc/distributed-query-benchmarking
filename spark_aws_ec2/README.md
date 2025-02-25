# Overview

This folder contains utilities to run Spark clusters on AWS EC2.

We use the [`flintrock` utility](https://github.com/nchammas/flintrock) to do so.

## Setup

### AWS Setup

Create a keypair in the EC2 console, download the `.pem` file and save it to `~/.ssh/my-keypair.pem`.

Then, fix its permissions with:

```bash
chmod 600 ~/.ssh/my-keypair.pem
```

### Local Machine

> NOTE: This requires `uv` to be installed.

Install `flintrock` as a uv tool.

```bash
uv tool install flintrock
```

Now configure the tool.

```bash
uvx flintrock configure
```

Ensure that inside your configuration:

1. You configure the appropriate AWS Region to spin your cluster up in
2. You select the appropriate machine types
3. You select the appropriate AMI
4. You select the correct **name** of the keypair (in AWS), and also the corresponding path to the `.pem` file that was saved to your local machine
5. Ensure that Spark is installed

An example configuration is provided here in `example-flintrock-config.yaml`.

## Start a cluster

Let's decide on a cluster name:

```bash
SPARK_CLUSTER=my-spark-cluster
```

Now let's launch the cluster

```bash
uvx flintrock launch $SPARK_CLUSTER
```

When this is done, you can describe the cluster with:

```bash
uvx flintrock describe
```

The output looks as follows:

```
$SPARK_CLUSTER:
  state: running
  node-count: 2
  master: ec2-XX-XX-XX-XX.us-west-2.compute.amazonaws.com
  slaves:
    - ec2-XX-XX-XX-XX.us-west-2.compute.amazonaws.com
```

Some additional setup steps can now be ran:

1. Install Python deps

```bash
uvx flintrock run-command $SPARK_CLUSTER "python3 -m pip install pandas"
```

## Launch a job

Spark launches jobs using the `spark-submit` binary, which is packaged in releases of Spark. However, one significant limitation of the `spark-submit`
mechanism is that it is unable to submit a **Python job to run in the cluster**. Python jobs will always run the Python script on the client (i.e. your laptop),
which leads to lots of problems including Python client-cluster environment mismatches and high latency of communication between the client and cluster.

Thus the preferred way for launching work on this newly created cluster of yours is to launch it from inside the cluster itself, by SSH'ing into the master node.

1. Copy the necessary Python file(s) into the cluster

```bash
uvx flintrock copy-file \
    --master-only \
    $SPARK_CLUSTER \
    ./tpcds_main.py \
    /home/ec2-user/tpcds_main.py
uvx flintrock copy-file \
    --master-only \
    $SPARK_CLUSTER \
    ./spark_submit_harness.py \
    /home/ec2-user/spark_submit_harness.py
```

2. Login to your master node and launch your work: we recommend launching using `tmux` so that your jobs will continue running even after you disconnect from the cluster.

```bash
uvx flintrock login $SPARK_CLUSTER
```

Now that you are in the cluster, launch a new tmux session:

```bash
sudo yum install tmux vim
tmux
```

You can then run all of the TPC-DS questions like so:

```bash
python3 spark_submit_harness.py
```

> ## Spark-Submit
> Under the hood, this will submit work to the Spark cluster using `spark-submit`. Here are some notes on what it does.
> 
> Now you can run your work, and use the cluster's `spark-submit` binary. Note all the magical configs in here that enable it to talk to S3, and that
> you should also know to use the magical `s3a://` protocol when referring to S3 paths in Spark. Don't ask any questions...

> * `--packages org.apache.hadoop:hadoop-aws:3.3.4`: ensure that this is the same version of Hadoop that the current release of Spark was built with (see: [Spark's pom.xml](https://github.com/apache/spark/blob/v3.5.4/pom.xml))
> * `--conf "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider"`: tells Spark to use the instance IAM profiles
> * `--executor-memory 122g`: tells Spark to use more memory per executor (otherwise it seems to default to 1G and will OOM)

> ```bash
> spark-submit \
>     --master spark://$(eval hostname):7077 \
>     --packages org.apache.hadoop:hadoop-aws:3.3.4 \
>     --conf "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider" \
>     --executor-memory 122g \
>     sample-job.py
> ```

## Spark history server (optional)

You may want the Spark UI to view the workflow and get more detailed information.

1. Logout of the cluster if needed.

2. Create a path on every node in the cluster for saving event logs:

```bash
uvx flintrock run-command $SPARK_CLUSTER "mkdir -p /home/ec2-user/tmp/spark-logs"
```

3. Log back into the cluster:

```bash
uvx flintrock login $SPARK_CLUSTER
```

4. Modify the default config for the spark cluster. Essentially turn on event logging, set the directory where event logs will be saved, then point the Spark history server to read from this same directory:

```bash
cp /home/ec2-user/spark/conf/spark-defaults.conf.template /home/ec2-user/spark/conf/spark-defaults.conf;
echo "spark.eventLog.enabled true" >> /home/ec2-user/spark/conf/spark-defaults.conf;
echo "spark.eventLog.dir file:///home/ec2-user/tmp/spark-logs" >> /home/ec2-user/spark/conf/spark-defaults.conf;
echo "spark.history.fs.logDirectory file:///home/ec2-user/tmp/spark-logs" >> /home/ec2-user/spark/conf/spark-defaults.conf;
```

5. Start the Spark history server:

```bash
./home/ec2-user/spark/sbin/start-history-server.sh
```

6. Run your query

7. Go to `http://<public-ip-or-dns-of-spark-cluster-master>:18080` to view the Spark UI. You may need to modify the inbound rules for the EC2 instance that your Spark driver lives on to accept inbound connections to port 18080.

## Tearing Down

Teardown the cluster using:

```bash
uvx flintrock destroy $SPARK_CLUSTER
```
