# Create a cluster (change the InstanceCount as necessary)
aws emr create-cluster \
    --name "TPCHBenchmarking" \
    --log-uri s3://eventual-dev-benchmarking-results/distributed-query-benchmarking/emr_logs \
    --applications Name=Spark Name=Hadoop \
    --release-label emr-6.10.0 \
    --service-role EMR_DefaultRole \
    --instance-groups '[{"InstanceCount":8,"EbsConfiguration":{"EbsOptimized":true},"InstanceGroupType":"CORE","InstanceType":"i3.2xlarge","Name":"Core - 2"},{"InstanceCount":1,"EbsConfiguration":{"EbsOptimized":true},"InstanceGroupType":"MASTER","InstanceType":"i3.2xlarge","Name":"Master - 1"}]' \
    --ec2-attributes AvailabilityZone=us-west-2c,InstanceProfile=arn:aws:iam::941892620273:instance-profile/EMR_EC2_TPCH_Benchmarking_Role \
     --bootstrap-actions Path=s3://eventual-dev-benchmarking-fixtures/scripts/bootstrap-emr-pip-installs.sh,Name=PipInstalls

# Copy spark_sql_main.py to a location in AWS S3
aws s3 cp spark_sql_main.py s3://eventual-dev-benchmarking-fixtures/scripts/spark_sql_main.py

# Add a step to the cluster (change the Cluster ID as necessary)
aws emr add-steps \
    --cluster-id $CLUSTER_ID  \
    --steps 'Type=spark,Name=TPCHJob,Args=[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=true,s3://eventual-dev-benchmarking-fixtures/scripts/spark_sql_main.py],ActionOnFailure=CONTINUE'
