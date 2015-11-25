#!/bin/bash

CLUSTER_NAME="awscn-up-emr-regionCount"
LOG_URI="s3://logarchive.ym/emr-logs/"
AMI_VERSION="3.8.0"
HIVE_VERSION="0.13.1"
KEY_NAME="cn-ec2-test-key"
SUBNET_ID="subnet-21edf043"
MASTER_NAME="up-MASTER"
MASTER_NUM=1
MASTER_TYPE="m1.large"
CORE_NAME="up-CORE"
CORE_NUM=4
CORE_TYPE="m1.large"
TASK_NAME="up-TASK"
TASK_NUM=0
TASK_TYPE="m1.large"
LOG_DATE=`date -d "1 day ago" +%Y-%m-%d`
echo "start creating cluster"
# create cluster

create_result=`aws emr create-cluster \
    --name $CLUSTER_NAME \
    --no-termination-protected \
    --log-uri $LOG_URI \
    --ami-version $AMI_VERSION \
    --applications Name=Hive,Args=--version,$HIVE_VERSION \
    --ec2-attributes KeyName=$KEY_NAME,SubnetId=$SUBNET_ID \
    --instance-groups InstanceGroupType=MASTER,InstanceCount=$MASTER_NUM,InstanceType=$MASTER_TYPE,Name=$MASTER_NAME InstanceGroupType=CORE,InstanceCount=$CORE_NUM,InstanceType=$CORE_TYPE,Name=$CORE_NAME \
    --visible-to-all-users \
    --use-default-roles \
    --bootstrap-actions Name=InstallSpark,Path=s3://support.elasticmapreduce/spark/install-spark \
    --steps Type=CUSTOM_JAR,Name=SparkHistoryServer,Jar=s3://cn-north-1.elasticmapreduce/libs/script-runner/script-runner.jar,ActionOnFailure=CONTINUE,Args=s3://support.elasticmapreduce/spark/start-history-server`


# get cluster id
cluster_id=`echo $create_result | grep -Po '(?<="ClusterId": ")[^"]*'`
if [ -z $cluster_id ]; then
    echo "error:cluster_id empty"
    exit
fi
echo "cluster_id:" $cluster_id

# check create cluster done
timeout=1
while :
do
    total_instance="$(($MASTER_NUM+$CORE_NUM+$TASK_NUM))"
    current_instance=`aws emr list-instances --cluster-id $cluster_id | grep -o RUNNING | wc -l`
    echo "total_instance:" $total_instance "current_instance:" $current_instance
    if [ $total_instance -eq $current_instance ]; then
        break
    fi
    if [ $timeout -gt 30 ]; then
        echo "create cluster timeout"
        exit
    fi
    timeout=$(($timeout+1))
    sleep 60
done

# get master and slave ip
echo "get cluster ip"
master_ip=`aws emr list-instances --cluster-id $cluster_id --instance-group-types MASTER | grep -Poi '(?<="privateipaddress": ")[^"]*'`
slave_ip=`aws emr list-instances --cluster-id $cluster_id --instance-group-types CORE TASK | grep -Poi '(?<="privateipaddress": ")[^"]*'`
echo "master_ip:" $master_ip
echo "slave_ip:" $slave_ip
if [ -z "$master_ip" ] || [ -z "$slave_ip" ]; then
    echo "error: master or slave ip empty"
    exit
fi
slave_ip=($slave_ip)

# generate slaves file
echo  "generate slaves file"
rm -f ./spark/slaves
for ip in ${slave_ip[@]}; do
    echo $ip >> ./spark/slaves
done

chmod +x ./spark/start_cluster

# put files to emr
echo "put files to emr"
aws emr put --cluster-id $cluster_id --key-pair-file ./key/id_rsa --src ./key/id_rsa --dest /home/hadoop/.ssh/id_rsa
aws emr put --cluster-id $cluster_id --key-pair-file ./key/id_rsa --src ./key/id_rsa.pub --dest /home/hadoop/.ssh/id_rsa.pub
aws emr put --cluster-id $cluster_id --key-pair-file ./key/id_rsa --src ./spark/slaves --dest /home/hadoop/spark/conf/slaves
aws emr put --cluster-id $cluster_id --key-pair-file ./key/id_rsa --src ./spark/start_cluster --dest /home/hadoop/start_cluster
aws emr put --cluster-id $cluster_id --key-pair-file ./key/id_rsa --src ./spark/region_count.py --dest /home/hadoop/region_count.py

# start cluster
echo "start cluster"
aws emr add-steps --cluster-id $cluster_id --steps Type=CUSTOM_JAR,Name=StartCluster,Jar=s3://cn-north-1.elasticmapreduce/libs/script-runner/script-runner.jar,ActionOnFailure=TERMINATE_CLUSTER,Args=["/home/hadoop/start_cluster"]

# run spark job
ip_arr=(${master_ip//./ })
SPARK_MASTER="spark://ip-${ip_arr[0]}-${ip_arr[1]}-${ip_arr[2]}-${ip_arr[3]}:7077"
DATE=${LOG_DATE}
echo "Now Processing ${DATE}"
INPUT_PATH=s3://logarchive.ym/hive/gztables/default_log/dt=${DATE}/sv=3/ac=304/*
OUTPUT_PATH=s3://datamining.ym/regionCount/${DATE}

step_result=`aws emr add-steps --cluster-id $cluster_id --steps Type=CUSTOM_JAR,Name=GetLast5Package,Jar=s3://cn-north-1.elasticmapreduce/libs/script-runner/script-runner.jar,ActionOnFailure=TERMINATE_CLUSTER,Args=[/home/hadoop/spark/bin/spark-submit,--name,"Count region $DATE",--master,$SPARK_MASTER,--executor-memory,3G,--total-executor-cores,10,/home/hadoop/region_count.py,$INPUT_PATH,$OUTPUT_PATH]`
step_id=`echo $step_result | grep -Po 's-[^"]*'`
echo "step_id:" $step_id

# check job done
timeout=1
while :
do
    step_status=`aws emr list-steps --cluster-id $cluster_id --step-ids $step_id | grep -Po '(?<="State": ")[^"]*'`
    if [ $step_status = "COMPLETED" ] || [ $step_status = "FAILED" ];then
        echo "job status $step_status, terminate clusters"
        aws emr terminate-clusters --cluster-ids $cluster_id
        exit
    fi
    if [ $timeout -gt 480 ]; then
        echo "job running timeout"
        aws emr terminate-clusters --cluster-ids $cluster_id
        exit
    fi
    echo "job running:" $timeout "minute"
    timeout=$(($timeout+2))
    sleep 120
done
