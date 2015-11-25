#!/bin/bash

CLUSTER_NAME="awscn-up-emr-pro_tm_pairs"
LOG_URI="s3://logarchive.ym/emr-logs/"
AMI_VERSION="3.8.0"
HIVE_VERSION="0.13.1"
KEY_NAME="cn-ec2-test-key"
SUBNET_ID="subnet-21edf043"
MASTER_NAME="up-MASTER"
MASTER_NUM=1
MASTER_TYPE="m1.large"
CORE_NAME="up-CORE"
CORE_NUM=20
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
aws emr put --cluster-id $cluster_id --key-pair-file ./key/id_rsa --src ./spark/log_analysis_1123.py --dest /home/hadoop/log_analysis_1123.py

# start cluster
echo "start cluster"
aws emr add-steps --cluster-id $cluster_id --steps Type=CUSTOM_JAR,Name=StartCluster,Jar=s3://cn-north-1.elasticmapreduce/libs/script-runner/script-runner.jar,ActionOnFailure=TERMINATE_CLUSTER,Args=["/home/hadoop/start_cluster"]

# run spark job
ip_arr=(${master_ip//./ })
SPARK_MASTER="spark://ip-${ip_arr[0]}-${ip_arr[1]}-${ip_arr[2]}-${ip_arr[3]}:7077"
DATE=${LOG_DATE}
echo "Now Processing ${DATE}"

days_ago=1
#while((days_ago>=2))
#do
THIS_DAY=`date -d"$days_ago day ago" +"%Y-%m-%d"`

CID_PATH=s3://logarchive.ym/cid_property/1510/youmi_device_info_cid_0_*.txt.gz
LOG_PATH=s3://logarchive.ym/hive/gztables/default_log/dt=${THIS_DAY}/sv=3/ac=304/part-2*_123.gz
# OUTPUT_PATH=s3://datamining.ym/log_analysis/log_analysis/${THIS_DAY}
PROVINCE_PATH=s3://datamining.ym/log_analysis/temp/province/${THIS_DAY}
TIME_PATH=s3://datamining.ym/log_analysis/temp/time/${THIS_DAY}
DEVICE_PATH=s3://datamining.ym/log_analysis/temp/device/${THIS_DAY}
HEI_WID_PATH=s3://datamining.ym/log_analysis/temp/hei_wid/${THIS_DAY}
APP_PATH=s3://datamining.ym/log_analysis/temp/app/${THIS_DAY}

step_result=`aws emr add-steps --cluster-id $cluster_id --steps Type=CUSTOM_JAR,Name=GetLast5Package,Jar=s3://cn-north-1.elasticmapreduce/libs/script-runner/script-runner.jar,ActionOnFailure=TERMINATE_CLUSTER,Args=[/home/hadoop/spark/bin/spark-submit,--name,"get log_analysis $DATE",--master,$SPARK_MASTER,--executor-memory,3G,--driver-memory,3G,--total-executor-cores,40,/home/hadoop/log_analysis_1123.py,$CID_PATH,$LOG_PATH,$PROVINCE_PATH,$TIME_PATH,$DEVICE_PATH,$HEI_WID_PATH,$APP_PATH]`
step_id=`echo $step_result | grep -Po 's-[^"]*'`
echo "step_id:" $step_id
#days_ago=`expr $days_ago - 1`
#done

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
    if [ $timeout -gt 4800 ]; then
        echo "job running timeout"
        aws emr terminate-clusters --cluster-ids $cluster_id
        exit
    fi
    echo "job running:" $timeout "minute"
    timeout=$(($timeout+2))
    sleep 120
done
