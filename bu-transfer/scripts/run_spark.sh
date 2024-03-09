#!/bin/bash

log_creation(){
        job_name=$1
        LOG_PATH="/log/bu-transfer/logs"
        RUN_DT=$(date + '%Y%m%d')
        LOG_DIR-$LOG_PATH/$RUN_DT
        app_list-$(yarn application -list -appStates FINISHED,KILLED,FAILED | grep $job_name | cut -f1)
        start_time=()
        echo "$app_list"
        for i in $(yarn application -list -appStates FINISHED,KILLED,FAILED | grep $job_name | cut -f1)
        do
              yarn application -status $i > /tmp/"$i.log"
              start_time+=($(cat /tmp/"$i.log" | grep "Start-Time" | awk -F":" '{print $2}' | tr -d '[:blank:]'))
        done
        IFS=$'\n' sorted_start_time=($(sort <<<"${start_time[*]}"))
        unset IFS
        echo "${sorted_start_time[@]}"
        echo "LATEST ENTRY: ${sorted_start_time[${#sorted_start_time[@]} - 1]}"
        latest_time=${sorted_start_time[${#sorted_start_time[@]} - 1]}
        for i in $(yarn application -list -appStates FINISHED,KILLED,FAILED | grep $job_name | cut -f1)
        do
                cnt=$(cat /tmp/"$i.log" | grep "$latest_time" | wc -1)
            if [[ $cnt == 1 ]]
            then
                yarn logs -applicationId $i >> $LOG_DIR/${job_name}.log
                echo "files are written from yarn to server"
                cat $LOG_DIR/${job_name}.log | grep "~#~" | sed 's/~#~/~/g' >> /log/bu-transfer/logs/CC_$(date +'%Y%m%d').csv


                break
            fi
        done
}

process_name=$1
home_path=$2
log_id=$3
LOG_PATH="/log/bu-transfer/logs"
CONFIG_PATH="/conf/bu-transfer/config/setvar.conf"
. $CONFIG_PATH
RUN_DT=$(date + '%Y%m%d')
mkdir -p $LOG_PATH/$RUN_DT
LOG_DIR=$LOG_PATH/SRUN_DT

date=$(date '+%Y-%m-%d %H:%M:%S')
echo "Process Name is :: " $process_name

spark3-submit --master yarn --deploy-mode cluster --name $process_name --queue report --jars $jarFile --conf spark.port.maxRetries-100 \
--conf spark.driver.maxResultSize=10g --conf spark.sql.legacy.timeParserPolicy=LEGACY --conf spark.sql.shuffle.partitions=2001 \
--executor-memory 30g --driver-memory 40g --conf spark.cores.max=9 --conf spark.executor.cores=3 \
--conf spark.sql.debug.maxToStringFields=20000 --conf spark.debug.maxToStringFields=20000 --conf spark.network.timeout=10000000 \
--conf spark.executor.heartbeatInterval=10000000 --conf spark.storage.blockManagerSlaveTimeoutMs=10000000 \
--conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=10g --conf spark.sql.broadcastTimeout=36000 \
--conf spark.shuffle.io.maxRetries=10 --conf spark.shuffle.compress=true --conf spark.shuffle.service.removeShuffle=true \
--files $sparkConfig $sparkRun $process_name $destName bu_transfer_config.ini $log_id

if [ $? == 0 ]
then
        log_creation $process_name
fi