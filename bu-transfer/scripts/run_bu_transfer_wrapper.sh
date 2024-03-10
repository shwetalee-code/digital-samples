#!/bin/bash
HOME_PATH=/scripts/bu-transfer/
SCRIPT_PATH=/scripts/framework/files/bu-transfer
LOG_PATH=$HOME_PATH/logs
CONFIG_PATH="$HOME_PATH/config/setvar.conf"
. $CONFIG_PATH
RUN_DT=$(date + '%Y%m%d')
#RUN_DT="20240226"
DAY=$(date -d "$RUN_DT" '+%d')
MONTH=$(date -d "$RUN_DT" '+%m')
YEAR=$(date -d "$RUN_DT" '+%Y')

################### Importing Logging Module ###################
loggingFile=$genericLogFile
source $logFile
sourceName="BB"
reportCategory="bu_transfer"
reportType="datatransfer"
reportName="bu_data_transfer"

logFunc "${sourceName}" "${reportCategory)" "${reportType}" "${reportName}" 'BB_BU_RANSFER_START' 'SUCCESS' "${sourceName}.${reportName}" "Report ${sourceName}.${reportName) Started sucessfully"
echo "${logid}~~BB_BU_TRANSFER~unit~~AIRFLOW DAG~SUCCESS~~AIRFLOW DAG STARTED~$(date + '%Y-%m-%d %H:%M:%S')~$(date + '%Y-%m-%d')" > /scripts/bu-transfer/logs/BB_BU_TRANSFER_$(date +'%Y%m%d').csv

echo -e "Dear User, " > /tmp/temp-bb-bu-transfer.status
echo -e "The BU data transfer processes have been started successfully for $RUN_DT." >> /tmp/temp-bb-bu-transfer.status
echo -e "" >> /tmp/temp-bb-bu-transfer.status
echo -e "Regards," >> /tmp/temp-bb-bu-transfer.status
echo -e "Team" >> /tmp/temp-bb-bu-transfer.status
mail -s "Started : Data Transfer Process Started - $RUN_DT" $emailList < /tmp/temp-bb-bu-transfer.status

mkdir -p $LOG_PATH/$RUN_DT
LOG_DIR=$LOG_PATH/$RUN_DT
inputList=$1
cp $1 $HOME_PATH/scripts/nonE_list
echo $logid

while true
do
    grep -i 'Data Transfer Successful :' $LOG_DIR/* | cut -d ':' -f1 | cut -d '/' -f 7 | cut -d '.' -f1 | sort > $LOG_PATH/completed_tables.txt
    echo "Completed table list"
    cat $LOG_PATH/completed_tables.txt

    sort $HOME_PATH/scripts/nonE_list > $1

    diff $1 $LOG_PATH/completed_tables.txt | grep "<" | sed 's/^< //g' > $LOG_PATH/pending_tables.txt
    echo "Pending table list"
    cat $LOG_PATH/pending_tables.txt

    transferPending=()
    ingestionCompleted=()
    ingestionPending=()

    for i in `cat $LOG_PATH/pending_tables.txt`
    do
        transferPending+=($i)
    done

    transferPendingCount="${#transferPending[@]}"
    if [ $transferPendingCount -ne 0 ]
    then
        for i in `cat $LOG_PATH/pending_tables.txt`
        do
            src=$(echo $i | cut -d '~' -f1)
            db=$(echo $i | cut -d '~' -f2)
            table=$(echo $i | cut -d '~' -f3)
            indi=$(echo $i | cut -d '~' -f4)
            if [[ $indi == "t" ]]
            then
                hdfs dfs -test -e $jobStatusDir/year=$YEAR/month=$MONTH/day=$DAY/success-$src-$db-$table*
                if [[ $? != 0 ]]
                then
                    echo "Success File Not Available"
                    ingestionPending+=($i)
                    logFunc "${sourceName}" "${reportCategory)" "${reportType}" "${reportName)" 'Ingestion Check' 'PENDING' "$(sourceName).${reportName}" "Ingestion Pending for $src $db $table."
                else
                    echo "Success File Available"
                    ingestionCompleted+=($i)
                fi
            else
            ingestionCompleted+=($i)
            fi
        done

        ingestionPendingCount="${#ingestionPending[@]}"
        ingestionCompletedCount="${#ingestionCompleted[@]}"
        echo "Ingestion Pending Count is : " $ingestionPendingCount
        echo "Ingestion Completed Count is : " $ingestionCompletedCount

        if [ $ingestionPendingCount -ne 0 ]
        then
            echo -e "Dear User, " > /tmp/temp-bb-bu-transfer.status
            echo -e "Ingestion for below processes still not completed. Please check." >> /tmp/temp-bb-bu-transfer.status
            for i in "${!ingestionPending[@]}";
              do
                  echo -e "$((i+1)). ${ingestionPending[$i]}" >> /tmp/temp-bb-bu-transfer.status
              done
            echo -e "" >> /tmp/temp-bb-bu-transfer.status
            echo -e "Regards," >> /tmp/temp-bb-bu-transfer.status
            echo -e "Team" >> /tmp/temp-bb-bu-transfer.status

            logFunc "${sourceName}" "${reportCategory}" "${reportType}" "${reportName}" 'Data Transfer' 'PENDING' "${sourceName}.${reportName}" "Transfer Pending as Ingestion Not Completed for $ingestionPendingCount tables"
            mail -s "Info: Daily Ingestion of Tables Not Completed for BU Transfer - $RUN_DT" $emaillist < /tmp/temp-bb-bu-transfer.status
        fi

        if [ $ingestionCompletedCount -ne 0 ]
        then
            #echo "Run Spark Job"
            python3 /scripts/framework/files/bu-transfer/deal_multi_calls.py "$(IFS=,; echo "${ingestionCompleted [*]}")" $sparkCode $SCRIPT_PATH $parallerSparkSessions $logId
        else
            echo "Ingestion is pending... Waiting..."
            sleep
        fi

        if [ `date +"%H%M%S" | bc` -ge $waitUntil ]
        then
            grep -i 'Data Transfer Successful :' $LOG_DIR/* | cut -d ':' -f1 | cut -d '/' -f 7 | cut -d '.' -f1 | sort > $LOG_PATH/completed_tables.txt
            echo "Completed table list"
            cat $LOG_PATH/completed_tables.txt

            sort $HOME_PATH/scripts/nonE_list > $1
            diff $1 $LOG_PATH/completed_tables.txt | grep "<" | sed 's/^< //g' > $LOG_PATH/pending_tables.txt
            echo "Pending table list"
            cat $LOG_PATH/pending_tables.txt

            cat $LOG_DIR/*.log | grep '~' > $LOG_DIR/$(date + '%Y%m%d')_bu_transfer.csv
            hdfs dfs -put -f $LOG_DIR/$(date + '%Y%m%d')_bu_transfer.csv /scripts/report/logs/
            transferPending=()

            for i in `cat $LOG_PATH/pending_tables.txt`
            do
                transferPending+=($i)
            done

            transferPendingCount="${#transferPending[@]}"
            echo "Transfer pending table list"
            echo $transferPending

            echo -e "Dear User," > /tmp/temp-bb-biu-transfer.status
            echo -e "BU Data Transfer process aborted for $RUN_DT. Data Transfer for below processes not completed. Please check the logs at $LOG_DIR and rerun the process." >> /tmp/temp-bb-bu-transfer.status

            for i in "${!transferPending[@]}";
            do
                echo -e "$((i+1)). ${transferPending[$i]}" >> /tmp/temp-bb-bu-transfer.status
            done
            echo -e "" >> /tmp/temp-bb-bu-transfer.status
            echo -e "Regards," >> /tmp/temp-bb-bu-transfer.status
            echo -e "Team" >> /tmp/temp-bb-bu-transfer.status

            logFunc "${sourceName}" "${reportCategory}" "${reportType}" "${reportName}" 'Data Transfer' 'FAILED' "${sourceName}.${reportName}" "Transfer failed as waiting time ($waitUntil) exceeded. Please check the logs for details."
            mail -s "Failure: BU Data Transfer Process Failed $RUN_DT" $emailList < /tmp/temp-bb-bu-transfer.status
            echo `date`
            exit 1
        else
            echo "Still Running"
        fi
    else
        echo -e "Dear User, " > /tmp/temp-bb-bu-transfer.status
        echo -e "All the BU Data Transfer processes have been completed successfully for $RUN_DT." >> /tmp/temp-bb-bu-transfer.status
        echo -e "" >> /tmp/temp-bb-bu-transfer.status
        echo -e "Regards, " >> /tmp/temp-bb-bu-transfer.status
        echo -e "Team" >> /tmp/temp-bb-bu-transfer.status

        logFunc "${sourceName)" "${reportCategory}" "${reportType)" "${reportName}" 'Data Transfer' 'SUCCESS' "${sourceName}.${reportName}" "Transfer Completed for all the Processes."
        mail -s "Success: BU Data Transfer Process Completed - $RUN_DT" $emaillist < /tmp/temp-bb-bu-transfer.status

        logFunc "${sourceName}" "${reportCategory)" "${reportType}" "${reportName}" 'BB_BU_TRANSFER_COMPLETED' 'SUCCESS' "${sourceName).${reportName}" "Report ${sourceName).${reportName} Completed sucessfully"
        break
    fi
done
echo `date`

if [ $? == 0 ]
then
    echo "${logid}~~BB_BU_TRANSFER~unit~~AIRFLOW DAG~SUCCESS~~AIRFLOW DAG Completed~$(date +'%Y-%m-%d %H:%M:%S')~$(date + '%Y-%m-%d')" >> /scripts/bu-transfer/logs/BB_BU_TRANSFER_$(date + '%Y%m%d').csv
    hdfs dfs -put -f /scripts/bu-transfer/logs/BB_BU_TRANSFER_$(date + '%Y%m%d').csv /hdfs/log/report/logs/
    exit 0
else
    echo "${logid)~~BB_BU_TRANSFER~unit~~AIRFLOW DAG-SUCCESS~~AIRFLOW DAG Failed~$(date + '%Y-%m-%d %H:%M:%S')~$(date +'%Y-%m-%d')" >> /scripts/bu-transfer/logs/BB_BU_TRANSFER_$(date + '%Y%m%d').csv
    hdfs dfs -put -f /scripts/bu-transfer/logs/BB_BU_TRANSFER_$(date + '%Y%m%d').csv /hdfs/log/report/logs/
    exit 1
fi