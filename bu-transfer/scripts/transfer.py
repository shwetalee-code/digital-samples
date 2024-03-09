import os
import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql import *
from pyspark.sql.functions import *
import time
import configparser
import argparse
import socket

#def audit(process_name, database_name, table_name, message, status, start_time, end_time, spark, auditPath):
#   print("Inside Audit")
#   message_string = process_name + "|spark_framework|" + database_name + "|" + table_name + "|" + message + "|" + status + "|" + start_time + "|" + end_time
#   rdd1 = spark.sparkContext.parallelize([message_string])
#   row_rdd = rdd1.map(lambda x: Row(x))
#   df = spark.createDataFrame(row_rdd, ['string'])
#   print("audit writing start")
#   df.coalesce(1).write.format("csv").mode("append").save(auditPath)
#   print("audit end")

def main(argv):
    try:
        print("Inside Main")
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        timestr = time.strftime("%Y%m%d")

        nw_in = datetime.now()
        out_date_in=nw_in.strftime("%Y%m%d")

        ################## Reading Arguments ###################
        process_name = argv[1]
        dest_host = argv[2]
        spark_conf = argv[3]
        logId = argv[4]
        ################ Reading config file ############
        configObject = configparser.ConfigParser()
        configObject.read(spark_conf)

        # jarName = configObject.get(dest_host, "jarName")
        jdbcURL = configObject.get(dest_host, "jdbcURL")
        jdbcDriver = configObject.get(dest_host, "jdbcDriver")
        password = configObject.get(dest_host, "password")
        user = configObject.get(dest_host, "user")
        batchsize = configObject.get(dest_host, "batchsize")

        log_level = configObject.get("spark", "log_level")

        staging = configObject.get("hdfs", "staging")
        refined = configObject.get("hdfs", "refined")
        table_list_path = configObject.get("hdfs", "ssisConfig")
        # auditPath = configObject.get("hdfs", "auditPath")

        ################ Importing Logging Framework ###########
        dPath = configObject.get("logging", "dPath")
        logDir = configObject.get("logging", "logDir")
        sourceName = configObject.get("logging", "sourceName")
        reportCategory = configObject.get("logging", "reportCategory")
        reportType = configObject.get("logging", "reportType")
        reportName = configObject.get("logging", "reportName")

        ################ Initialising Spark Context #################
        dbName = process_name.split("~")[1]
        tableName = process_name.split("~")[2]
        sessionFor = str(dbName)+"_"+str(tableName)
        print("jdbcURL : " +jdbcURL)
        try:
            print("SPARK CONNECT SUCCESS")
            spark = SparkSession.builder.getOrCreate()
            sc = spark.sparkContext
            print("{0}~#~{1}~#~~#~~#~{2)~#~SPARK_CONNECT~#~SUCCESS~#~~#~Spark Session Created~#~(3)~#~{4}".format(logId,sourceName,tableName,datetime.now().strftime("%Y-%m-%d %H:%M:%S"),datetime.now().strftime("%Y-%m-%d")))
            # auditAndLogging(logDir, logId, sourceName, reportName, "SPARK_CONNECT", "SUCCESS", sessionFor, "Spark Session Created")
        except Exception as e:
            df_err = spark.sql("""select "{0}" as TableName, current_timestamp as start_time, "{1}" as error""".format(tableName, str(e)))
            df_err.show()
            df_err.write.mode("append").parquet("/err/report/bu_transfer/error_logs/"+out_date_in)
            pass

        ############### Transformations ################
        readwriteSourceData(table_list_path, process_name, spark, jdbcURL, jdbcDriver, password, user, batchsize, logDir, logId, sourceName, reportName, sessionFor)
    except Exception as e:
        nw = datetime.now()
        end_time = nw.strftime("%Y-%m-%d %H:%M:%S")
        try:
            print("SPARK CONNECT FAILED")
            print("{0}~#~{1}~#~~#~~#~{2}~#~SPARK_CONNECT~#~FAILED~#~~#~{3}~#~{4}~#~{5}".format(logId,sourceName,tableName,str(e),datetime.now().strftime("%Y-%m-%d %H:%M:%S"),datetime.now().strftime("%Y-%m-%d")))
            # auditAndLogging(logDir, logId, sourceName, reportName, "SPARK_CONNECT", "FAILED", "", str(e))
        except Exception as e:
            df_err = spark.sql("""select "{0}" as TableName, current_timestamp as start_time, "{1}" as error""".format(tableName, str(e)))
            df_err.show()
            df_err.write.mode("append").parquet("/err/report/bu_transfer/error_logs/"+out_date_in)
            pass
        # audit(process_name, '', '', str(e), 'ERROR', start_time, end_time, spark, auditPath)
        print(str(e))

def readwriteSourceData(table_list_path, process_name, spark, jdbcURL, jdbcDriver, password, user, batchsize, logDir, logId, sourceName, reportName, sessionFor):
    ni = datetime.now()
    start_time = ni.strftime("%Y-%m-%d %H:%M:%S")
    year = time.strftime("%Y")
    month = time.strftime("%m")
    day = time.strftime("%d")

    storageLocation = ''
    dataframeName = ''
    tableName = process_name.split("~")[2]

    prev_dt = datetime.today() - timedelta(days=1)
    prev_dt_year = prev_dt.strftime("%Y")
    prev_dt_month = prev_dt.strftime("%m")
    prev_dt_day = prev_dt.strftime("%d")

    try:
        tableList = spark.read.option("delimiter","|").option("header","true").csv(table_list_path).filter(lower(col("PROCESS_NAME")).isin(process_name.lower())).rdd.collect()
        print(tableList)
        for i in tableList:
            dataframeName = i.DATAFRAME_NAME
            source = i.SOURCE_TYPE
            dest_table_name = i.DEST_TABLE_NAME
            dest_write_mode = i.DEST_WRITE_MODE
            dest_audit_table = i.DEST_AUDIT_TABLENAME

            if ( i.M_INDICATOR.lower() == 'm' or i.M_INDICATOR is None ):
                # To get the actual storage location
                storageLocation = i.STORAGE_LOCATION
                # To get temp location
                #storageLocation - i.TEMP_STORAGE_LOCATION
            else:
                if process_name.upper() == "ON~CC~30":
                        storageLocation = i.STORAGE_LOCATION + "/year=" + prev_dt_year + "/month=" + prev_dt_month + "/day=" + prev_dt_day + "/"
                else:
                    storageLocation = i.STORAGE_LOCATION + "/year=" + year + "/month=" + month + "/day=" + day + "/"

            log_df = spark.read.format("jdbc").option("url", jdbcURL).option("dbtable", dest_audit_table).option("user", user).option("password", password).option("driver", jdbcDriver).load()
            log_df.createOrReplaceTempView("DL_Audit_n2")
            count_df=spark.sql("""select count(1) as count from DL_Audit_n2 where TableName='{0}' and status = 'Finished' and LoadDate = current_date""".format(dest_table_name))
            finished_count=count_df.collect()[0][0]

            print("Finished count for "+str(dest_table_name)+" is :" +str(finished_count))

            if finished_count <= 0:
                print("Fresh load for the table started")
                if ( source == 'csv' ):
                    df = spark.read.format(source).option("header","true").option("delimiter","|").load(storageLocation).cache()
                else:
                    df = spark.read.format(source).option("header","true").load(storageLocation)

                if i.COLUMN_LIST is None:
                    df1=df
                else:
                    df.createOrReplaceTempView("tmpDb")
                    df1=spark.sql("select {0} from tmpDb where {1} ".format(i.COLUMN_LIST,i.WHERE_CLAUSE))
                    nw = datetime.now()
                    out_date=nw.strftime("%Y%m%d")

                    print("Dest table data has been started")
                    df3 = spark.sql("select concat('"'{0}'"') as TableName,current_date as LoadDate, 'Started' as Status, current_timestamp as ProcessDateTime ".format(dest_table_name))
                    df3.show()
                    df3.write.format("jdbc").mode(dest_write_mode).option("url", jdbcURL).option("dbtable", dest_audit_table).option("user", user).option("password", password).option("driver", jdbcDriver).save()
                    df_start = spark.sql("select concat('"'{0}'"') as TableName, concat('"'{1}'"') as process_name, current_date as LoadDate, 'Started' as Status, current_timestamp as ProcessDateTime ".format(dest_table_name,process_name))
                    df_start.write.mode("append").parquet("/audit/report/bu_transfer/audit/"+out_date)
                    print("writing data")
                    try:
                        print("DATA TRANSFER STARTED FOR " + sourceName + " AND REPORT: " +reportName)
                        print("{0}~#~{1}~#~~#~~#~{2}~#~Data Transfer~#~STARTED~#~~#~Transfer Started~#~{3}~#~{4}".format(logId, sourceName, tableName, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), datetime.now().strftime("%Y-%m-%d")))
                        # auditAndLogging(logDir, logId, sourceName, reportName, "Data Transfer", "STARTED", sessionFor, "Data Transfer Started")
                    except Exception as e:
                        df_err = spark.sql("""select "{0}" as TableName, current_timestamp as start_time, "{1}" as error""".format(tableName, str(e)))
                        df_err.show()
                        df_err.write.mode("append").parquet("/err/report/bu_transfer/error_logs/"+out_date)
                        pass
                    df1.write.format("jdbc").mode(dest_write_mode).option("url", jdbcURL).option("dbtable", dest_table_name).option("uder",user).option("password",password).option("driver",jdbcDriver).option("batchsize",batchsize).save()
                    print("Dest Audit Update going on")
                    print(dest_table_name)
                    df2=spark.sql("select concat('"'{0}'"') as TableName, current_data as LoadDate, 'Finished' as Status, current_timestamp as ProcessDateTime ".format(dest_table_name))
                    df2.show()
                    df2.write.format("jdbc").mode(dest_write_mode).option("url", jdbcURL).option("dbtable", dest_audit_table).option("user",user).option("password",password).option("driver",jdbcDriver).save()
                    print("########################################")
                    print("Data Transfer Successful :" +str(dest_table_name))
                    print("########################################")

                    # Write logs to HDFS
                    df_finish = spark.sql("select concat('"'{0}'"') as TableName, concat('"'{1}'"') as process_name, current_date as LoadDate, 'Finished' as Status, current_timestamp as ProcessDateTime ".format(dest_table_name,process_name))
                    df_finish.write.mode("append").parquet("/audit/report/bu_transfer/audit/"+out_date)
                    ne=datetime.now()
                    end_time = ne.strftime("%Y-%m-%d %H:%M:%S")
                    try:
                        print("DATA TRANSFER COMPLETED FOR " + sourceName + " AND REPORT: " + reportName)
                        print("{0}~#~{1}~#~~#~~#~{2}~#~Data Transfer~#~SUCCESS~#~~#~Data Transfer Completed~#~{3}~#~{4}".format(logId,sourceName,tableName,datetime.now().strftime("%Y-%m-%d %H:%M:%S"),datetime.now().strftime("%Y-%m-%d")))
                        #auditAndLogging(logDir, logId, sourceName, reportName, "Data Transfer", "SUCCESS", sessionFor, "Data Transfer Completed")
                    except Exception as e:
                        df_err = spark.sql("""select "{0}" as TableName, current_timestamp as start_time, "{1}" as error""".format(tableName, str(e)))
                        df_err.show()
                        df_err.write.mode("append").parquet("/err/report/bu_transfer/error_logs/"+out_date)
                        pass
            else:
                print("################################")
                print("Data Transfer Successful : " + str(dest_table_name))
                print("################################")
    except Exception as e:
        nw = datetime.now()
        end_time = nw.strftime("%Y-%m-%d %H:%M:%S")
        try:
            print("DATA TRANSFER FAILED FOR " + sourceName + "AND REPORT :" +reportName)
            print("{0}~#~{1}~#~~#~~#~{2}~#~Data Transfer~#~FAILED~#~~#~{3}~#~{4}~#~{5}".format(logId,sourceName,tableName,str(e),datetime.now().strftime("%Y-%m-%d %H:%M:%S"),datetime.now().strftime("%Y-%m-%d")))
        except Exception as e:
            df_err=spark.sql("""select "{0}" as TableName, current_timestamp as start_time, "{1}" as error""".format(tableName, str(e)))
            df_err.show()
            df_err.write.mode("append").parquet("/err/report/bu_transfer/error_logs/"+out_date)
            pass
        print(str(e))

if __name__ == "__manin__":
    main(sys.argv)