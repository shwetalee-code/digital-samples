from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Row
from datetime import datatime
import sys
from pyspark.sql.functions import lit, col, upper, lower, trim, udf, from_json, row_number
import time
import argparse
import socket
import configparser
import os
from pyspark.sql.window import Window
from cat_util import *
from json_schema import *
from pyspark.sql.types import StructField, StringType

# Taking inputs
def takeArgs():
    try:
        parser = argparse.ArgumentParser()
        
        # Mandatory Arguments
        requiredArgs = parser.add_argument_group('Required Arguments')
        requiredArgs.add_argument('-f', '--fType', help='F Type : either A, B or C', required=True)
        args = parser.parse_args()
        return args.fType
        
    except Exception as e:
        timeNow = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(timeNow + "Error : Failed while taking arguments. " + str(e))
        
        
        
# Pre function
def preProcessing(spark, kafka_bootstrap_servers, kafka_topic, fType, schema, consumerGroupId):
    try:
        # Kafka Source Options
        kafka_source_options = {
            "kafka.bootstrap.servers": kafka_bootstrap_servers,
            "subscribe": kafka_topic,
            "startingOffsets": "latest",
            "kafka.group.id": consumerGroupId,
            "failOnDataLoss": "false"
        }
        print("# {0} # INFO: Kafka parameters setup.".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        
        # Read data from Kafka topic
        streamingInput = spark \
            .readStream \
            .format("kafka") \
            .options(**kafka_source_options) \
            .load().selectExpr("timestamp as msg_time", "CAST (value AS STRING) as val_str")
        print("# {0} # INFO: Readstream created.".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        
        # parse json schema
        jsonParsedDf = streamingInput.withColumn("jsonData", from_json(col("val_str"), schema)).select("msg_time", "jsonData.*")
        
        # Adding extra fields, missing in C stream
        if(fType == 'C'):
            jsonParsedDf = jsonParsedDf.withColumn('paperNum', lit('')) \
            .withColumn('due', lit('')) \
            .withColumn('jour', lit('')) \
            .withColumn('merch', lit('')) \
            .withColumn('location', lit('')) \
            .withColumn('bCode', lit(''))
            
        # Add extra fields from batch data
        preProcessedDf = jsonParsedDf.withColumn('accCatDets', lit('')) \
            .withColumn('accDisWhenCreate', lit('')) \
            .withColumn('accLim', lit('')) \
            .withColumn('accN', lit('')) \
            .withColumn('bookedAmt', lit('')) \
            .withColumn('isHidden', lit('')) \
            .withColumn('amtId', when(col('amt') < 0, 1).otherwise(0))
            
            
        return preProcessedDf
        
    except Exception as e:
        timeNow = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(timeNow + "Error : Failed while preprocessing streamed data. " + str(e))
        
        
        
# Post Function
def postCatProcessing(spark, catOutput, kafkaBootstrapServers, kafkaOutput Topic, fType):
    try:
        # Merch Columns Added
        if(fType in ('A', 'C')):
            postProcessingDf = catOutput.withColumn('merchN', lit('')) \
                .withColumnRenamed('cat_id', "catId") \
                .withColumnRenamed('cat_N', "catN") \
                .withColumnRenamed('cat_type', "catType") \
                .withColumnRenamed('Var', "isCat") \
                .withColumnRenamed('SubText_Created', "subTCreate") \
                .withColumnRenamed('sub_text', "subtext") \
                .withColumnRenamed('Final_Output', "Output") \
                .withColumnRenamed('detection', "detectedCat")
                
        # Add default columns to non-cat
        if(fType == 'B'):
            postProcessingDf = catOutput \
                .withColumn('catStruct', when(col('amount') < 0, struct(lit('956').alias('catId'), lit('OP (T)').alias('catN'), lit('Exp').alias('catType')))) \
                .otherwise(struct(lit('953').alias('catId'), lit('OI').alias('catN'), lit('Exp').alias('catType')))) \
                .withColumn('det_Cat', lit('')) \
                .withColumn('acc', lit('')) \
                .withColumn('subTCreate', lit('')) \
                .withColumn('merchN', lit('')) \
                .withColumn('subtext',lit('')) \
                .withColumn('isCat', lit(' ')) \
                .withColumn('catOut',lit('')) \
                .withColumn('hasUncertainCat', lit(''))
                
                
            postProcessingDf = postProcessingDf.select('*','catStruct.*')
            
            
        # Convert to json format
        categorizedDF = postProcessingDf.withColumn("key", col("accId")).withColumn("value", to_json(struct(postProcessedDf.columns), options={"ignoreNullFields":False}))
        
        # Formatting final data
        outputDf = categorizedDF.withColumn("value", translate('value', '\\', '')) \
        .withColumn("value", regexp_replace('value', '\"\{', '{')) \
        .withColumn("value", regexp_replace('value', '\}\"', '}'))
        
        # Push data to output kafka topic
        outputDf.select("key","value")\ 
            .write\ 
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaBootstrapServers)\
            .option("topic", kafkaOutputTopic)\
            .save()
            
        print("{0} # Data Written to Kafka Topic. ".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        print("=======================###=======================")
        
    except Exception as e:
        timeNow = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(timeNow + "Error : Failed while postprocessing function. " + str(e))
        
        
        
# Function to create micro-batches and process the same
def batchProcessing(df, epochId, spark, kafkaBootstrapServers, kafkaoutputTopic, fType):
    try:
        df.persist()
        windowSpec = Window.partitionBy(trim("tranId"), trim("amount"), trim("tranText")).orderBy(desc("msg_time"))
        filteredDf = df.withColumn("msg_number", rank().over(windowSpec)).where("trim(msg_number) = '1'")
        
        # Call Cat function for each micro-batch of A and C
        if(fType in ('A', 'C')):
            print("{0} # Info : Calling cat function for {1}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), str(fType)))
            catOutputDf = catFunction(spark, filteredDf)
            print("{0} # Info : Calling post function for {1}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), str(fType)))
            postCatProcessing(spark, catOutputDf, kafkaBootstrapServers, kafkaOutputTopic, fType)
        df.unpersist()
        
    except Exception as e:
        timeNow = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(timeNow + "Error : Failed while batch function. " + str(e))
        
        
        
        
# Main Function
def main():
    try:
        #Imposing UTF8 encoding
        sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
        
        # Check current host
        dataNode = socket.gethostname()
        print("{0} # Driver is on DataNode: {1}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), dataNode))
        
        # Function call to take user input
        fType = takeArgs()
        
        # Read config file
        configobject = configparser.ConfigParser()
        configObject.read("config.ini")
        dcDataNodeList = configobject.get("Generic", "dc_data_node_list")
        drDataNodeList = configObject.get("Generic", "dr_data_node_list")
        
        host = "DC" if (dataNode in dcDataNodeList) else "DR" if (dataNode in drDataNodeList) else 'UAT'
        kafkaBootstrapServers = configObject.get(host, "kafka_bootstrap_servers")
        A_input_kafka_topic = configObject.get(host, "A_input_kafka_topic")
        B_input_kafka_topic = configObject.get(host, "B_input_kafka_topic")
        C_input_kafka_topic = configObject.get(host, "C_input_kafka_topic")
        A_output_kafka_topic = configObject.get(host, "A_output_kafka_topic")
        B_output_kafka_topic = configObject.get(host, "B_output_kafka_topic")
        C_output_kafka_topic = configObject.get(host, "C_output_kafka_topic")
        A_consumer_group_id = configObject.get(host, "A_consumer_group_id")
        B_consumer_group_id = configObject.get(host, "B_consumer_group_id")
        C_consumer_group_id = configObject.get(host, "C_consumer_group_id")
        
        # Get JSON schema
        ASchema, CSchema, BSchema = getSchema()
        
        # Define Kafka broker, input topic, schema and output topic
        kafkaInputTopic, schema, kafkaOutputTopic, consumerGroupId = (A_input_kafka_topic, ASchema, A_output_kafka_topic, A_consumer_group_id) if fType=='A'\ 
            else (C_input_kafka_topic, CSchema, C_output_kafka_topic, C_consumer_group_id) if fType=='C'\
            else (B_input_kafka_topic, BSchema, B_output_kafka_topic, B_consumer_group_id)
            
        # Spark Session Initiation
        applicationName = "{0}_Realtime".format(fType)
        spark = SparkSession.builder.appName(applicationName).getOrCreate()
        sc = spark.sparkContext
        sc.setLogLevel("OFF")
        
        # Call Pre Function
        postProcessingDf = preProcessing(spark, kafkaBootstrapServers, kafkaInputTopic, fType, schema, consumerGroupId)
        
        # Convert realtime stream into micro-batch
        postProcessingDf.writeStream\
        .outputMode("append")\ 
        .foreachBatch(lambda df, epochId: batchProcessing(df, epochId, kafkaBootstrapServers, kafkaOutputTopic, fType)) \ 
        .start() \
        .awaitTermination()
        
    except Exception as e:
        timeNow = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(timeNow + "Error : Error in Main Function. " + str(e))
        
        
main()