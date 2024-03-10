import sys
import os
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window

# Cat Function for streaming Data.
def catFunction(spark, inputDf):
    try:
        df1 = inputDf.withColumn("Var", when(
            ((col('accIdent').isNull()) | (col('tranDt').isNull()) | (col('cost').isNull()) | \
             (col('tranT').isNull()) | (col('tranI').isNull())), 0).otherwise(1))
        df1 = df1.withColumn("tranT1", col('tranT'))\
            .withColumn("tranT", upper(col('tranT')))\
            .withColumn("tranT", regexp_replace('tranT', '\\s+', " "))

        feature_group = df1.columns

        df2 = df1.filter(col('Var') > 0).withColumn('word1', split(col("tranT"), '/|~'))

        df2 = df2.withColumn('TT_0', col('word1').getItem(0))\
            .withColumn('TT_1', col('word1').getItem(1)) \
            .withColumn('TT_2', col('word1').getItem(2)) \
            .withColumn('TT_3', col('word1').getItem(3)) \
            .withColumn('TT_4', col('word1').getItem(4))\

        df2 = df2.drop('word1')

        conditions = when(col("TT_0").rlike("^U"), '') \
                .when(col("TT_0").rlike("^A"), col('TT_1')) \
                .when(col("TT_0").rlike("^M"), col('TT_3')) \
                .when(col("TT_0").rlike("^E"), col('TT_2')) \
                .when(col("TT_0").rlike("^C"), col('TT_1')) \
                .when(col("TT_0").rlike("^B"), col('TT_1')) \
                .when(col("TT_0").rlike("^N"), col('TT_2')) \
                .when(col("TT_0").rlike("^I"), col('TT_3')) \
                .when(lower(col("payType")).rlike("^remit"), '') \
                .when((lower(col("payType")) == "bill") & (col("TT_1") == "AP"), col("TT_3")) \
                .when(lower(col("payType")) == "others") & (col("tranT").rlike("^(C|I|S)")), col('TT_0')\
                .otherwise('')

        df2 = df2.withColumn("counP_Name_TT", conditions)

        