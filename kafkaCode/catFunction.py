import sys
import os
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
import pyspark.sql.functions as f

# Cat Function for streaming Data.
def catFunction(spark, inputDf):
    try:
        df1 = inputDf.withColumn("Var", when(
            ((col('accIdent').isNull()) | (col('tranDt').isNull()) | (col('cost').isNull()) | \
             (col('tranT').isNull()) | (col('tranI').isNull())), 0).otherwise(1))
        df1 = df1.withColumn("tranT1", col('tranT'))\
            .withColumn("tranT", upper(col('tranT')))\
            .withColumn("tranT", regexp_replace('tranT', '\\s+', " "))

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

        m_map = spark.read.option("header",True).csv("/hdfs/kafka/master/m_map.csv")
        m_map = m_map.withColumn('m_c', m_map['m_c'].cast(IntegerType())).withColumn("Dec_Wt", lit(1.5))

        dictionary_schema = StructType([StructField("catIdEdit", StringType(), True), \
                                        StructField("accOfDec", StringType(), True)])
        m_map = m_map.withColumn("udId", from_json(col('udCat'), dictionary_schema)["catIdEdit"]) \
            .withColumn("udAcc", from_json(col('udCat'), dictionary_schema)["accOfDec"])
        
        others=['1','119','882','444']
        in_cat=['1','2','3','657','786','898']

        fg = df2.columns
        fg_1 = ['subT_create', 'subT']

        col_list = fg + fg_1

        condition = when((col('udId').isin(others)) | \
             ((col('udId').isin(in_cat)) != (col('cat_id').isin(in_cat))), None)\
            .otherwise(col('udId'))

        m_map = m_map.withColumn('udId', condition)
        w = Window.partitionBy(col_list).orderBy(f.desc('udAcc'))
        df3 = m_map.withColumn('usDecCatId', f.collect_list('udId').over(w))\
            .groupBy(col_list) \
            .agg(f.max('usDecCatId').alias('usDecCatId'))
        df4 = df3.withColumn('usDecCatId',
                          array_union(split(col('cat_id'), '-'), col('usDecCatId')))

        df4 = df4.withColumn('usDecCatId', array_distinct(col('usDecCatId')))
        dd = df4.withColumn('cat', explode_outer('dec_cat'))

        dictionary_schema = StructType([StructField("catId", StringType(), True),\
                                        StructField("score", StringType(), True)])

        dd1 = dd.withColumn('cat_id', from_json(col('cat'), dictionary_schema)['categoryId']) \
            .withColumn('cat_wt', from_json(col('cat'),dictionary_schema)['score'])

        dd2 = dd1.withColumn("nextBestCat1", coalesce(col('nextBestCat'), array()))
        dd3 = dd2.withColumn("setNext", array_union(col('usDecCatId'), \
                                                    array_union(col('decCatId'),\
                                                                array_union(col('nextBestCat1'),\
                                                                            col('nextBest_catId')))))

        dd4 = dd3.withColumn("nextBest0", f.to_json(f.struct(dd3['nextBest0'].alias("catId"),\
                                                             dd3['catWt0'].alias("score"))))
        df = df1.filter(col('var') == 0)

        tree = spark.read.option("header",True).csv("/hdfs/kafka/master/tree.csv")

        treeJoined = dd4.join(tree, ['catId'], 'left')

        final_output = treeJoined.unionByName(df, allowMissingColumns=True)

        return final_output

    except Exception as e:
        print("ERROR : While processing Cat Function : " + str(e))