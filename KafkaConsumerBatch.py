# -*- coding: utf-8 -*-
"""
Created on 14/10/20 11:37 AM

@author : Nirav Langaliya
"""

# File Name : KafkaConsumerBatch.py

# Enter feature description here

# Enter steps here
from pyspark.sql import SparkSession
from pyspark.sql.types import *
"""
from pyspark import SparkContext
from pyspark.sql.functions import from_json,to_json, struct, window, col
from pyspark import  SparkContext as sc
from pyspark.sql.window import *
import os,uuid
"""

if __name__=='__main__':
    spark=SparkSession.builder.master("local").appName("Kafka Consumer").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions",2)
    """
    kafkaDf = spark.read.format("kafka") \
                        .option("kafka.bootstrap.servers","moped-01.srvs.cloudkafka.com:9094,moped-02.srvs.cloudkafka.com:9094,moped-03.srvs.cloudkafka.com:9094") \
                        .option("subscribe", "sypd2avr-test_topic") \
                        .option("kafka.security.protocol", "SASL_SSL") \
                        .option("kafka.sasl.mechanism","SCRAM-SHA-256") \
                        .option("kafka.sasl.jaas.config","org.apache.kafka.common.security.scram.ScramLoginModule required username='' password='';").load()
    """
    kafkaDf = spark.read.format("kafka") \
                        .option("kafka.bootstrap.servers","localhost:9092") \
                        .option("subscribe", "mysql-01-emp") \

    #kafkaDf.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    kafka_Df = kafkaDf.withColumn("key", kafkaDf["key"].cast(StringType())) \
        .withColumn("value", kafkaDf["value"].cast(StringType())) \
        .withColumn("topic", kafkaDf["topic"].cast(StringType())) \
        .withColumn("partition", kafkaDf["partition"].cast(StringType())) \
        .withColumn("offset", kafkaDf["offset"].cast(StringType())) \
        .withColumn("timestamp", kafkaDf["timestamp"].cast(StringType())) \
        .withColumn("timestampType", kafkaDf["timestampType"].cast(StringType()))

    print(kafka_Df.printSchema())

    kafka_Df.write.format("delta").mode("overwrite").save("/Users/nlangaliya/Downloads/mysql_audit_tb")

    df_audit_read = spark.read.format("delta").load("/Users/nlangaliya/Downloads/mysql_audit_tb")
    df_audit_read.show()

    #df1.createOrReplaceTempView("kafka_ip_tbl")
    #df_out=spark.sql("select key, SUM(CAST(value as int)) as value from kafka_ip_tbl group by key order by value desc")

    #print(df_out.show())
