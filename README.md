# KafkaSpark


**<u>Start Kafka Connector</u>**

```
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d '{
      "name": "jdbc_source_mysql_01",
      "config": {
              "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
              "connection.url": "jdbc:mysql://localhost:3306/demo",
              "connection.user": "",
              "connection.password": "",
              "topic.prefix": "mysql-01-",
              "poll.interval.ms" : 3600000,
              "mode":"bulk"
              }
      }'
```


![List_bulk_load_topics](https://github.com/NiravLangaliya/KafkaSpark/blob/main/List_bulk_load_topics.png)

**List Kafka topic:**

`kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic mysql-01-emp --from-beginning`

![List_topic_mysql-01-emp](https://github.com/NiravLangaliya/KafkaSpark/blob/main/List_topic_mysql-01-emp.png)

**Spark Submit Command**


`pyspark --class "org.myspark.KafkaStream" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,io.delta:delta-core_2.12:0.7.0`




```python
# Enter steps here
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import from_json,to_json, struct, window, col
from pyspark import  SparkContext as sc
from pyspark.sql.window import *

df = spark \
.read \
.format("kafka") \
.option("kafka.bootstrap.servers", "localhost:9092") \
.option("subscribe", "mysql-01-emp") \
.load()

df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

df.show()
```
![Load_mysql-01-emp_Spark](https://github.com/NiravLangaliya/KafkaSpark/blob/main/Load_mysql-01-emp_Spark.png)




`spark-shell --packages io.delta:delta-core_2.12:0.7.0`

```scala

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
val schema = StructType(
            Seq(
            StructField("key", StringType, true),  
            StructField("value", StringType, true),
            StructField("topic", TimestampType, true),
            StructField("partition", StringType, true),
            StructField("offset", StringType, true),
            StructField("timestamp", StringType, true),
            StructField("timestampType", StringType, true)
            )
          )
val df = spark.createDataFrame(sc.emptyRDD[Row], schema)
val audit_path = "/Users/nlangaliya/Downloads/mysql_audit"
df.write.format("delta").save(audit_path)
```





**<u>creating incremental processing</u>**

```
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d '{
      "name": "jdbc_source_mysql_05",
      "config": {
              "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
              "connection.url": "jdbc:mysql://localhost:3306/demo",
              "connection.user": "root",
              "connection.password": "QWEasd234",
              "topic.prefix": "mysql-05-",
              "poll.interval.ms" : 3600000,
              "mode": "incrementing",
              "catalog.pattern" : "demo",
              "table.whitelist" : "emp_1",
              "incrementing.column.name": "empno"
              }
      }'
```





**<u>Check Kafka Producer Code:</u>**

```python
from confluent_kafka import Producer

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: {0}: {1}"
              .format(msg.value(), err.str()))
    else:
        print("Message produced: {0}".format(msg.value()))
p = Producer({'bootstrap.servers': 'localhost:9092'})

try:
    for val in range(1, 1000):
            p.produce('EDW_TEST', 'myvalue #{0}'.format(val), callback=acked)
            p.poll(0.5)

except KeyboardInterrupt:
    pass

p.flush(30)
```
