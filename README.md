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


`pyspark --class "org.myspark.KafkaStream" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1`




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
