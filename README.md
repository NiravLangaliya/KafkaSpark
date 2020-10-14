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
