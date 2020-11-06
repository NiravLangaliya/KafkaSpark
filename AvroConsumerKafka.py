from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
import threading,time , pprint
import json

settings = {
            'bootstrap.servers': 'localhost:9092'
            ,'group.id': 'edwdevgroupid'
            ,'client.id': 'client-1'
            ,'enable.auto.commit': False
            ,'session.timeout.ms': 6000
            ,'default.topic.config': {'auto.offset.reset': 'smallest'}
            ,'schema.registry.url': 'http://localhost:8081'
          }

def run(name):
    c = AvroConsumer(settings)

    c.subscribe(['EDW_TEST_2_DATA'])

    while True:
        try:
            msg = c.poll(10)

        except SerializerError as e:
            print("Message deserialization failed for {}: {}".format(msg, e))
            break

        if msg is None:
            continue

        if msg.error():
            print("AvroConsumer error: {}".format(msg.error()))
            continue

        print("%s Data processed %s [%d] at offset %d with key %s:\n" % (name, msg.topic(), msg.partition(), msg.offset(), str(msg.key())))
        print (msg.value()['Name'])
        print(msg.value()['ID'])
        print(msg.value()['DateTime'])
        print(msg.value()['RecNumber'])
        print(msg.value()['RecCounter'])
        print(msg.value()['Contact']['Email'])
        print(msg.value()['Contact']['PhoneNumber'])
        print(msg.value()['LocationDetails']['CountryCode'])
        print(msg.value()['LocationDetails']['City'])
        print(msg.value()['LocationDetails']['Country'])
        print(msg.value()['LocationDetails']['PostalCode'])
        print(msg.value()['LocationDetails']['Address'])

    print("Shutting down consumer..")

    c.close()


##Thread is only require when you want to invoke mulitple consumer instance.
workers = []
for index in range(4):
    w = threading.Thread(target=run, args=(index,))
    w.start()
    workers.append(w)
time.sleep(2)

for w in workers:
    w.join()
