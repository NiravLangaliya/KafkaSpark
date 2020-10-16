# -*- coding: utf-8 -*-
"""
Created on 10/1/2020 12:00 PM

@author : Nirav Langaliya
"""

# File Name : AvroProducerKafkaFakeData.py

# Enter feature description here

# Enter steps here

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import json
from faker_schema.faker_schema import FakerSchema
import datetime,time
import threading
import traceback

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}] {}'.format(msg.topic(), msg.partition(),msg.offset()))


now = datetime.datetime.now()

schema_path = "com_example_avro_testV2.avsc"

schema_read = avro.load(schema_path)

avroProducer = AvroProducer(
        {
            'bootstrap.servers': 'localhost:9092',
            'schema.registry.url': 'http://localhost:8081'
        }
        , default_value_schema=schema_read)


schema = {'Name': 'name',
          'ID' : 'uuid4',
           'Contact': {'Email': 'email', 'PhoneNumber': 'phone_number','WorkAddress':'address'},
          'LocationDetails': {'CountryCode': 'country_code','City': 'city', 'Country': 'country', 'PostalCode': 'postalcode','Address': 'street_address'}
          }
def run(avroProducer,name):
 faker = FakerSchema()
 RecCounter=0
 print (name)
 for i in range(1,100):
    try:
        data = faker.generate_fake(schema)
        line = str(data).replace("'",'"')
        try:
            line = json.loads(line.strip())
            line['DateTime'] = now.strftime("%Y-%m-%d %H:%M:%S")
            line['RecNumber'] = str(time.time()).replace('.','')
            RecCounter = RecCounter + 1
            line['RecCounter'] = str(RecCounter)
        except:
            print (data)
            #print (line)
            print("Error while trying to convert data to json", data)
            continue
        print(line)
        avroProducer.produce(topic='NEW_EDW_TEST_SCHEMA', value=line,callback=delivery_report)
        avroProducer.poll(1)
        #time.sleep(1)
    except Exception as e:
        print("Error while trying to publish data",data)
        print(traceback.format_exc())
        raise


workers = []
for index in range(1):
    w = threading.Thread(target=run, args=(avroProducer,index,))
    w.start()
    workers.append(w)
time.sleep(2)

for w in workers:
    w.join()
avroProducer.flush()
