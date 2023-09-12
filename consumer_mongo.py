# %%
# Import required libraries
import pandas as pd 
import json
import pymongo
import configparser
from confluent_kafka import Consumer

conf = configparser.ConfigParser()
conf.read('config.ini')

# %%
# Setup consumer config
config = {
    'bootstrap.servers' : 'pkc-ew3qg.asia-southeast2.gcp.confluent.cloud:9092',
    'security.protocol' : 'SASL_SSL',
    'sasl.mechanisms' : 'PLAIN',
    'sasl.username' : 'ZXY2LQ7VXL7465XI',
    'sasl.password' : conf['confluent']['pwd'],
    'group.id' : 'group-python-1',
    'auto.offset.reset' : 'earliest'

}

# %%
# Instantiate consumer and its subcription
consumer = Consumer(config)
consumer.subscribe(['topic_1'])

# %%
# MongoDB connection
mongodb_uri = conf['mongo']['uri']
mongo_client = pymongo.MongoClient(mongodb_uri)
db = mongo_client['aditya']
collection = db['project/homework'] 

# Run Consumer and load data to mongodb
try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f'Reached end of partition: {msg.topic()} [{msg.partition()}]')
            else:
                print(f'Error: {msg.error()}')
        else:
            # Insert message into MongoDB
            document = {
                'message': msg.value().decode("utf-8")
            }
            collection.insert_one(document)
            print(f'Inserted message into MongoDB: {document}')

except KeyboardInterrupt:
    pass

finally:
    consumer.close()
    mongo_client.close()









# %%
