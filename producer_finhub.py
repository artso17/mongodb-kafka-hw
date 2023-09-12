# ## Setup Producer

# %%
# Import required Libraries
from confluent_kafka import Producer
from time import sleep
import json
import websocket
import pandas as pd 
import configparser

conf = configparser.ConfigParser()
conf.read('config.ini')
# %%
# Setup producer config
config = {
    'bootstrap.servers' : 'pkc-ew3qg.asia-southeast2.gcp.confluent.cloud:9092',
    'security.protocol' : 'SASL_SSL',
    'sasl.mechanisms' : 'PLAIN',
    'sasl.username' : 'ZXY2LQ7VXL7465XI',
    'sasl.password' : conf['confluent']['pwd'],

}

# %%
# Instantiate producer
producer = Producer(config)

# %%
# Define producer callback function
def producer_callback(err,msg):
    if err is not None:
        print(f'message delivery failed: {err}')
    else:
        print(f'message delivered to {msg.topic()} [{msg.partition()}]')

# %%
# Define websocket message function to transform and load finhub data to message broker
def on_message(ws, message):
    sleep(10)
    json_data = json.loads(message)['data'][0]
    data = json.dumps(json_data)
    producer.produce('topic_1',key='',value=data,callback=producer_callback)
    producer.flush()
    
# Define websocket error function
def on_error(ws, error):
    print(f" {error}")

# Define websocket close function 
def on_close(ws):
    print("### closed ###")

# Define websocket open function
def on_open(ws):
    ws.send('{"type":"subscribe","symbol":"AAPL"}')
    ws.send('{"type":"subscribe","symbol":"AMZN"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')
    ws.send('{"type":"subscribe","symbol":"IC MARKETS:1"}')

# Instantiate websocket and run
websocket.enableTrace(True)
ws = websocket.WebSocketApp(F"wss://ws.finnhub.io?token={conf['finhub']['token']}",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
ws.on_open = on_open
ws.run_forever()


