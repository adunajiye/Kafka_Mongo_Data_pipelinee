import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json
from kafka import KafkaProducer
import logging
from time import sleep
from pykafka import KafkaClient
import threading



def get_data():
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    # print(res)
    ress = res['results'][0]
    # ress = json.dumps(ress,indent = 4)
    print (ress)
get_data()


def format_data():
    get_dataa = get_data()
    print(get_dataa)
    data = {}
    data['id'] = uuid.uuid4()
    # print(data['id'])
    location = get_dataa["location"]
    print(location)
    
    get_dataa['first_name'] = get_dataa['name']['first']
    # print(get_dataa['first_name'])
    data['last_name'] = get_dataa['name']['last']
    data['gender'] = get_dataa['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = get_dataa['email']
    print(data['email'])
    data['username'] = get_dataa['login']['username']
    data['dob'] = get_dataa['dob']['date']
    data['registered_date'] = get_dataa['registered']['date']
    data['phone'] = get_dataa['phone']
    data['picture'] = get_dataa['picture']['medium']

    print(data)
    # print(data)
format_data()


def stream_data():
    Kafka_host = "137.184.109.96:9092"
    client = KafkaClient(hosts=Kafka_host)
    topic = client.topics["stream_data"]
    
    with topic.get_sync_producer() as producer:
        for i in range(10):
            message = get_data() 
            message_1 = format_data()
            encoded_message = json.dumps(message_1).encode("utf-8")
            producer.start()
            producer.produce(encoded_message)
            
            sleep(0.5)
           
            producer.stop()
            
stream_data()

