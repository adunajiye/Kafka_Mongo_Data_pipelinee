import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import logging
# from cassandra.cluster import Cluster
import pymongo
from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from pyspark.sql.types import StructType, StructField, StringType
from Get_Data_dag import  get_data
# from Get_Data_dag import format_data
from kafka import KafkaProducer
import logging
from time import sleep
import json




def create_connection():
    print('Connecting to the Mongo database...')
    try:
        moongo_uri = "mongodb://localhost:27017"
        database_name = "kafka_admin"
        collection_name = "api_data"
        
        client = pymongo.MongoClient(moongo_uri)
        
        # Accesss database if exists
        data_created = client[database_name]
        
        # create a colllection in database 
        collection_created = data_created[collection_name]
        
        json_object = get_data()
        print(json_object)
        # # Insert Bulk Data 
        # result = json_object.insert_one()
        # result.inserted_ids
    


    except (Exception, pymongo.DatabaseError) as error:
        print(error)
    finally:
        if client is not None:
            client.close()
    print('Database connection closed.')

create_connection()