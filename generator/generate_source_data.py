import time
from kafka import KafkaProducer
from kafka import errors 
from json import dumps
from time import sleep
import csv
import os

DATA_PATH = './data/data_sorted.csv'

def write_data(producer, frequency=2):
    
    topic = "trip_msg"
    
    with open(DATA_PATH, newline='', encoding='utf-8') as csvfile:
    
        # Create a CSV reader object
        csvreader = csv.DictReader(csvfile)
    
        # Loop through each row in the CSV file
        for row in csvreader:
            # Convert the row to a JSON object 
            cur_data = {"create_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), 
                        "id" : row['id'],
                        "vendor_id" : row['vendor_id'],
                        "pickup_datetime" : row['pickup_datetime'],
                        "dropoff_datetime" : row['dropoff_datetime'],
                        "passenger_count" : row['passenger_count'],
                        "pickup_longitude" :row['pickup_longitude'],
                        "pickup_latitude": row['pickup_latitude'],
                        "dropoff_longitude" : row['dropoff_longitude'],
                        "dropoff_latitude" : row['dropoff_latitude'],
                        "store_and_fwd_flag" : row['store_and_fwd_flag'],
                        "trip_duration" : row['trip_duration']}
            print(cur_data)
            producer.send(topic, value=cur_data)
            sleep(2)

def create_producer():
    print("Connecting to Kafka brokers")
    for i in range(0, 6):
        try:
            producer = KafkaProducer(bootstrap_servers=['kafka:9092'],
                            value_serializer=lambda x: dumps(x).encode('utf-8'))
            print("Connected to Kafka")
            return producer
        except errors.NoBrokersAvailable:
            print("Waiting for brokers to become available")
            sleep(10)

    raise RuntimeError("Failed to connect to brokers within 60 seconds")

if __name__ == '__main__':
    producer = create_producer()
    write_data(producer)
