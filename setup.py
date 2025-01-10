from elasticsearch import Elasticsearch
from pyflink.datastream import StreamExecutionEnvironment

def es_setup():
    # Connect to Elasticsearch
    es = Elasticsearch(["http://elasticsearch:9200"])

    # Define the index name and mappings
    index_names = ["trip_data_full", "trip_duration"]
    mappings = [{
        "mappings": {
            "properties": {
                "create_time": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
                },
                "id": {
                    "type": "text"
                },
                "vendor_id": {
                    "type": "integer"
                },
                "pickup_datetime": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
                },
                "dropoff_datetime": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
                },
                "passenger_count": {
                    "type": "integer"
                },
                "pickup_location": {
                    "type": "geo_point"
                },
                "dropoff_location": {
                    "type": "geo_point"
                },
                "store_and_fwd_flag": {
                    "type": "text"
                },
                "trip_duration":{
                    "type": "integer"
                } 

            }
        }
    },
    {
        "mappings": {
            "properties": {
                "create_time": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
                },
                "id": {
                    "type": "text"
                },
                "duration_formatted":{
                    "type": "date",
                    "format": "hour_minute_second||HH:mm:ss"
                } 
            }
        }
    }]
    for index_name, mapping in zip(index_names, mappings):
        try:
            # Create the index with mappings
            response = es.indices.create(index=index_name, body=mapping)
            print("ES INDEX ", index_name, " ", response)
        except BaseException as e:
            print("ES INDEX ", index_name, " ", e)

def kafka_setup(t_env: StreamExecutionEnvironment):
    create_kafka_source_ddl = """
    CREATE TABLE trip_msg (
        create_time TIMESTAMP,
        id VARCHAR ,
        vendor_id INT,
        pickup_datetime TIMESTAMP,
        dropoff_datetime TIMESTAMP,
        passenger_count INT,
        pickup_longitude DOUBLE,
        pickup_latitude DOUBLE,  
        dropoff_longitude DOUBLE,
        dropoff_latitude DOUBLE,  
        store_and_fwd_flag VARCHAR ,
        trip_duration INT 
    )  
    WITH (
        'connector' = 'kafka',
        'topic' = 'trip_msg',
        'properties.bootstrap.servers' = 'kafka:9092',
        'properties.group.id' = 'test',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json'
    )
    """
    t_env.execute_sql(create_kafka_source_ddl)