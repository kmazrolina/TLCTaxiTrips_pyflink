from pyflink.table.expressions import col, call
from pyflink.table import StreamTableEnvironment
from pyflink.table.statement_set import StatementSet
from udfs import *


def sink_all_data(t_env: StreamTableEnvironment, stmt_set: StatementSet):
    #Create Elasticsearch sink 
    create_es_sink_ddl = """
            CREATE TABLE trip_data_full (
                create_time TIMESTAMP,
                id VARCHAR ,
                vendor_id INT,
                pickup_datetime TIMESTAMP,
                dropoff_datetime TIMESTAMP,
                passenger_count INT,
                pickup_location VARCHAR, 
                dropoff_location VARCHAR, 
                store_and_fwd_flag VARCHAR ,
                trip_duration INT 
            )
            WITH (
                'connector' = 'elasticsearch-7',
                'hosts' = 'http://elasticsearch:9200',
                'index' = 'trip_data_full',
                'document-id.key-delimiter' = '$',
                'sink.bulk-flush.max-size' = '42mb',
                'sink.bulk-flush.max-actions' = '32',
                'sink.bulk-flush.interval' = '1000',
                'sink.bulk-flush.backoff.delay' = '1000',
                'format' = 'json'
            )
    """
    t_env.execute_sql(create_es_sink_ddl)
    t_env.register_function('parse_location_from_coords', parse_location_from_coords)
    input_table = t_env.from_path("trip_msg").select(
        col('create_time'),
        col('id'),
        col('vendor_id'),
        col('pickup_datetime'),
        col('dropoff_datetime'),
        col('passenger_count'),
        call( 'parse_location_from_coords', col('pickup_longitude'), col('pickup_latitude')).alias('pickup_location'),
        call( 'parse_location_from_coords', col('dropoff_longitude'), col('dropoff_latitude')).alias('dropoff_location'),
        col('store_and_fwd_flag'),
        col('trip_duration')
    )
    stmt_set.add_insert("trip_data_full",input_table)
    return stmt_set



def process_trip_duration(t_env: StreamTableEnvironment, stmt_set: StatementSet ):
    #Create Elasticsearch sink 
    create_es_sink_ddl = """
            CREATE TABLE trip_duration (
                create_time TIMESTAMP,
                id VARCHAR,
                duration_formatted STRING 
            )
            WITH (
                'connector' = 'elasticsearch-7',
                'hosts' = 'http://elasticsearch:9200',
                'index' = 'trip_duration',
                'document-id.key-delimiter' = '$',
                'sink.bulk-flush.max-size' = '42mb',
                'sink.bulk-flush.max-actions' = '32',
                'sink.bulk-flush.interval' = '1000',
                'sink.bulk-flush.backoff.delay' = '1000',
                'format' = 'json'
            )
    """
    t_env.execute_sql(create_es_sink_ddl)
    
    t_env.register_function('format_time_duration', format_time_duration)
    input_table = t_env.from_path("trip_msg").select(
        col('create_time'),
        col('id'),
        call('format_time_duration', col('trip_duration')).alias("duration_formatted")
    )
    stmt_set.add_insert("trip_duration",input_table)
    return stmt_set
