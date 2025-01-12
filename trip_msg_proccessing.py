from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from setup import *
from es_sinks import *


def log_processing():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    t_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)
    
    stmt_set = t_env.create_statement_set()
   
    # Create predefined elasticsearch indexes
    es_setup()
    
    #Setup Kafka Datasource
    kafka_setup(t_env)
    
    #Send full dataset (all columns) to elasticsearch
    stmt_set = sink_all_data(t_env, stmt_set)
    
    #Send data about trip durations in formatted way
    #stmt_set = process_trip_duration(t_env, stmt_set)
    stmt_set.execute()

    
if __name__ == '__main__':
    log_processing()

