import datetime
from pyflink.datastream import StreamExecutionEnvironment
from datetime import datetime, timedelta
from sklearn.linear_model import SGDRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error
from pyflink.table import StreamTableEnvironment
import pandas as pd
import numpy as np
from pyflink.table.expressions import col, call

def trip_duration_regression(t_env: StreamTableEnvironment):
    
    input_table = t_env.from_path("trip_msg").select(
        col('create_time'),
        col('trip_duration')
    )

    df = input_table.to_pandas()
    
    # Online model and scaler
    model = SGDRegressor(learning_rate="constant", eta0=0.01, random_state=42)
    scaler = StandardScaler()


    X_train = np.array(df['create_time']).reshape(-1, 1)
    y_train = df['trip_duration'].values
            
    # Scale and train the model
    X_scaled = scaler.fit_transform(X_train)
    model.partial_fit(X_scaled, y_train)
    
    # Predict the average trip duration for the next hour
    next_hour_start = current_time + timedelta(hours=1)
    next_hour_feature = np.array([(next_hour_start - current_time).total_seconds() / 3600]).reshape(-1, 1)
    next_hour_scaled = scaler.transform(next_hour_feature)
    prediction = model.predict(next_hour_scaled)[0]
    
    print(f"Round {i+1}: Prediction for next hour: {prediction:.2f} minutes")
        
    # Move to the next time step
    current_time += timedelta(minutes=30)
    

if __name__ == '__main__':
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(3)
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    t_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)
    
    stmt_set = t_env.create_statement_set()
    trip_duration_regression(t_env)