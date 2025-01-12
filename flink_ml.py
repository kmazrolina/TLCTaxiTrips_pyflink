from pyflink.table import StreamTableEnvironment, col, call, lit
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.ml.core.linalg import Vectors, DenseVectorTypeInfo
from pyflink.ml.lib.classification.logisticregression import LogisticRegression
from pyflink.common import Types

def logistic_regression_trip_duration():
    # Set up the StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)

    # Load the data from the `trip_data_full` table
    input_table = t_env.from_path("trip_data_full").select(
        col('pickup_datetime'),
        col('trip_duration')
    )

    # Extract the hour from `pickup_datetime` as a feature
    input_table = input_table.select(
        call('EXTRACT', col('pickup_datetime'), lit('HOUR')).alias('hour_feature'),
        col('trip_duration').alias('label')
    )

    # Convert the data into the format required by PyFlink ML
    formatted_table = input_table.select(
        call('vectorize', col('hour_feature')).alias('features'),  # Wrap feature into a vector
        col('label')
    )

    # Create a Logistic Regression model
    logistic_regression = LogisticRegression()

    # Train the model on the data
    model = logistic_regression.fit(formatted_table)

    # Use the trained model to make predictions
    prediction_result_table = model.transform(formatted_table)[0]

    # Print the results
    field_names = prediction_result_table.get_schema().get_field_names()
    for result in t_env.to_data_stream(prediction_result_table).execute_and_collect():
        features = result[field_names.index(logistic_regression.get_features_col())]
        label = result[field_names.index(logistic_regression.get_label_col())]
        prediction = result[field_names.index(logistic_regression.get_prediction_col())]
        print(f"Features (Hour): {features} \tLabel: {label} \tPrediction: {prediction}")


if __name__ == '__main__':
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(3)
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    t_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)
    
    stmt_set = t_env.create_statement_set()
    logistic_regression_trip_duration(t_env, env)