# NYC Taxi Trips with Pyflink

## Background

This project is an end-to-end PyFlink pipeline for data analytics, covering the following steps:

* Reading data from a Kafka source;
* Creating data using a [UDF](https://ci.apache.org/projects/flink/flink-docs-release-1.16/dev/python/table-api-users-guide/udfs/python_udfs.html);
* Performing a simple aggregation over the source data;
* Writing the results to Elasticsearch and visualizing them in Kibana.

The environment is based on Docker Compose, so the only requirement is that you have [Docker](https://docs.docker.com/get-docker/) 
installed on your machine.

### New York City Taxi Trip Duration Dataset
The dataset is based on the 2016 NYC Yellow Cab trip record data made available in Big Query on Google Cloud Platform. The data was originally published by the NYC Taxi and Limousine Commission (TLC). The data used in this project is sourced from [Kaggle Competition Site](https://www.kaggle.com/competitions/nyc-taxi-trip-duration/data?select=test.zip). 

#### Data fields
`id` - a unique identifier for each trip \
`vendor_id` - a code indicating the provider associated with the trip record \
`pickup_datetime` - date and time when the meter was engaged \
`dropoff_datetime` - date and time when the meter was disengaged \
`passenger_count` - the number of passengers in the vehicle (driver entered value) \
`pickup_longitude` - the longitude where the meter was engaged \
`pickup_latitude` - the latitude where the meter was engaged \
`dropoff_longitude` - the longitude where the meter was disengaged \
`dropoff_latitude` - the latitude where the meter was disengaged \
`store_and_fwd_flag` - This flag indicates whether the trip record was held in vehicle memory before sending to the vendor because the vehicle did not have a connection to the server - Y=store and forward; N=not a store and forward trip \
`trip_duration` - duration of the trip in seconds 

### Apache Kafka

Kafka is used to store sample input data about Taxi Trips. A data generator [generate_source_data.py](generator/generate_source_data.py) is provided to
continuously write new records to the `trip_msg` Kafka topic. 

### PyFlink

The transaction data will be processed with PyFlink using the Python script [trip_msg_processing.py](trip_msg_proccessing.py).


### Elasticsearch

Elasticsearch is used to store the results and to provide an efficient query service.

### Kibana

Kibana is an open source data visualization dashboard for Elasticsearch. You will use it to visualize
the total transaction tripAmount and proportion for each provinces in this PyFlink pipeline through a dashboard.

## Setup

As mentioned, the environment for this walkthrough is based on Docker Compose; It uses a custom image
to spin up Flink (JobManager+TaskManager), Kafka+Zookeeper, the data generator, and Elasticsearch+Kibana containers.

You can find the [docker-compose.yaml](docker-compose.yml) file of the pyflink-walkthrough in the `pyflink-walkthrough` root directory.

### Prep Taxi Trips Data
Download TLC Trips Data from [Kaggle Competition Site](https://www.kaggle.com/competitions/nyc-taxi-trip-duration/data?select=test.zip) into `generator/data/` directory. Unizip the data and run `generator/data/sort_data.py` to prepare it for streaming. 

In this project only `train.csv` data is used, as test competition data doesn not contain all data fields. 

### Building the Docker image

First, build the Docker image by running:

```bash
$ cd pyflink-walkthrough
$ docker-compose build
```

### Starting the Pipeline

Once the Docker image build is complete, run the following command to start the playground:

```bash
$ docker-compose up -d
```

One way of checking if the playground was successfully started is to access some of the services that are exposed:

1. visiting Flink Web UI [http://localhost:8081](http://localhost:8081).
2. visiting Elasticsearch [http://localhost:9200](http://localhost:9200).
3. visiting Kibana [http://localhost:5601](http://localhost:5601).

**Note:** you may need to wait around 1 minute before all the services come up.

### Checking the Kafka service

You can use the following command to read data from the Kafka topic and check whether it's generated correctly:

```shell script
$ docker-compose exec kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic trip_msg

"{'create_time': '2024-12-29 20:11:51', 'id': 'id2875421', 'vendor_id': '2', 'pickup_datetime': '2016-03-14 17:24:55', 'dropoff_datetime': '2016-03-14 17:32:30', 'passenger_count': '1', 'pickup_location': 'POINT(40.767936706542969 -73.982154846191406)', 'dropoff_location': 'POINT(40.765602111816406 -73.964630126953125)', 'store_and_fwd_flag': 'N', 'trip_duration': '455'}"
```


## Running the PyFlink job

1. Submit the PyFlink job.

```shell script
$ docker-compose exec jobmanager ./bin/flink run -py /opt/pyflink-walkthrough/trip_msg_proccessing.py -d -pyFiles /opt/pyflink-walkthrough/
```

Navigate to the [Flink Web UI](http://localhost:8081) after the job is submitted successfully. There should be a job in the running job list.
Click the job to get more details. You should see that the `StreamGraph` of the `trip_msg_proccessing`.

2. Navigate to the [Kibana UI](http://localhost:5601), open the menu list by clicking the menu button in the upper left corner, then choose the Dashboard item to turn to the dashboard page and choose the pre-created dashboard `trip_dashboard`.





3. Stop the PyFlink job:

Visit the Flink Web UI at [http://localhost:8081/#/overview](http://localhost:8081/#/overview) , select the job, and click `Cancel Job` in the upper right corner.


### Stopping the Pipeline

To stop the playground, run the following command:

```bash
$ docker-compose down
```

## Resources
- Pyflink Walkthrough: https://github.com/apache/flink-playgrounds
- TLC Trip Record Data: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Apache Flink Docs: https://nightlies.apache.org/flink/flink-docs-release-1.10/