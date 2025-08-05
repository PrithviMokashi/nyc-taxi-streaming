import os

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC_NAME = "nyc_taxi_topic"
PARQUET_PATH = os.path.join(os.getcwd(),'datasets/yellow_tripdata_2025-01.parquet')
BATCH_SIZE = 10000
SLEEP_BETWEEN_BATCHES = 2