import time
import os
from kafka import KafkaProducer
import pandas as pd

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: str(v).encode('utf-8'))

df = pd.read_parquet(os.path.join(os.getcwd(),'datasets/yellow_tripdata_2025-01.parquet'))

for _, row in df.iterrows():
    producer.send('nyc_taxi_topic', value=row.to_dict())
    time.sleep(1)
