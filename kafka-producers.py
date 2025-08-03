from datetime import datetime
import time
import os
import json
from kafka import KafkaProducer
import pandas as pd


def json_serializer(obj):
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()  # or obj.strftime('%Y-%m-%d %H:%M:%S')
    elif isinstance(obj, datetime):
        return obj.isoformat()
    return obj


producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(dict(v), default=json_serializer).encode('utf-8'))

df = pd.read_parquet(os.path.join(os.getcwd(),'datasets/yellow_tripdata_2025-01.parquet'))

for _, row in df.iterrows():
    producer.send('nyc_taxi_topic', value=row.to_dict())
    time.sleep(1)
