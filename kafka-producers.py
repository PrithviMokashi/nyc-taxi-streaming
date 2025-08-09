from datetime import datetime
import json
from kafka import KafkaProducer
import pandas as pd
from threading import Thread
from time import sleep
from constants import BATCH_SIZE, KAFKA_TOPIC_NAME, PARQUET_PATH, SLEEP_BETWEEN_BATCHES


def json_serializer(obj):
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif isinstance(obj, datetime):
        return obj.isoformat()
    return obj


def send_batch(df_batch, batch_number):
    """Send a batch of data to Kafka with its own producer instance"""
    # Create producer for this thread to avoid thread safety issues
    thread_producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(dict(v), default=json_serializer).encode('utf-8')
    )

    try:
        for i, (_, row) in enumerate(df_batch.iterrows()):
            row_data = row.to_dict()

            thread_producer.send(KAFKA_TOPIC_NAME, value=row_data)
            if (i + 1) % 1000 == 0:
                print(f"Batch {batch_number} - Sent {i+1}/{len(df_batch)} records")

        thread_producer.flush()
        print(f">>> Batch {batch_number} complete and flushed.")

    except Exception as e:
        print(f"Error in batch {batch_number}: {e}")
    finally:
        thread_producer.close()


def main():
    try:
        # Load data
        df = pd.read_parquet(PARQUET_PATH)
        total_records = len(df)
        threads = []

        print(f"Loaded {total_records} records from Parquet.")

        # Create and start threads
        for i in range(0, total_records, BATCH_SIZE):
            batch = df.iloc[i:i+BATCH_SIZE]
            batch_number = (i // BATCH_SIZE) + 1
            thread = Thread(target=send_batch, args=(batch, batch_number))
            thread.start()
            threads.append(thread)

            sleep(SLEEP_BETWEEN_BATCHES)

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        print("✅ All batches sent to Kafka.")

    except Exception as e:
        print(f"Error in main process: {e}")


if __name__ == "__main__":
    main()