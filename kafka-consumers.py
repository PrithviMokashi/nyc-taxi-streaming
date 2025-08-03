from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from constants import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_NAME

spark = SparkSession.builder \
    .appName("KafkaTaxiStreamConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .getOrCreate()

taxi_schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True),
    StructField("cbd_congestion_fee", DoubleType(), True)
])

kafka_raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC_NAME)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

kafka_df = kafka_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), taxi_schema).alias("data")) \
    .select("data.*") \
    .withColumn("tpep_pickup_datetime", 
                to_timestamp(col("tpep_pickup_datetime"), "yyyy-MM-dd'T'HH:mm:ss")) \
    .withColumn("tpep_dropoff_datetime", 
                to_timestamp(col("tpep_dropoff_datetime"), "yyyy-MM-dd'T'HH:mm:ss"))

query = kafka_df.writeStream \
    .format("parquet") \
    .option("path", "data/nyc_trips/") \
    .option("checkpointLocation", "data/nyc_trips/_checkpoint/") \
    .trigger(processingTime="30 seconds") \
    .outputMode("append") \
    .start()

query.awaitTermination()
