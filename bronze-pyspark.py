from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("BronzeDataLakeWriter") \
    .config("spark.jars.packages", 
            "io.delta:delta-spark_2.13:4.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


bronze_df = spark.read \
    .format("delta") \
    .load("data_lake/bronze/nyc_trips/")

print(f"Loaded {bronze_df.count()} records from the Bronze Data Lake.")

bronze_df.groupBy("pickup_date").count().sort("pickup_date").show(truncate=False)

spark.sql("CREATE OR REPLACE TEMP VIEW bronze_nyc_trips USING delta OPTIONS (path 'data_lake/bronze/nyc_trips/')")
spark.sql("SELECT tpep_pickup_datetime, COUNT(*) as trip_count FROM bronze_nyc_trips GROUP BY tpep_pickup_datetime ORDER BY tpep_pickup_datetime").show(truncate=True)
