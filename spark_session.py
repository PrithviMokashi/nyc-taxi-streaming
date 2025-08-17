import os
from pyspark.sql import SparkSession

DELTA_VERSION = "2.13:4.0.0"
SPARK_HIVE_VERSION = "2.13:4.0.0"
DERBY_VERSION = "10.14.2.0"
WAREHOUSE_DIR = os.getenv("SPARK_WAREHOUSE_DIR", "spark_warehouse")
METASTORE_DB = os.getenv("SPARK_METASTORE_DB", "metastore_db")

JARS_PACKAGES = ",".join([
    f"io.delta:delta-spark_{DELTA_VERSION}",
    f"org.apache.spark:spark-hive_{SPARK_HIVE_VERSION}",
    f"org.apache.derby:derby:{DERBY_VERSION}"
])

def get_spark_session(app_name="nyc-taxi-streaming"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", JARS_PACKAGES)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
        .config("javax.jdo.option.ConnectionURL", f"jdbc:derby:;databaseName={METASTORE_DB};create=true")
        .enableHiveSupport()
        .getOrCreate()
    )