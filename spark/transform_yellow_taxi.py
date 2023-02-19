from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, TimestampType, FloatType
import pyspark.sql.functions as pf
import sys
from datetime import datetime

GCS_BUCKET_PATH = sys.argv[1]
SRC_PATH = sys.argv[2]
DEST_PATH = sys.argv[3]
FILE_DATE = sys.argv[4]

file_date = datetime.strptime(FILE_DATE, "%Y-%m-%d")
file_year = file_date.year
file_month = file_date.month

if __name__ == "__main__":

    rate_code_map = {
        1: "Standard Rate",
        2: "JFK",
        3: "Newark",
        4: "Nassau or Westchester",
        5: "Negotiated Fare",
        6: "Group Ride"
    }

    payment_type_map = {
        1: "Credit Card",
        2: "Cash",
        3: "No Charge",
        4: "Dispute",
        5: "Unknown",
        6: "Voided Trip"
    }

    def get_udf_replace(map):
        return pf.udf(lambda id: map.get(id, "Unknown"))

    spark = SparkSession.builder \
        .master("yarn") \
        .appName("Transform Yellow Taxi Data") \
        .getOrCreate()

    df = spark.read \
        .format("parquet") \
        .load(f"gs://{GCS_BUCKET_PATH}/raw/{SRC_PATH}")

    # Fix inconsistent / incorrect types
    df_transformed = df \
        .withColumn("year", pf.lit(file_year).cast(IntegerType())) \
        .withColumn("month", pf.lit(file_month).cast(IntegerType())) \
        .withColumn("VendorID", pf.col("VendorID").cast(IntegerType())) \
        .withColumn("tpep_pickup_datetime", pf.col("tpep_pickup_datetime").cast(TimestampType())) \
        .withColumn("tpep_dropoff_datetime", pf.col("tpep_dropoff_datetime").cast(TimestampType())) \
        .withColumn("store_and_fwd_flag", pf.col("store_and_fwd_flag").cast(StringType())) \
        .withColumn("rate_code", get_udf_replace(rate_code_map)(pf.col("RatecodeID").cast(IntegerType()))) \
        .drop(pf.col("RatecodeID")) \
        .withColumn("PULocationID", pf.col("PULocationID").cast(IntegerType())) \
        .withColumn("DOLocationID", pf.col("DOLocationID").cast(IntegerType())) \
        .withColumn("passenger_count", pf.col("passenger_count").cast(IntegerType())) \
        .withColumn("trip_distance", pf.col("trip_distance").cast(FloatType())) \
        .withColumn("fare_amount", pf.col("fare_amount").cast(FloatType())) \
        .withColumn("extra", pf.col("extra").cast(FloatType())) \
        .withColumn("mta_tax", pf.col("mta_tax").cast(FloatType())) \
        .withColumn("tip_amount", pf.col("tip_amount").cast(FloatType())) \
        .withColumn("tolls_amount", pf.col("tolls_amount").cast(FloatType())) \
        .withColumn("improvement_surcharge", pf.col("improvement_surcharge").cast(FloatType())) \
        .withColumn("congestion_surcharge", pf.col("congestion_surcharge").cast(FloatType())) \
        .withColumn("airport_fee", pf.col("airport_fee").cast(FloatType())) \
        .withColumn("total_amount", pf.col("total_amount").cast(FloatType())) \
        .withColumn("payment_type", get_udf_replace(payment_type_map)(pf.col("payment_type").cast(IntegerType()))) \
        .fillna(0, subset=["improvement_surcharge", "congestion_surcharge"]) \
        .fillna("Unknown", subset=["store_and_fwd_flag"]) \

    df_transformed.coalesce(1).write \
        .format("parquet") \
        .mode("overwrite") \
        .save(f"gs://{GCS_BUCKET_PATH}/transform/{DEST_PATH}")
