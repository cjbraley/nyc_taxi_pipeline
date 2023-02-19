import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("borough", StringType(), True),
    StructField("zone", StringType(), True),
    StructField("service_zone", StringType(), True),
])

# Get arguments specifying GCS locations
GCS_BUCKET_PATH = sys.argv[1]
SRC_PATH = sys.argv[2]
DEST_PATH = sys.argv[3]


if __name__ == "__main__":

    spark = SparkSession.builder \
        .master("yarn") \
        .appName("Transform - Taxi Zone") \
        .getOrCreate()

    df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(schema) \
        .load(f"gs://{GCS_BUCKET_PATH}/raw/{SRC_PATH}")

    df_transformed = df.drop_duplicates().orderBy('id')

    # Write to GCS
    df_transformed.coalesce(1).write \
        .format("parquet") \
        .mode("overwrite") \
        .save(f"gs://{GCS_BUCKET_PATH}/transform/{DEST_PATH}")

    spark.stop()
