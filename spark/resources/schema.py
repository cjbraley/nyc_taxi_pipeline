from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema_taxi_zone = StructType([
    StructField("id", IntegerType(), False),
    StructField("borough", StringType(), True),
    StructField("zone", StringType(), True),
    StructField("service_zone", StringType(), True),
])
