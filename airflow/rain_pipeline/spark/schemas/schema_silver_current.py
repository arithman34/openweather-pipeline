from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, LongType
)


silver_schema = StructType([
    StructField("coord", StructType([
        StructField("lon", DoubleType(), True),
        StructField("lat", DoubleType(), True),
    ]), True),

    StructField("main", StructType([
        StructField("temp", DoubleType(), True),
        StructField("pressure", IntegerType(), True),
        StructField("humidity", IntegerType(), True),
    ]), True),


    StructField("wind", StructType([
        StructField("speed", DoubleType(), True),
    ]), True),

    StructField("clouds", StructType([
        StructField("all", IntegerType(), True),
    ]), True),

    StructField("rain", StructType([
        StructField("1h", DoubleType(), True),
    ]), True),

    StructField("dt", LongType(), True),

    StructField("sys", StructType([
        StructField("country", StringType(), True),
    ]), True),

    StructField("id", LongType(), True),  # city id
    StructField("name", StringType(), True),
])
