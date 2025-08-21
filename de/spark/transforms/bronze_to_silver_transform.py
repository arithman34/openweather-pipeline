import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

import datetime
from pyspark.sql import SparkSession, functions as F

from de.spark.schemas.schema_silver_current import silver_schema
from utils.openweather import get_logger


BRONZE_DIR = "data/bronze/openweather/current"
SILVER_DIR = "data/silver/current_weather"

#  --- Logging ---
logger = get_logger(__name__)


# --- Spark Session ---
def get_spark_session():
    """Create and return a Spark session."""
    spark = (
        SparkSession.builder
        .appName("openweather_transform_current_weather")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    return spark


# --- Transformations ---
def transform_current_weather(spark: SparkSession) -> None:
    """Transform current weather data from bronze to silver layer."""
    start_time = datetime.datetime.now()
    logger.info("Starting transformation of current weather data...")
    try:
        df_raw = (
            spark.read
            .option("recursiveFileLookup", "true")
            .schema(silver_schema)
            .json(BRONZE_DIR)
        )

        # If rain is not present, set it to 0.0
        df_raw = df_raw.withColumn("rain_1h",F.coalesce(F.col("rain.`1h`"), F.lit(0.0)))
        
        df = df_raw.select(
            F.col("id").alias("city_id"),
            F.col("name").alias("city_name"),
            F.col("sys.country").alias("country"),
            F.col("coord.lat").alias("latitude"),
            F.col("coord.lon").alias("longitude"),
            F.from_unixtime("dt").cast("timestamp").alias("timestamp"),
            F.col("clouds.all").alias("cloud_cover"),
            F.col("main.temp").alias("temp"),
            F.col("main.pressure").alias("pressure"),
            F.col("main.humidity").alias("humidity"),
            F.col("wind.speed").alias("wind_speed"),
            F.col("rain_1h").cast("double").alias("rain_1h")
        )

        # De-duplication
        df_dedup = df.dropDuplicates(["city_id", "timestamp"])

        # Partition columns
        df_out = (
            df_dedup
            .withColumn("date", F.to_date("timestamp"))
            .withColumn("hour", F.hour("timestamp"))
            .withColumn("city_name", F.regexp_replace(F.col("city_name"), r"[/\\\s:]", "_"))  # Replace invalid characters
            .filter(F.col("date").isNotNull() & F.col("city_name").isNotNull())
        )

        # Write to silver
        df_out.repartition("date", "city_name") \
            .write.mode("overwrite") \
            .partitionBy("date", "city_name") \
            .parquet(SILVER_DIR)
        
        logger.info(f"Transformation completed successfully. Job completed in {(datetime.datetime.now() - start_time).total_seconds():.2f} seconds.")
    except Exception as e:
        logger.error(f"Error during transformation: {e}")
        sys.exit(1)
    
    finally:
        spark.stop()


# --- Main Execution ---
def main():
    spark = get_spark_session()
    transform_current_weather(spark)


if __name__ == "__main__":
    main()
