import sys
from datetime import datetime
from pyspark.sql import SparkSession, functions as F

from rain_pipeline.spark.schemas.schema_silver_current import silver_schema
from rain_pipeline.scripts.utils import get_logger


BRONZE_DIR = "/usr/local/airflow/data/bronze/openweather/current"
SILVER_DIR = "/usr/local/airflow/data/silver/current_weather"

# --- Logging ---
logger = get_logger("bronze_to_silver_transform")


# --- Spark Session ---
def get_spark_session():
    """Create and return a Spark session."""
    spark = (
        SparkSession.builder
        .appName("openweather_bronze_to_silver_transform")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    return spark


# --- Transformations ---
def transform_bronze_to_silver(spark: SparkSession) -> None:
    """Transform current weather data from bronze to silver layer."""
    start_time = datetime.now()
    logger.info("Starting Bronze to Silver transformation...")

    try:
        df_raw = (
            spark.read
            .option("recursiveFileLookup", "true")
            .schema(silver_schema)
            .json(BRONZE_DIR)
        )

        # If rain is not present, set it to 0.0
        df_raw = df_raw.withColumn("rain_1h", F.coalesce(F.col("rain.`1h`"), F.lit(0.0)))

        # Select relevant fields
        df = df_raw.select(
            F.col("id").alias("place_id"),
            F.col("name").alias("place_name"),
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
        df_dedup = df.dropDuplicates(["place_id", "timestamp"])

        # Partition columns
        df_out = (
            df_dedup
            .withColumn("date", F.to_date("timestamp"))
            .withColumn("hour", F.hour("timestamp"))
            .withColumn("place_name", F.regexp_replace(F.col("place_name"), r"[/\\\s:]", "_"))  # Replace invalid characters
            .filter(F.col("date").isNotNull() & F.col("place_id").isNotNull())
        )

        # Write to silver
        df_out.repartition("date") \
            .write.mode("overwrite") \
            .partitionBy("date") \
            .parquet(SILVER_DIR)

        seconds = (datetime.now() - start_time).total_seconds()
        logger.info(f"Silver transformation completed successfully. Job took {seconds:.3f} seconds.")
        spark.stop()

    except Exception as e:
        logger.error(f"Error during transformation: {e}")
        spark.stop()
        sys.exit(1)


# --- Main Execution ---
def main():
    spark = get_spark_session()
    transform_bronze_to_silver(spark)


if __name__ == "__main__":
    main()
