import sys
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql import Window

from rain_pipeline.scripts.utils import get_logger

SILVER_DIR = "/usr/local/airflow/data/silver/current_weather"
GOLD_DIR = "/usr/local/airflow/data/gold/current_weather"

# --- Logging ---
logger = get_logger("silver_to_gold_transform")


# --- Spark Session ---
def get_spark_session():
    """Create and return a Spark session."""
    spark = (
        SparkSession.builder
        .appName("openweather_silver_to_gold_transform")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    return spark


# --- Transformations ---
def transform_silver_to_gold(spark: SparkSession) -> None:
    """Transform silver weather data into gold dataset for ML (classification)."""
    start_time = datetime.now()
    logger.info("Starting Silver to Gold transformation...")

    try:
        df_silver = (
            spark.read
            .option("recursiveFileLookup", "true")
            .parquet(SILVER_DIR)
        )

        df_gold = df_silver.withColumn("hour", F.hour("timestamp"))

        # Window by place and order by timestamp
        window_spec = Window.partitionBy("place_id").orderBy("timestamp")

        df_gold = df_gold.withColumn(
            "rain_next_hour",
            F.lead("rain_1h").over(window_spec)
        )

        # Create rain label
        df_gold = df_gold.withColumn(
            "rain_label",
            F.when(F.col("rain_next_hour") > 0.0, F.lit("rain")).otherwise(F.lit("no_rain"))
        )

        # Add back partitioning columns
        df_gold = (
            df_gold
            .withColumn("date", F.to_date("timestamp"))
            .withColumn("place_name", F.regexp_replace(F.col("place_name"), r"[/\\\s:]", "_"))
        )

        # Drop rows with NULL labels (last record per place)
        df_gold = df_gold.dropna(subset=["place_id", "timestamp", "rain_label"])

        # Write Gold
        df_gold.repartition("date") \
            .write.mode("overwrite") \
            .partitionBy("date") \
            .parquet(GOLD_DIR)

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
    transform_silver_to_gold(spark)


if __name__ == "__main__":
    main()


# --- Additional Context ---

# In powershell:

# $env:PYSPARK_PYTHON="C:\Users\arith\Documents\Projects\uk-rain-prediction\.venv\Scripts\python.exe"
# $env:PYSPARK_DRIVER_PYTHON="C:\Users\arith\Documents\Projects\uk-rain-prediction\.venv\Scripts\python.exe"
# spark-submit de/spark/transforms/silver_to_gold_transform.py