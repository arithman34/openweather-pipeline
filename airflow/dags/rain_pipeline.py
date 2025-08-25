from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

from rain_pipeline.ingestion.fetch_current import main as fetch_current


# --- DAG Definition ---
with DAG(
    dag_id="rain_pipeline",
    description="End-to-end pipeline: ingestion -> bronze_to_silver -> silver_to_gold",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["rain", "spark", "pipeline"],
) as dag:

    # Ingest bronze data
    ingest = PythonOperator(
        task_id="ingest_current",
        python_callable=fetch_current
    )

    # Bronze -> Silver transform
    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application="rain_pipeline/spark/transforms/bronze_to_silver_transform.py",
        conn_id="spark_default"
    )

    # Silver -> Gold transform
    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application="rain_pipeline/spark/transforms/silver_to_gold_transform.py",
        conn_id="spark_default"
    )

    # Task ordering
    ingest >> bronze_to_silver >> silver_to_gold
