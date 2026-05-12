from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from pathlib import Path


def validate_outputs_exist() -> None:
    required_paths = [
        Path("/opt/airflow/src/data/processed/spark_cleaned_reviews"),
        Path("/opt/airflow/src/data/metrics/spark_metrics"),
    ]

    missing = [str(path) for path in required_paths if not path.exists()]

    if missing:
        raise FileNotFoundError(f"Missing expected outputs: {missing}")


with DAG(
    dag_id="snackshack_training_data_pipeline",
    description="Training data quality pipeline for SnackShack review data",
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    tags=["data-quality", "pyspark", "training-data"],
) as dag:

    generate_raw_data = BashOperator(
        task_id="generate_raw_data",
        bash_command="cd /opt/airflow && python -m src.generate_data",
    )

    run_spark_pipeline = BashOperator(
        task_id="run_spark_pipeline",
        bash_command="cd /opt/airflow && python -m src.spark_pipeline",
    )

    validate_outputs = PythonOperator(
        task_id="validate_outputs_exist",
        python_callable=validate_outputs_exist,
    )

    generate_raw_data >> run_spark_pipeline >> validate_outputs