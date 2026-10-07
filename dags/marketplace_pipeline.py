from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "arlen",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="marketplace_pipeline",  
    default_args=default_args,
    description="Pipeline de engenharia de dados para marketplace",
    schedule=None,
    catchup=False,
    tags=["data-engineering"],
) as dag:

    ingest_api = BashOperator(
        task_id="ingest_api",
        bash_command="python /opt/airflow/data/ingestion/ingest_api.py",
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="python /opt/airflow/data/spark_jobs/bronze_to_silver.py",
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command="python /opt/airflow/data/spark_jobs/silver_to_gold.py",
    )

    load_gold_to_postgres = BashOperator(
        task_id="load_gold_to_postgres",
        bash_command="python /opt/airflow/data/database/load_gold_to_postgres.py",
    )

    ingest_api >> bronze_to_silver >> silver_to_gold >> load_gold_to_postgres