from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "leo",
    "depends_on_past": False,
    "retries": 1
}

with DAG(
    dag_id="abs_building_approvals_pipeline",
    default_args=default_args,
    description="ABS building approvals lakehouse pipeline",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["data-engineering", "abs", "lakehouse"]
) as dag:

    unzip_abs = BashOperator(
        task_id="unzip_abs_zip",
        bash_command="cd /opt/airflow/project && python3 src/ingestion/unzip_abs_data.py"
    )

    ingest_abs_batch = BashOperator(
        task_id="ingest_abs_batch",
        bash_command="cd /opt/airflow/project && python3 src/ingestion/fetch_abs_building_data_batch.py"
    )

    clean_abs_batch = BashOperator(
        task_id="clean_abs_batch",
        bash_command="cd /opt/airflow/project && python3 src/etl/clean_abs_building_data_batch.py"
    )

    build_gold_batch = BashOperator(
        task_id="build_gold_batch",
        bash_command="cd /opt/airflow/project && python3 src/transform/build_abs_gold_table_batch.py"
    )

    quality_check_batch = BashOperator(
        task_id="quality_check_batch",
        bash_command="cd /opt/airflow/project && python3 src/quality/check_abs_gold_batch.py"
    )

    load_to_postgres_batch = BashOperator(
        task_id="load_to_postgres_batch",
        bash_command="cd /opt/airflow/project && python3 src/transform/load_abs_gold_batch_to_postgres.py"
    )

    unzip_abs >> ingest_abs_batch >> clean_abs_batch >> build_gold_batch >> quality_check_batch >> load_to_postgres_batch
