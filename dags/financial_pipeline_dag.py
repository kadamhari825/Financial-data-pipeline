"""
Financial pipeline DAG — runs daily at 22:00 UTC, processes T-1 date.

Three tasks executed via BashOperator:
  1. extract  — Yahoo Finance → GCS
  2. load     — GCS → fin_raw.stock_prices_raw + dq_results
  3. transform — fin_raw → fin_curated (dim_company, fact_daily_prices, fact_weekly_summary)

Each task runs as a standalone Python script via the Airflow venv. BashOperator
chosen over PythonOperator deliberately: each task starts a fresh process and
releases its memory on exit, which matters on a 1 GB VM.
"""
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Paths on the VM — set these in the environment before starting Airflow
PROJECT_DIR = os.environ["PIPELINE_PROJECT_DIR"]
PYTHON_BIN = os.environ["PIPELINE_PYTHON_BIN"]
SA_KEY = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

# Default args applied to every task
default_args = {
    "owner": "hari",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email": [os.environ["PIPELINE_ALERT_EMAIL"]],
    "email_on_failure": True,
    "email_on_retry": False,
}

with DAG(
    dag_id="financial_pipeline",
    description="Daily SP500 ingest: Yahoo → GCS → BigQuery raw → curated",
    default_args=default_args,
    start_date=datetime(2026, 2, 1),
    schedule_interval="0 22 * * 1-5",  # 22:00 UTC Mon-Fri (execution_date = trading day)
    catchup=False,
    max_active_runs=1,                 # only one run at a time on a 1 GB VM
    tags=["financial", "yahoo", "bigquery"],
) as dag:

    common_env = (
        f"export GOOGLE_APPLICATION_CREDENTIALS={SA_KEY} && "
        f"cd {PROJECT_DIR} && "
    )

    extract = BashOperator(
        task_id="extract_from_yahoo",
        bash_command=common_env + f"{PYTHON_BIN} extract_from_yahoo.py --date {{{{ ds }}}}",
    )

    load = BashOperator(
        task_id="load_to_bigquery",
        bash_command=common_env + f"{PYTHON_BIN} load_to_bigquery.py --date {{{{ ds }}}}",
    )

    transform = BashOperator(
        task_id="transform_curated",
        bash_command=common_env + f"{PYTHON_BIN} transform_curated.py --date {{{{ ds }}}}",
    )

    extract >> load >> transform
