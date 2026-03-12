import logging
import tempfile
from pathlib import Path

import duckdb
import pendulum
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Конфигурация DAG
OWNER = "v.kim"
DAG_ID = "raw_to_s3"

# Используемые таблицы в DAG
LAYER = "raw"
SOURCE = "f1"

# S3
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

LONG_DESCRIPTION = """
# LONG DESCRIPTION
"""

SHORT_DESCRIPTION = "SHORT DESCRIPTION"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 2, 15, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 0,
    "retry_delay": pendulum.duration(minutes=1),
}


def get_dates(**context) -> str:
    """"""
    date = context["data_interval_start"].format("YYYY-MM-DD")

    return date

def get_and_transfer_api_data_to_s3(**context):
    """"""

    date = get_dates(**context)
    logging.info(f"💻 Start load for dates: {date}")
    
    # Fetch data from API with proper headers
    url = f'https://api.openf1.org/v1/sessions?date_start={date}&csv=true'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    
    # Write CSV to temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp:
        tmp.write(response.text)
        csv_path = tmp.name
    
    try:
        # Load data into DuckDB and push to S3
        con = duckdb.connect()

        con.sql(
            f"""
            SET TIMEZONE='UTC+3';
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_url_style = 'path';
            SET s3_endpoint = 'minio:9000';
            SET s3_access_key_id = '{ACCESS_KEY}';
            SET s3_secret_access_key = '{SECRET_KEY}';
            SET s3_use_ssl = FALSE;

            COPY
            (
                SELECT
                    *
                FROM
                    read_csv_auto('{csv_path}') AS res
            ) TO 's3://lakehouse/{LAYER}/{SOURCE}/{date}/sessions_{date}.gz.parquet';

            """,
        )

        con.close()
        logging.info(f"✅ Download for date success: {date}")
    finally:
        # Clean up temporary file
        Path(csv_path).unlink(missing_ok=True)


with DAG(
    dag_id=DAG_ID,
    schedule="59 23 * * 1,5,6,7",
    default_args=args,
    tags=["s3", "raw"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    get_and_transfer_api_data_to_s3 = PythonOperator(
        task_id="get_and_transfer_api_data_to_s3",
        python_callable=get_and_transfer_api_data_to_s3,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> get_and_transfer_api_data_to_s3 >> end