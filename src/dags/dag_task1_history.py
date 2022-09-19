from airflow import DAG
from datetime import datetime, date, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from plugins.exchangerate_host_api_utils import (
    history_split_by_year, 
    csv_format_response_timeseries_insert_to_clickhouse, 
    HISTORY_START_DATE, 
    BASE, 
    CODE, 
    PRECISION, 
    HISTORY_LOAD
    )

YESTERDAY = date.today() - timedelta(1)
HISTORY_YEAR_RANGES = history_split_by_year(HISTORY_START_DATE, YESTERDAY)

default_args = {
    'owner': 'denis.patrakhin@gmail.com',
}

with DAG(
    dag_id='dag_exchangerate_host_api_history',
    description="historical exchange rates load from some date in the past and up until yesterday.",
    tags=['homework'],
    default_args=default_args,
    start_date=datetime.now(),
    schedule_interval='@once' if HISTORY_LOAD else None,
) as dag:

    ingest_start = EmptyOperator(task_id = "ingest_start")
    ingest_end = EmptyOperator(task_id = "ingest_end")
 
    for (start_date, end_date) in HISTORY_YEAR_RANGES:
        ingest_delta = SimpleHttpOperator(
            task_id=f"ingest_delta_{start_date}_{end_date}",
            http_conn_id='exchangerate_host_api',
            method='GET',
            endpoint=f"timeseries?start_date={start_date}&end_date={end_date}&base={BASE}&symbols={CODE}&format=CSV&places={PRECISION}",
            response_check=lambda response: 200 <= response.status_code < 299 and response.text,
            response_filter=csv_format_response_timeseries_insert_to_clickhouse,
            retries = 3,
            retry_delay = 30,
            dag=dag,
            )

        ingest_start >> ingest_delta >> ingest_end