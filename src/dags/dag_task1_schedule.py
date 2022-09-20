from airflow import DAG
from datetime import datetime, date
from airflow.operators.http_operator import SimpleHttpOperator
from exchangerate_host_api_utils import csv_response_insert_into_dwh, BASE, CODE, PRECISION

TODAY = date.today()

default_args = {
    'owner': 'denis.patrakhin@gmail.com',
}

with DAG(
    dag_id="dag_exchangerate_host_api_schedule",
    description="scheduled exchnage rates data loads",
    tags=["homework"],
    default_args=default_args,
    start_date=datetime(TODAY.year, TODAY.month, TODAY.day),
    schedule_interval="0 */3 * * *",
    catchup=True,
) as dag:

    ingest = SimpleHttpOperator(
        task_id="ingest",
        http_conn_id="exchangerate_host_api",
        method="GET",
        endpoint=f"latest?&base={BASE}&symbols={CODE}&format=CSV&places={PRECISION}",
        response_check=lambda response: 200 <= response.status_code < 299 and response.text,
        response_filter=csv_response_insert_into_dwh,
        retries = 3,
        retry_delay = 15,
        dag=dag,
        )