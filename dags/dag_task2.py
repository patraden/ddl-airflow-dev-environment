import datetime
from pathlib import Path
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from extensions.utils import save_data_as_csv, STORAGE_MOUNT_POINT

# https://github.com/coder2j/airflow-docker/blob/main/dags/dag_with_postgres_hooks.py

TODAY = datetime.date.today()

default_args = {
    'owner': 'denis.patrakhin@gmail.com',
}

def postgres_to_csv(ti):
    hook = PostgresHook(postgres_conn_id="test_db")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("select count(*) from mt4.trades")
    save_data_as_csv(header=[i[0] for i in cursor.description], rows=cursor, filepath=Path(STORAGE_MOUNT_POINT) / "task1.csv")
    cursor.close()
    conn.close()

with DAG(
    dag_id="dag_test",
    description="testing queries",
    tags=["homework"],
    default_args=default_args,
    start_date=datetime.datetime(TODAY.year, TODAY.month, TODAY.day),
    schedule_interval="@once",
) as dag:
    task1 = PythonOperator(
        task_id="postgres_to_csv",
        python_callable=postgres_to_csv
    )
    task1