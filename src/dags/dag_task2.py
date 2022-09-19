import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils import postgres_dql_to_csv, load_text_file, STORAGE_MOUNT_POINT

# https://github.com/coder2j/airflow-docker/blob/main/dags/dag_with_postgres_hooks.py

TODAY = datetime.date.today()

default_args = {
    'owner': 'denis.patrakhin@gmail.com'
    }

with DAG(
    dag_id="dag_test_v2",
    description="testing queries",
    tags=["homework"],
    default_args=default_args,
    start_date=datetime.datetime(TODAY.year, TODAY.month, TODAY.day),
    schedule_interval="@once",
) as dag:
    
    task2_sub12 = PythonOperator(
        task_id="task2_subtask12",
        op_kwargs={
            "sql" : load_text_file("dags/templates/task2-sub12.sql"),
            "filename": "task2_subtask12.csv"
            },
        python_callable=postgres_dql_to_csv
    )
    
    task2_sub3 = PythonOperator(
        task_id="task2_subtask3",
        op_kwargs={
            "sql" : load_text_file("dags/templates/task2-sub3.sql"),
            "filename": "task2_subtask3.csv"
            },
        python_callable=postgres_dql_to_csv
    )
    
    task2_sub12
    task2_sub3