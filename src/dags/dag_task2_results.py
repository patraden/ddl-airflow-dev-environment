import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from exchangerate_host_api_utils import postgres_dql_to_csv, read_text_file, STORAGE_MOUNT_POINT

TODAY = datetime.date.today()
default_args = {
    'owner': 'denis.patrakhin@gmail.com'
    }

with DAG(
    dag_id="dag_task2_results",
    description="generates csv files requested in the 2nd task",
    tags=["homework"],
    default_args=default_args,
    start_date=datetime.datetime(TODAY.year, TODAY.month, TODAY.day),
    schedule_interval="@once",
) as dag:
    
    task2_sub12 = PythonOperator(
        task_id="task2_subtask12",
        op_kwargs={"sql" : read_text_file("dags/templates/task2-sub12.sql"), "filename": "task2_subtask12.csv"},
        python_callable=postgres_dql_to_csv
        )
    
    task2_sub3 = PythonOperator(
        task_id="task2_subtask3",
        op_kwargs={"sql" : read_text_file("dags/templates/task2-sub3.sql"), "filename": "task2_subtask3.csv"},
        python_callable=postgres_dql_to_csv
        )
    
    task2_sub12 >> task2_sub3