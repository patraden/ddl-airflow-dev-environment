"""
This is an example DAG which uses the DatabricksSubmitRunOperator.
In this example, we create two tasks which execute sequentially.
The first task is to run a notebook at the workspace path "/test"
and the second task is to run a JAR uploaded to DBFS. Both,
tasks use new clusters.
Because we have set a downstream dependency on the notebook task,
the spark jar task will NOT run until the notebook task completes
successfully.
The definition of a successful run is if the run has a result_state of "SUCCESS".
For more information about the state of a run refer to
https://docs.databricks.com/api/latest/jobs.html#runstate
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from datetime import datetime

default_args = {
    'owner': 'd.patrakhin@ddl.com',
    'op_args': ["databricks"],
}

with DAG(
        dag_id="databricks_hook_test_dag_v4",
        schedule_interval='@once',
        description="databricks hook test",
        default_args=default_args,
        start_date=datetime(2022, 10, 23),
        tags=['databricks-hook-test'],
        catchup=False,
) as dag:
    list_jobs = PythonOperator(
        task_id="list_jobs",
        python_callable=lambda conn_db: DatabricksHook(conn_db).list_jobs()
    )

    get_job_run_id = PythonOperator(
        task_id="get_job_run_id",
        python_callable=lambda conn_db: DatabricksHook(conn_db).get_run_state_str(run_id=256402)
    )

    get_job_run_out = PythonOperator(
        task_id="get_job_run_out",
        python_callable=lambda conn_db: DatabricksHook(conn_db).get_run_output(run_id=254585)
    )
