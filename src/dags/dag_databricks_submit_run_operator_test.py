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
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'd.patrakhin@ddl.com',
}

with DAG(
        dag_id="databricks_submit_run_operator_dag_v3",
        schedule_interval='@once',
        description="databricks submit run operator test",
        start_date=datetime(2022, 10, 23),
        tags=['databricks-submit-run-test'],
        catchup=False,
) as dag:
    # [START howto_operator_databricks_json]
    # Example of using the JSON parameter to initialize the operator.
    new_cluster = {
        "num_workers": 0,
        "cluster_name": "",
        "spark_version": "10.4.x-scala2.12",
        "spark_conf": {
            "spark.hadoop.google.cloud.auth.service.account.enable": "true",
            "spark.hadoop.fs.gs.project.id": "fbs-ga360",
            "spark.master": "local[*, 4]",
            "spark.hadoop.fs.gs.auth.service.account.email": "databricks-emarsys@fbs-ga360.iam.gserviceaccount.com",
            "spark.databricks.cluster.profile": "singleNode"
        },
        "gcp_attributes": {
            "use_preemptible_executors": "false",
            "availability": "ON_DEMAND_GCP",
            "zone_id": "HA"
        },
        "node_type_id": "e2-standard-4",
        "driver_node_type_id": "e2-standard-4",
        "ssh_public_keys": [],
        "custom_tags": {
            "ResourceClass": "SingleNode"
        },
        "spark_env_vars": {},
        "enable_elastic_disk": "true",
        "cluster_source": "JOB",
        "init_scripts": [],
        "data_security_mode": "LEGACY_SINGLE_USER_STANDARD",
        "runtime_engine": "STANDARD"
    }

    notebook_task_params = {
        'new_cluster': new_cluster,
        'notebook_task': {
            'notebook_path': '/Users/d.patrakhin@fbs-m.com/airflow-integration-test',
            "source": "WORKSPACE",
            "base_parameters": {
                "note_param1": "42",
                "note_param2": "24"
            }
        },
    }

    notebook_task = DatabricksSubmitRunOperator(
        task_id='notebook_task',
        json=notebook_task_params,
        databricks_conn_id="databricks",
        polling_period_seconds=60
    )

    get_job_run_out = PythonOperator(
        task_id="get_job_run_out",
        op_kwargs={"run_id": """{{ ti.xcom_pull(task_ids="notebook_task", key="run_id") }}"""},
        python_callable=lambda run_id: DatabricksHook("databricks").get_run_output(run_id)
    )

    notebook_task >> get_job_run_out
