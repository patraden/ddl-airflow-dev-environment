from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from elt.settings import DEFAULT_ARGS

ENV = "prod"
PRODUCT = "eu"
DATASET = "change_contact"
KAFKA_SERVERS = "10.166.0.24:9092,10.166.0.23:9092"
TOPIC = f"{ENV}.{PRODUCT}.emarsys.{DATASET}"
DATABRICKS_CONN = "databricks"
JOURNAL_CONN = "api_proxy_fbs_eu"
NOTEBOOK_PATH = f"/RELEASE/emarsys/ingest_kafka"
CLUSTER_TEMPLATE_PATH = "databricks/templates/E2_STANDARD4_SINGLE_NODE_CLUSTER.json-tpl"
GOOGLE_BUCKET = f"gs://databricks-emarsys-v2"
VALIDATE = True

with DAG(
        dag_id=f"emarsys_databricks_{PRODUCT}_{DATASET}",
        schedule_interval='0 0 * * *',
        description=f"daily ingest of kafka {TOPIC} topic to google storage",
        template_searchpath="dags",
        render_template_as_native_obj=True,
        default_args=DEFAULT_ARGS,
        tags=["databricks", "emarsys", ENV, PRODUCT],
        catchup=True,
) as dag:

    notebook_task = DatabricksSubmitRunOperator(
        task_id="notebook_task",
        json={
            'new_cluster': CLUSTER_TEMPLATE_PATH,
            'notebook_task': {
                "notebook_path": NOTEBOOK_PATH,
                "source": "WORKSPACE",
                "base_parameters": {
                    "env": ENV,
                    "product": PRODUCT,
                    "dataset": DATASET,
                    "gsBucket": GOOGLE_BUCKET,
                    "kafkaServers": KAFKA_SERVERS,
                    "startDate": "{{ data_interval_start.add(hours=3) | ds }}",
                    "endDate": "{{ data_interval_end.add(hours=3) | ds }}"
                }
            }
        },
        databricks_conn_id=DATABRICKS_CONN,
        polling_period_seconds=60,
        dag=dag
    )

    notebook_task_output = PythonOperator(
        task_id="notebook_task_output",
        op_kwargs={"run_id": """{{ ti.xcom_pull(task_ids="notebook_task", key="run_id") }}"""},
        python_callable=lambda run_id: DatabricksHook("databricks").get_run_output(run_id)["notebook_output"]["result"],
        dag=dag
    )

    journal_output = SimpleHttpOperator(
        task_id="journal_output",
        http_conn_id=JOURNAL_CONN,
        headers={"Content-Type": "application/json", "X-Request-ID": "databricks"},
        method="GET",
        data={
            "topic": TOPIC,
            "tsStart": """{{ data_interval_start.add(hours=3).to_datetime_string() }}""",
            "tsEnd": """{{ data_interval_end.add(hours=3).to_datetime_string() }}"""
        },
        endpoint=f"2/v1/emarsys/get-count-messages",
        response_check=lambda response: 200 <= response.status_code < 299 and response.text,
        response_filter=lambda response: response.json()["count"],
        retries=3,
        retry_delay=10,
        dag=dag,
    )

    validate = PythonOperator(
        task_id="validate_against_journal",
        op_kwargs={
            "jrl": """{{ ti.xcom_pull(task_ids="journal_output", key="return_value") }}""",
            "nbk": """{{ ti.xcom_pull(task_ids="notebook_task_output", key="return_value") }}"""
        },
        python_callable=lambda jrl, nbk: 1/0 if VALIDATE and int(jrl) != int(nbk) else int(jrl) - int(nbk),
        dag=dag
    )

    notebook_task >> notebook_task_output >> validate
    notebook_task >> journal_output >> validate
