from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.dates import days_ago
from airflow.models import Variable
import logging

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
}

with DAG(
    dag_id="dag_wls_hdl_summery_check",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    def run(**context):
        config = context['dag_run'].conf
        text = config.get("text")
        id = config.get("id")
        pr = config.get("pr")


        ple_project = pr
        ple_conn_id = id

        pl = BigQueryHook(gcp_conn_id=ple_conn_id, use_legacy_sql=False)
        plc=pl.get_client(project_id=ple_project)
        plc.query(text)


    run_task = PythonOperator(
        task_id="run_task",
        python_callable=run,
        provide_context=True,
    )
