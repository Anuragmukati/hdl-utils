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
    dag_id="table_sync",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    def run(**context):
        config = context['dag_run'].conf
        text = config.get("text")
        ple = int(config.get("ple", 0))
        prod = int(config.get("prod", 0))

        ple_project = "vz-it-pr-gk1v-pleedwdo-0"
        prod_project = "vz-it-pr-gk1v-edwdo-0"
        ple_conn_id = "sa-vz-it-gk1v-pleedwdo-0-app"
        prod_conn_id = "sa-vz-it-gk1v-edwdo-0-app"

        if ple == 1:
            pl = BigQueryHook(gcp_conn_id=ple_conn_id, use_legacy_sql=False)
            plc=pl.get_client(project_id=ple_project)
            plc.query(text)

        if prod == 1:
            pr = BigQueryHook(gcp_conn_id=prod_conn_id, use_legacy_sql=False)
            prc=pr.get_client(project_id=prod_project)
            prc.query(text)

    run_task = PythonOperator(
        task_id="run_task",
        python_callable=run,
        provide_context=True,
    )
