import airflow
import datetime
from airflow import models
from airflow.models.taskinstance import TaskInstance as TI

from airflow.utils.state import State

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from airflow.operators.python_operator import PythonOperator

from airflow.decorators import dag, task, task_group
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from kubernetes.client import models as k8s
from airflow.providers.cncf.kubernetes.secret import Secret
from datetime import date, timedelta, timezone, tzinfo
from airflow.models import Variable

tables = []
def getMetadata():
    # Function to query BigQuery and retrieve metadata values
    hook = BigQueryHook(gcp_conn_id='sa-vz-it-gk1v-edwpr-0-app',use_legacy_sql=False)
    sql = """
    SELECT
    *
    FROM `vz-it-pr-gk1v-edwpr-0.udm_prdstg_tbls.pos_install_loan` limit 10
    """
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    tables = rows
    Variable.set(key="metadata", value=rows, serialize_json=True)


def getStatus(ti):
    
    print(Variable.get(key="metadata", deserialize_json=True))
    print(tables)


with models.DAG(
        dag_id="getMetaData",
        schedule_interval=None,
        start_date=airflow.utils.dates.days_ago(1),
        max_active_tasks=1,
        default_args={
            "retries": 1,
            "retry_delay": datetime.timedelta(seconds=10),
        },
) as dag:
    # Archiving the source file
    getMetaDataTask  = PythonOperator(
        task_id= 'getMetaDataTask',
        python_callable=getMetadata,
        retries = 1,
         do_xcom_push=True,
        retry_delay = timedelta(minutes=1)
    )

    # Archiving the source file
    getMetaDataTask1  = PythonOperator(
        task_id= 'getStatusTask',
        python_callable=getStatus,
        retries = 1,
        retry_delay = timedelta(minutes=1)
    )


