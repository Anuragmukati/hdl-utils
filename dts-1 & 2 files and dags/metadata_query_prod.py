from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.secret_manager import SecretsManagerHook
from airflow.utils.dates import days_ago
import pandas as pd
import zipfile
import os
import tempfile

BASE_QUERY = "SELECT * FROM vz-it-pr-gk1v-edwdo-0.edw_data_migration_ops_tbls.historical_load_metadata_table"
DEFAULT_EMAILS = ["abhijit.mahajan@verizon.com", "anurag.mukati@verizon.com"]

def access_secret_version(secret_id, secret_version="latest"):
    hook = SecretsManagerHook(gcp_conn_id="sa-vz-it-gk1v-edwdo-0-app")
    return hook.get_secret(secret_id=secret_id, secret_version=secret_version)

def run_query_and_email(**kwargs):
    config = kwargs['dag_run'].conf or {}

    select_sql = config.get("select_sql")
    where_clause = config.get("where_clause")
    recipient_email = config.get("email")

    if select_sql:
        query = select_sql
    elif where_clause:
        query = f"{BASE_QUERY} WHERE {where_clause}"
    else:
        raise ValueError("You must provide 'where_clause' in the config.")

    print(f"Running query: {query}")

    bq_hook = BigQueryHook(gcp_conn_id="sa-vz-it-gk1v-edwdo-0-app", use_legacy_sql=False)
    df = bq_hook.get_pandas_df(query)

    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = os.path.join(tmpdir, "result.csv")
        zip_path = os.path.join(tmpdir, "result.zip")

        df.to_csv(csv_path, index=False)

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(csv_path, arcname="result.csv")
        
        recipients = list(DEFAULT_EMAILS)
        if recipient_email:
            recipients.append(recipient_email)

        
        email_operator = EmailOperator(
            task_id='send_result_email',
            to=recipients,
            subject="BigQuery Query Result Metadata Table",
            html_content="<p>Attached zip</p>",
            files=[zip_path]
        )
        email_operator.execute(context=kwargs)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1
}

with DAG(
    dag_id='metadata_query_zip_email',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
) as dag:

    run_and_email_task = PythonOperator(
        task_id='run_query_and_send_email',
        python_callable=run_query_and_email,
        provide_context=True
    )
