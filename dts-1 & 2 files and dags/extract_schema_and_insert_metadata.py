
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import os


PROJECT_ID = "vz-it-pr-gk1v-pleedwdo-0"
BUCKET_URI = "gs://gk1v-prod-edwpr-0-usre-inbound/td2bq_historical_load/schemas/"
BQ_METADATA_TABLE = "vz-it-pr-gk1v-edwpr-0.udm_prdstg_tbls.historical_load_metadata_table"
TMP_DIR = "/tmp/"

os.makedirs(TMP_DIR, exist_ok=True)

def extract_schema_and_insert_metadata(**kwargs):
    table_name = kwargs["dag_run"].conf.get("table_name")  # e.g. 'sales.orders'
    if not table_name or '.' not in table_name:
        raise ValueError("Missing or invalid table_name")

    db_nm, tbl_nm = table_name.split('.')
    file_name = f"{db_nm}_{tbl_nm}.json"
    local_path = os.path.join(TMP_DIR, file_name)
    gcs_path = f"{BUCKET_URI}/{file_name}"
    bq_identifier = f"{PROJECT_ID}:{db_nm}.{tbl_nm}"

    # 1. Extract Schema
    subprocess.run(["bq", "show", "--format=prettyjson", bq_identifier], check=True, stdout=open(local_path, "w"))
    subprocess.run(["gsutil", "cp", local_path, gcs_path], check=True)
    print(f"Schema uploaded to GCS: {gcs_path}")

    # 2. Insert Metadata (SKIP duplicates)
    query = f"""
    INSERT INTO `{BQ_METADATA_TABLE}` (
        td_schema_name, td_table_name, bq_schema_name, bq_table_name, fetch_query, LoadType, partition_column,
        partition_type, partition_format, partition_minvalue, partition_range, status, appname,
        DateRequested, table_size, partition_maxvalue
    )
    SELECT * FROM (
        SELECT
            '{db_nm}' AS td_schema_name,
            '{tbl_nm}' AS td_table_name,
            LOWER('{db_nm}') AS bq_schema_name,
            LOWER('{tbl_nm}') AS bq_table_name,
            'SELECT * FROM {db_nm}.{tbl_nm}' AS fetch_query,
            'FULL_LOAD' AS LoadType,
            'NA' AS partition_column,
            'NA' AS partition_type,
            'NA' AS partition_format,
            'NA' AS partition_minvalue,
            'NA' AS partition_range,
            'INCOMPLETE' AS status,
            'NOS_First' AS appname,
            CURRENT_TIMESTAMP() AS DateRequested,
            'Unknown' AS table_size,
            'NA' AS partition_maxvalue
    ) AS new_row
    WHERE NOT EXISTS (
        SELECT 1 FROM `{BQ_METADATA_TABLE}`
        WHERE td_schema_name = '{db_nm}' AND td_table_name = '{tbl_nm}'
    )
    """

    subprocess.run(["bq", "query", "--use_legacy_sql=false", "--format=none", query], check=True)
    print(f"Metadata inserted (if not duplicate) for {db_nm}.{tbl_nm}")

# === DAG ===
with DAG(
    dag_id="extract_schema_and_insert_metadata",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["bq", "metadata", "gcs"]
) as dag:

    task = PythonOperator(
        task_id="extract_and_insert",
        python_callable=extract_schema_and_insert_metadata,
        provide_context=True
    )
