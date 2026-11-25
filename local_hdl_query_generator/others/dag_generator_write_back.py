import json
from datetime import datetime, timezone, timedelta
from airflow import DAG
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def build_select_query_and_columns(client, dataset_id, table_id):
    table_ref = f"{dataset_id}.{table_id}"
    table = client.get_table(table_ref)

    select_exprs = []
    column_names = []

    for field in table.schema:
        col = field.name
        dtype = field.field_type.upper()
        column_names.append(col)

        if dtype == "STRING":
            expr = f"{col}"
        elif dtype == "DATE":
            expr = f"FORMAT_DATE('%Y-%m-%d', {col}) AS {col}"
        elif dtype == "DATETIME":
            expr = f"FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', {col}) AS {col}"
        elif dtype == "TIMESTAMP":
            expr = f"FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', {col}) AS {col}"
        elif dtype in ("INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC"):
            expr = f"SAFE_CAST({col} AS STRING) AS {col}"
        else:
            expr = f"SAFE_CAST({col} AS STRING) AS {col}"

        select_exprs.append(expr)

    query = f"SELECT {', '.join(select_exprs)} FROM `{table_ref}`"
    comma_separated_cols = ", ".join(column_names)
    return query, comma_separated_cols


def generate_jsons_and_store(**context):

    project_id = context["params"].get("project_id", "dmgcp-del-110")
    composer = context["params"].get("composer", "vz-it-pr-wdwg-aidcom-0-cc-edw-dts-1")
    bucket_name = context["params"].get("bucket_name", "tpt-metadata-jsons")
    blob_path=context["params"].get("blob_path", "vz-it-gk1v-edwdo-0/wireless/writeback_metadata")
    min = context["params"].get("min_sessions", "30")
    max = context["params"].get("max_sessions", "40")

    tables = context["dag_run"].conf.get("tables", [])
    functional_id = context["dag_run"].conf.get("functional_id")
    if not tables:
        raise ValueError("You must provide { 'tables': ['dataset.table', ...] } in dag_run.conf")

    bq_hook = BigQueryHook(gcp_conn_id="sa-vz-it-gk1v-edwdo-0-app", use_legacy_sql=False)
    gcs_hook = GCSHook(gcp_conn_id="sa-vz-it-gk1v-edwdo-0-app")
    client = bq_hook.get_client(project_id="vz-it-pr-gk1v-edwdo-0",location="US")
    
    files_uploaded=[]

    for tbl in tables:
        try:
            if "." not in tbl:
                raise ValueError(f"Invalid table format: {tbl}, expected dataset.table")
            dataset_id, table_id = tbl.split(".", 1)

            query, cols = build_select_query_and_columns(client, dataset_id, table_id)

            metadata_query = f"""
                SELECT * FROM edw_data_migration_audit_tbls.edw_mig_bq_to_td_writeback_metadata  WHERE bq_table_name = '{tbl}'
            """
            metadata = list(client.query(metadata_query).result())
            if not metadata:
                print(f"No metadata found for table: {table_id}")
                continue

            meta = metadata[0]
            bq_table_name=meta.bq_table_name
            bq_dataset=bq_table_name.split(".")[0]
            bq_table=bq_table_name.split(".")[1]
            td_table_name = meta.td_table_name
            td_dataset=td_table_name.split(".")[0]
            td_table=td_table_name.split(".")[1]
            td_database_exists = meta.td_database_exists
            td_table_exists = meta.td_table_exists
            load_type = meta.load_type
            load_functionality = meta.load_functionality
            batch = meta.batch
            priority = meta.priority
            table_owner = meta.table_owner
            functional_id = meta.function_id or "mkt_aid_p2"

            row = {
                "Name": f"{dataset_id}_{table_id}",
                "Type": "Batch",
                "Start_Date": str(datetime.now(timezone.utc) - timedelta(days=1)),
                "composer": composer,
                "Schedule_interval": "None",
                "max_active_runs": "1",
                "Timezone": "EST",
                "Catchup": "False",
                "custom_tags": [
                    "test:optimization",
                    "lob:vcg",
                    "sub_lob:wireless",
                    "module:writeback",
                    "load_type:full_load",
                    f"owner:{table_owner}"
                ],
                "Project_space": "vz-it-gk1v-edwdo-0",
                "Steps": [
                    {
                        "Name": "Start",
                        "Id": "start",
                        "Description": "This stage indicates the starting point of the task relationship in the dag",
                        "OkTo": "bq_to_gcs"
                    },
                    {
                        "Name": "bq_to_gcs",
                        "Id": "bq_to_gcs",
                        "Description": "bq to gc",
                        "Type": "run sql query from file",
                        "Params": {
                            "hook": {
                                "gcp_conn_id": "gcp_conn_id_gk1v-edwdo",
                                "query": f"CREATE TEMP TABLE {bq_dataset}_{bq_table} AS {query}; EXPORT DATA OPTIONS (uri = 'gs://gk1v-prod-edwdo-0-usre-outbound/{bq_table}/*.csv', format = 'CSV', overwrite = true, header = false, field_delimiter = '^') AS SELECT * FROM  {bq_dataset}_{bq_table};",
                                "projectId": "projectId",
                                "useLegacySql": "false"
                                },
                            "obs": {
                                "sub_process": "bqTransformLoad",
                                "keyvalue": "NA",
                                "project_name": "EDW_MODERNIZATION",
                                "source_type": "Table",
                                "source_name": f"{bq_dataset}.{bq_table}",
                                "target_name": f"{td_dataset}.{td_table}",
                                "target_type": "Table",
                                "tool_name": "GCP",
                                "source_environment": "GCP_BQ",
                                "source_servername": "bq_servername",
                                "target_environment": "GCP_BQ",
                                "target_servername": "bq_servername",
                                "scheduler": "airflow",
                                "system_name": "consumer_wls_ingestion",
                                "logger_name": "dof_gcp_process_log",
                                "rerun_indicator": "NA"
                            }
                        },
                        "OkTo": "deleteTDtgtTable"
                    },
                    {
                        "Name": "deleteTDtgtTable",
                        "Id": "deleteTDtgtTable",
                        "Description": "delete the target table before data load",
                        "Type": "run jdbc delete query",
                        "Params": {
                            "hook": {
                                "parameters": {
                                    "target_dbtype": "teradata",
                                    "save_mode": "overwrite",
                                    "dbinstance": "mkt_aid_p2",
                                    "query": f"CREATE TABLE WIRELESS_WRITEBACK.{td_table}_temp as WIRELESS_WRITEBACK.{td_table} with no data;DROP TABLE WIRELESS_WRITEBACK.{td_table};RENAME TABLE WIRELESS_WRITEBACK.{td_table}_temp to WIRELESS_WRITEBACK.{td_table};",
                                    "OpsgeniePriority": "None"
                                }
                            },
                            "obs": {
                                "gcp_project": "gk1v_project_id",
                                "sub_process": "deleteTDtgtTable",
                                "keyvalue": "NA",
                                "project_name": "consumer_jdbc_ingestion",
                                "source_type": "Table",
                                "source_name": f"{td_table}",
                                "target_name": f"{td_table}",
                                "target_type": "Table",
                                "tool_name": "EDW_TD",
                                "source_environment": "EDW_TD",
                                "source_servername": "teradata_servername",
                                "target_environment": "EDW_TD",
                                "target_servername": "teradata_servername",
                                "scheduler": "airflow",
                                "system_name": "consumer_wls_bq2td",
                                "logger_name": "dof_gcp_process_log",
                                "rerun_indicator": "NA"
                            }
                        },
                        "OkTo": "gcs_to_td_load"
                    },
                    {
                        "Name": "gcs_to_td_load",
                        "Id": "gcs_to_td_load",
                        "Description": "GCS to TD target load",
                        "Type": "load from gcs to td",
                        "Params": {
                            "hook": {
                                "ssh_conn_id": "ssh_conn_id",
                                "remote_host": "tpt_remote_host",
                                "dbinstance": f"{functional_id}",
                                "delimiter": "^",
                                "min_sessions": f"{min}",
                                "max_sessions": f"{max}",
                                "skiprows": "0",
                                "gcp_conn_id": "gcp_conn_id_gk1v-edwdo",
                                "source_gcs_bucket": "tpt_source_bucket",
                                "source_gcs_path": f"{bq_table}/*.csv",
                                "target_dbname": "wireless_writeback",
                                "target_table": f"{td_table}",
                                "final_target_table": f"{td_table}",
                                "target_log_dbname": "wireless_writeback",
                                "audit_table_db": "wireless_writeback",
                                "OpsgeniePriority": "None",
                                "allow_quoted_newlines": "true",
                                "allow_jagged_rows": "true"
                            },
                            "obs": {
                                "gcp_project": "gk1v_project_id",
                                "sub_process": "gcs_to_td_load",
                                "keyvalue": "NA",
                                "project_name": "consumer_jdbc_ingestion",
                                "source_type": "File",
                                "source_name": f"gs://aid-gk1v-prod-edwdo-0-usre-outbound/cwls-ingestion-files/{bq_table}/*.csv",
                                "target_name": f"wireless_writeback.{td_table}",
                                "target_type": "Table",
                                "tool_name": "EDW_TD",
                                "source_environment": "GCP_GCS",
                                "source_servername": "gcs_servername",
                                "target_environment": "EDW_TD",
                                "target_servername": "teradata_servername",
                                "scheduler": "airflow",
                                "system_name": "consumer_wls_bq2td",
                                "logger_name": "dof_gcp_process_log",
                                "rerun_indicator": "NA"
                            }
                        },
                        "OkTo": "JDBCTransformLoad"
                    },
                    {
                        "Name": "JDBCTransformLoad",
                        "Id": "JDBCTransformLoad",
                        "Description": "Staging to Target JDBC Load",
                        "Type": "run jdbc query",
                        "Params": {
                            "hook": {
                                "parameters": {
                                    "OpsgeniePriority": "None",
                                    "dbinstance": "mkt_aidwb_p2",
                                    "target_dbtype": "teradata",
                                    "query": f"delete from {td_dataset}.{td_table} all; insert into {td_dataset}.{td_table} ({cols}) select {cols} from wireless_writeback.{td_table};delete from wireless_writeback.{td_table} all;"
                                }
                            },
                            "obs": {
                                "sub_process": "JDBCTransformLoad",
                                "keyvalue": "NA",
                                "project_name": "consumer_jdbc_ingestion",
                                "source_type": "Table",
                                "source_name": f"wireless_writeback.{td_table}",
                                "target_name": f"{td_dataset}.{td_table}",
                                "target_type": "Table",
                                "tool_name": "EDW_TD",
                                "source_environment": "EDW_TD",
                                "source_servername": "teradata_servername",
                                "target_environment": "EDW_TD",
                                "target_servername": "teradata_servername",
                                "scheduler": "airflow",
                                "rerun_indicator": "NA",
                                "system_name": "consumer_wls_bq2td",
                                "logger_name": "dof_gcp_process_log"
                            }
                        },
                        "OkTo": "End"
                    },
                    {
                        "Name": "End",
                        "Id": "End",
                        "Description": "This stage indicates the ending point of the task relationship in the dag",
                        "OkTo": "End"
                    }
                ]
            }

            object_name = f"{blob_path}/pipeline_metadata_{dataset_id}_{table_id}.json"
            gcs_hook.upload(
                bucket_name=bucket_name,
                object_name=object_name,
                data=json.dumps(row, indent=4),
                mime_type="application/json"
            )
            print(f"✅ Stored JSON for {dataset_id}.{table_id} -> gs://{bucket_name}/{object_name}")
            files_uploaded.append(f"{dataset_id}.{table_id}:gs://{bucket_name}/{object_name}")
        except Exception as e:
            print(f"error :{e}")  
            continue  
    print(files_uploaded)


with DAG(
    dag_id="bq_to_td_metadata_generator_export_query_v2",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    params={
        "project_id": "vz-it-pr-gk1v-edwdo-0",
        "composer": "vz-it-wdwg-aidcom-0-cc-edw-dts-2",
        "bucket_name": "gk1v-ple-edwdo-0-usre-archive",
        "blob_path":"vz-it-gk1v-edwdo-0/wireless/writeback_metadata",
        "functional_id":"mkt_aid_p2",
        "min_sessions":"30",
        "max_sessions":"40",
        "tables":[]
    }
) as dag:

    generate_jsons = PythonOperator(
        task_id="generate_jsons_and_store",
        python_callable=generate_jsons_and_store,
        provide_context=True,
    )