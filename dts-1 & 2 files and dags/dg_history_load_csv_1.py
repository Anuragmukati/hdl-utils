import airflow
import datetime
import sys
from airflow import models
from airflow.models.taskinstance import TaskInstance as TI
import json
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud.storage import Blob
import teradatasql
from airflow.hooks.base import BaseHook
from airflow.providers.teradata.operators.teradata import TeradataOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryHook,BigQueryInsertJobOperator

BASE_DIR = "/home/airflow/gcs/dags/vz-it-gk1v-edwpr-0/"
sys.path.append(f"{BASE_DIR}/")
def read_config():
    with open('/home/airflow/gcs/dags/vz-it-gk1v-edwpr-0/history_load_config_prod.json', 'r') as f:
        confid_details = json.load(f)
    return confid_details
config = read_config()
from BqUtilities import insertAuditTable, createSQLsmtUsingPartition, selectAuditTable, \
    updateMetadataTable, updateTableStatus
from airflow.utils.state import State
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.decorators import dag, task, task_group
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.models import Variable
import random
import string
import re
import pandas as pd
from datetime import datetime,timedelta
from airflow.operators.email import EmailOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import BigQueryToBigQueryOperator

# gcs details
GCP_CONN_ID = config["GCP_CONN_ID"]
GCP_CONN_DO_ID = config["GCP_CONN_DO_ID"]
GCS_BUCKET = config["GCS_BUCKET"]
GCS_PREFIX = config["GCS_PREFIX"]

DAG_NAME = "dg_history_load_csv_1"


BQ_SOURCE_PROJECT = config["bq_source_project"]
BQ_TARGET_PROJECT = config["bq_target_project"]
METADATA_DATASET = config["metadata_dataset"]
BQ_TARGET_DATASET = config["bq_target_dataset"]
GCP_CONN_DO_ID_PLE = "sa-vz-it-gk1v-pleedwdo-0-app"
BQ_PLE_PROJECT = "vz-it-pr-gk1v-pleedwdo-0"
GCP_CONN_DO_ID_PROD= "sa-vz-it-gk1v-edwdo-0-app"


TABLES_TO_EXPORT = []
hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
conn = hook.get_conn()

def get_metadata(where_clause,filter_dict_list):
    print("Getting Metadata...")
    hook = BigQueryHook(gcp_conn_id="sa-vz-it-gk1v-edwdo-0-app", use_legacy_sql=False)
    sql = """
    With
    metadata_table as (
    sELECT td_schema_name,td_table_name,bq_schema_name,bq_table_name,fetch_query,LoadType,partition_column,partition_type,partition_format,partition_minvalue,partition_range,status,appname,DateRequested,DateCompleted, table_size, partition_maxvalue
        FROM `""" + BQ_SOURCE_PROJECT + """.""" + METADATA_DATASET + """.historical_load_metadata_table_nos_adhoc_ms`m where """+str(where_clause)+""" and appname='NOS_First' )
    select td_schema_name,td_table_name,bq_schema_name,bq_table_name,fetch_query,LoadType,partition_column,partition_type,partition_format,partition_minvalue,partition_range,status,appname,DateRequested,DateCompleted,"Not found", table_size, partition_maxvalue
    from metadata_table
    """

    print(sql)

    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()

    LOFtableDict = []
    filter_map = {entry['tablename']: entry.get('filter_query', '') for entry in filter_dict_list}
    print(filter_map)

    for row in rows:
        tableDict = {}
        print("Table working on...")
        tableDict['td_schema_name'] = row[0]
        tableDict['td_table_name'] = row[1]
        tableDict['bq_schema_name'] = row[2]
        tableDict['bq_table_name'] = row[3]
        tableDict['fetch_query'] = row[4]
        tableDict['LoadType'] = row[5]
        tableDict['partition_column'] = row[6]
        tableDict['partition_type'] = row[7]
        tableDict['partition_format'] = row[8]
        tableDict['partition_minvalue'] = row[9]
        tableDict['partition_range'] = row[10]
        tableDict['status'] = row[11]
        tableDict['appname'] = row[12]
        tableDict['DateRequested'] = row[13]
        tableDict['DateCompleted'] = row[14]
        tableDict['schema_fields'] = row[15]
        tableDict['table_size'] = row[16]
        tableDict['partition_maxvalue'] = row[17]

        print(tableDict)
        #LOFtableDict.append(tableDict)

        matching_filter = next(
            (f for f in filter_dict_list if f["tablename"] == row[2]+"."+row[3]), None
        )
        if matching_filter and matching_filter.get("filter_query"):
            original_query = tableDict['fetch_query'].strip().rstrip(';')
            filter_condition = matching_filter["filter_query"].strip()
            if "where" in original_query.lower():
                tableDict['fetch_query'] = f"{original_query} AND {filter_condition[6:]}"
            else:
                tableDict['fetch_query'] = f"{original_query} {filter_condition}"

        print("Table working on...")
        print(tableDict)
        LOFtableDict.append(tableDict)

    print("List of tables to run for... ")
    print(LOFtableDict)

    print("List of tables to run for... ")
    print(LOFtableDict)

    TABLES_TO_EXPORT = LOFtableDict
    Variable.set(key=f'{DAG_NAME}_TABLE_TO_EXPORT', value=TABLES_TO_EXPORT,
                     serialize_json=True)

def cleanup_gcs_on_failure(context):

    ti = context['ti']
    TABLES_TO_EXPORT = Variable.get(key=f'{DAG_NAME}_TABLE_TO_EXPORT', deserialize_json=True)
    table_name = TABLES_TO_EXPORT[ti.map_index].get('td_table_name')

    gcs_path = f"{GCS_PREFIX}/table_name={table_name}/try_number={ti.try_number}/"
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)

    try:
        object_list = gcs_hook.list(bucket_name=GCS_BUCKET, prefix=gcs_path)
        if object_list:
            for obj in object_list:
                gcs_hook.delete(bucket_name=GCS_BUCKET, object_name=obj)
            print(f"Successfully deleted: {object_list}")
        else:
            print(f"No objects found to delete at {gcs_path}")
    except Exception as e:
        print(f"Failed to clean up GCS path: {gcs_path}. Error: {str(e)}")
    updateMetadataTable(conn,TABLES_TO_EXPORT[ti.map_index], "FAILED")


@dag(
    dag_id=DAG_NAME,
    tags=["bundle:dif", "module:historical"],
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(1),
    max_active_tasks=1,
    default_args={
        "retries": 0
        
    },
    render_template_as_native_obj=True,
)
def dynamic_task_group_tpt():

    def audit_logging():
        TABLES_TO_EXPORT=Variable.get(key=f'{DAG_NAME}_TABLE_TO_EXPORT', deserialize_json=True)
        for i in range(0, len(TABLES_TO_EXPORT)):

            print("Inserting Audit entry for " + TABLES_TO_EXPORT[i].get('td_table_name'))

            if TABLES_TO_EXPORT[i].get('LoadType') == "FULL_LOAD":
                insertAuditTable(conn, tableDetails=TABLES_TO_EXPORT[i])
                updateMetadataTable(conn, TABLES_TO_EXPORT[i], "INPROGRESS")
            else:
                table = TABLES_TO_EXPORT[i].get('bq_table_name')
                dataset = TABLES_TO_EXPORT[i].get('bq_schema_name')

                print(table)
                print(dataset)

                select_row = selectAuditTable(conn, table, dataset)

                if select_row == False:
                    if insertAuditTable(conn, tableDetails=TABLES_TO_EXPORT[i]) == True:
                        updateMetadataTable(conn, TABLES_TO_EXPORT[i], "INPROGRESS")
                        newSQLStatement = createSQLsmtUsingPartition(conn,
                                                                     TABLES_TO_EXPORT[i],
                                                                     "null")
                        print("Fetch query for the new table")
                        print(newSQLStatement)
                        TABLES_TO_EXPORT[i]['fetch_query'] = newSQLStatement
                        print("Updated fetch query for the +" + TABLES_TO_EXPORT[i].get(
                            'fetch_query'))
                    else:
                        print("Could not create sql statement partition")
                else:
                    print("Audit Value")
                    print(select_row)
                    print(select_row[9])
                    insertAuditTable(conn, tableDetails=TABLES_TO_EXPORT[i])
                    updateMetadataTable(conn, TABLES_TO_EXPORT[i], "INPROGRESS")
                    newSQLStatement = createSQLsmtUsingPartition(conn,
                                                                 TABLES_TO_EXPORT[i],
                                                                 select_row[9])

                    print("Fetch query for the new table")
                    print(newSQLStatement)

                    TABLES_TO_EXPORT[i]['fetch_query'] = newSQLStatement
                    TABLES_TO_EXPORT[i]['parition_minvalue'] = select_row[9]
                    print("Update fetch query for the + " + TABLES_TO_EXPORT[i].get(
                        'fetch_query'))

        Variable.set(key=f'{DAG_NAME}_TABLE_TO_EXPORT', value=TABLES_TO_EXPORT,
                     serialize_json=True)



    @task
    def get_tables_to_export(**context):
        dag_conf = context['dag_run'].conf or {}

        if 'tableDict' in dag_conf :
            manual_table_dict_list = dag_conf.get("tableDict", [])
            if not isinstance(manual_table_dict_list, list):
                raise ValueError("Expected 'tableDict' to be a list of dictionaries.")

            for entry in manual_table_dict_list:
                if not isinstance(entry, dict):
                    raise ValueError("Each entry in 'tableDict' must be a dictionary.")

            print("Using manually provided tableDict list:")
            print(manual_table_dict_list)

            Variable.set(key=f'{DAG_NAME}_TABLE_TO_EXPORT', value=manual_table_dict_list, serialize_json=True)

        else:
            manual_table_dict_list = dag_conf.get("Table_List", [])
            table_names = [entry["tablename"] for entry in manual_table_dict_list]
            joined_tables = ", ".join([f"'{name}'" for name in table_names])
            where_clause = f"CONCAT(bq_schema_name,'.', bq_table_name) IN ({joined_tables})"

            filter_dict_list = [
                {
                    "tablename": entry["tablename"],
                    "filter_query": entry.get("filter_query", "")
                }
                for entry in manual_table_dict_list
            ]

            get_metadata(where_clause, filter_dict_list)
        audit_logging()

        return Variable.get(key=f'{DAG_NAME}_TABLE_TO_EXPORT', deserialize_json=True)

    @task_group
    def extract_table(table):

        @task(multiple_outputs=True)
        def create_kpo_args(table_to_extract,**kwargs):
            ti = kwargs['ti']
            fetch_query = table_to_extract.get('fetch_query')
            table_name = table_to_extract.get('bq_table_name')
            def compare_column_counts(table_to_extract, bq_conn_id=GCP_CONN_DO_ID_PLE):
                table = table_to_extract.get('bq_table_name')
                ple_dataset = table_to_extract.get('bq_schema_name')
                prod_dataset = table_to_extract.get('bq_schema_name')
                ple_project = BQ_PLE_PROJECT
                prd_project= BQ_TARGET_PROJECT
                td_schema = table_to_extract.get('td_schema_name')
                td_table = table_to_extract.get('td_table_name')
                fetch_query = table_to_extract.get('fetch_query')
                print(table_to_extract)

                def ple_bq_count(project,dataset):
                    hook = BigQueryHook(gcp_conn_id=GCP_CONN_DO_ID_PLE, use_legacy_sql=False)
                    query = f"SELECT COUNT(1) AS cnt FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{table}'"
                    print(query)
                    return int(hook.get_pandas_df(sql=query)["cnt"][0])
                def prd_bq_count(project,dataset):
                    hook = BigQueryHook(gcp_conn_id=GCP_CONN_DO_ID_PROD,use_legacy_sql=False)
                    query = f"SELECT COUNT(1) AS cnt FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{table}'"
                    print(query)
                    return int(hook.get_pandas_df(sql=query)["cnt"][0])
                conn = BaseHook.get_connection("UGCPDTSNOS1")
                username = conn.login
                password = conn.password

                # Teradata column count
                def td_count():
                    with teradatasql.connect(
                        host=conn.host,
                        user=username,
                        password=password
                    ) as con:
                        cur = con.cursor()
                        cur.execute(f"SELECT COUNT(1) FROM dbc.columnsV WHERE databasename='{td_schema}' AND tablename='{td_table}'")
                        return cur.fetchone()[0]

                ple = ple_bq_count(ple_project,ple_dataset)
                prod = prd_bq_count(prd_project,prod_dataset)
                td = td_count()
                print(ple)
                print(prod)
                print(td)

                if ple != prod or prod != td:
                    raise ValueError(f"Column count mismatch for {table}: PLE={ple}, PROD={prod}, Teradata={td}")
                print(f"Column count match for {table}: {ple}")
            compare_column_counts(table_to_extract, bq_conn_id=GCP_CONN_DO_ID_PLE)
            gcs_path=f"{GCS_PREFIX}/table_name={table_name}/try_number={ti.try_number}/"
            sql_query = f"""
                SELECT * FROM WRITE_NOS (
                    ON ( {fetch_query} )
                    USING
                    LOCATION('/gs/storage.googleapis.com/{GCS_BUCKET}/{gcs_path}')
                    AUTHORIZATION(TD_SERVER_DB.NOS_AUTH_PRD_GK1V_EDWPR)
                    STOREDAS('CSV')
                    ROWFORMAT('{{"field_delimiter":"~","record_delimiter":"\\n"}}')
                    MAXOBJECTSIZE('16MB')
                    COMPRESSION('GZIP')
                ) AS d;
                """
            ti = kwargs['ti']
            key_name=table_name+"_sql_query"
            ti.xcom_push(key=key_name, value=sql_query)
           
        create_load_config = create_kpo_args(table)
       
        @task(trigger_rule="all_success", multiple_outputs=True, on_failure_callback=cleanup_gcs_on_failure)
        def nos_to_gcs_load(**kwargs):
            ti = kwargs['ti']
            print(ti)
            i=ti.map_index
            TABLES_TO_EXPORT = Variable.get(key=f'{DAG_NAME}_TABLE_TO_EXPORT', deserialize_json=True)
            table_name=TABLES_TO_EXPORT[i]['bq_table_name']
            gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
            source_folder = f"{GCS_PREFIX}/table_name={table_name}/try_number={ti.try_number}/"
            source_bucket = config["source_bucket"]
            source_files = gcs_hook.list(source_bucket, prefix=source_folder)
            if source_files:
                for file_path in source_files:
                    relative_path = file_path[len(source_folder):]
                    gcs_hook.delete(source_bucket, file_path)

            key_name=table_name+"_sql_query"
            print(TABLES_TO_EXPORT)
            sql= ti.xcom_pull(task_ids='extract_table.create_kpo_args', key=key_name)
            print(sql)
            t1=TeradataOperator(
                task_id='nos_to_gcs_load',
                conn_id='UGCPDTSNOS1',
                sql=sql,
                retries=0,
                on_failure_callback=cleanup_gcs_on_failure,
                )
            t1.execute(dict())
        nos_to_gcs_load=nos_to_gcs_load()
       
        @task(trigger_rule="all_success", multiple_outputs=True, on_failure_callback=cleanup_gcs_on_failure)
        def get_tpt_output(**kwargs):
            ti = kwargs['ti']
            TABLES_TO_EXPORT = Variable.get(key=f'{DAG_NAME}_TABLE_TO_EXPORT',
                                            deserialize_json=True)
            table_name = TABLES_TO_EXPORT[ti.map_index].get('bq_table_name')
            schema_name = TABLES_TO_EXPORT[ti.map_index].get('bq_schema_name')
            load_type = TABLES_TO_EXPORT[ti.map_index].get('LoadType')

            if load_type == "FULL_LOAD":
                write= f"WRITE_TRUNCATE"
            else:
                write= f"WRITE_APPEND"

            execution_ts = kwargs['logical_date'].strftime('%Y%m%d_%H%M%S')

            schema_object_bucket = GCS_BUCKET
            schema_object = f"td2bq_historical_load/schemas/{schema_name}_{table_name}.json"          
            bq_target_project = BQ_TARGET_PROJECT
            null_marker={"nullMarker": ""}

            print("Table name using a variable:- " + str(table_name))
            return {
                "source_objects": f"gs://{GCS_BUCKET}/{GCS_PREFIX}/table_name={table_name}/try_number={ti.try_number}/*.csv.gz",
                "destination_project_dataset_table": f'{bq_target_project}.{schema_name}.{table_name}',
                "schema_object_bucket": schema_object_bucket,
                "schema_object": schema_object,
                "write_des": write,
                "src_fmt_cfg":null_marker,
                "dataset":schema_name,
                "table_name":table_name        
                                        }

        gcs_input = get_tpt_output()

        create_gcs_to_bq_load_configs=BigQueryInsertJobOperator(
            task_id='gcs_to_bq_prod_load',
            configuration={
                "load":{
                    "sourceUris":[gcs_input["source_objects"]],
                "destinationTable":{
                    "projectId":BQ_TARGET_PROJECT,
                    "datasetId":gcs_input["dataset"],
                    "tableId":gcs_input["table_name"]
                },
                "sourceFormat":"CSV",
                "fieldDelimiter":"~",
                "skipLeadingRows":1,
                "Compression":"GZIP",
                "createDisposition":"CREATE_NEVER",
                "autodectect":False,
                "writeDisposition":"WRITE_TRUNCATE",
                "allowQuotedNewlines":True
                }
            },
            location='US',
            gcp_conn_id=GCP_CONN_DO_ID
        )

        @task(trigger_rule="all_success", multiple_outputs=True)
        def prod_to_ple(**kwargs):
            ti = kwargs['ti']
            dag_conf = kwargs['dag_run'].conf or {}
            if 'ple_flag' in dag_conf :
                TABLES_TO_EXPORT = Variable.get(key=f'{DAG_NAME}_TABLE_TO_EXPORT',
                                                deserialize_json=True)
                table_name = TABLES_TO_EXPORT[ti.map_index].get('bq_table_name')
                schema_name = TABLES_TO_EXPORT[ti.map_index].get('bq_schema_name')
                sql=f"BEGIN TRANSACTION ; TRUNCATE TABLE {BQ_PLE_PROJECT}.{schema_name}.{table_name}; INSERT INTO {BQ_PLE_PROJECT}.{schema_name}.{table_name} SELECT * FROM {BQ_TARGET_PROJECT}.{schema_name}.{table_name}; COMMIT TRANSACTION;"
                hook = BigQueryHook(gcp_conn_id=GCP_CONN_DO_ID_PLE, use_legacy_sql=False)
                conn = hook.get_conn()
                cursor = conn.cursor()
                try:
                    print(sql)
                    cursor.execute(sql)
                except Exception as e:
                    print('PROD TO PLE got failed!!!!!')
                    print(f"reason: {e}")
                    raise
            else:
                pass

        prod_to_ple_load=prod_to_ple()
        @task(trigger_rule="all_success", multiple_outputs=True, on_failure_callback=cleanup_gcs_on_failure)
        def move_gcs_folder(**kwargs):
            ti = kwargs['ti']
            TABLES_TO_EXPORT = Variable.get(key=f'{DAG_NAME}_TABLE_TO_EXPORT',
                                            deserialize_json=True)
            table_name = TABLES_TO_EXPORT[ti.map_index].get('bq_table_name')
            table_schema = TABLES_TO_EXPORT[ti.map_index].get('bq_schema_name')

            source_bucket = config["source_bucket"]
            destination_bucket = config["backup_destination_bucket"]
            source_folder = f"{GCS_PREFIX}/table_name={table_name}/try_number={ti.try_number}/"
            destination_folder = f"{GCS_PREFIX}/archive/"
            gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
            source_files = gcs_hook.list(source_bucket, prefix=source_folder)
            if not source_files:
                print(f"No files found in {source_folder} to move.")
                return

            for file_path in source_files:
                relative_path = file_path[len(source_folder):]
                destination_path = f'{destination_folder}{relative_path}'
                gcs_hook.delete(source_bucket, file_path)
            print(f"Folder {source_folder} has been successfully moved to {destination_folder}")
        delete_datafile_table = move_gcs_folder()

        @task(trigger_rule="all_success", multiple_outputs=True)
        def update_metadata(**kwargs):
            ti = kwargs['ti']
            TABLES_TO_EXPORT = Variable.get(key=f'{DAG_NAME}_TABLE_TO_EXPORT',
                                            deserialize_json=True)
            print(TABLES_TO_EXPORT)

            updateTableStatus(conn, tableDetails=TABLES_TO_EXPORT[ti.map_index])


        add_details_to_audit = update_metadata()
        create_load_config >> nos_to_gcs_load >> gcs_input >> create_gcs_to_bq_load_configs >> prod_to_ple_load >> add_details_to_audit >> delete_datafile_table


    extract_table.expand(table=get_tables_to_export())

dynamic_task_group_tpt()
