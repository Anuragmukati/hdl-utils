from airflow import models
import airflow
import sys
import random
import string
import re
import pandas as pd
import json
import teradatasql
from datetime import datetime,timedelta
from airflow.models.taskinstance import TaskInstance as TI
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud.storage import Blob
from airflow.hooks.base import BaseHook
from airflow.operators.email import EmailOperator
from airflow.providers.teradata.operators.teradata import TeradataOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryHook,BigQueryInsertJobOperator
from airflow.operators.dummy import DummyOperator
BASE_DIR = "/home/airflow/gcs/dags/vz-it-gk1v-edwpr-0/"
sys.path.append(f"{BASE_DIR}/")

def read_config():
    with open('/home/airflow/gcs/dags/vz-it-gk1v-edwpr-0/history_load_config_prod.json', 'r') as f:
        confid_details = json.load(f)
    return confid_details

config = read_config()

from airflow.utils.state import State
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.decorators import dag, task, task_group
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import BigQueryToBigQueryOperator

DAG_NAME="dg_history_load_nos_adhoc_parquet_UGCPDTSNOS1"
BQ_SOURCE_PROJECT = config["bq_source_project"]
METADATA_DATASET = config["metadata_dataset"]
NOS_CONN_ID='UGCPDTSNOS1'


def styled_html_table(df: pd.DataFrame, dag_id: str, run_id: str) -> str:
    if list(df.columns) == df.iloc[0].tolist():
        df = df.iloc[1:].reset_index(drop=True)
    table_style = (
        "border-collapse: collapse; width: 100%; font-family: Arial, sans-serif; font-size: 14px;"
    )
    th_style = (
        "background-color: #003366; color: white; padding: 10px; text-align: left; border: 1px solid #ddd;"
    )
    td_style = (
        "padding: 10px; border: 1px solid #ddd;"
    )
    completed_style = "background-color: #d4edda; color: #155724; font-weight: bold;"
    failed_style = "background-color: #f8d7da; color: #721c24; font-weight: bold;"

    # Build header
    html = f"""
    <div style="font-family: Arial, sans-serif; padding: 16px;">
      <h2 style="color: #003366;">📊 BigQuery Table Load Report</h2>
      <p><b>COMPOSER:</b> <code>vz-it-pr-wdwg-aidcom-0-cc-edw-dts-2</code><br>
         <b>DAG:</b> <code>{dag_id}</code><br>
         <b>Run ID:</b> <code>{run_id}</code></p>
      <table style="{table_style}">
        <thead>
          <tr>
            <th style="{th_style}">BQ_PROJECT</th>
            <th style="{th_style}">BQ_DATASET</th>
            <th style="{th_style}">BQ_TABLE</th>
            <th style="{th_style}">BQ_COUNT</th>
            <th style="{th_style}">TD_COUNT</th>
            <th style="{th_style}">RESULT</th>
          </tr>
        </thead>
        <tbody>
    """

    # Build rows
    for i, row in df.iterrows():
        status_style = completed_style if row["RESULT"] == "Completed" else failed_style
        row_bg = "#f9f9f9" if i % 2 == 1 else "#ffffff"
        html += f"""
          <tr style="background-color: {row_bg};">
            <td style="{td_style}">{row['BQ_PROJECT']}</td>
            <td style="{td_style}">{row['BQ_DATASET']}</td>
            <td style="{td_style}">{row['BQ_TABLE']}</td>
            <td style="{td_style}">{row['BQ_COUNT']}</td>
            <td style="{td_style}">{row['TD_COUNT']}</td>
            <td style="{td_style} {status_style}">{row['RESULT']}</td>
          </tr>
        """

    # Close table and footer
    html += """
        </tbody>
      </table>
      <p style="font-size:12px; color: #666;">This is an automated report sent by your Airflow DAG.</p>
    </div>
    """
    return html
    
def replace_special_chars_and_spaces(s):
    return re.sub(r'[^A-Za-z0-9]', '_', s)


def getCounts(tableDetails,BQ_TARGET_PROJECT,GCP_CONN_DO_ID_PROD):
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_DO_ID_PROD, use_legacy_sql=False)
    conn = hook.get_conn()
    BQTable = str(tableDetails.get('bq_table_name'))
    BQDataset = str(tableDetails.get('bq_schema_name'))
    sql = """ SELECT count(1) FROM `"""+BQ_TARGET_PROJECT+"""`."""+str(BQDataset)+"""."""+str(BQTable)+""";"""
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        row = cursor.fetchone()
        print("Count found for "+BQDataset+"."+BQTable+": "+str(row[0]))
        return row[0]
    except:
        print("Count found for  "+BQDataset+"."+BQTable)
        return "0"

def get_teradata_table_count(td_table_name):
    conn = BaseHook.get_connection(NOS_CONN_ID)
    host = 'tddp.tdc.vzwcorp.com'
    username = conn.login
    password = conn.password
    try:
        connection = teradatasql.connect(
           host=host,
           user=username,
           password=password
        )
        cursor = connection.cursor()
        query = f"SELECT COUNT(*) FROM {td_table_name}"
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        connection.close()
        return count
    except Exception as e:
        print(f"Error: {e!r}")

def updateTableStatus(tableDetails,status,dag_id,run_id,BQ_TARGET_PROJECT,BQ_TARGET_PROJECT_AUDIT,GCP_CONN_DO_ID_PROD,GCP_CONN_DO_ID_PROD_AUDIT):
    BQTable = str(tableDetails.get('bq_table_name'))
    td_table_name=str(tableDetails.get('td_table_name'))
    td_schema_name=str(tableDetails.get('td_schema_name'))
    td_full_name=td_schema_name+"."+td_table_name
    BQDataset = str(tableDetails.get('bq_schema_name'))
    PartitionColumn = str(tableDetails.get('partition_column'))
    PartitionType = str(tableDetails.get('partition_type'))
    
    if status=="Inprogress":
        bqCount=0
        tdCount=0
    else:
        bqCount = getCounts(tableDetails,BQ_TARGET_PROJECT,GCP_CONN_DO_ID_PROD)
        tdCount=get_teradata_table_count(td_full_name)
    td_schema_name=str(tableDetails.get('td_schema_name'))
    if bqCount==tdCount and status!="Inprogress":
        status="Completed"
    elif status!="Inprogress" and bqCount!=tdCount :
        status="Failed"
    tableid = BQDataset + "_" + BQTable
    start = datetime.now()
    end = datetime.now()

    hook = BigQueryHook(gcp_conn_id=GCP_CONN_DO_ID_PROD_AUDIT, use_legacy_sql=False)
    conn = hook.get_conn()
    sql = f"""INSERT INTO `"""+str(BQ_TARGET_PROJECT_AUDIT)+"""."""+"udm_prdstg_tbls"+""".historical_load_audit_table_updated`(dag_id,run_id,Table_ID,TdTable,BQDataset,BQTable, Error, Start_time, End_time, PartitionColumn, PartitionType, MaxPartitionValue, Bq_Records,Td_Records, Status) VALUES ('"""+dag_id+"""','"""+run_id+"""','"""+tableid+"""','"""+BQTable+"""', '"""+BQDataset+"""', '"""+BQTable+"""', "",'"""+str(start)+"""','"""+str(end)+"""','"""+PartitionColumn+"""','"""+PartitionType+"""','"""+"NA"+"""', """+str(bqCount)+""","""+str(tdCount)+""",'"""+status+"""');""" 
    print(sql)
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        print("Insert entry done for "+BQDataset+"."+BQTable)
    except:
        print("Insert entry failed for  "+BQDataset+"."+BQTable)
    return True


def get_metadata(where_clause,filter_dict_list):
    hook = BigQueryHook(gcp_conn_id="sa-vz-it-gk1v-edwdo-0-app", use_legacy_sql=False)
    sql = """
    With
    metadata_table as (
    sELECT td_schema_name,td_table_name,bq_schema_name,bq_table_name,fetch_query,LoadType,partition_column,partition_type,partition_format,partition_minvalue,partition_range,status,appname,DateRequested,DateCompleted, table_size, partition_maxvalue
        FROM `""" + BQ_SOURCE_PROJECT + """.""" + METADATA_DATASET + """.bkp_historical_load_metadata_table_nos_adhoc_parquet`m where """+str(where_clause)+""" and appname='NOS_First' )
    select td_schema_name,td_table_name,bq_schema_name,bq_table_name,fetch_query,LoadType,partition_column,partition_type,partition_format,partition_minvalue,partition_range,status,appname,DateRequested,DateCompleted,"Not found", table_size, partition_maxvalue
    from metadata_table
    """
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()

    LOFtableDict = []
    filter_map = {entry['tablename']: entry.get('filter_query', '') for entry in filter_dict_list}
 
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

        LOFtableDict.append(tableDict)
    TABLES_TO_EXPORT = LOFtableDict
    Variable.set(key=f'{DAG_NAME}_TABLE_TO_EXPORT',value=TABLES_TO_EXPORT,serialize_json=True)

def on_failure_audit_update(context):
    ti = context['ti']
    dag_id=context['dag'].dag_id
    run_id=context['dag_run'].run_id
    TABLES_TO_EXPORT = Variable.get(key=f'{DAG_NAME}_TABLE_TO_EXPORT', deserialize_json=True)
    BQ_TARGET_PROJECT=str(ti.xcom_pull(task_ids='create_project_specs',key=f'{DAG_NAME}_BQ_TARGET_PROJECT'))
    GCP_CONN_DO_ID_PROD_AUDIT=str(ti.xcom_pull(task_ids='create_project_specs',key=f'{DAG_NAME}_GCP_CONN_DO_ID_PROD_AUDIT'))
    GCP_CONN_DO_ID_PROD=str(ti.xcom_pull(task_ids='create_project_specs',key=f'{DAG_NAME}_GCP_CONN_DO_ID_PROD'))
    BQ_TARGET_PROJECT_AUDIT=str(ti.xcom_pull(task_ids='create_project_specs',key=f'{DAG_NAME}_BQ_TARGET_PROJECT_AUDIT'))
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_DO_ID_PROD_AUDIT, use_legacy_sql=False)
    conn = hook.get_conn()
    
    try:
        updateTableStatus(TABLES_TO_EXPORT[ti.map_index], "Failed",dag_id,run_id,BQ_TARGET_PROJECT,BQ_TARGET_PROJECT_AUDIT,GCP_CONN_DO_ID_PROD,GCP_CONN_DO_ID_PROD_AUDIT)
    except Exception as e:
        print(f"Error: {str(e)}")
    
@dag(
    dag_id=DAG_NAME,
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(1),
    max_active_runs=1,
    default_args={
        "retries": 0
    },
    render_template_as_native_obj=True,
)
def dynamic_task_group_tpt():
    start_task = DummyOperator(task_id='start_task')
    
    
    @task
    def create_project_specs(**context):
        dag_conf = context['dag_run'].conf or {}
        ti=context['ti']
        DAG_NAME=context['dag'].dag_id
        if 'project' in dag_conf :
# gk1v            
            if dag_conf.get("project")=='gk1v':
                print("gk1v project setting up")
                BQ_TARGET_PROJECT=config["bq_target_project"]
                GCP_CONN_DO_ID_PLE="sa-vz-it-gk1v-pleedwdo-0-app"
                BQ_PLE_PROJECT= "vz-it-pr-gk1v-pleedwdo-0"
                GCP_CONN_DO_ID_PROD="sa-vz-it-gk1v-edwdo-0-app"
                GCS_BUCKET="gk1v-prod-edwpr-0-usre-nos-inbound"
                GCS_PREFIX="nos"
                STAGE_DATASET="edw_data_migration_opsstg_tbls" 
                ti.xcom_push(key=f'{DAG_NAME}_GCP_CONN_DO_ID_PROD', value=GCP_CONN_DO_ID_PROD) 
# fpardo                
            elif dag_conf.get("project")=='fpardo':
                print("fpardo project setting up")
                BQ_TARGET_PROJECT="vz-nonit-pr-k65v-fpardo-0"
                GCP_CONN_DO_ID_PLE="sa-vz-nonit-k65v-plefpardo-0-app"
                BQ_PLE_PROJECT = "vz-nonit-pr-k65v-plefpardo-0"
                GCP_CONN_DO_ID_PROD="sa-vz-nonit-k65v-fpardo-0-app"
                GCS_BUCKET="k65v-prod-fpardo-0-usre-nos"
                GCS_PREFIX="nos"
                STAGE_DATASET="edw_data_migration_opsstg_tbls"
                ti.xcom_push(key=f'{DAG_NAME}_GCP_CONN_DO_ID_PROD', value=GCP_CONN_DO_ID_PROD) 
# enabdo    
            elif dag_conf.get("project")=='enabdo':
                print("enabdo project setting up")
                BQ_TARGET_PROJECT="vz-nonit-pr-k45v-enabdo-0"
                GCP_CONN_DO_ID_PLE="sa-vz-nonit-k45v-pleenabdo-0-app"
                BQ_PLE_PROJECT = "vz-nonit-pr-k45v-pleenabdo-0"
                GCP_CONN_DO_ID_PROD="sa-vz-nonit-k45v-enabdo-0-app"
                GCS_BUCKET="k45v-prod-enabdo-0-usre-nos-inbound"
                GCS_PREFIX="nos"
                STAGE_DATASET="edw_data_migration_opsstg_tbls"
                ti.xcom_push(key=f'{DAG_NAME}_GCP_CONN_DO_ID_PROD', value=GCP_CONN_DO_ID_PROD)                
# pblsdo
            elif dag_conf.get("project")=='pblsdo':
                print("pblsdo project setting up")
                BQ_TARGET_PROJECT="vz-nonit-pr-k45v-pblsdo-0"
                GCP_CONN_DO_ID_PLE="sa-vz-nonit-k45v-plepblsdo-0-app"
                BQ_PLE_PROJECT = "vz-nonit-pr-k45v-plepblsdo-0"
                GCP_CONN_DO_ID_PROD="sa-vz-nonit-k45v-pblsdo-0-app"
                GCS_BUCKET="k45v-prod-pblsdo-0-usre-nos-inbound"
                GCS_PREFIX="nos"
                STAGE_DATASET="edw_data_migration_opsstg_tbls"
                ti.xcom_push(key=f'{DAG_NAME}_GCP_CONN_DO_ID_PROD', value=GCP_CONN_DO_ID_PROD)
# msgcdo
            elif dag_conf.get("project")=='msgcdo':
                print("fpardo project setting up")
                BQ_TARGET_PROJECT="vz-nonit-pr-k45v-msgcdo-0"
                GCP_CONN_DO_ID_PLE="sa-vz-nonit-k45v-plemsgcdo-0-app"
                BQ_PLE_PROJECT = "vz-nonit-pr-k45v-plemsgcdo-0"
                GCP_CONN_DO_ID_PROD="sa-vz-nonit-k45v-msgcdo-0-app"
                GCS_BUCKET="k45v-prod-msgcdo-0-usre-nos-inbound"
                GCS_PREFIX="nos"
                STAGE_DATASET="edw_data_migration_opsstg_tbls"
                ti.xcom_push(key=f'{DAG_NAME}_GCP_CONN_DO_ID_PROD', value=GCP_CONN_DO_ID_PROD)
# vzendo
            elif dag_conf.get("project")=='vzendo':
                print("fpardo project setting up")
                BQ_TARGET_PROJECT="vz-nonit-pr-k45v-vzendo-0"
                GCP_CONN_DO_ID_PLE="sa-vz-nonit-k45v-plevzendo-0-app"
                BQ_PLE_PROJECT = "vz-nonit-pr-k45v-plevzendo-0"
                GCP_CONN_DO_ID_PROD="sa-vz-nonit-k45v-vzendo-0-app"
                GCS_BUCKET="k45v-prod-vzendo-0-usre-nos-inbound"
                GCS_PREFIX="nos"
                STAGE_DATASET="edw_data_migration_opsstg_tbls"
                ti.xcom_push(key=f'{DAG_NAME}_GCP_CONN_DO_ID_PROD', value=GCP_CONN_DO_ID_PROD)
# zdwv         
            elif dag_conf.get("project")=='zdwv':
                print("fpardo project setting up")
                BQ_TARGET_PROJECT="vz-it-pr-zdwv-edwdo-0"
                GCP_CONN_DO_ID_PLE="sa-vz-it-zdwv-pleedwdo-0-app"
                BQ_PLE_PROJECT = "vz-it-pr-zdwv-pleedwdo-0"
                GCP_CONN_DO_ID_PROD="sa-vz-it-zdwv-edwdo-0-app"
                GCS_BUCKET="zdwv-prod-edwdo-0-usre-nos-inbound"
                GCS_PREFIX="nos"
                STAGE_DATASET="edw_data_migration_opsstg_tbls"
                ti.xcom_push(key=f'{DAG_NAME}_GCP_CONN_DO_ID_PROD', value=GCP_CONN_DO_ID_PROD)

                  
            ti.xcom_push(key=f'{DAG_NAME}_BQ_TARGET_PROJECT_AUDIT', value="vz-it-pr-gk1v-edwpr-0")  
            ti.xcom_push(key=f'{DAG_NAME}_GCP_CONN_DO_ID_PROD_AUDIT', value="sa-vz-it-gk1v-edwdo-0-app")  
            
            
            return {
                    "BQ_TARGET_PROJECT":BQ_TARGET_PROJECT,
                    "GCP_CONN_DO_ID_PLE":GCP_CONN_DO_ID_PLE,
                    "BQ_PLE_PROJECT":BQ_PLE_PROJECT,
                    "GCP_CONN_DO_ID_PROD":GCP_CONN_DO_ID_PROD,
                    "GCS_BUCKET":GCS_BUCKET,
                    "GCS_PREFIX":GCS_PREFIX,
                    "STAGE_DATASET":STAGE_DATASET,
                }
    create_project_specs=create_project_specs()


    @task
    def get_tables_to_export(**context):
        dag_conf = context['dag_run'].conf or {}
        if 'table_metadata_dict' in dag_conf :
            manual_table_dict_list = dag_conf.get("table_metadata_dict", [])
            if not isinstance(manual_table_dict_list, list):
                raise ValueError("Expected 'table_metadata_dict' to be a list of dictionaries.")
            for entry in manual_table_dict_list:
                if not isinstance(entry, dict):
                    raise ValueError("Each entry in 'table_metadata_dict' must be a dictionary.")
            print("Using manually provided table_metadata_dict list:")
            print(manual_table_dict_list)
            Variable.set(key=f'{DAG_NAME}_TABLE_TO_EXPORT',value=manual_table_dict_list, serialize_json=True)

        else:
            manual_table_dict_list = dag_conf.get("table_name_list", [])
            table_names = [entry["table_name"] for entry in manual_table_dict_list]
            joined_tables = ", ".join([f"'{name}'" for name in table_names])
            where_clause = f"CONCAT(bq_schema_name,'.', bq_table_name) IN ({joined_tables})"
            filter_dict_list = [
                {
                    "tablename": entry["table_name"],
                    "filter_query": entry.get("filter_query", "")
                }
                for entry in manual_table_dict_list
            ]
            get_metadata(where_clause, filter_dict_list)
        return Variable.get(key=f'{DAG_NAME}_TABLE_TO_EXPORT', deserialize_json=True)
    get_tables_to_export=get_tables_to_export()

    @task_group
    def extract_table(table):
        @task(trigger_rule="all_success", multiple_outputs=True,on_failure_callback=on_failure_audit_update)
        def add_stagging_table(create_project_specs,table_to_extract,**kwargs):
            ti = kwargs['ti']
            dag_id=kwargs['dag'].dag_id
            run_id=kwargs['dag_run'].run_id
            status="Inprogress"
            BQ_TARGET_PROJECT=str(create_project_specs['BQ_TARGET_PROJECT'])
            GCP_CONN_DO_ID_PROD_AUDIT=str(ti.xcom_pull(task_ids='create_project_specs',key=f'{DAG_NAME}_GCP_CONN_DO_ID_PROD_AUDIT'))
            BQ_TARGET_PROJECT_AUDIT=str(ti.xcom_pull(task_ids='create_project_specs',key=f'{DAG_NAME}_BQ_TARGET_PROJECT_AUDIT'))
            GCP_CONN_DO_ID_PROD=str(ti.xcom_pull(task_ids='create_project_specs',key=f'{DAG_NAME}_GCP_CONN_DO_ID_PROD'))
            updateTableStatus(table_to_extract,status,dag_id,run_id,BQ_TARGET_PROJECT,BQ_TARGET_PROJECT_AUDIT,GCP_CONN_DO_ID_PROD,GCP_CONN_DO_ID_PROD_AUDIT)
            TABLES_TO_EXPORT = Variable.get(key=f'{DAG_NAME}_TABLE_TO_EXPORT',deserialize_json=True)
            table_name = TABLES_TO_EXPORT[ti.map_index].get('bq_table_name')
            schema_name = TABLES_TO_EXPORT[ti.map_index].get('bq_schema_name')
            STAGE_DATASET=create_project_specs['STAGE_DATASET']
            sql=f"DROP TABLE IF EXISTS {BQ_TARGET_PROJECT}.{schema_name}.{table_name}_hdl; CREATE TABLE IF NOT EXISTS {BQ_TARGET_PROJECT}.{schema_name}.{table_name}_hdl LIKE {BQ_TARGET_PROJECT}.{schema_name}.{table_name}; CREATE TABLE IF NOT EXISTS {BQ_TARGET_PROJECT}.{STAGE_DATASET}.{table_name}_hdl LIKE {BQ_TARGET_PROJECT}.{schema_name}.{table_name}_hdl;"
            hook = BigQueryHook(gcp_conn_id=GCP_CONN_DO_ID_PROD, use_legacy_sql=False)
            conn = hook.get_conn()
            cursor = conn.cursor()
            
            fetch_query = table_to_extract.get('fetch_query')
            table_name = table_to_extract.get('bq_table_name')
            sanitize_run_id=replace_special_chars_and_spaces(run_id)
            GCS_BUCKET=create_project_specs['GCS_BUCKET']
            GCS_PREFIX=create_project_specs['GCS_PREFIX']
            gcs_path=f"{GCS_PREFIX}/{sanitize_run_id}/table_name={table_name}/try_number={ti.try_number}/"
            sql_query = f"""
                SELECT * FROM WRITE_NOS (
                    ON ( {fetch_query} )
                    USING
                    LOCATION('/gs/storage.googleapis.com/{GCS_BUCKET}/{gcs_path}')
                    AUTHORIZATION(TD_SERVER_DB.NOS_AUTH_PRD_GK1V_EDWPR)
                    STOREDAS('parquet')
                    MAXOBJECTSIZE('16MB')
                    COMPRESSION('SNAPPY')
                ) AS d;
                """
            key_name=table_name+"_sql_query"
            ti.xcom_push(key=key_name, value=sql_query)            
            try:
                print(sql)
                cursor.execute(sql)
            except Exception as e:
                print('Stagging table creation failed!!!!')
                print(f"reason: {e}")
                raise
        add_stagging_table=add_stagging_table(create_project_specs,table)
       
        @task(trigger_rule="all_success", multiple_outputs=True, on_failure_callback=on_failure_audit_update)
        def nos_to_gcs_load(create_project_specs,**kwargs):
            ti = kwargs['ti']
            i=ti.map_index
            TABLES_TO_EXPORT = Variable.get(key=f'{DAG_NAME}_TABLE_TO_EXPORT', deserialize_json=True)
            table_name=TABLES_TO_EXPORT[i]['bq_table_name']
            key_name=table_name+"_sql_query"
            sql= ti.xcom_pull(task_ids='extract_table.add_stagging_table', key=key_name)
            t1=TeradataOperator(
                task_id='nos_to_gcs_load',
                conn_id=NOS_CONN_ID,
                sql=sql,
                retries=0,
                on_failure_callback=on_failure_audit_update,
                )
            t1.execute(dict())
        nos_to_gcs_load=nos_to_gcs_load(create_project_specs)
        
        @task(trigger_rule="all_success", multiple_outputs=True, on_failure_callback=on_failure_audit_update)
        def gcs_to_bq_stg_load(create_project_specs,**kwargs):
            ti = kwargs['ti']
            run_id=kwargs['dag_run'].run_id
            TABLES_TO_EXPORT = Variable.get(key=f'{DAG_NAME}_TABLE_TO_EXPORT',deserialize_json=True)
            table_name = TABLES_TO_EXPORT[ti.map_index].get('bq_table_name')
            BQ_TARGET_PROJECT=str(create_project_specs['BQ_TARGET_PROJECT'])
            GCP_CONN_DO_ID_PROD=str(create_project_specs['GCP_CONN_DO_ID_PROD'])
            GCS_PREFIX=str(create_project_specs['GCS_PREFIX'])
            GCS_BUCKET=str(create_project_specs['GCS_BUCKET'])
            STAGE_DATASET=str(create_project_specs['STAGE_DATASET'])
            sanitize_run_id=replace_special_chars_and_spaces(run_id)
            source_object=f"gs://{GCS_BUCKET}/{GCS_PREFIX}/{sanitize_run_id}/table_name={table_name}/try_number={ti.try_number}/*.parquet"
            
            gcs_to_bq_stg_load=BigQueryInsertJobOperator(
            task_id='gcs_to_bq_stg_load',
            configuration={
                "load":{
                    "sourceUris":[source_object],
                "destinationTable":{
                    "projectId":BQ_TARGET_PROJECT,
                    "datasetId":STAGE_DATASET,
                    "tableId":f"{table_name}_hdl"
                },
                "sourceFormat":"PARQUET",
                "createDisposition":"CREATE_NEVER",
                "autodectect":False,
                "writeDisposition":"WRITE_TRUNCATE",
                "allowQuotedNewlines":True
                }
            },
            location='US',
            gcp_conn_id=GCP_CONN_DO_ID_PROD)          
            gcs_to_bq_stg_load.execute(context=kwargs)         

        gcs_to_bq_stg_load = gcs_to_bq_stg_load(create_project_specs)
     
        @task(trigger_rule="all_success", multiple_outputs=True,on_failure_callback=on_failure_audit_update,)
        def bq_to_bq_tgt_load(create_project_specs,**kwargs):
            ti = kwargs['ti']
            TABLES_TO_EXPORT = Variable.get(key=f'{DAG_NAME}_TABLE_TO_EXPORT',deserialize_json=True)
            table_name = TABLES_TO_EXPORT[ti.map_index].get('bq_table_name')
            schema_name = TABLES_TO_EXPORT[ti.map_index].get('bq_schema_name')
            BQ_TARGET_PROJECT=str(create_project_specs['BQ_TARGET_PROJECT'])
            GCP_CONN_DO_ID_PROD=str(create_project_specs['GCP_CONN_DO_ID_PROD'])
            STAGE_DATASET=str(create_project_specs['STAGE_DATASET'])
            sql=f"BEGIN CALL `{BQ_TARGET_PROJECT}.{STAGE_DATASET}.sp_stage_to_target_bq_load`('{STAGE_DATASET}','{schema_name}','{table_name}_hdl'); END;"
            hook = BigQueryHook(gcp_conn_id=GCP_CONN_DO_ID_PROD, use_legacy_sql=False)
            conn = hook.get_conn()
            cursor = conn.cursor()
            try:
                print(sql)
                cursor.execute(sql)
            except Exception as e:
                print(f"reason: {e}")
                raise

        bq_to_bq_tgt_load=bq_to_bq_tgt_load(create_project_specs)

        @task(trigger_rule="all_success", multiple_outputs=True,on_failure_callback=on_failure_audit_update,)
        def prod_to_ple_load(create_project_specs,**kwargs):
            ti = kwargs['ti']
            dag_conf = kwargs['dag_run'].conf or {}
            if 'ple_flag' in dag_conf :
                TABLES_TO_EXPORT = Variable.get(key=f'{DAG_NAME}_TABLE_TO_EXPORT',deserialize_json=True)
                table_name = TABLES_TO_EXPORT[ti.map_index].get('bq_table_name')
                schema_name = TABLES_TO_EXPORT[ti.map_index].get('bq_schema_name')
                BQ_TARGET_PROJECT=str(create_project_specs['BQ_TARGET_PROJECT'])
                GCP_CONN_DO_ID_PLE=str(create_project_specs['GCP_CONN_DO_ID_PLE'])
                BQ_PLE_PROJECT=str(create_project_specs['BQ_PLE_PROJECT'])
                prod_to_ple_load = BigQueryToBigQueryOperator(
                    task_id="prod_to_ple_load",
                    source_project_dataset_tables=f"{BQ_TARGET_PROJECT}.{schema_name}.{table_name}_hdl",
                    destination_project_dataset_table=f"{BQ_PLE_PROJECT}.{schema_name}.{table_name}",
                    write_disposition="WRITE_TRUNCATE",
                    gcp_conn_id=GCP_CONN_DO_ID_PLE)
                prod_to_ple_load.execute(context=kwargs)

            else:
                pass

        prod_to_ple_load=prod_to_ple_load(create_project_specs)

        @task(trigger_rule="all_success", multiple_outputs=True)
        def update_metadata(create_project_specs,**kwargs):
            ti = kwargs['ti']
            dag_id=kwargs['dag'].dag_id
            run_id=kwargs['dag_run'].run_id
            TABLES_TO_EXPORT = Variable.get(key=f'{DAG_NAME}_TABLE_TO_EXPORT',deserialize_json=True)
            status="Completed"
            BQ_TARGET_PROJECT=str(create_project_specs['BQ_TARGET_PROJECT'])
            GCP_CONN_DO_ID_PROD_AUDIT=str(ti.xcom_pull(task_ids='create_project_specs',key=f'{DAG_NAME}_GCP_CONN_DO_ID_PROD_AUDIT'))
            GCP_CONN_DO_ID_PROD=str(ti.xcom_pull(task_ids='create_project_specs',key=f'{DAG_NAME}_GCP_CONN_DO_ID_PROD'))
            BQ_TARGET_PROJECT_AUDIT=str(ti.xcom_pull(task_ids='create_project_specs',key=f'{DAG_NAME}_BQ_TARGET_PROJECT_AUDIT'))
            updateTableStatus(TABLES_TO_EXPORT[ti.map_index],status,dag_id,run_id,BQ_TARGET_PROJECT,BQ_TARGET_PROJECT_AUDIT,GCP_CONN_DO_ID_PROD,GCP_CONN_DO_ID_PROD_AUDIT)
        add_details_to_audit = update_metadata(create_project_specs)
        add_stagging_table >> nos_to_gcs_load  >> gcs_to_bq_stg_load >> bq_to_bq_tgt_load >> prod_to_ple_load >> add_details_to_audit

    extract_table=extract_table.expand(table=get_tables_to_export)
     
    @task(trigger_rule="all_done")
    def generate_load_report(create_project_specs,**context):
        ti=context["ti"]
        dag_id = context["dag"].dag_id
        run_id = context["run_id"]
        
        dag_conf = context['dag_run'].conf or {}
        if 'email' in dag_conf :
            email=dag_conf.get("email")
        else:
            email="anurag.mukati@verizon.com"
        
        GCP_CONN_DO_ID_PROD_AUDIT=str(ti.xcom_pull(task_ids='create_project_specs',key=f'{DAG_NAME}_GCP_CONN_DO_ID_PROD_AUDIT'))
        BQ_TARGET_PROJECT=str(ti.xcom_pull(task_ids='create_project_specs',key=f'{DAG_NAME}_BQ_TARGET_PROJECT_AUDIT'))
        BQ_TARGET_PROJECT_PR=str(create_project_specs['BQ_TARGET_PROJECT'])
        

        query = f"""
        WITH input_data AS (
            SELECT dag_id as DAG_NAME,run_id,BQDataset as BQ_DATASET,BQTable as BQ_TABLE,Bq_Records as BQ_COUNT,Td_Records as TD_COUNT,Status as STATUS FROM `{BQ_TARGET_PROJECT}.udm_prdstg_tbls.historical_load_audit_table_updated`
            WHERE dag_id = '{dag_id}' AND run_id = '{run_id}'
        ),
        status_flags AS (
            SELECT
            DAG_NAME,
            BQ_DATASET,
            BQ_TABLE,
            MAX(BQ_COUNT) AS BQ_COUNT,
            MAX(TD_COUNT) AS TD_COUNT,
                MAX(CASE WHEN STATUS = 'Completed'  THEN 1 ELSE 0 END) AS has_completed
            FROM input_data
            GROUP BY DAG_NAME,BQ_DATASET,BQ_TABLE
        )
        SELECT
            '{BQ_TARGET_PROJECT_PR}' as BQ_PROJECT,
            BQ_DATASET,
            BQ_TABLE,
            BQ_COUNT,
            TD_COUNT,
            CASE,
                WHEN has_completed = 1 THEN 'Completed'
                ELSE 'Failed'
            END AS RESULT
        FROM status_flags;
        """
        hook = BigQueryHook(gcp_conn_id=GCP_CONN_DO_ID_PROD_AUDIT, use_legacy_sql=False)
        df = hook.get_pandas_df(sql=query)
        print(df)
        

        if df.empty:
            html = "<p>No matching records found for this DAG run.</p>"
        else:
            html = styled_html_table(df,dag_id,run_id)
        send_load_report = EmailOperator(
        task_id="send_load_report",
        to=f"{email},anurag.mukati@verizon.com,anurag.mukati@onixnet.com",
        subject=f"HDL Report Parquet",
        html_content=html)
        send_load_report.execute(dict())
    generate_load_report=generate_load_report(create_project_specs)
    

    
    end_task = DummyOperator(task_id='end_task',  trigger_rule='all_done')
    
    start_task >> create_project_specs >> get_tables_to_export >> extract_table >> generate_load_report >>end_task
    
    

dynamic_task_group_tpt=dynamic_task_group_tpt()
