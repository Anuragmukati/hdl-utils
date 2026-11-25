import airflow
import datetime
import sys
import json
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context 
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
import logging
import traceback
from google.cloud import bigquery
import teradatasql
from airflow.utils.state import State
from airflow.providers.cncf.kubernetes.secret import Secret

BASE_DIR = "/home/airflow/gcs/dags/vz-it-gk1v-edwpr-0/"
sys.path.append(f"{BASE_DIR}/")

DAG_NAME = "dg_historical_load_td_to_bq_incr_fw_v1"
# app_name = "NOS_First"

host = 'tddp.tdc.vzwcorp.com'
username = 'UDBABQGCPDTS3'
password = 'Pass#2hdp'

UDBABQGCPDTS3_PASSWORD = Secret(
    deploy_type="env",
    deploy_target="UDBABQGCPDTS3_PASSWORD",
    secret="tpt-secrets",
    key="UDBABQGCPDTS3_PASSWORD",
)

GCS_ACCESS_KEY = Secret(
    deploy_type="env",
    deploy_target="GCS_ACCESS_KEY_ID",
    secret="tpt-secrets",
    key="GK1V_GCS_ACCESS_KEY_ID",
)

GCS_SECRET_ACCESS_KEY = Secret(
    deploy_type="env",
    deploy_target="GCS_SECRET_ACCESS_KEY",
    secret="tpt-secrets",
    key="GK1V_GCS_SECRET_ACCESS_KEY",
)


## Utility Functions

def read_config():
    with open('/home/airflow/gcs/dags/vz-it-gk1v-edwpr-0/history_load_config_prod_incr.json', 'r') as f:
        confid_details = json.load(f)
    return confid_details

def read_kube_config(table_size):
    with open('/home/airflow/gcs/dags/vz-it-gk1v-edwpr-0/kube_config_extralarge.json', 'r') as f:
        kube_details = json.load(f)
    return kube_details

def get_audit_row(data):
    row = {
    "dag_id" : data.get("dag_id"),
    "dag_run_id" : data.get("dag_run_id"),
    "dag_run_type" : data.get("dag_run_type"),
    "table_uid" : data.get("table_uid"),
    "data_loading_state" : data.get("data_loading_state"),
    "proc_run_id" : data.get("proc_run_id"),
    "delta_column_max_value" : data.get("delta_column_max_value"),
    "delta_where_condition" : data.get("delta_where_condition"),
    "src_schema" : data.get("src_schema"),
    "src_table" : data.get("src_table"),
    "stg_schema" : data.get("stg_schema"),
    "stg_table" : data.get("stg_table"),
    "tgt_schema" : data.get("tgt_schema"),
    "tgt_table" : data.get("tgt_table"),
    "start_time" : data.get("start_time"),
    "end_time" : data.get("end_time"),
    "src_row_count" : data.get("src_row_count"),
    "tgt_row_count" : data.get("tgt_row_count"),
    "is_row_count_matched" : data.get("is_row_count_matched"),
    "error_message" : data.get("error_message"),
    "job_status" : data.get("job_status"),
    "load_type" : data.get("load_type"),
    "pelican_trigger_id" : data.get("pelican_trigger_id"),
    "td_functional_id" : data.get("td_functional_id")
    }
    return row


def get_current_job_info(table_info,context):

    job_info = {
        "dag_id": context['dag'].dag_id,
        "dag_run_id" : context['dag_run'].run_id,
        "dag_run_type" : context['dag_run'].run_type,
        "table_uid" : table_info.get("table_uid"),
        "data_loading_state" : "start",
        "src_schema" : table_info.get("src_schema"),
        "src_table" : table_info.get("src_table"),
        "stg_schema" : table_info.get("stg_schema"),
        "stg_table" : table_info.get("stg_table"),
        "tgt_schema" : table_info.get("tgt_schema"),
        "tgt_table" : table_info.get("tgt_table"),
        "start_time" : datetime.datetime.now(datetime.timezone.utc),
        "end_time" : table_info.get("end_time"),
        "src_row_count" : table_info.get("src_row_count"),
        "tgt_row_count" : table_info.get("tgt_row_count"),   
        "is_row_count_matched" : table_info.get("is_row_count_matched"), 
        "job_status" : "inprogress",
        "load_type" : table_info.get("load_type"),
        "error_message": table_info.get("load_type"),
        # scd info
        "proc_run_id" : table_info.get("proc_run_id"),
        "delta_column_max_value": table_info.get("delta_column_max_value"),
        "delta_where_condition": table_info.get("delta_where_condition"),
        "pelican_trigger_id": table_info.get("pelican_trigger_id"),
        "td_functional_id" : table_info.get("td_functional_id")
    }
    return  job_info
            

def escape_string(string):
    if string:
        json.dumps(string)[1:-1]
    return None


def insert_audit(conn, data):

    config = read_config()
    
    bq_metadata_project= config["metadata_project"]
    bq_metadata_dataset= config["metadata_dataset"]
    bq_target_project = config["bq_target_project"]
    bq_metadata_audit_table = config["metadata_audit_table"]
    bq_metadata_info_table = config["metadata_info_table"]

    print("Running Audit insertion query: ")
    sql = f"""INSERT INTO `{bq_metadata_project}.{bq_metadata_dataset}.{bq_metadata_audit_table}`
    (
        dag_id,
        dag_run_id,
        dag_run_type,
        table_uid ,
        data_loading_state,
        proc_run_id,
        delta_column_max_value,
        delta_where_condition,
        stg_schema,
        stg_table,
        tgt_schema,
        tgt_table,
        start_time,
        end_time,
        src_row_count,
        tgt_row_count,
        is_row_count_matched,
        error_message,
        job_status,
        load_type,
        pelican_trigger_id,
        td_functional_id
    ) 
    VALUES ( 
    {safe_sql(data.get('dag_id'))},
    {safe_sql(data.get('dag_run_id'))},
    {safe_sql(data.get('dag_run_type'))},
    {safe_sql(data.get('table_uid'))},
    {safe_sql(data.get('data_loading_state'))},
    {safe_sql(data.get('proc_run_id'))},
    {safe_sql(data.get('delta_column_max_value'))},
    {safe_sql(data.get('delta_where_condition').replace("'","") if data.get("delta_where_condition") else None)},
    {safe_sql(data.get('stg_schema'))},
    {safe_sql(data.get('stg_table'))},
    {safe_sql(data.get('tgt_schema'))},
    {safe_sql(data.get('tgt_table'))},
    {safe_sql(data.get('start_time'))},
    {safe_sql(data.get('end_time'))},
    {safe_sql(data.get('src_row_count'))},
    {safe_sql(data.get('tgt_row_count'))},
    {safe_sql(data.get('is_row_count_matched'))},
    {safe_sql(escape_string(data.get('error_message')))},
    {safe_sql(data.get('job_status'))},
    {safe_sql(data.get('load_type'))},
    {safe_sql(data.get('pelican_trigger_id'))},
    {safe_sql(data.get('td_functional_id'))}
    )
    """
    print(f"**** Insert Audit query: \n {sql}")
    
    
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        print(f"Audit insert completed for {data.get('table_uid')}")
        return True
    except Exception as e:
        print(f"Audit insert failed for {data.get('table_uid')}")
        print(f"Error: ",str(e))
        print(f"Traceback: ",traceback.format_exc())
        return False


def safe_sql(val):
    if val is None or val == 'NULL':
        return "NULL"
    elif isinstance(val, str):
        return f"'{val}'"
    elif isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    elif isinstance(val, (int, float)):
        return str(val)
    elif hasattr(val, "isoformat"):
        return f"'{val.isoformat(sep=' ', timespec='seconds')}'"
    else:
        raise ValueError(f"Unsupported type: {type(val)}")

def get_bq_row_count(conn, bq_schema,bq_table):
    config = read_config()
    bq_target_project = config["bq_target_project"]
    print("**** Getting bq row count")
    try:
        # sql = f""" SELECT CAST(COUNT(*) AS INT64) FROM `{bq_target_project}.{bq_schema}.{bq_table}`;"""
        sql = f""" SELECT CAST(COUNT(*) AS INT64) FROM {bq_schema}.{bq_table};"""
        print("sql: ",sql)
        cursor = conn.cursor()
        cursor.execute(sql)
        row = cursor.fetchone()
        print("Row cont for table "+bq_schema+"."+bq_table+": "+str(row[0]))
        return row[0]
    except Exception as e:
        print(f"**** Error while getting bq row count for table {bq_schema}.{bq_table}: {str(e)}")
        print(f"Traceback: ",traceback.format_exc())
        return None

def get_td_row_count(td_schema,td_table, query=None):
    
    
    # host = config['TERADATA_HOSTNAME']
    # username = config['TERADATA_USERNAME']
    # password ="Pdts2025apr!"

    try:
        password = access_secret_version("td_pwd")
        print("**** Getting td row count")
        connection = teradatasql.connect(
            host=host,
            user=username,
            password=password
        )

        cursor = connection.cursor()
        if not query:
            query = f"SELECT CAST(COUNT(1) AS BIGINT) FROM {td_schema}.{td_table}"
        print("sql: ",query)
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        connection.close()
        # return 100

    except Exception as e:
        print(f"**** Error while getting td row count for table {td_schema}.{td_table}: {str(e)}")
        print(f"Traceback: ",traceback.format_exc())
        return e



def check_if_task_running(conn, table_uid):

    config = read_config()
    
    bq_metadata_project= config["metadata_project"]
    bq_metadata_dataset= config["metadata_dataset"]
    bq_target_project = config["bq_target_project"]
    bq_metadata_audit_table = config["metadata_audit_table"]
    bq_metadata_info_table = config["metadata_info_table"]

    sql = f""""SELECT * FROM `{bq_metadata_project}.{bq_metadata_dataset}.{bq_metadata_audit_table}`
    WHERE table_uid="{table_uid}" AND job_status IN ("RUNNING","INPROGRESS","QUEUE") """


    print(f"**** Checking if table is already running: \n {sql}")

    
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        return True if rows else None
    except Exception as e:
        print(f"Error while fetching running tasks")
        print(f"Error: ",str(e))
        print(f"Traceback: ",traceback.format_exc())
        return False

def generate_where_conditon( col, col_type, last_val):
    if col_type.strip().lower() == 'date':
        return f"WHERE {col} >= DATE ''{last_val}''"
    elif col_type.strip().lower() in ['datetime', 'timestamp']:
        return f" WHERE {col} >= TIMESTAMP ''{last_val}''"
    elif col_type.strip().lower() == 'integer':
        return f"WHERE {col} >= ''{last_val}''"
    else:
        return None
    
def generate_range_conditon( col, col_type, min_value, interval_range):
#  + INTERVAL '10' DAY;
# BETWEEN TIMESTAMP '2023-01-01 08:00:00' AND TIMESTAMP '2023-01-01 08:00:00' + INTERVAL '10' DAY;
    if col_type.strip().lower() == 'date':
        return f"WHERE {col} BETWEEN DATE ''{min_value}'' AND DATE ''{min_value}'' + INTERVAL ''{interval_range}'' DAY "
    elif col_type.strip().lower() in ['datetime', 'timestamp']:
        return f" WHERE {col} BETWEEN TIMESTAMP ''{min_value}'' AND TIMESTAMP ''{min_value}'' + INTERVAL ''{interval_range}'' DAY "
    elif col_type.strip().lower() == 'integer':
        return f"WHERE {col} BETWEEN ''{min_value}'' and  ''{min_value}'' + {interval_range} "
    else:
        return None


def get_delta_catchup_conditions(col, col_type, interval_range):
    if col_type.strip().lower() == 'date':
        return f"WHERE {col} >= CURRENT_DATE - INTERVAL ''{interval_range}'' DAY "
    elif col_type.strip().lower() in ['datetime', 'timestamp']:
        return f" WHERE {col} >= CURRENT_TIMESTAMP - INTERVAL ''{interval_range}'' DAY "
    elif col_type.strip().lower() == 'integer':
        return f"WHERE {col} >=  + {interval_range} "
    else:
        return None

def get_delta_td_min_value(td_schema,td_table,delta_column):
    
    try:
        query = f""" SELECT MIN({delta_column}) as min_value FROM {td_schema}.{td_table};"""
        print(f"**** Get min value from td, query: {query}")
        connection = teradatasql.connect(
            host=host,
            user=username,
            password=password
        )

        cursor = connection.cursor()
        cursor.execute(query)
        min_value = cursor.fetchone()[0]
        cursor.close()
        connection.close()
        return min_value

    except Exception as e:
        print(f"**** Error while getting td row count for table {td_schema}.{td_table}: {str(e)}")
        print(f"Traceback: ",traceback.format_exc())
        return None


def get_delta_max_value(conn, project_id,schema,table_name,delta_column):
    # query = f""" SELECT MAX({delta_column}) as max_value FROM `{project_id}.{schema}.{table_name}`;"""
    query = f""" SELECT MAX({delta_column}) as max_value FROM {schema}.{table_name};"""

    print(f"**** get max value query: {query}")
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        return rows[0][0] if rows and rows[0][0] is not None else None
    except Exception as e:
        print(f"Error while fetching running tasks")
        print(f"Error: ",str(e))
        print(f"Traceback: ",traceback.format_exc())
        return None


def execute_td_query(query):
    try:
        print(f"**** Executing TD query: {query}")
        connection = teradatasql.connect(
            host=host,
            user=username,
            password=password
        )

        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        connection.close()
        return result

    except Exception as e:
        print(f"**** Executing TD query: {str(e)}")
        print(f"Traceback: ",traceback.format_exc())
        return None

config = read_config()


TERADATA_HOSTNAME = "tddd.tdc.vzwcorp.com"
TERADATA_USERNAME = "UDBABQGCPDTS"



# gcs details
GCP_CONN_ID = config["GCP_CONN_ID"]
GCP_CONN_DO_ID = config["GCP_CONN_DO_ID"]
GCS_BUCKET = config["GCS_BUCKET"]
GCS_PREFIX = config["GCS_PREFIX"]
GCS_SCHEMA_DIR = config["GCS_SCHEMA_DIR"]


BQ_SOURCE_PROJECT = config["bq_source_project"]
BQ_TARGET_PROJECT = config["bq_target_project"]
BQ_TARGET_DATASET = config["bq_target_dataset"]
METADATA_DATASET = config["metadata_dataset"]
METADATA_TABLE = config["metadata_info_table"]
AUDIT_TABLE = config["metadata_audit_table"]

# METADATA_TABLE = "coe_historical_load_metadata_table"
# AUDIT_TABLE = "coe_historical_load_audit_table"

TABLES_TO_EXPORT = []
job_start_time = datetime.datetime.now(datetime.timezone.utc) 

hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
conn = hook.get_conn()
cursor = conn.cursor()

def extract_metadata():
    '''
    dag_conf = context['dag_run'].conf or {}
    if "Table_List" in dag_conf:
        manual_table_dict_list = dag_conf.get("Table_List", [])
        table_names = [entry["tablename"] for entry in manual_table_dict_list]
        joined_tables = ", ".join([f"'{name}'" for name in table_names])
        where_clause = f"CONCAT(bq_schema_name,'.', bq_table_name) IN ({joined_tables})"
    print("**** Extracting Metadata...")
    '''
    sql = f"""select * from `{BQ_SOURCE_PROJECT}.{METADATA_DATASET}.{METADATA_TABLE}`
    where  is_table_active=True;"""
    print(f"**** Metadata select query: \n {sql}")

    cursor.execute(sql)
    rows = cursor.fetchall()
    print(f"**** Total metadata table rows fetched: {len(rows)}")
    tables_metadata_info = []
    for row in rows:    
        table_info = {}
        table_info['is_table_active'] = row[0]
        table_info['table_uid'] = row[1]
        table_info['app_name'] = row[2]
        table_info['src_schema'] = row[3]
        table_info['src_table'] = row[4]
        table_info['stg_schema'] = row[5]
        table_info['stg_table'] = row[6]
        table_info['tgt_schema'] = row[7]
        table_info['tgt_table'] = row[8]
        table_info['load_type'] = row[9]
        table_info['delta_column'] = row[10]
        table_info['delta_column_type'] = row[11]
        table_info['delta_column_format'] = row[12]
        table_info['delta_interval'] = row[13]
        table_info['src_table_size'] = row[14]
        table_info['src_table_row_count'] = row[15]
        table_info['src_table_type'] = row[16]
        table_info['bus_key_column'] = row[17]
        table_info['change_value_column'] = row[18]
        table_info['start_date_column'] = row[19]
        table_info['end_date_column'] = row[20]
        table_info['stg_load_enabled'] = row[21]
        table_info['pelican_validation_enabled'] = row[22]
        table_info['select_query'] = row[23]
        table_info['load_functionality'] = row[24]
        #table_info['ple_load_enabled'] = row[25]
        #table_info['data_governance_enabled'] = row[26]
        #table_info['backup_destination_dataset'] = row[27]
        #table_info['where_condition'] = row[27]

        tables_metadata_info.append(table_info)

    print("**** List of tables to run for ... ")
    print(tables_metadata_info)
    return tables_metadata_info


TABLES_TO_EXPORT = extract_metadata()
print(f"***** TABLES_TO_EXPORT ******: {TABLES_TO_EXPORT} ")

def cleanup_gcs_on_failure(context):
    audit_on_task_failure(context)
    print(f"***** Cleanup started ******")
    ti = context['ti']
    TABLES_TO_EXPORT = Variable.get(key=DAG_NAME, deserialize_json=True)
    table_name = TABLES_TO_EXPORT[ti.map_index].get('tgt_table')

    gcs_path = f"{GCS_PREFIX}/table_name={table_name}/try_number=1/"
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
    # updateMetadataTable(conn,TABLES_TO_EXPORT[ti.map_index], "FAILED")


def audit_on_task_failure(context):
    print(f"***** Audit logs started ******")
    ti = context['ti']
    exception = context.get('exception')
    job_info = ti.xcom_pull(key=f"job_info_{ti.map_index}",task_ids="extract_table.start_loading_process", map_indexes = ti.map_index) 
    print(f"****** job_info: {job_info}")
    job_info['job_status'] = "failed"
    job_info["end_time"] = datetime.datetime.now(datetime.timezone.utc) # tpt end time
    job_info['error_message'] = f"""{str(exception)[:1000] if len(str(exception)) >= 1000 else str(exception)}""" if exception else  None
    insert_audit(conn,job_info)
    print(f"***** Audit insertion done ******")



    


@dag(
    dag_id=DAG_NAME,
    tags=["bundle:dif", "module:historical"],
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(1),
    max_active_tasks=4,
    default_args={
        "retries": 0,
        "retry_delay": datetime.timedelta(seconds=10),
    },
    render_template_as_native_obj=True,
)
def dynamic_task_group_tpt():
    
    def check_td_row_count(**context):
        ti = context['ti']
        print(f"****** map_index: {ti.map_index}")
        print(f"****** map_index: {context.get('map_index')}")
        job_info = ti.xcom_pull(key=f"job_info_{ti.map_index}",task_ids="extract_table.start_loading_process", map_indexes = ti.map_index) 
        print("Job Info: ", job_info)
        td_schema = job_info['src_schema']
        td_table = job_info['src_table']
        where_condition = job_info.get('delta_where_condition')
        query = f"SELECT  CAST(COUNT(*) AS BIGINT) FROM {td_schema}.{td_table}"
        if where_condition:
            query = f"{query} {where_condition}".replace("''","'")
        
        print("****query: ",query)
        row_count = get_td_row_count(td_schema,td_table, query=query)
        print("row_count: ",row_count)
        # row_count = 1
        if row_count and row_count > 0:
            return "extract_table.create_kpo_args"
        return "extract_table.end"

    

    def read_export_tpt():
        with open("/home/airflow/gcs/dags/vz-it-gk1v-edwpr-0/export_large.tpt", "r") as f:
            return f.read().replace("$", r"\$")

    @task
    def get_user_input(**context):
        tables_to_run = context['dag_run'].conf.get("table_list",[])
        print(f"**** Received tables to run:\n {tables_to_run}")
        return tables_to_run

    @task
    def get_tables_to_export(user_input_table_list):
        final_tables_to_export = []
        global TABLES_TO_EXPORT
        print(f"**** Tables to export: {TABLES_TO_EXPORT}")
        print(f"*** user_input_table_list: {user_input_table_list}")


        if user_input_table_list:
            # TABLES_TO_EXPORT = [ table for table in TABLES_TO_EXPORT if f"{table.get('src_schema')}.{table.get('src_table')}" in user_input_table_list ]
            for table in TABLES_TO_EXPORT:
                print(f"*** table: {table.get('src_schema')}.{table.get('src_table')}")
                if f"{table.get('src_schema')}.{table.get('src_table')}" in user_input_table_list:
                    final_tables_to_export.append(table)
        else:
            final_tables_to_export = TABLES_TO_EXPORT
        
        # Add logic to update the select query here
        for table in final_tables_to_export:
            select_query = table.get('select_query').strip().replace(";","")
            
            try:
                # custom where conditon query
                delta_interval = table.get('delta_interval')
                custom_where_condition = table.get('where_condition')
                if custom_where_condition:
                    select_query = f"""{select_query} {custom_where_condition} ;"""
                    print(f"**** custom_where_condition select_query:{select_query} ")
                elif delta_interval:
                    delta_catchup_condition = get_delta_catchup_conditions( col=table.get('delta_column'), col_type=table.get('delta_column_type'), 
                                        interval_range=delta_interval)
                    select_query = f"""{select_query} {delta_catchup_condition} ;"""
                    print(f"**** delta_interval select_query:{select_query} ")

                elif table.get('load_type').upper() in ["SCD_TYPE1","SCD_TYPE2"]:
                        bq_target_project = config["bq_target_project"]
                        last_val = get_delta_max_value(conn, bq_target_project,table.get('stg_schema'),table.get('stg_table'),table.get('delta_column'))
                        print(f"***** Delta column Max value: {last_val}")
                        
                        if last_val:
                            where_condition = generate_where_conditon(
                                col=table.get('delta_column'), 
                                col_type=table.get('delta_column_type'), 
                                last_val=last_val
                                )
                            select_query = f"""{select_query} {where_condition} ;""" 
                            print(f"**** Incremental(delta) select_query:{select_query} ")
                            table['delta_column_max_value'] = last_val
                            table['delta_where_condition'] = str(where_condition)

                        else:
                            # check for bq count, and if bq count is 0, Full load the table
                            print(f"***** Max value not found for delta column:{table.get('delta_column')} ,table:{table.get('tgt_schema')}.{table.get('tgt_table')}")
                            bq_row_count = get_bq_row_count(conn,bq_schema=table.get('stg_schema'),bq_table=table.get('stg_table'))
                            if bq_row_count == 0:
                                print(f"***** Table {table.get('tgt_schema')}.{table.get('tgt_table')} is empty, Hence going for full load") 
                                
                                ## Login to get the first load in range rather full load
                                # min_value = get_delta_td_min_value(table.get('src_schema'),table.get('src_table'),table.get('delta_column'))
                                # print(f"****Min value: {min_value}")
                                # interval_range = 10
                                # range_condition = generate_range_conditon( col=table.get('delta_column'), col_type=table.get('delta_column_type'), 
                                #                                         min_value=min_value, interval_range=interval_range)
                                # range_condition_select_query = f"""{select_query} {range_condition} ;"""
                
                else:
                    # "FULL_LOAD"
                    print(f"***** Full Load Table name:{table.get('src_schema')}.{table.get('src_table')}, query: {table['select_query']}")
                    pass

                   
                print(f"***** Generated select query for {table.get('src_schema')}.{table.get('src_table')} ---> {table.get('stg_schema')}.{table.get('stg_table')}, Type={table.get('load_type').upper()}:\n {select_query}")
                table['select_query'] = f"{select_query}"

            except Exception as e:
                print(f"**** Error while generating tdselect query for the  {table.get('src_schema')}.{table.get('src_table')} : {str(e)}")
                print(f"Traceback: ",traceback.format_exc())
                raise e
            
        print(f"**** Final Tables to export: {final_tables_to_export}")
        
        Variable.set(key=DAG_NAME, value=final_tables_to_export,
                     serialize_json=True)
        return Variable.get(key=DAG_NAME, deserialize_json=True)

    @task_group
    def extract_table(table):
        

        branch = BranchPythonOperator(
            task_id="check_table_count",
            python_callable=check_td_row_count,
            provide_context=True,
        )

        @task(multiple_outputs=True, provide_context=True,  on_failure_callback=audit_on_task_failure)
        def start_loading_process(table_to_extract,**kwargs):
            
            DAG_RUN_ID = kwargs['dag_run'].run_id
            DAG_ID = kwargs['dag'].dag_id
            print(f"**** In start_loading_process: \n DAG_ID: {DAG_ID},\n DAG_RUN_ID: {DAG_RUN_ID}")
            job_info = get_current_job_info(table_to_extract,kwargs)
            job_info['select_query'] = table_to_extract['select_query'] 
            job_info['delta_where_condition'] = table_to_extract.get('delta_where_condition')
             
            ti = kwargs['ti']
            ti.xcom_push(key=f"job_info_{ti.map_index}",value=job_info)  
            insert_audit(conn,job_info)
            return table_to_extract


        @task(multiple_outputs=True, provide_context=True, on_failure_callback=audit_on_task_failure)
        def create_kpo_args(table_to_extract, **kwargs):
            print(f"---------------------Select Query---------------------\n: {table_to_extract.get('select_query')}")
                        #Write a function to get all the details for large, medium or small table.
            
            kubeConfig = read_kube_config(table_size=table_to_extract.get('src_table_type'))
            
            cpu_req = kubeConfig["cpu_request"]
            memory_req = kubeConfig["memory_request"]

            cpu_li = kubeConfig["cpu_limit"]
            memory_li = kubeConfig["memory_limit"]

            arguments = [
                "-c",
                rf"""
                cd /tmp &&\
                echo "{read_export_tpt()}" > export_{table_to_extract.get('table_size')}.tpt && \
                more export_{table_to_extract.get('table_size')}.tpt && \
                (tbuild -C -f export_{table_to_extract.get('table_size')}.tpt -u "\
                    jobvar_tdpid='{TERADATA_HOSTNAME}', \
                    jobvar_username='{TERADATA_USERNAME}', \
                    jobvar_password='Pdts2025apr!', \
                    jobvar_num_read_instances={kubeConfig["num_read_instances"]}, \
                    jobvar_num_write_instances={kubeConfig["num_write_instances"]}, \
                    jobvar_selectstmt='{table_to_extract.get('select_query')}', \
                    jobvar_accessmoduleinitstr='\
                    Bucket={kubeConfig["gcs_bucket"]} \
                    Prefix={kubeConfig["gcs_prefix"]}/table_name={table_to_extract.get('tgt_table')}/try_number=$AIRFLOW_RETRY_NUMBER/ \
                    Object={kubeConfig["gcs_object_name"]} \
                    MaxObjectSize={kubeConfig["gcs_max_object_size"]} \
                    BufferSize={kubeConfig["gcs_buffer_size"]} \
                    BufferCount={kubeConfig["gcs_buffer_count"]} \
                    ConnectionCount={kubeConfig["gcs_connection_count"]}'") \

                tbuild_exit_code=$?
                if [[ $tbuild_exit_code -eq 0 || $tbuild_exit_code -eq 4 ]]; then
                    exit 0
                fi
                exit $tbuild_exit_code

                """,
            ]
            
            #print("---------------------------------------------") 
            #print(container_resources_assigned)
            tpt_start_time = datetime.datetime.now(datetime.timezone.utc)
            tpt_start_time = tpt_start_time.isoformat()
            ti = kwargs["ti"]
            ti.xcom_push(key=f"tpt_start_time_{ti.map_index}",value=tpt_start_time)
            return {"arguments": arguments, "cpu": cpu_req, "mem": memory_req, "cpu_limit": cpu_li, "mem_limit": memory_li}

        def get_kubernertes_operator(create_load_config):
            tpt = KubernetesPodOperator(
                task_id="extract_teradata_data",
                name="tpt",
                cmds=["bash"],
                container_resources=k8s.V1ResourceRequirements(
                    requests={
                        "cpu": create_load_config["cpu"],
                        "memory": create_load_config["mem"],
                    },
                    limits={
                        "cpu": create_load_config["cpu_limit"],
                        "memory": create_load_config["mem_limit"],
                    },
                ),
                # Increase pod startup timeout to 10 minutes
                startup_timeout_seconds=config["startup_timeout_seconds"],
                log_events_on_failure=True,
                namespace="composer-user-workloads",
                image=config["image"],
                config_file=config["config_file"],
                kubernetes_conn_id=config["kubernetes_conn_id"],
                secrets=[UDBABQGCPDTS3_PASSWORD, GCS_ACCESS_KEY, GCS_SECRET_ACCESS_KEY],
                env_vars={"AIRFLOW_RETRY_NUMBER": "'{{ task_instance.try_number }}'"},
                arguments=create_load_config["arguments"],
                on_failure_callback=cleanup_gcs_on_failure
            )
            return tpt

        @task(trigger_rule="all_success", multiple_outputs=True, on_failure_callback=cleanup_gcs_on_failure,provide_context=True)
        def get_tpt_output(tpt_output, **kwargs):
            # job_info = job_info.get("job_info")
            ti = kwargs['ti']
            
            dag_run = ti.get_dagrun()
            current_map_index = ti.map_index
            tpt_task_id = "extract_table.extract_teradata_data"
            state = None
            error = ""
            for task_ins in dag_run.get_task_instances():
                print(f"**** [{task_ins.map_index}]- Task ID: {task_ins.task_id}, State: {task_ins.state}")
                if task_ins.task_id == tpt_task_id and task_ins.map_index == current_map_index:
                    state = task_ins.state
                    error = task_ins.task.fail_reason if hasattr(task_ins.task, 'fail_reason') else 'No fail reason found'

            print("TPT state: ",state)
            if state == State.SUCCESS:
                print("*** TPT Export Success")
                error = None
            else:
                print("*** TPT Export Failed")
                print(f"***Error: {error}")

            print("*** Adding Tpt audits ")
            job_info = ti.xcom_pull(key=f"job_info_{ti.map_index}",task_ids="extract_table.start_loading_process", map_indexes = ti.map_index) 
            tpt_start_time = ti.xcom_pull(key=f"tpt_start_time_{ti.map_index}",task_ids="extract_table.create_kpo_args", map_indexes = ti.map_index) 
    
            job_info["data_loading_state"] =  "td-to-gcs"
            job_info["start_time"] = tpt_start_time # add Tpt star time here
            job_info["end_time"] = datetime.datetime.now(datetime.timezone.utc) # tpt end time
            job_info["job_status"] = state
            job_info["error_message"] = str(error) if  error else None

            insert_audit(conn,job_info)
            print("*** Adding Tpt audits done")
            
            TABLES_TO_EXPORT = Variable.get(key=DAG_NAME,
                                            deserialize_json=True)
            table_name = TABLES_TO_EXPORT[ti.map_index].get('tgt_table')
            schema_name = TABLES_TO_EXPORT[ti.map_index].get('tgt_schema')

            #WRITE DESPOSITION
            load_type = TABLES_TO_EXPORT[ti.map_index].get('load_type')

            if load_type == "FULL_LOAD":
                write_mode= f"WRITE_TRUNCATE"
            else:
                write_mode= f"WRITE_APPEND"
                table_name = TABLES_TO_EXPORT[ti.map_index].get('stg_table')
                schema_name = TABLES_TO_EXPORT[ti.map_index].get('stg_schema')

            execution_ts = kwargs['logical_date'].strftime('%Y%m%d_%H%M%S')
            actual_schema_name = schema_name.split('.')[-1]
            src_tgt_details = {
                "source_objects": f"{GCS_PREFIX}/table_name={table_name}/try_number=1/*.csv",
                # "destination_project_dataset_table": f'{BQ_TARGET_PROJECT}.{schema_name}.{table_name}',
                "destination_project_dataset_table": f'{actual_schema_name}.{table_name}',

                "schema_object_bucket": GCS_BUCKET,
                "schema_object": f"{GCS_SCHEMA_DIR}/{actual_schema_name}_{table_name}.json",
                "write_mode": write_mode,
                "tpt_status": state
            }

            print(f"*** Return data from get_tpt_output:\n {src_tgt_details}")
            return  src_tgt_details
    
        @task(trigger_rule="all_success", multiple_outputs=True, on_failure_callback=cleanup_gcs_on_failure,provide_context=True)
        def get_gsc_to_bq_operator(gcs_input, **kwargs):
            start_time = datetime.datetime.now(datetime.timezone.utc)
            gcs_to_bq_operator =  GCSToBigQueryOperator(
                task_id="create_gcs_to_bq_load_configs",
                bucket=GCS_BUCKET,
                write_disposition=gcs_input["write_mode"],
                source_format="CSV",
                allow_jagged_rows=True,
                allow_quoted_newlines=True,
                quote_character="\"",
                gcp_conn_id=GCP_CONN_ID,
                field_delimiter="|",
                source_objects=gcs_input["source_objects"],
                destination_project_dataset_table=gcs_input[
                    "destination_project_dataset_table"],
                schema_object_bucket=gcs_input["schema_object_bucket"],
                schema_object=gcs_input["schema_object"],
            ).execute(context=kwargs)
            print("*** Adding gcs to bq audits ")
            ti = kwargs['ti']
            job_info = ti.xcom_pull(key=f"job_info_{ti.map_index}",task_ids="extract_table.start_loading_process", map_indexes = ti.map_index) 
            job_info["data_loading_state"] =  "gcs-to-bq"
            job_info["start_time"] = start_time 
            job_info["end_time"] = datetime.datetime.now(datetime.timezone.utc) 
            job_info["job_status"] = "success"
            job_info["error_message"] = None
            insert_audit(conn,job_info)
            print("*** Adding gcs to bq audits done")

            return gcs_to_bq_operator

        
        @task(trigger_rule="all_success", multiple_outputs=True,provide_context=True, on_failure_callback=cleanup_gcs_on_failure)
        def call_store_proc(gcs_to_bq_load,**kwargs):
            proc_run_id =int(datetime.datetime.now(datetime.timezone.utc).timestamp())
            status = None
            row_count = None
            DAG_RUN_ID = kwargs['dag_run'].run_id
            DAG_ID = kwargs['dag'].dag_id
            ti = kwargs['ti']
            job_info = ti.xcom_pull(key=f"job_info_{ti.map_index}",task_ids="extract_table.start_loading_process", map_indexes = ti.map_index) 
            job_info["data_loading_state"] =  "stg-to-bq"

            if job_info.get('load_type') == "FULL_LOAD":
                print("****Skipping store proc call for FULL Load")
                job_info["job_status"] = "skipped"
                insert_audit(conn,job_info)
                return {}
            else:
                try:
                    job_info["start_time"] =  datetime.datetime.now(datetime.timezone.utc) 
                    job_info["data_loading_state"] =  "stg-to-bq"
                    
                    time_now = datetime.datetime.now(datetime.timezone.utc)
                    
                    run_id = proc_run_id
                    job_id = job_info["table_uid"]
                    run_date = datetime.date.today().strftime('%Y-%m-%d')
                    bq_target_project = config["bq_target_project"]
                    # tgt_table = f"`{bq_target_project}`.{job_info.get('tgt_schema')}.{job_info.get('tgt_table')}"
                    tgt_table = f"{job_info.get('tgt_schema')}.{job_info.get('tgt_table')}"
                    sp_start_time = time_now.replace(tzinfo=None).isoformat(timespec="seconds")  # YYY-MM-DDTHH:MM:S
                    query = f"""
                    DECLARE RECORD_COUNT INT64; 
                    DECLARE SP_STATUS INT64;
                    CALL `vz-it-np-gk1v-dev-edwpr-0.udm_prdstg_tbls.PROC_SCD_DATA_LOAD_FRAMEWORK` 
                    ({safe_sql(run_id)}, {safe_sql(job_id)}, '{run_date}', NULL, NULL,
                     {safe_sql(tgt_table)}, {safe_sql(sp_start_time)}, RECORD_COUNT, SP_STATUS);
                    """
                    print(f"******* SP QUERY: {query}")

                    cursor = conn.cursor()
                    cursor.execute(query)
                    result = cursor.fetchall()
                    print(f"******* Result of sp: {result}")
                    if result:
                        row_count, status = result[0]
                    print(f"****Sp status: {status}, row_count: {row_count}, proc_run_id:{proc_run_id} ")
                    # status = rows[0][0] if rows and rows[0][0] is not None else None
                    job_info["end_time"] = datetime.datetime.now(datetime.timezone.utc) 
                    job_info['proc_run_id'] = proc_run_id
                    if status == 0:
                        job_info["job_status"] = "success"
                    else:
                        job_info["job_status"] = "failed"
                        job_info["error_message"] = "Store proc failed"
                    
                    
                    # return rows[0][0] if rows and rows[0][0] is not None else None
                except Exception as e:
                    print(f"Error while running store proc")
                    print(f"Error: ",str(e))
                    print(f"Traceback: ",traceback.format_exc())
                    job_info["job_status"] = "Failed"
                    job_info["error_message"] = str(e)
                    raise e
                
                print("*** Adding gcs to bq audits ")
                insert_audit(conn,job_info)
                print("*** Adding gcs to bq audits done")

                return {
                        "proc_name":"",
                        "proc_run_id": proc_run_id,
                        "proc_status": status,
                        "row_count": row_count
                    }
          
        @task(trigger_rule="all_success", multiple_outputs=True,provide_context=True)
        def update_audit(store_proc_call, **kwargs):
            ti = kwargs['ti']

            print(f"""**** Loading done: {TABLES_TO_EXPORT[ti.map_index].get("src_schema")}.{TABLES_TO_EXPORT[ti.map_index].get("src_table")} ----> {TABLES_TO_EXPORT[ti.map_index].get("tgt_schema")}.{TABLES_TO_EXPORT[ti.map_index].get("tgt_table")}""")
            
            job_info = ti.xcom_pull(key=f"job_info_{ti.map_index}",task_ids="extract_table.start_loading_process", map_indexes = ti.map_index) 

            bq_rows = get_bq_row_count(conn, job_info.get('tgt_schema'),job_info.get('tgt_table'))
            td_rows = get_td_row_count(job_info.get('src_schema'),job_info.get('src_table'))
            
            print(f"***bq_rows: {bq_rows}")         
            print(f"***td_rows: {td_rows}")         
            
            job_info["data_loading_state"] =  "td-to-bq"
            job_info["start_time"] = job_start_time 
            job_info["end_time"] = datetime.datetime.now(datetime.timezone.utc) # tpt end time
            job_info["job_status"] = "success"
            job_info["error_message"] = None
            job_info["src_row_count"] = td_rows
            job_info["tgt_row_count"] = bq_rows
            job_info["is_row_count_matched"] = bq_rows == td_rows
                       
            print("*** Adding final audits ")
            insert_audit(conn,job_info)
            print("*** Adding final audits done")
            
        @task(trigger_rule="all_success", multiple_outputs=True, on_failure_callback=cleanup_gcs_on_failure,provide_context=True)
        def archive(**kwargs):
            ti = kwargs['ti']
            TABLES_TO_EXPORT = Variable.get(key=DAG_NAME,
                                            deserialize_json=True)
            table_name = TABLES_TO_EXPORT[ti.map_index].get('tgt_table')
            table_schema = TABLES_TO_EXPORT[ti.map_index].get('tgt_schema')
            

            source_bucket = config["source_bucket"]
            destination_bucket = config["backup_destination_bucket"]
            source_folder = f"{GCS_PREFIX}/table_name={table_name}/try_number=1/"
            destination_folder = f"{GCS_PREFIX}/archive/"

            gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)

            source_files = gcs_hook.list(source_bucket, prefix=source_folder)
            if not source_files:
                print(f"**** No files found in {source_folder} to move.")
                return

            for file_path in source_files:
                relative_path = file_path[len(source_folder):]
                destination_path = f'{destination_folder}{relative_path}'
                gcs_hook.copy(source_bucket, file_path, destination_bucket, destination_path)
                gcs_hook.delete(source_bucket, file_path)
            print(f"**** Folder {source_folder} has been successfully moved to {destination_folder}")

        @task(trigger_rule="all_done")
        def end(**kwargs):
            print("Ending the task")

        table_to_extract = start_loading_process(table)
        create_load_config = create_kpo_args(table_to_extract)
        tpt = get_kubernertes_operator(create_load_config)
        gcs_input = get_tpt_output(tpt)
        gcs_to_bq_load = get_gsc_to_bq_operator(gcs_input)
        store_proc_call = call_store_proc(gcs_to_bq_load)
        add_details_to_audit = update_audit(store_proc_call)
        archive_and_backup_table = archive()
        finish = end()
        table_to_extract >> branch >> [create_load_config, finish]  
        create_load_config >> tpt >> gcs_input >> gcs_to_bq_load >> store_proc_call >> add_details_to_audit >> archive_and_backup_table >> finish

    
    user_input_table_list = get_user_input()
    get_tables_to_load = get_tables_to_export(user_input_table_list)
    extract_table.expand(table=get_tables_to_load)


# Instantiate the DAG
dynamic_task_group_tpt()
