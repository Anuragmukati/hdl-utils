# Refactored DAG: Sequential, Single Table Only, Audit Logic Preserved

import airflow
import datetime
import sys
import json
from airflow import DAG
from airflow.exceptions import AirflowFailException,AirflowSkipException
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.decorators import dag, task, task_group
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from google.cloud.exceptions import NotFound
import traceback
import teradatasql
import re

from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.hooks.base import BaseHook

BASE_DIR = "/home/airflow/gcs/dags/vz-it-gk1v-edwpr-0/"
sys.path.append(f"{BASE_DIR}/")

### Connection details for teradata "
conn_id='UGCPDTSNOS1'
conn=BaseHook.get_connection(conn_id)
host = conn.host
username = conn.login
password = conn.password


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



###### project level config #########
def read_config():
    with open('/home/airflow/gcs/dags/vz-it-gk1v-edwpr-0/history_load_config_prod.json', 'r') as f:
        confid_details = json.load(f)
    return confid_details
config = read_config()
GCP_CONN_ID = config["GCP_CONN_ID"]
GCP_CONN_DO_ID = config["GCP_CONN_DO_ID"]
GCS_BUCKET = config["GCS_BUCKET"]
GCS_PREFIX = config["GCS_PREFIX"]
Audit_db=config["Audit_Dataset"]
Metadata_db=config["Metadata_Dataset"]
Metadata_tbl=config["Metadata_Tbl"]
Audit_tbl=config["Audit_Table"]
Queue_Table=str(config["Queue_Table_large"])+"_Small"
bq_prod_project=config["bq_prod_project"]
bq_ple_project=config["bq_ple_project"]
# function_ids=['UGCPDTSNOS2','UGCPDTSNOS5','mktgcpprdsel','Mktaidgcpprdsel']
# functional_id_used=function_ids[random.randint(0,len(function_ids)-1)]
GCP_CONN_DO_ID_PLE='sa-vz-it-gk1v-pleedwdo-0-app'
gcs_bucket="gk1v-prod-edwpr-0-usre-nos-inbound"

###### BQ PROD DO HOOK ######
prd_hook = BigQueryHook(gcp_conn_id=GCP_CONN_DO_ID, use_legacy_sql=False)
prd_conn = prd_hook.get_conn()
prd_cursor = prd_conn.cursor()

###### BQ PLE DO HOOK ######
pled_hook = BigQueryHook(gcp_conn_id=GCP_CONN_DO_ID_PLE, use_legacy_sql=False)
pled_conn = pled_hook.get_conn()
pled_cursor = pled_conn.cursor()

######## Kubernetes configuration ############
def read_kube_config(table_size):
    table_size=table_size.lower()
    with open(f'/home/airflow/gcs/dags/vz-it-gk1v-edwpr-0/kube_config_extralarge.json', 'r') as f:
        kube_details = json.load(f)
    return kube_details

####### get bq row count #######
def get_bq_row_count( bq_schema,bq_table,sql=None):
    print("**** Getting bq row count")
    try:
        if not sql:
            sql = f""" SELECT CAST(COUNT(*) AS INT64) FROM {bq_prod_project}.{bq_schema}.{bq_table};"""
        print("sql: ",sql)
        prd_cursor.execute(sql)
        row = prd_cursor.fetchone()
        print("Row cont for table "+str(bq_schema)+"."+str(bq_table)+": "+str(row[0]))
        return int(row[0])
    except Exception as e:
        print(f"**** Error while getting bq row count for table {bq_schema}.{bq_table}: {str(e)}")
        print(f"Traceback: ",traceback.format_exc())
        return -1

####### get td row count ##########
def get_td_row_count(td_schema,td_table,query=None):
    try:
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
        return count

    except Exception as e:
        print(f"**** Error while getting td row count for table {td_schema}.{td_table}: {str(e)}")
        print(f"Traceback: ",traceback.format_exc())
        return -1

def prod_to_ple(**kwargs):
    ti = kwargs['ti']
    job_info = ti.xcom_pull(key='table_info',task_ids='extract_metadata') 
    db_nm=job_info.get('tgt_schema')
    tbl_nm=job_info.get('tgt_table')
    ple_load=job_info.get('ple_load')
    if ple_load:
        sql=f"TRUNCATE TABLE {bq_ple_project}.{db_nm}.{tbl_nm}; INSERT INTO {bq_ple_project}.{db_nm}.{tbl_nm} SELECT * FROM {bq_prod_project}.{db_nm}.{tbl_nm}"
        hook = BigQueryHook(gcp_conn_id=GCP_CONN_DO_ID_PLE, use_legacy_sql=False)
        conn = hook.get_conn()
        cursor = conn.cursor()
        try:
            print(sql)
            cursor.execute(sql)
        except Exception as e :
            print('PROD TO PLE got failed!!!!!',str(e))
    else:
        print("Ple flag is disabled")

def extract_metadata(**context):
    tables_to_run = context['dag_run'].conf.get("table_list",'')
    ple_load=context['dag_run'].conf.get("ple_load",'')
    if tables_to_run:
        print(f"**** Received tables to run:\n {tables_to_run}")
    else:
        print("No Input got from config")
        raise AirflowFailException("Update Queue Table Query Got Failed")
    sql = f"""select is_table_active,table_uid,src_schema,src_table,stg_schema,stg_table,tgt_schema,tgt_table,load_type,delta_column,delta_column_type,delta_column_format,delta_interval,app_name,src_table_size,src_table_row_count,src_table_type,bus_key_column,change_value_column,start_date_column,end_date_column,stg_load_enabled,plican_validation_enabled,select_query,load_functionality,select_query_tpt from `{bq_ple_project}.{Metadata_db}.{Metadata_tbl}`
    where  is_table_active=True and concat(tgt_schema,'.',tgt_table)='{tables_to_run}' limit 1;"""
    print(f"**** Metadata select query: \n {sql}")
    
    prd_cursor.execute(sql)
    row = prd_cursor.fetchone()
    
    print(f"**** Total metadata table rows fetched: {len(row)}") 
    table_info = {}
    table_info['is_table_active'] = row[0]
    table_info['table_uid'] = row[1]
    table_info['src_schema'] = row[2]
    table_info['src_table'] = row[3]
    table_info['stg_schema'] = row[4]
    table_info['stg_table'] = row[5]
    table_info['tgt_schema'] = row[6]
    table_info['tgt_table'] = row[7]
    table_info['load_type'] = row[8]
    table_info['delta_column'] = row[9]
    table_info['delta_column_type'] = row[10]
    table_info['delta_column_format'] = row[11]
    table_info['delta_interval'] = row[12]
    table_info['app_name'] = row[13]
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
    table_info['select_query_tpt'] = row[25]
    dag_id_raw= context['dag'].dag_id
    table_info['dag_id']=dag_id_raw[0] if isinstance(dag_id_raw,list) else str(dag_id_raw)
    dag_run_id = context['dag_run'].run_id
    table_info['dag_run_id']=dag_run_id[0] if isinstance(dag_run_id,list) else str(dag_run_id)
    dag_run_type = context['dag_run'].run_type
    table_info['dag_run_type']=dag_run_type[0] if isinstance(dag_run_type,list) else str(dag_run_type)
    table_info['ple_load']=ple_load

    # td_row_count = get_td_row_count(row[2],row[3])
    # bq_row_count=get_bq_row_count(row[6],row[7])
    bq_row_count=-1
    td_row_count=1
    
    table_info["src_row_count"]=td_row_count
    
    print("TD row_count: ",td_row_count)
    print("BQ row_count: ",bq_row_count)
    ti = context['ti']
    ti.xcom_push(key='table_info', value=table_info)
    if td_row_count==0:
        audit_update(status="SKIPPED",error='Source have Zero Count',**context)
        print("Table skipped due to zero count")
        # del_queue_tbl(tbl_uid,app_name)
        raise AirflowSkipException("Table skipped due to zero count")
    elif td_row_count==bq_row_count:
        audit_update(status="SKIPPED",error='Source and Target count Are Matching but data got refreshed in Ple',**context)
        print("Table skipped due to count matching")
        # del_queue_tbl(tbl_uid,app_name)
        prod_to_ple(**context)
        raise AirflowSkipException("Count getting matched to skipping the rest of task")
    

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

def escape_string(string):
    if string:
        json.dumps(string)[1:-1]
    return None

def insert_audit(pled_conn, data):
    sql = f"""INSERT INTO `{bq_ple_project}.{Audit_db}.{Audit_tbl}`
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
    
    
    try:
        pled_cursor.execute(sql)
        print(f"Audit insert completed for {data.get('table_uid')}")
        return True
    except Exception as e:
        print(f"Audit insert failed for {data.get('table_uid')}")
        print(f"Error: ",str(e))
        print(f"Traceback: ",traceback.format_exc())
        return False


def audit_update(status=None,error=None,**context):
    print(f"***** Audit logs started ******")
    ti = context['ti']
    exception = error
    job_info = ti.xcom_pull(key='table_info',task_ids='extract_metadata') 
    print(f"****** job_info: {job_info}")
    job_info['job_status'] = status
    job_info["end_time"] = datetime.datetime.now() # tpt end time
    job_info['error_message'] = f"""{str(exception)[:1000] if len(str(exception)) >= 1000 else str(exception)}""" if exception else  None
    
    if status=='SUCCESS' or status.lower()=='skipped':
        bq_row_count=0
        if job_info.get('load_type')=='FULL_LOAD':
            bq_row_count=get_bq_row_count(job_info.get('tgt_schema'),job_info.get('tgt_table'))
        else:
            bq_row_count=get_bq_row_count(job_info.get('stg_schema'),job_info.get('stg_table'))
        job_info["tgt_row_count"]=bq_row_count
    insert_audit(pled_conn,job_info)
    print(f"***** Audit insertion done ******")

def clean_error_msg(error_msg):
    msg=str(error_msg)
    msg=re.sub(r"[\n\r\t]+"," ",msg)
    msg=re.sub(r"[\"']","",msg)
    msg=re.sub(r"\s{2,}"," ",msg)
    return msg.strip()

def fail_task(context):
    task_instance=context.get('task_instance')
    exception=context.get('exception')
    error=clean_error_msg(str(task_instance)+" "+str(exception))
    # updt_sql('Dag Failed At '+str(task_instance),exception)
    audit_update(status=f"Dag_Failed at {task_instance}",error_message=error,**context)
    print("DAG execution failed!",error)

def check_tables_details(**kwargs):
    ti=kwargs['ti']
    job_info = ti.xcom_pull(key='table_info',task_ids='extract_metadata') 
    db_nm=job_info.get('tgt_schema')
    tbl_nm=job_info.get('tgt_table')
    src_db_nm=job_info.get('src_schema')
    src_tbl_nm=job_info.get('src_table')
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_DO_ID, use_legacy_sql=False)
    client=hook.get_client()
    tbl_fl_nm=f'{bq_prod_project}.{db_nm}.{tbl_nm}'
    try:
        client.get_table(tbl_fl_nm)
        print(f"Table is exists:{tbl_fl_nm}")
    except NotFound:
        print(f"Table is not exists:{tbl_fl_nm}")
        audit_update('Skipped',error_message='Table Not Exists',**kwargs)
        raise AirflowSkipException(f"skipping because table {tbl_fl_nm} not found.")
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_DO_ID_PLE, use_legacy_sql=False)
    query = f"SELECT COUNT(1) AS cnt FROM `{bq_ple_project}.{db_nm}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{tbl_nm}'"
    print(query)
    # ple_col_cnt=int(hook.get_pandas_df(sql=query)["cnt"][0])
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_DO_ID,use_legacy_sql=False)
    query = f"SELECT COUNT(1) AS cnt FROM `{bq_prod_project}.{db_nm}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{tbl_nm}'"
    print(query)
    # prd_col_cnt=int(hook.get_pandas_df(sql=query)["cnt"][0])
# Teradata column count
    connection = teradatasql.connect(
            host=host,
            user=username,
            password=password
        )
    cursor = connection.cursor()
    cursor.execute(f"SELECT COUNT(1) FROM dbc.columnsV WHERE databasename='{src_db_nm}' AND tablename='{src_tbl_nm}'")
    # td_col_cnt=cursor.fetchone()[0]

    # if td_col_cnt != ple_col_cnt or prd_col_cnt != td_col_cnt:
        # raise AirflowFailException(f"Column count mismatch for {db_nm}.{tbl_nm}: PLE={ple_col_cnt}, PROD={prd_col_cnt}, Teradata={td_col_cnt}")
    # print(f"Column count match for {db_nm}.{tbl_nm}: {ple_col_cnt}")
def read_export_tpt():
    with open("/home/airflow/gcs/dags/vz-it-gk1v-edwpr-0/export_large_v2.tpt", "r") as f:
        return f.read().replace("$", r"\$")

def create_kpo_args(**kwargs):
    #Write a function to get all the details for large, medium or small table.
    ti=kwargs['ti']
    job_info = ti.xcom_pull(key='table_info',task_ids='extract_metadata') 
    print(f"---------------------Select Query---------------------\n: {job_info.get('select_query_tpt')}")
    table_size=job_info.get('src_table_type')
    kubeConfig = read_kube_config(table_size)
    dt=kwargs['execution_date'].strftime('%Y%m%dT%H%M%S')
    cpu_req = kubeConfig["cpu_request"]
    memory_req = kubeConfig["memory_request"]

    cpu_li = kubeConfig["cpu_limit"]
    memory_li = kubeConfig["memory_limit"]
    gcs_obj_path=f"{kubeConfig['gcs_prefix']}/{job_info.get('tgt_schema')}_{job_info.get('tgt_table')}/{dt}/"
    arguments =rf"""
        cd /tmp &&\
        echo "{read_export_tpt()}" > export_{job_info.get('src_table_type')}.tpt && \
        more export_{job_info.get('src_table_type')}.tpt && \
        (tbuild -C -f export_{job_info.get('src_table_type')}.tpt -u "\
            jobvar_tdpid='{host}', \
            jobvar_username='{conn_id}', \
            jobvar_password='$UDBABQGCPDTS3_PASSWORD', \
            jobvar_num_read_instances={kubeConfig["num_read_instances"]}, \
            jobvar_num_write_instances={kubeConfig["num_write_instances"]}, \
            jobvar_selectstmt='{job_info.get('select_query_tpt')}', \
            jobvar_accessmoduleinitstr='\
            Bucket={kubeConfig["gcs_bucket"]} \
            Prefix={gcs_obj_path}\
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

        """
    
    #print("---------------------------------------------") 
    #print(container_resources_assigned)
    
    tpt_start_time = datetime.datetime.now(datetime.timezone.utc)
    tpt_start_time = tpt_start_time.isoformat()
    ti = kwargs["ti"]
    ti.xcom_push(key=f"tpt_start_time",value=tpt_start_time)
    ti.xcom_push(key=f"tpt_arguments",value=arguments)
    ti.xcom_push(key=f"tpt_cpu_req",value=cpu_req)
    ti.xcom_push(key=f"tpt_memory",value=memory_req)
    ti.xcom_push(key=f"tpt_cpu_limit",value=cpu_li)
    ti.xcom_push(key=f"tpt_memory_li",value=memory_li)
    ti.xcom_push(key=f"gcs_path_tpt",value=gcs_obj_path)


def get_tpt_output(**kwargs):
    ti=kwargs['ti']
    job_info = ti.xcom_pull(key='table_info',task_ids='extract_metadata') 
    load_type = job_info.get('load_type')
    if load_type == "FULL_LOAD":
        write_mode= "WRITE_TRUNCATE_DATA"
        table_name=job_info.get('tgt_table')
        schema_name = job_info.get('tgt_schema')
    else:
        write_mode= "WRITE_APPEND"
        table_name = job_info.get('stg_table')
        schema_name = job_info.get('stg_schema')
    destination_project_dataset_table=f"{bq_prod_project}.{schema_name}.{table_name}"
    ti.xcom_push(key="write_mode",value=write_mode)
    ti.xcom_push(key="destination_project_dataset_table",value=destination_project_dataset_table)
    ti.xcom_push(key="tgt_schema",value=job_info.get('tgt_schema'))
    ti.xcom_push(key="tgt_table",value=job_info.get('tgt_table'))


def call_store_proc(**kwargs):
    proc_run_id =int(datetime.datetime.now(datetime.timezone.utc).timestamp())
    status = None
    row_count = None
    ti = kwargs['ti']
    job_info = ti.xcom_pull(key='table_info',task_ids='extract_metadata') 
    job_info["data_loading_state"] =  "stg-to-bq"

    if job_info.get('load_type') == "FULL_LOAD":
        print("****Skipping store proc call for FULL Load")
        job_info["job_status"] = "skipped"
        insert_audit(pled_conn,job_info)
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
            tgt_table = f"`{bq_target_project}`.{job_info.get('tgt_schema')}.{job_info.get('tgt_table')}"
            # tgt_table = f"{job_info.get('tgt_schema')}.{job_info.get('tgt_table')}"
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
        insert_audit(pled_conn,job_info)
        print("*** Adding gcs to bq audits done")

        return {
                "proc_name":"",
                "proc_run_id": proc_run_id,
                "proc_status": status,
                "row_count": row_count
            }
          

def audit_update_success(**kwargs):
    audit_update('SUCCESS',error_message=None,**kwargs)

def get_delta_max_value(conn, project_id,schema,table_name,delta_column):
    query = f""" SELECT MAX({delta_column}) as max_value FROM `{project_id}.{schema}.{table_name}`;"""
    # query = f""" SELECT MAX({delta_column}) as max_value FROM {schema}.{table_name};"""

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
    
def get_delta_catchup_conditions(col, col_type, interval_range):
    if col_type.strip().lower() == 'date':
        return f"WHERE {col} >= CURRENT_DATE - INTERVAL ''{interval_range}'' DAY "
    elif col_type.strip().lower() in ['datetime', 'timestamp']:
        return f" WHERE {col} >= CURRENT_TIMESTAMP - INTERVAL ''{interval_range}'' DAY "
    elif col_type.strip().lower() == 'integer':
        return f"WHERE {col} >=  + {interval_range} "
    else:
        return None

def generate_where_conditon( col, col_type, last_val):
    if col_type.strip().lower() == 'date':
        return f"WHERE {col} >= DATE ''{last_val}''"
    elif col_type.strip().lower() in ['datetime', 'timestamp']:
        return f" WHERE {col} >= TIMESTAMP ''{last_val}''"
    elif col_type.strip().lower() == 'integer':
        return f"WHERE {col} >= {last_val}"
    else:
        return None

def get_tables_to_export(job_info):
    
    select_query = job_info.get('select_query_tpt').strip().replace(";","")
        
    try:
        # custom where conditon query
        delta_interval = job_info.get('delta_interval')
        custom_where_condition = job_info.get('where_condition')
        if custom_where_condition:
            select_query = f"""{select_query} {custom_where_condition} ;"""
            print(f"**** custom_where_condition select_query:{select_query} ")
        elif delta_interval:
            delta_catchup_condition = get_delta_catchup_conditions( col=job_info.get('delta_column'), col_type=job_info.get('delta_column_type'), interval_range=delta_interval)
            select_query = f"""{select_query} {delta_catchup_condition} ;"""
            print(f"**** delta_interval select_query:{select_query} ")

        elif job_info.get('load_type').upper() in ["SCD_TYPE1","SCD_TYPE2"]:
                bq_target_project = config["bq_target_project"]
                last_val = get_delta_max_value(conn, bq_target_project,job_info.get('stg_schema'),job_info.get('stg_table'),job_info.get('delta_column'))
                print(f"***** Delta column Max value: {last_val}")
                
                if last_val:
                    where_condition = generate_where_conditon(
                        col=job_info.get('delta_column'), 
                        col_type=job_info.get('delta_column_type'), 
                        last_val=last_val
                        )
                    select_query = f"""{select_query} {where_condition} ;""" 
                    print(f"**** Incremental(delta) select_query:{select_query} ")
                    job_info['delta_column_max_value'] = last_val
                    job_info['delta_where_condition'] = str(where_condition)

                else:
                    # check for bq count, and if bq count is 0, Full load the table
                    print(f"***** Max value not found for delta column:{job_info.get('delta_column')} ,table:{job_info.get('tgt_schema')}.{job_info.get('tgt_table')}")
                    bq_row_count = get_bq_row_count(bq_schema=job_info.get('stg_schema'),bq_table=job_info.get('stg_table'))
                    if bq_row_count == 0:
                        print(f"***** Table {job_info.get('tgt_schema')}.{job_info.get('tgt_table')} is empty, Hence going for full load") 
        
        else:
            # "FULL_LOAD"
            print(f"***** Full Load Table name:{job_info.get('src_schema')}.{job_info.get('src_table')}, query: {job_info['select_query_tpt']}")
            select_query={job_info['select_query']}
            pass

            
        print(f"***** Generated select query for {job_info.get('src_schema')}.{job_info.get('src_table')} ---> {job_info.get('stg_schema')}.{job_info.get('stg_table')}, Type={job_info.get('load_type').upper()}:\n {select_query}")
        job_info['select_query'] = f"{select_query}"

    except Exception as e:
        print(f"**** Error while generating tdselect query for the  {job_info.get('src_schema')}.{job_info.get('src_table')} : {str(e)}")
        print(f"Traceback: ",traceback.format_exc())
        raise e


def load_data_bq(kwargs):
    ti=kwargs['ti']
    gcs_path=ti.xcom_pull(task_ids='create_tpt_args', key='gcs_path_tpt')
    destination_project_dataset_table=ti.xcom_pull(task_ids='get_tpt_output', key='destination_project_dataset_table')
    try:
        load_query = f""" LOAD DATA OVERWRITE {destination_project_dataset_table} FROM FILES (
        format='CSV',
        uris=['gs://{GCS_BUCKET}/{gcs_path}/*.csv'],
        field_delimiter='~',
        quote='"',
        allow_quoted_newlines=true,
        null_marker=''
        );"""
        print("sql: ",load_query)
        prd_hook.run_query(sql=load_query)
    except Exception as e:
        print(f"**** Error while getting bq row count for table {destination_project_dataset_table}: {str(e)}")
        print(f"Traceback: ",traceback.format_exc())
        return None


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2025, 4, 9),
    'retries': 0
}

dag=DAG(dag_id='dts_historical_load_tpt_incr_v1',
    tags=["bundle:dif", "module:historical"],
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(1),
    max_active_tasks=1,
    default_args=default_args
)


get_metadata = PythonOperator(
    task_id='extract_metadata',
    python_callable=extract_metadata,
    provide_context=True,  # Ensures kwargs include `dag_run`
    dag=dag,
    on_failure_callback=fail_task
)  

check_target_table_existance = PythonOperator(
    task_id='verify_target_table_details',
    python_callable=check_tables_details,
    provide_context=True,  # Ensures kwargs include `dag_run`
    dag=dag,
    on_failure_callback=fail_task
) 

create_tpt_args = PythonOperator(
    task_id='create_tpt_args',
    python_callable=create_kpo_args,
    provide_context=True,  # Ensures kwargs include `dag_run`
    dag=dag,
    on_failure_callback=fail_task
)

extract_teradata_data= KubernetesPodOperator(
        task_id="extract_teradata_data",
        name="tpt",
        cmds=["bash"],
        container_resources=k8s.V1ResourceRequirements(
            requests={
                "cpu": "{{ti.xcom_pull(task_ids='create_tpt_args', key='tpt_cpu_req')}}",
                "memory": "{{ti.xcom_pull(task_ids='create_tpt_args', key='tpt_memory')}}",
            },
            limits={
                "cpu": "{{ti.xcom_pull(task_ids='create_tpt_args', key='tpt_cpu_limit') }}",
                "memory": "{{ti.xcom_pull(task_ids='create_tpt_args', key='tpt_memory_li') }}",
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
        arguments=["-c","{{ti.xcom_pull(task_ids='create_tpt_args', key='tpt_arguments')}}"],
        dag=dag,
        is_delete_operator_pod=True,
        on_failure_callback=fail_task
    )



get_tpt_output_dtl = PythonOperator(
    task_id='get_tpt_output_dtl',
    python_callable=get_tpt_output,
    provide_context=True,  # Ensures kwargs include `dag_run`
    dag=dag,
    on_failure_callback=fail_task
)

# gcs_to_bq_load =  PythonOperator(
#     task_id='gcs_to_bq_load',
#     python_callable=load_data_bq,
#     provide_context=True,  # Ensures kwargs include `dag_run`
#     dag=dag,
#     on_failure_callback=fail_task
# )

gcs_to_bq_load=GCSToBigQueryOperator(
                task_id="create_gcs_to_bq_load_configs",
                bucket=GCS_BUCKET,
                write_disposition="WRITE_TRUNCATE_DATA",
                source_format="CSV",
                allow_jagged_rows=True,
                allow_quoted_newlines=True,
                quote_character="\"",
                gcp_conn_id=GCP_CONN_DO_ID,
                field_delimiter="~",
                source_objects=["{{ti.xcom_pull(task_ids='create_tpt_args', key='gcs_path_tpt') +'*.csv'}}"],
                destination_project_dataset_table="{{ti.xcom_pull(task_ids='get_tpt_output_dtl', key='destination_project_dataset_table')}}",
                schema_object="{{ 'td2bq_historical_load/schemas/' ~  ti.xcom_pull(task_ids='get_tpt_output_dtl', key='tgt_schema') ~ '_' ~ ti.xcom_pull(task_ids='get_tpt_output_dtl', key='tgt_table') ~ '.json' }}",
                schema_object_bucket="gk1v-prod-edwpr-0-usre-inbound",
                dag=dag,
                on_failure_callback=fail_task
            )


store_proc_call = PythonOperator(
    task_id='store_proc_call',
    python_callable=call_store_proc,
    provide_context=True,  # Ensures kwargs include `dag_run`
    dag=dag,
    on_failure_callback=fail_task
)

end=PythonOperator(
    task_id='end',
    python_callable=audit_update_success,
    provide_context=True,  # Ensures kwargs include `dag_run`
    dag=dag,
    on_failure_callback=fail_task
)


get_metadata >> check_target_table_existance >> create_tpt_args >> extract_teradata_data >> get_tpt_output_dtl >> gcs_to_bq_load >> store_proc_call >> end