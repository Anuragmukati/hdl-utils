# Refactored DAG: Sequential, Single Table Only, Audit Logic Preserved

import airflow
import datetime
import sys
import json
from airflow import DAG
from airflow.exceptions import AirflowFailException,AirflowSkipException
from airflow.providers.google.cloud.operators.bigquery import BigQueryHook,BigQueryInsertJobOperator
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
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
    with open('/home/airflow/gcs/dags/vz-it-gk1v-edwpr-0/history_load_config_prod_hdl.json', 'r') as f:
        confid_details = json.load(f)
    return confid_details
config = read_config()
prj='gk1vdo'
App_Name='Gk1vdo'
GCP_CONN_DO_ID = config[f"GCP_CONN_DO_ID_{prj}"]
GCP_CONN_DO_ID_PLE = config[f"GCP_CONN_DO_ID_PLE_{prj}"]
GCS_BUCKET = config["GCS_BUCKET"]
GCS_PREFIX = config["GCS_PREFIX"]
Audit_db=config["Audit_Dataset"]
Metadata_db=config["Metadata_Dataset"]
Metadata_tbl=config["Metadata_Tbl"]
Audit_tbl=config["Audit_Table"]
Queue_Table=str(config["Queue_Table_large"]).replace('Nos','Tpt')+"_FullLoad"
bq_prod_project=config[f"bq_prod_project_{prj}"]
bq_ple_project=config[f"bq_ple_project_{prj}"]
HDL_project=config["hdl_source_project"]
hdl_source_connection_id=config["hdl_source_connection_id"]
gcs_bucket="gk1v-prod-edwpr-0-usre-nos-inbound"


######## Kubernetes configuration ############
def read_kube_config(table_size):
    table_size=table_size.lower()
    with open(f'/home/airflow/gcs/dags/vz-it-gk1v-edwpr-0/kube_config_extralarge.json', 'r') as f:
        kube_details = json.load(f)
    return kube_details

####### get bq row count #######
def get_bq_row_count( bq_schema,bq_table,sql=None):
    print("**** Getting bq row count")
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_DO_ID, use_legacy_sql=False)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        if not sql:
            sql = f""" SELECT CAST(COUNT(*) AS INT64) FROM {bq_prod_project}.{bq_schema}.{bq_table};"""
        print("sql: ",sql)
        cursor.execute(sql)
        row = cursor.fetchone()
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

    sql=f"drop table if exists edw_data_migration_opsstg_tbls.{db_nm}_{tbl_nm}_curr;Create table edw_data_migration_opsstg_tbls.{db_nm}_{tbl_nm}_curr copy {db_nm}.{tbl_nm}"
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_DO_ID, use_legacy_sql=False)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        print(sql)
        cursor.execute(sql)
    except Exception as e :
        print('PROD TO PLE got failed!!!!!',str(e))

    if ple_load:
        sql=f"CREATE OR REPLACE TABLE {HDL_project}.{db_nm}.{tbl_nm} COPY {bq_prod_project}.{db_nm}.{tbl_nm}"
        hook = BigQueryHook(gcp_conn_id=hdl_source_connection_id, use_legacy_sql=False)
        conn = hook.get_conn()
        cursor = conn.cursor()
        try:
            print(sql)
            cursor.execute(sql)
        except Exception as e :
            print('PROD TO PLE got failed!!!!!',str(e))
    else:
        print("Ple flag is disabled")

def del_queue_tbl(tbl_id,app_nm):
    updt_queue_tbl_sql="delete from "+str(HDL_project)+"."+str(Metadata_db)+"."+str(Queue_Table)+" Where lower(table_uid)=lower('"+str(tbl_id)+"') and lower(app_name)=lower('"+str(app_nm)+"')"
    hook = BigQueryHook(gcp_conn_id=hdl_source_connection_id, use_legacy_sql=False)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(updt_queue_tbl_sql)
    except:
        print("Delete Queue Table Query Got Failed")


def updt_queue_tbl(tbl_id,app_nm):
    updt_queue_tbl_sql="update "+str(HDL_project)+"."+str(Metadata_db)+"."+str(Queue_Table)+" set STATUS='INPROGRESS' Where lower(table_uid)='"+str(tbl_id)+"' and app_name='"+str(app_nm)+"'"
    hook = BigQueryHook(gcp_conn_id=hdl_source_connection_id, use_legacy_sql=False)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(updt_queue_tbl_sql)
    except:
        print("Update Queue Table Query Got Failed")
        raise AirflowFailException("Update Queue Table Query Got Failed")

def extract_metadata(**context):
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_DO_ID, use_legacy_sql=False)
    conn = hook.get_conn()
    cursor = conn.cursor()
    queue_tbl_sql="select table_uid,app_name,ple_load,prod_load,dag_run_id from "+str(HDL_project)+"."+str(Metadata_db)+"."+str(Queue_Table)+" where STATUS!='INPROGRESS' and app_name like '%"+str(App_Name)+"%' order by INSERT_DT limit 1"
    hook = BigQueryHook(gcp_conn_id=hdl_source_connection_id, use_legacy_sql=False)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(queue_tbl_sql)
    except:
        print("Queue Table Query Got Failed")
        raise AirflowFailException("Queue Table Query Got Failed")
    Queue_Tbl_details=cursor.fetchone()
    print(Queue_Tbl_details)
    #### Taking Data from Queue Table
    if Queue_Tbl_details:
        tbl_uid=Queue_Tbl_details[0]
        app_name=Queue_Tbl_details[1]
        ple_load=Queue_Tbl_details[2]
        prod_load=Queue_Tbl_details[3]
        dag_run_id=Queue_Tbl_details[4]
    else:
        print("Queue table is empty")
        raise AirflowSkipException("No Table To load in Queue") 
    updt_queue_tbl(tbl_uid,app_name)
    sql = f"""select is_table_active,table_uid,src_schema,src_table,stg_schema,stg_table,tgt_schema,tgt_table,load_type,delta_column,delta_column_type,delta_column_format,delta_interval,app_name,src_table_size,src_table_row_count,src_table_type,bus_key_column,change_value_column,start_date_column,end_date_column,stg_load_enabled,plican_validation_enabled,select_query,load_functionality,select_query_tpt from `{HDL_project}.{Metadata_db}.{Metadata_tbl}`
    where  is_table_active=True and lower(table_uid)=lower('{tbl_uid}') and Lower(App_Name) like lower('%{str(app_name)}%') limit 1;"""
    print(f"**** Metadata select query: \n {sql}")
    
    cursor.execute(sql)
    row = cursor.fetchone()
    if row:
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
        dag_run_id1 = context['dag_run'].run_id
        dag_id = context['dag'].dag_id
        dag_id=str(dag_id)+'_'+str(dag_run_id1.split('_')[2])
        table_info['dag_id']=dag_id
        table_info['dag_run_id']=dag_run_id[0] if isinstance(dag_run_id,list) else str(dag_run_id)
        dag_run_type = context['dag_run'].run_type
        table_info['dag_run_type']=dag_run_type[0] if isinstance(dag_run_type,list) else str(dag_run_type)
        table_info['ple_load']=ple_load

        td_row_count = get_td_row_count(row[2],row[3])
        bq_row_count=get_bq_row_count(row[6],row[7])
        # bq_row_count=-1
        # td_row_count=1
        
        table_info["src_row_count"]=td_row_count
        
        print("TD row_count: ",td_row_count)
        print("BQ row_count: ",bq_row_count)
        ti = context['ti']
        ti.xcom_push(key='table_info', value=table_info)
        if td_row_count==0:
            audit_update(status="SKIPPED",error='Source have Zero Count',**context)
            print("Table skipped due to zero count")
            del_queue_tbl(tbl_uid,app_name)
            raise AirflowSkipException("Table skipped due to zero count")
        elif td_row_count==bq_row_count:
            audit_update(status="SKIPPED",error='Source and Target count Are Matching but data got refreshed in Ple',**context)
            print("Table skipped due to count matching")
            del_queue_tbl(tbl_uid,app_name)
            prod_to_ple(**context)
            raise AirflowSkipException("Count getting matched to skipping the rest of task")
    else:
        print("metadata not found")
        audit_update(status="FAILED",error='Metadata not Found',**context)
        raise AirflowFailException("Metadata not found for table ",tbl_uid," and app name",app_name)      

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

def insert_audit(data,st_tm):
    sql = f"""INSERT INTO `{HDL_project}.{Audit_db}.{Audit_tbl}`
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
        td_functional_id,
        App_name
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
    {safe_sql(st_tm)},
    {safe_sql(data.get('end_time'))},
    {safe_sql(data.get('src_row_count'))},
    {safe_sql(data.get('tgt_row_count'))},
    {safe_sql(data.get('is_row_count_matched'))},
    {safe_sql(escape_string(data.get('error_message')))},
    {safe_sql(data.get('job_status'))},
    {safe_sql(data.get('load_type'))},
    {safe_sql(data.get('pelican_trigger_id'))},
    {safe_sql(data.get('td_functional_id'))},
    {safe_sql(data.get('app_name'))}
    )
    """
    print(f"**** Insert Audit query: \n {sql}")
    hook = BigQueryHook(gcp_conn_id=hdl_source_connection_id, use_legacy_sql=False)
    conn = hook.get_conn()
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


def get_bq_max_date(cnt_tbl_sql):
    if not cnt_tbl_sql:
        print("Please provide the cnt_tbl_sql !!!!!!!!!!!!!!!")
        return -1
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_DO_ID, use_legacy_sql=False)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(cnt_tbl_sql)
    except:
        print("Count Query Got Failed")
    max_dt_details=cursor.fetchone()
    print(max_dt_details)
    if max_dt_details:
        max_dt_val=max_dt_details[0]
        return max_dt_val
    else:
        print("Table not found")
        return -1

def audit_update(status=None,error=None,**context):
    print(f"***** Audit logs started ******")
    ti = context['ti']
    exception = error
    job_info = ti.xcom_pull(key='table_info',task_ids='extract_metadata') 
    print(f"****** job_info: {job_info}")
    job_info['job_status'] = status
    job_info["end_time"] = datetime.datetime.now(datetime.timezone.utc)
    job_info['error_message'] = f"""{str(exception)[:1000] if len(str(exception)) >= 1000 else str(exception)}""" if exception else  None
    
    if status=='SUCCESS' or status.lower()=='skipped':
        max_dt_sql=f"select max({job_info['delta_column']}) as max_dt from {job_info['tgt_schema']}.{job_info['tgt_table']}"
        job_info['delta_column_max_value']=get_bq_max_date(max_dt_sql)
        bq_row_count=0
        if job_info.get('load_type')=='FullLoad':
            bq_row_count=get_bq_row_count(job_info.get('tgt_schema'),job_info.get('tgt_table'))
        else:
            bq_row_count=get_bq_row_count(job_info.get('stg_schema'),job_info.get('stg_table'))
        job_info["tgt_row_count"]=bq_row_count
    if ti.xcom_pull(key='tpt_start_time',task_ids='create_tpt_args'):
        st_tm=ti.xcom_pull(key='tpt_start_time',task_ids='create_tpt_args')
    else:
        st_tm= datetime.datetime.now(datetime.timezone.utc)
        st_tm = st_tm.isoformat()
    insert_audit(job_info,st_tm)
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
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_DO_ID,use_legacy_sql=False)
    query = f"SELECT COUNT(1) AS cnt FROM `{bq_prod_project}.{db_nm}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{tbl_nm}'"
    print(query)

    connection = teradatasql.connect(
            host=host,
            user=username,
            password=password
        )
    cursor = connection.cursor()
    cursor.execute(f"SELECT COUNT(1) FROM dbc.columnsV WHERE databasename='{src_db_nm}' AND tablename='{src_tbl_nm}'")

def read_export_tpt():
    with open("/home/airflow/gcs/dags/vz-it-gk1v-edwpr-0/export_large_v2.tpt", "r") as f:
        return f.read().replace("$", r"\$")

def create_kpo_args(**kwargs):    
    ti=kwargs['ti']
    job_info=get_tables_to_export(ti.xcom_pull(key='table_info',task_ids='extract_metadata') )
    if job_info == -1:
        raise AirflowFailException("Incremental Query Genration failed")
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
    ti.xcom_push(key=f"gcs_bucket",value=GCS_BUCKET)


def get_tpt_output(**kwargs):
    ti=kwargs['ti']
    job_info = ti.xcom_pull(key='table_info',task_ids='extract_metadata') 
    load_type = job_info.get('load_type')
    if load_type == "FullLoad":
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
    ti.xcom_push(key="tgt_schema",value=schema_name)
    ti.xcom_push(key="tgt_table",value=table_name)


def call_store_proc(**kwargs):
    proc_run_id =int(datetime.datetime.now(datetime.timezone.utc).timestamp())
    status = None
    row_count = None
    ti = kwargs['ti']
    job_info = ti.xcom_pull(key='table_info',task_ids='extract_metadata') 
    job_info["data_loading_state"] =  "stg-to-bq"
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_DO_ID, use_legacy_sql=False)
    conn = hook.get_conn()
    cursor = conn.cursor()
    if job_info.get('load_type') == "FullLoad":
        print("****Skipping store proc call for FULL Load")
        job_info["job_status"] = "skipped"
        if ti.xcom_pull(key='tpt_start_time',task_ids='create_tpt_args'):
            st_tm=ti.xcom_pull(key='tpt_start_time',task_ids='create_tpt_args')
        else:
            st_tm= datetime.datetime.now(datetime.timezone.utc)
            st_tm = st_tm.isoformat()
        insert_audit(job_info,st_tm)
        return {}
    else:
        try:
            job_info["start_time"] =  datetime.datetime.now(datetime.timezone.utc) 
            job_info["data_loading_state"] =  "stg-to-bq"
            
            time_now = datetime.datetime.now(datetime.timezone.utc)
            
            run_id = proc_run_id
            job_id = job_info["table_uid"]
            run_date = datetime.date.today().strftime('%Y-%m-%d')
            bq_target_project = config["bq_prod_project"]
            tgt_table = f"`{bq_target_project}`.{job_info.get('tgt_schema')}.{job_info.get('tgt_table')}"
            # tgt_table = f"{job_info.get('tgt_schema')}.{job_info.get('tgt_table')}"
            sp_start_time = time_now.replace(tzinfo=None).isoformat(timespec="seconds")  # YYY-MM-DDTHH:MM:S
            query = f"""
            DECLARE RECORD_COUNT INT64; 
            DECLARE SP_STATUS INT64;
            CALL `{bq_target_project}.{Metadata_db}.PROC_SCD_DATA_LOAD_FRAMEWORK` 
            ({safe_sql(run_id)}, {safe_sql(job_id)}, '{run_date}', NULL, NULL,
                {safe_sql(tgt_table)}, {safe_sql(sp_start_time)}, RECORD_COUNT, SP_STATUS);
            """
            print(f"******* SP QUERY: {query}")

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
        if ti.xcom_pull(key='tpt_start_time',task_ids='create_tpt_args'):
            st_tm=ti.xcom_pull(key='tpt_start_time',task_ids='create_tpt_args')
        else:
            st_tm= datetime.datetime.now(datetime.timezone.utc)
            st_tm = st_tm.isoformat()
        print("*** Adding gcs to bq audits ")
        insert_audit(job_info,st_tm)
        print("*** Adding gcs to bq audits done")

        return {
                "proc_name":"",
                "proc_run_id": proc_run_id,
                "proc_status": status,
                "row_count": row_count
            }
          

def audit_update_success(**kwargs):
    ti=kwargs['ti']
    audit_update('SUCCESS',error_message=None,**kwargs)
    job_info = ti.xcom_pull(key='table_info',task_ids='extract_metadata') 
    del_queue_tbl(job_info['table_uid'],job_info['app_name'])

def get_delta_max_value(schema,table_name):
    query=f"""SELECT delta_column_max_value FROM {HDL_project}.{Audit_db}.{Audit_tbl} WHERE tgt_schema = '{schema}' and tgt_table='{table_name}' and app_name='{App_Name}' AND JOB_STATUS = 'SUCCESS' order by end_time desc limit 1"""
    hook = BigQueryHook(gcp_conn_id=hdl_source_connection_id, use_legacy_sql=False)
    conn = hook.get_conn()
    cursor = conn.cursor()
    print(f"**** get max value query: {query}")
    try:
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

        elif job_info.get('load_type').upper() in ["SCD1","SCD2","APPEND"]:
                bq_target_project = config["bq_prod_project"]
                last_val = get_delta_max_value( bq_target_project,job_info.get('tgt_schema'),job_info.get('tgt_table'),job_info.get('delta_column'),job_info.get('delta_column_type'))
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
                    bq_row_count = get_bq_row_count(bq_schema=job_info.get('tgt_schema'),bq_table=job_info.get('tgt_table'))
                    if bq_row_count == 0:
                        print(f"***** Table {job_info.get('tgt_schema')}.{job_info.get('tgt_table')} is empty, Hence going for full load")
        
        else:
            # "FULL_LOAD"
            print(f"***** Full Load Table name:{job_info.get('src_schema')}.{job_info.get('src_table')}, query: {job_info['select_query_tpt']}")
            select_query={job_info['select_query_tpt']}
            pass

            
        print(f"***** Generated select query for {job_info.get('src_schema')}.{job_info.get('src_table')} ---> {job_info.get('stg_schema')}.{job_info.get('stg_table')}, Type={job_info.get('load_type').upper()}:\n {select_query}")
        job_info['select_query_tpt'] = f"{select_query}"
        return job_info
    except Exception as e:
        print(f"**** Error while generating tdselect query for the  {job_info.get('src_schema')}.{job_info.get('src_table')} : {str(e)}")
        print(f"Traceback: ",traceback.format_exc())
        return -1
        raise e
    



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2025, 4, 9),
    'retries': 0
}

dag=DAG(dag_id='history_load_dag_tpt_SCD1',
    tags=["bundle:dif", "module:historical"],
    schedule_interval='*/3 * * * *',
    start_date=airflow.utils.dates.days_ago(1),
    max_active_runs=3,
    default_args=default_args,
    catchup=False
)


get_metadata = PythonOperator(
    task_id='extract_metadata',
    python_callable=extract_metadata,
    provide_context=True, 
    dag=dag,
    on_failure_callback=fail_task
)  

check_target_table_existance = PythonOperator(
    task_id='verify_target_table_details',
    python_callable=check_tables_details,
    provide_context=True, 
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


gcs_to_bq_load=BigQueryInsertJobOperator(
    task_id='create_gcs_to_bq_load_configs',
    configuration={
        "load":{
            "sourceUris":[
                "gs://{{ti.xcom_pull(task_ids='create_tpt_args', key='gcs_bucket')}}/{{ti.xcom_pull(task_ids='create_tpt_args', key='gcs_path_tpt')}}*.csv"
            ],
        "destinationTable":{
            "projectId":bq_prod_project,
            "datasetId":"{{ti.xcom_pull(task_ids='get_tpt_output_dtl', key='tgt_schema') }}",
            "tableId":"{{ti.xcom_pull(task_ids='get_tpt_output_dtl', key='tgt_table') }}"
        },
        "sourceFormat":"CSV",
        "fieldDelimiter":"~",
        "Compression":"GZIP",
        "autodectect":False,
        "quote":"\"",
        "writeDisposition":"{{ti.xcom_pull(task_ids='get_tpt_output_dtl', key='write_mode') }}"
        }
    },
    location='US',
    gcp_conn_id=GCP_CONN_DO_ID,
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

prod_to_ple_load = PythonOperator(
    task_id='prod_to_ple_load',
    python_callable=prod_to_ple,
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


get_metadata >> check_target_table_existance >> create_tpt_args >> extract_teradata_data >> get_tpt_output_dtl >> gcs_to_bq_load >> store_proc_call >> prod_to_ple_load>> end
