from airflow import DAG
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowFailException,AirflowSkipException
from airflow.providers.teradata.operators.teradata import TeradataOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryHook,BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from google.cloud.exceptions import NotFound
from airflow.hooks.base import BaseHook
import json
from datetime import datetime
import teradatasql
from datetime import datetime
import sys
import random
import re
import time

BASE_DIR = "/home/airflow/gcs/dags/vz-it-gk1v-edwpr-0/"
sys.path.append(f"{BASE_DIR}/")


def read_config():
    with open('/home/airflow/gcs/dags/vz-it-gk1v-edwpr-0/history_load_config_prod.json', 'r') as f:
        confid_details = json.load(f)
    return confid_details


config = read_config()

# gcs details
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
function_ids=['UGCPDTSNOS4','UGCPDTSNOS5','UGCPDTSNOS3','UGCPDTSNOS2','UGCPDTSNOS1']
functional_id_used=function_ids[random.randint(0,len(function_ids)-1)]
GCP_CONN_DO_ID_PLE='sa-vz-it-gk1v-pleedwdo-0-app'
gcs_bucket="gk1v-prod-edwpr-0-usre-nos-inbound"

time.sleep(random.randint(1,18))

# def choose_path(**kwargs):
#     ti=kwargs['ti']
#     ple_flag=ti.xcom_pull(task_ids='fetch_metadata', key='ple_load')
#     prod_flag=ti.xcom_pull(task_ids='fetch_metadata', key='prod_load')
#     if ple_flag and prod_flag:
#         return ['gcs_to_bq_prod_load','gcs_to_bq_ple_load']
#     elif (ple_flag=='' or not ple_flag)  and (prod_flag=='' or not prod_flag):
#         return 'gcs_to_bq_prod_load'
#     elif ple_flag and (prod_flag=='' or not prod_flag):
#         return 'gcs_to_bq_ple_load'
#     else:
#         return 'gcs_to_bq_ple_load'

def prod_to_ple(**kwargs):
    ti = kwargs['ti']
    db_nm=ti.xcom_pull(task_ids='fetch_metadata', key='tgt_db')
    tbl_nm=ti.xcom_pull(task_ids='fetch_metadata', key='tgt_tbl')
    ple_load=ti.xcom_pull(task_ids='fetch_metadata', key='ple_load')
    if ple_load:
        sql=f"TRUNCATE TABLE {bq_ple_project}.{db_nm}.{tbl_nm}; INSERT INTO {bq_ple_project}.{db_nm}.{tbl_nm} SELECT * FROM {bq_prod_project}.{db_nm}.{tbl_nm}"
        hook = BigQueryHook(gcp_conn_id=GCP_CONN_DO_ID_PLE, use_legacy_sql=False)
        conn = hook.get_conn()
        cursor = conn.cursor()
        try:
            print(sql)
            cursor.execute(sql)
        except:
            print('PROD TO PLE got failed!!!!!')
    else:
        print("Ple flag is disabled")

def get_td_tbl_count(td_schema,td_table, cnt_tbl_sql=None):
    conn=BaseHook.get_connection('UGCPDTSNOS4')
    try:
        #password = access_secret_version("td_pwd")
        print("**** Getting td row count")
        connection = teradatasql.connect(
            host=conn.host,
            user=conn.login,
            password=conn.password
        )

        cursor = connection.cursor()
        if not cnt_tbl_sql:
            cnt_tbl_sql = f"SELECT CAST(COUNT(1) AS BIGINT) FROM {td_schema}.{td_table}"
        print("sql: ",cnt_tbl_sql)
        cursor.execute(cnt_tbl_sql)
        count = cursor.fetchone()[0]
        cursor.close()
        connection.close()
        return count

    except Exception as e:
        print(f"**** Error while getting td row count for table {td_schema}.{td_table}: {str(e)}")
        return -1


def get_bq_tbl_count(db_nm,tbl_nm,cnt_tbl_sql=None):
    if not cnt_tbl_sql:
        cnt_tbl_sql="select count(*) from "+str(db_nm)+"."+str(tbl_nm)
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_DO_ID, use_legacy_sql=False)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(cnt_tbl_sql)
    except:
        print("Count Query Got Failed")
    cnt_tbl_details=cursor.fetchone()
    print(cnt_tbl_details)
    if cnt_tbl_details:
        bq_tbl_cnt=cnt_tbl_details[0]
        return bq_tbl_cnt
    else:
        print("Table not found")
        return -1

    
def check_table_exists(**kwargs):
    ti=kwargs['ti']
    db_nm=ti.xcom_pull(task_ids='fetch_metadata', key='tgt_db')
    tbl_nm=ti.xcom_pull(task_ids='fetch_metadata', key='tgt_tbl') 
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_DO_ID_PLE, use_legacy_sql=False)
    client=hook.get_client()
    tbl_fl_nm=f'{bq_ple_project}.{db_nm}.{tbl_nm}'
    try:
        client.get_table(tbl_fl_nm)
        print(f"Table is exists:{tbl_fl_nm}")
    except NotFound:
        print(f"Table is not exists:{tbl_fl_nm}")
        updt_sql('Skipped','Table Not Exists',kwargs)
        raise AirflowSkipException(f"skipping because table {tbl_fl_nm} not found.")


def updt_sql_success(**kwargs):
    ti=kwargs['ti']
    dag_run_id = kwargs['dag_run'].run_id
    dag_id = kwargs['dag'].dag_id
    dag_id=str(dag_id)+'_'+str(dag_run_id.split('_')[2])
    dag_run_type=kwargs['dag_run'].run_type
    db_nm=ti.xcom_pull(task_ids='fetch_metadata', key='tgt_db')
    tbl_nm=ti.xcom_pull(task_ids='fetch_metadata', key='tgt_tbl')
    tbl_uid=ti.xcom_pull(task_ids='fetch_metadata', key='tbl_uid')
    src_db=ti.xcom_pull(task_ids='fetch_metadata', key='src_db')
    src_tbl=ti.xcom_pull(task_ids='fetch_metadata', key='src_tbl')
    stg_db=ti.xcom_pull(task_ids='fetch_metadata', key='stg_db')
    stg_tbl=ti.xcom_pull(task_ids='fetch_metadata', key='stg_tbl')
    app_nm=ti.xcom_pull(task_ids='fetch_metadata', key='app_name')
    start_time=ti.xcom_pull(task_ids='fetch_metadata', key='start_time')
    dag_run_id=ti.xcom_pull(task_ids='fetch_metadata', key='dag_run_id')
    functional_id_used=ti.xcom_pull(task_ids='fetch_metadata', key='functional_id_used')
    bq_count=get_bq_tbl_count(db_nm,tbl_nm)
    td_count=get_td_tbl_count(src_db,src_tbl)

    if bq_count==td_count:
        is_row_count_matched=True
    elif bq_count<0:
        is_row_count_matched=False
        error_msg="BQ Row Count Query Failed"
    elif td_count<0:
        is_row_count_matched=False
        error_msg="TD Row Count Query Failed"
    else:
        is_row_count_matched=False
    cur_time=datetime.now()
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_DO_ID, use_legacy_sql=False)
    sql=f'insert into '+str(bq_ple_project)+'.'+str(Audit_db)+'.'+str(Audit_tbl)+'(dag_id,dag_run_id,dag_run_type,table_uid,stg_schema,stg_table,tgt_schema,tgt_table,start_time,end_time,src_row_count,tgt_row_count,is_row_count_matched,job_status,load_type,td_functional_id) values("'+str(dag_id)+'","'+str(dag_run_id)+'","'+str(dag_run_type)+'","'+str(tbl_uid)+'","'+str(stg_db)+'","'+str(stg_tbl)+'","'+str(db_nm)+'","'+str(tbl_nm)+'","'+str(start_time)+'","'+str(cur_time)+'",'+str(td_count)+','+str(bq_count)+','+str(is_row_count_matched)+',"SUCCESS","NOS","'+str(functional_id_used)+'")'
    print(sql)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        del_queue_tbl(tbl_uid,app_nm)
    except:
        print("update stmt failed")
    
def updt_sql(status,error_message='',**kwargs):
    ti=kwargs['ti']
    dag_run_id = kwargs['dag_run'].run_id
    dag_id = kwargs['dag'].dag_id
    dag_id=str(dag_id)+'_'+str(dag_run_id.split('_')[2])
    dag_run_type=kwargs['dag_run'].run_type
    db_nm=ti.xcom_pull(task_ids='fetch_metadata', key='tgt_db')
    tbl_nm=ti.xcom_pull(task_ids='fetch_metadata', key='tgt_tbl')
    tbl_uid=ti.xcom_pull(task_ids='fetch_metadata', key='tbl_uid')
    src_db=ti.xcom_pull(task_ids='fetch_metadata', key='src_db')
    src_tbl=ti.xcom_pull(task_ids='fetch_metadata', key='src_tbl')
    stg_db=ti.xcom_pull(task_ids='fetch_metadata', key='stg_db')
    stg_tbl=ti.xcom_pull(task_ids='fetch_metadata', key='stg_tbl')
    app_nm=ti.xcom_pull(task_ids='fetch_metadata', key='app_name')
    start_time=ti.xcom_pull(task_ids='fetch_metadata', key='start_time')
    dag_run_id=ti.xcom_pull(task_ids='fetch_metadata', key='dag_run_id')
    functional_id_used=ti.xcom_pull(task_ids='fetch_metadata', key='functional_id_used')
    bq_count=get_bq_tbl_count(db_nm,tbl_nm)
    td_count=get_td_tbl_count(src_db,src_tbl)

    if bq_count==td_count:
        is_row_count_matched=True
    elif bq_count<0:
        is_row_count_matched=False
        error_message=str(error_message)+" BQ Row Count Query Failed"
    elif td_count<0:
        is_row_count_matched=False
        error_message=str(error_message)+" TD Row Count Query Failed"
    else:
        is_row_count_matched=False
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_DO_ID, use_legacy_sql=False)
    if status=='STARTED':
        sql=f'insert into '+str(bq_ple_project)+'.'+str(Audit_db)+'.'+str(Audit_tbl)+'(dag_id,dag_run_id,dag_run_type,table_uid,stg_schema,stg_table,tgt_schema,tgt_table,start_time,src_row_count,tgt_row_count,is_row_count_matched,job_status,load_type,td_functional_id) values("'+str(dag_id)+'","'+str(dag_run_id)+'","'+str(dag_run_type)+'","'+str(tbl_uid)+'","'+str(stg_db)+'","'+str(stg_tbl)+'","'+str(db_nm)+'","'+str(tbl_nm)+'","'+str(start_time)+'",'+str(td_count)+','+str(bq_count)+','+str(is_row_count_matched)+',"'+str(status)+'","NOS","'+str(functional_id_used)+'")'
    else:
        cur_time=datetime.now()
        sql=f'insert into '+str(bq_ple_project)+'.'+str(Audit_db)+'.'+str(Audit_tbl)+'(dag_id,dag_run_id,dag_run_type,table_uid,stg_schema,stg_table,tgt_schema,tgt_table,start_time,end_time,src_row_count,tgt_row_count,is_row_count_matched,error_message,job_status,load_type,td_functional_id) values("'+str(dag_id)+'","'+str(dag_run_id)+'","'+str(dag_run_type)+'","'+str(tbl_uid)+'","'+str(stg_db)+'","'+str(stg_tbl)+'","'+str(db_nm)+'","'+str(tbl_nm)+'","'+str(start_time)+'","'+str(cur_time)+'",'+str(td_count)+','+str(bq_count)+','+str(is_row_count_matched)+',"'+str(error_message)+'","'+str(status)+'","NOS","'+str(functional_id_used)+'")'
    print(sql)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
    except:
        print("update stmt failed")
    if status!='STARTED':
        del_queue_tbl(tbl_uid,app_nm)

def del_queue_tbl(tbl_id,app_nm):
    updt_queue_tbl_sql="delete from "+str(bq_ple_project)+"."+str(Metadata_db)+"."+str(Queue_Table)+" Where table_uid='"+str(tbl_id)+"' and app_name='"+str(app_nm)+"'"
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_DO_ID, use_legacy_sql=False)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(updt_queue_tbl_sql)
    except:
        print("Delete Queue Table Query Got Failed")
    

def updt_queue_tbl(tbl_id,app_nm):
    updt_queue_tbl_sql="update "+str(bq_ple_project)+"."+str(Metadata_db)+"."+str(Queue_Table)+" set STATUS='INPROGRESS' Where table_uid='"+str(tbl_id)+"' and app_name='"+str(app_nm)+"'"
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_DO_ID, use_legacy_sql=False)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(updt_queue_tbl_sql)
    except:
        print("Update Queue Table Query Got Failed")
        raise AirflowFailException("Update Queue Table Query Got Failed")
    
def fetch_metadata(**kwargs):
    #### Sql for fetching details from Queue table if any
    #queue_tbl_sql="select table_uid,app_name,ple_load,prod_load,dag_run_id from "+str(bq_ple_project)+"."+str(Metadata_db)+"."+str(Queue_Table)+" where STATUS!='INPROGRESS' and app_name like '%NOS_First%' order by INSERT_DT limit 1"
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_DO_ID, use_legacy_sql=False)
    conn = hook.get_conn()
    cursor = conn.cursor()
    #try:
    #    cursor.execute(queue_tbl_sql)
    #except:
    #    print("Queue Table Query Got Failed")
    #    raise AirflowFailException("Queue Table Query Got Failed")
    #Queue_Tbl_details=cursor.fetchone()
    #print(Queue_Tbl_details)
    #### Taking Data from Queue Table
    #if Queue_Tbl_details:
    BQ_Table_list=kwargs['dag_run'].conf.get('BQ_Table','')
    ple_load=kwargs['dag_run'].conf.get('Ple_Load_flag','')
    where_condition=kwargs['dag_run'].conf.get('Condition','')
    tbl_load_type=kwargs['dag_run'].conf.get('tbl_load_type','append')
    db_nm=str(BQ_Table_list).split('.')[0]
    tbl_nm=str(BQ_Table_list).split('.')[1]
    tbl_uid=str(db_nm)+"__"+str(tbl_nm)
    app_name='Gk1vdo'
    dag_run_id="Testing_Batch"
    prod_load=True

    #else:
    #    print("Queue table is empty")
    #    raise AirflowSkipException("No Table To load in Queue") 
    
    #### Updating the Queue Table 
    #updt_queue_tbl(tbl_uid,app_name)
    
    #### Fetching Metadata Details depending on Queue Table Details.
    metadata_sql="select * from "+str(bq_ple_project)+"."+str(Metadata_db)+"."+str(Metadata_tbl)+" where  is_table_active is True and lower(Table_uid)=lower('"+str(tbl_uid)+"') and Lower(App_Name) like lower('%"+str(app_name)+"%')"
    try:
        cursor.execute(metadata_sql)
    except:
        print("Metadata Table Query Got Failed")
        raise AirflowFailException("Metadata Table Query Got Failed")
    Metadata_details=cursor.fetchone()
    print(Metadata_details)
    cur_time=datetime.now()
    if Metadata_details:
        tbl_uid=Metadata_details[1]
        src_db=Metadata_details[2]
        src_tbl=Metadata_details[3]
        stg_db=Metadata_details[4]
        stg_tbl=Metadata_details[5]
        tgt_db=Metadata_details[6]
        tgt_tbl=Metadata_details[7]
        app_name=Metadata_details[13]
        stg_load_flg=Metadata_details[21]
        pelican_val_flg=Metadata_details[22]
        fetch_query=Metadata_details[23]
    else:
        print("No Details not Found in Metadata table",tbl_uid)
        del_queue_tbl(tbl_uid,app_name)
        raise AirflowSkipException("No Details not Found in Metadata table for ",tbl_uid) 
    #### Inserting the Audit Entry 
    if where_condition or where_condition.strip() != '':
        fetch_query=str(fetch_query)+" "+str(where_condition)
    td_count=get_td_tbl_count(src_db,src_tbl)
    bq_count=get_bq_tbl_count(tgt_db,tgt_tbl)
    ti = kwargs['ti']
    ti.xcom_push(key='tbl_uid', value=tbl_uid)
    ti.xcom_push(key='src_db', value=src_db)
    ti.xcom_push(key='src_tbl', value=src_tbl)
    ti.xcom_push(key='stg_db', value=stg_db)
    ti.xcom_push(key='stg_tbl', value=stg_tbl)
    ti.xcom_push(key='tgt_db', value=tgt_db)
    ti.xcom_push(key='tgt_tbl', value=tgt_tbl)
    ti.xcom_push(key='app_name',value=app_name)
    ti.xcom_push(key='stg_load_flg',value=stg_load_flg)
    ti.xcom_push(key='pelican_val_flg',value=pelican_val_flg)
    ti.xcom_push(key='ple_load',value=ple_load)
    ti.xcom_push(key='prod_load',value=prod_load)
    ti.xcom_push(key='fetch_query',value=fetch_query)
    ti.xcom_push(key='functional_id_used',value=functional_id_used)
    ti.xcom_push(key='start_time',value=cur_time)
    ti.xcom_push(key='dag_run_id',value=dag_run_id)
    if tbl_load_type.lower().strip()=='append':
        tltype='WRITE_APPEND'
    else:
        tltype='WRITE_TRUNCATE_DATA'
    ti.xcom_push(key='tbl_load_type',value=tltype)
    updt_sql("STARTED",**kwargs)
    if td_count==0:
        updt_sql("SKIPPED",error_message='Source have Zero Count',**kwargs)
        print("Table skipped due to zero count")
        del_queue_tbl(tbl_uid,app_name)
        raise AirflowSkipException("Table skipped due to zero count")
    elif td_count==bq_count:
        updt_sql("SKIPPED",error_message='Source and Target count Are Matching but data got refreshed in Ple',**kwargs)
        print("Table skipped due to count matching")
        del_queue_tbl(tbl_uid,app_name)
        prod_to_ple(**kwargs)

def generate_nos_query(**kwargs):
    #### Hitting Teradata and extracting data into GCS files
    ti = kwargs['ti']
    db_nm=ti.xcom_pull(task_ids='fetch_metadata', key='tgt_db') 
    tbl_nm=ti.xcom_pull(task_ids='fetch_metadata', key='tgt_tbl')
    fetch_query=ti.xcom_pull(task_ids='fetch_metadata', key='fetch_query') 
    dt=kwargs['execution_date'].strftime('%Y%m%dT%H%M%S')
    gcs_path = "nos/input/"+str(db_nm)+'_'+str(tbl_nm)+'/'+str(dt)
    #### Creating NOS SQL Query for Reading Data.
    sql_query = f"""
    SELECT * FROM WRITE_NOS (
        ON ( {fetch_query} )
        USING
        LOCATION('/gs/storage.googleapis.com/{gcs_bucket}/{gcs_path}/')
        AUTHORIZATION(TD_SERVER_DB.NOS_AUTH_PRD_GK1V_EDWPR)
        STOREDAS('CSV')
        ROWFORMAT('{{"field_delimiter":"|","record_delimiter":"\\n"}}')
        MAXOBJECTSIZE('16MB')
        COMPRESSION('GZIP')
    ) AS d;
    """

    ti.xcom_push(key='nos_query', value=sql_query)
    ti.xcom_push(key='gcs_path',value=gcs_path)
    ti.xcom_push(key='gcs_bucket',value=gcs_bucket)
    print("Generated Query:", sql_query)
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
    updt_sql("Dag_Failed",error_message=error,**context)
    print("DAG execution failed!",error)



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 9),
    'retries': 0
}

dag = DAG(
    dag_id='history_load_dag_nos_csv_adhoc_gk1vdo',
    default_args=default_args,
    description='DAG to generate NOS SQL query for Teradata',
    schedule_interval=None,
    max_active_runs=5,
    catchup=False
)
    
get_metadata = PythonOperator(
    task_id='fetch_metadata',
    python_callable=fetch_metadata,
    provide_context=True,  # Ensures kwargs include `dag_run`
    dag=dag,
    on_failure_callback=fail_task
)  

check_target_table_existance = PythonOperator(
    task_id='check_target_table_existance',
    python_callable=check_table_exists,
    provide_context=True,  # Ensures kwargs include `dag_run`
    dag=dag,
    on_failure_callback=fail_task
) 


generate_query_task = PythonOperator(
    task_id='generate_nos_query',
    python_callable=generate_nos_query,
    provide_context=True,  # Ensures kwargs include `dag_run`
    dag=dag,
    on_failure_callback=fail_task
)

nos_to_gcs_load = TeradataOperator(
    task_id='nos_to_gcs_load',
    conn_id=functional_id_used,
    sql="{{ ti.xcom_pull(task_ids='generate_nos_query', key='nos_query') }}",
    retries=0,
    dag=dag,
    on_failure_callback=fail_task,
)

gcs_to_bq_ple_load=BigQueryInsertJobOperator(
    task_id='gcs_to_bq_ple_load',
    configuration={
        "load":{
            "sourceUris":[
                "gs://{{ti.xcom_pull(task_ids='generate_nos_query', key='gcs_bucket') }}/{{ti.xcom_pull(task_ids='generate_nos_query', key='gcs_path') }}/*.csv.gz"
            ],
        "destinationTable":{
            "projectId":bq_ple_project,
            "datasetId":"{{ti.xcom_pull(task_ids='fetch_metadata', key='tgt_db') }}",
            "tableId":"{{ti.xcom_pull(task_ids='fetch_metadata', key='tgt_tbl') }}"
        },
        "sourceFormat":"CSV",
        "fieldDelimiter":"|",
        "skipLeadingRows":1,
        "Compression":"GZIP",
        "autodectect":False,
        "writeDisposition":"{{ti.xcom_pull(task_ids='fetch_metadata', key='tbl_load_type')}}"   
        }
    },
    location='US',
    gcp_conn_id=GCP_CONN_DO_ID_PLE,
    dag=dag,
    on_failure_callback=fail_task
)




end=PythonOperator(
    task_id='end',
    python_callable=updt_sql_success,
    dag=dag,
    provide_context=True
)

get_metadata >> check_target_table_existance >> generate_query_task >> nos_to_gcs_load  >> gcs_to_bq_ple_load >> end
