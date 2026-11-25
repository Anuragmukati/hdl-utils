from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.google.cloud.operators.bigquery import BigQueryHook
from airflow.exceptions import AirflowFailException,AirflowSkipException
from croniter import croniter
import sys
import json

BASE_DIR = "/home/airflow/gcs/dags/vz-it-gk1v-edwpr-0/"
sys.path.append(f"{BASE_DIR}/")


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
Queue_Table=config["Queue_Table_large"]
bq_prod_project=config["bq_prod_project"]
bq_ple_project=config["bq_ple_project"]


def queue_table_insert(table_list,**kwargs):
    dag_run_id=kwargs['dag_run'].run_id
    dag_id = kwargs['dag'].dag_id
    dag_run_id=str(str(dag_id)+'_'+str("-".join(str(dag_run_id.split('_')[2]).split('-')[:-1])))
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_DO_ID, use_legacy_sql=False)
    conn = hook.get_conn()
    cursor = conn.cursor()
    print(table_list)
    queue_tbl_sqll=f"insert into {bq_ple_project}.{Metadata_db}.{Queue_Table}_Large (table_uid,table_dataset,table_name,app_name,ple_load,prod_load,dag_run_id) select table_uid,tgt_schema,tgt_table,app_name,True,True,'{dag_run_id}' from {bq_ple_project}.{Metadata_db}.{Metadata_tbl} where concat(tgt_schema,'.',tgt_table) in ({table_list}) and src_table_type='Large'"
    queue_tbl_sqlm=f"insert into {bq_ple_project}.{Metadata_db}.{Queue_Table}_Medium (table_uid,table_dataset,table_name,app_name,ple_load,prod_load,dag_run_id) select table_uid,tgt_schema,tgt_table,app_name,True,True,'{dag_run_id}' from {bq_ple_project}.{Metadata_db}.{Metadata_tbl} where concat(tgt_schema,'.',tgt_table) in ({table_list}) and src_table_type='Medium'"
    queue_tbl_sqls=f"insert into {bq_ple_project}.{Metadata_db}.{Queue_Table}_Small (table_uid,table_dataset,table_name,app_name,ple_load,prod_load,dag_run_id) select table_uid,tgt_schema,tgt_table,app_name,True,True,'{dag_run_id}' from {bq_ple_project}.{Metadata_db}.{Metadata_tbl} where concat(tgt_schema,'.',tgt_table) in ({table_list}) and src_table_type='Small'"
    try:
        cursor.execute(queue_tbl_sqll)
        cursor.execute(queue_tbl_sqlm)
        cursor.execute(queue_tbl_sqls)
    except:
        print(queue_tbl_sqll)
        print(queue_tbl_sqlm)
        print(queue_tbl_sqls)
        print("Queue Table Query Got Failed")
    print(queue_tbl_sqll)
    print(queue_tbl_sqlm)
    print(queue_tbl_sqls)
    print("all tables inserted")
def check_and_trigger(**kwargs):
    now = datetime.now()
    window_start = now - timedelta(minutes=15)
    window_end = now + timedelta(minutes=15)

    hook = BigQueryHook(gcp_conn_id=GCP_CONN_DO_ID, use_legacy_sql=False)
    metadata_tbl_sql=f"SELECT table_list, cron_schedule FROM {bq_ple_project}.{Metadata_db}.table_schedule_config WHERE DATE('{now}') BETWEEN start_date AND end_date"
    scheduled_job_list=hook.get_pandas_df(
        sql=metadata_tbl_sql,
        parameters=[
            {
                "name":"now",
                "parameterType":{"Type":"TIMESTAMP"},
                "parameterValue":{"value":now.isoformat()}
            }
        ]
    )
    print("table_list:")
    print(scheduled_job_list)
    if not scheduled_job_list.empty:
        for _,job in scheduled_job_list.iterrows():
            cron_expr=job['cron_schedule']
            table=job['table_list']
            print(table)
            try:
                cron_iter = croniter(cron_expr, now)
                prev_time = cron_iter.get_prev(datetime)
                next_time = cron_iter.get_next(datetime)
                if window_start <= prev_time <= window_end or window_start <= next_time <= window_end:
                    print(f"Triggering process for table: {job['table_list']} with schedule {job}")
                    queue_table_insert(table,**kwargs)
                else:
                    print(f"[skip] No match for table:{table}, cron:{cron_expr}")
            except Exception as e:
                print(f"failed to parse cron '{cron_expr}' for table {table}:{e}")
    else:
        print("No scheduled table found")
        raise AirflowSkipException("No scheduled table found for Now")
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 9),
    'retries': 0
}

dag=DAG(
    dag_id='HDL_Scheduled_Table_Load',
    default_args=default_args,
    start_date=datetime(2025, 4, 29),
    schedule_interval='*/32 * * * *',
    max_active_runs=1,
    catchup=False
) 

poll_config = PythonOperator(
    task_id='poll_schedule_config',
    python_callable=check_and_trigger,
    provide_context=True,
    dag=dag
)

poll_config
