from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.exceptions import AirflowFailException
from teradatasql import connect
import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from google.cloud import bigquery

def count_and_update_data(td_conn_id: str, bq_conn_id: str, where_condition: str):
    bq_hook = BigQueryHook(gcp_conn_id=bq_conn_id, use_legacy_sql=False)
    bq_conn = bq_hook.get_conn()
    bq_cursor = bq_conn.cursor()
    client=bq_hook.get_client(project_id='vz-it-pr-gk1v-pleedwdo-0')
    
    full_table_query = f"SELECT distinct td_tbl_nm,bq_tbl_nm,bq_prj_id,object_type,tbl_sr_no,app_name,Table_Category FROM vz-it-pr-gk1v-pleedwdo-0.edw_data_migration_audit_tbls.td_bq_count_live {where_condition}"
    bq_cursor.execute(full_table_query)
    rows = bq_cursor.fetchall()
    column_names = [desc[0] for desc in bq_cursor.description]
    df_audit_tbl = pd.DataFrame(rows, columns=column_names)
    
    td_conn = BaseHook.get_connection(td_conn_id)
    
    try:
        td_connection = connect(
            host=td_conn.host,
            user=td_conn.login,
            password=td_conn.password
        )
        with td_connection.cursor() as td_cursor:
            for index, row in df_audit_tbl.iterrows():
                td_tbl_nm = row['td_tbl_nm']
                bq_tbl_nm = row['bq_tbl_nm']
                
                try:
                    # Get Teradata count using the TD cursor
                    td_query = f"SELECT cast(COUNT(*) as bigint) FROM {td_tbl_nm}"
                    td_cursor.execute(td_query)
                    td_count = td_cursor.fetchone()[0]
                except Exception as e:
                    print(f"Error processing tables TD: {td_tbl_nm},Error:{e}")
                    td_count=-999
                    
                try:
                    bq_query = f"SELECT COUNT(*) FROM `{bq_tbl_nm}`"
                    bq_cursor.execute(bq_query)
                    bq_count_tuple = bq_cursor.fetchone()
                    bq_count = bq_count_tuple[0] if bq_count_tuple else -2
                except Exception as e:
                    print(f"Error processing tables BQ: {bq_tbl_nm},Error:{e}")
                    bq_count=-999
                df_audit_tbl.loc[index, 'td_cnt'] = td_count
                df_audit_tbl.loc[index, 'bq_cnt'] = bq_count
                df_audit_tbl.loc[index, 'last_updt_dt'] = datetime.now()
                df_audit_tbl.loc[index, 'is_cnt_matching'] = (True if td_count == bq_count and td_count>-1 and bq_count>-1  else False)
    finally:
        print("Done!!!")
    
    print("All Tables Count Captured")

    bq_hook = BigQueryHook(gcp_conn_id='sa-vz-it-gk1v-pleedwdo-0-app', use_legacy_sql=False)
    client=bq_hook.get_client(project_id='vz-it-pr-gk1v-pleedwdo-0')
    job_config=bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=False,
    )
   
    job= client.load_table_from_dataframe(df_audit_tbl,'vz-it-pr-gk1v-pleedwdo-0.edw_data_migration_audit_tbls.td_bq_count_live',job_config=job_config)
    print(job.result())
    print("DataFrame with updated counts successfully loaded into BigQuery.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 4),
    'retries': 0
}

dag= DAG(
    dag_id='histroy_td_bq_count_check_dag_all_tbls',
    schedule_interval='30 17 * * *',
    catchup=False,
    tags=['data_migration', 'audit', 'prod_parallel'],
    default_args=default_args,
    max_active_runs=1
)
update_count_audit_1 = PythonOperator(
    task_id='update_count_audit_1',
    python_callable=count_and_update_data,
    op_kwargs={
        'td_conn_id': 'UGCPDTSNOS1',
        'bq_conn_id': 'sa-vz-it-gk1v-edwdo-0-app',
        'where_condition': "where tbl_sr_no > 0 and tbl_sr_no <= 3600"
    },
    dag=dag
)
update_count_audit_2 = PythonOperator(
    task_id='update_count_audit_2',
    python_callable=count_and_update_data,
    op_kwargs={
        'td_conn_id': 'UGCPDTSNOS2',
        'bq_conn_id': 'sa-vz-it-gk1v-edwdo-0-app',
        'where_condition': "where tbl_sr_no > 3600 and tbl_sr_no <= 7200"
    },
    dag=dag
)
update_count_audit_3 = PythonOperator(
    task_id='update_count_audit_3',
    python_callable=count_and_update_data,
    op_kwargs={
        'td_conn_id': 'UGCPDTSNOS3',
        'bq_conn_id': 'sa-vz-it-gk1v-edwdo-0-app',
        'where_condition': "where tbl_sr_no > 7200 and tbl_sr_no <= 10800"
    },
    dag=dag
)
update_count_audit_4 = PythonOperator(
    task_id='update_count_audit_4',
    python_callable=count_and_update_data,
    op_kwargs={
        'td_conn_id': 'UGCPDTSNOS4',
        'bq_conn_id': 'sa-vz-it-gk1v-edwdo-0-app',
        'where_condition': "where tbl_sr_no > 10800 and tbl_sr_no <= 14400"
    },
    dag=dag
)
update_count_audit_5 = PythonOperator(
    task_id='update_count_audit_5',
    python_callable=count_and_update_data,
    op_kwargs={
        'td_conn_id': 'UGCPDTSNOS2',
        'bq_conn_id': 'sa-vz-it-gk1v-edwdo-0-app',
        'where_condition': "where tbl_sr_no > 14400 and tbl_sr_no <= 18000"
    },
    dag=dag
)
update_count_audit_6 = PythonOperator(
    task_id='update_count_audit_6',
    python_callable=count_and_update_data,
    op_kwargs={
        'td_conn_id': 'UGCPDTSNOS1',
        'bq_conn_id': 'sa-vz-it-gk1v-edwdo-0-app',
        'where_condition': "where tbl_sr_no > 18000 and tbl_sr_no <= 21600"
    },
    dag=dag
)
start_task = EmptyOperator(task_id='start_task')
end_task = EmptyOperator(task_id='end_task')

start_task>>[update_count_audit_1,update_count_audit_2,update_count_audit_3,update_count_audit_4,update_count_audit_5,update_count_audit_6]>>end_task
