from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.email import EmailOperator
import pandas as pd
import teradatasql
import os
from airflow.hooks.base import BaseHook

def get_bigquery_data(project_id, gcp_conn_id, tables):
    bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id)

    dataset_tables = [t.split(".") for t in tables]
    table_filter = " OR ".join([f"(table_schema='{d}' AND table_name='{t}')" for d, t in dataset_tables])

    count_query = f"""
        SELECT table_schema, table_name, total_rows as row_count
        FROM `{project_id}`.`region-us`.INFORMATION_SCHEMA.TABLE_STORAGE
        WHERE {table_filter}
    """

    col_query = f"""
        SELECT table_schema, table_name, column_name, data_type
        FROM `{project_id}`.`region-us`.INFORMATION_SCHEMA.COLUMNS
        WHERE {table_filter}
        ORDER BY table_schema, table_name, ordinal_position
    """

    count_df = bq_hook.get_pandas_df(sql=count_query, dialect="standard", location="US")
    col_df = bq_hook.get_pandas_df(sql=col_query, dialect="standard", location="US")

    return count_df, col_df


def get_teradata_counts(tables):
    
    conn = BaseHook.get_connection("UGCPDTSNOS5")
    conn = teradatasql.connect(
        host = 'tddp.tdc.vzwcorp.com',
        user=conn.login,
        password=conn.password
    )
    cursor = conn.cursor()
    results = {}
    for tbl in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {tbl}")
            results[tbl] = cursor.fetchone()[0]
        except:
            results[tbl] = None
    cursor.close()
    conn.close()
    return results


def process_data(tables, ple_count, ple_cols, prd_count, prd_cols, td_counts):
    result = []
    for tbl in tables:
        schema, name = tbl.split(".")
        ple_row = ple_count[(ple_count.table_schema == schema) & (ple_count.table_name == name)]
        prd_row = prd_count[(prd_count.table_schema == schema) & (prd_count.table_name == name)]

        ple_cnt = int(ple_row.row_count.values[0]) if not ple_row.empty else None
        prd_cnt = int(prd_row.row_count.values[0]) if not prd_row.empty else None

        ple_ng_cnt = ple_cnt  # Placeholder for nogsam variant if needed
        prd_ng_cnt = prd_cnt

        td_cnt = td_counts.get(tbl, None)

        ple_col_df = ple_cols[(ple_cols.table_schema == schema) & (ple_cols.table_name == name)]
        prd_col_df = prd_cols[(prd_cols.table_schema == schema) & (prd_cols.table_name == name)]

        ple_col_list = list(ple_col_df.column_name)
        prd_col_list = list(prd_col_df.column_name)

        ple_col_type = list(ple_col_df.data_type)
        prd_col_type = list(prd_col_df.data_type)

        col_intersection = len(set(ple_col_list) & set(prd_col_list))
        bq_col_sync = f"{col_intersection}/{len(ple_col_list)}/{len(prd_col_list)}"
        ple_prd_col_sync = f"{len(ple_col_list)}/{len(prd_col_list)}"

        bcs_res = 'Y' if ple_col_list == prd_col_list else 'N'
        pcs_res = 'Y' if len(ple_col_list) == len(prd_col_list) else 'N'
        col_type_match_res = 'Y' if ple_col_type == prd_col_type else 'N'

        result.append({
            "tablename": tbl,
            "ple_bq_table_count": ple_cnt,
            "ple_bq_nogsam_count": ple_ng_cnt,
            "prod_bq_table_count": prd_cnt,
            "prod_nogsam_count": prd_ng_cnt,
            "td_table_count": td_cnt,
            "bq_col_sync": bq_col_sync,
            "ple_prd_col_sync": ple_prd_col_sync,
            "bcs_res": bcs_res,
            "pcs_res": pcs_res,
            "col_type_match_res": col_type_match_res
        })

    return pd.DataFrame(result)


def generate_report(**context):
    conf = context['dag_run'].conf
    tables = conf['tables']
    email = conf['email']
    ple_project_id = "vz-it-pr-gk1v-pleedwdo-0"
    prd_project_id = "vz-it-pr-gk1v-edwdo-0"
    ple_conn_id = "sa-vz-it-gk1v-pleedwdo-0-app"
    prd_conn_id = "sa-vz-it-gk1v-edwdo-0-app"

    ple_counts, ple_cols = get_bigquery_data(ple_project_id, ple_conn_id, tables)
    prd_counts, prd_cols = get_bigquery_data(prd_project_id, prd_conn_id, tables)
    td_counts = get_teradata_counts(tables)

    df = process_data(tables, ple_counts, ple_cols, prd_counts, prd_cols, td_counts)
    html_report = df.to_html(index=False)

    context['ti'].xcom_push(key='html_report', value=html_report)
    context['ti'].xcom_push(key='email', value=email)


with DAG(
    dag_id="bq_td_table_audit_report",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1
) as dag:

    generate_report_task = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report,
        provide_context=True
    )

    send_email_task = EmailOperator(
        task_id="send_email",
        to="{{ ti.xcom_pull(task_ids='generate_report', key='email') }}",
        subject="GCP & Teradata Table Audit Report",
        html_content="{{ ti.xcom_pull(task_ids='generate_report', key='html_report') }}",
        mime_subtype='html'
    )

    generate_report_task >> send_email_task
