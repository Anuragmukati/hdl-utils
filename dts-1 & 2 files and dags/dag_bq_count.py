from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.email import EmailOperator
from airflow.hooks.base import BaseHook

import pandas as pd
import teradatasql
import logging

def get_bigquery_table_count(project_id, gcp_conn_id, tables, is_view=False):
    bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
    counts = {}

    for tbl in tables:
        try:
            schema, name = tbl.split(".")
            dataset = f"{schema}_nogsampriv_v" if is_view else schema
            query = f"SELECT COUNT(*) as row_count FROM `{project_id}`.`{dataset}`.`{name}`"
            print(query)
            df = bq_hook.get_pandas_df(sql=query,dialect="standard", location="US")
            counts[tbl] = int(df['row_count'].iloc[0])
        except Exception as e:
            logging.warning(f"Skipping {tbl} in {'view' if is_view else 'table'} context due to error: {e}")
            counts[tbl] = None
    return counts

def get_bigquery_columns(project_id, gcp_conn_id, tables):
    bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
    cols = []

    for tbl in tables:
        try:
            schema, name = tbl.split(".")
            query = f"""
                SELECT column_name, data_type
                FROM `{project_id}`.`{schema}`.INFORMATION_SCHEMA.COLUMNS
                WHERE table_name = '{name}'
                ORDER BY ordinal_position
            """
            print(query)
            df = bq_hook.get_pandas_df(sql=query,dialect="standard", location="US")
            df['table'] = tbl
            cols.append(df)
        except Exception as e:
            logging.warning(f"Column metadata missing for {tbl} due to: {e}")
    return pd.concat(cols) if cols else pd.DataFrame(columns=["column_name", "data_type", "table"])

def get_teradata_counts(tables):
    conn = BaseHook.get_connection("UGCPDTSAUDIT")
    conn = teradatasql.connect(
        host='tddp.tdc.vzwcorp.com',
        user=conn.login,
        password=conn.password
    )
    cursor = conn.cursor()
    results = {}
    result_priv={}
    for tbl in tables:
        try:
            table_name=tbl.split(".")[1]
            dataset_name=tbl.split(".")[0]
            cursor.execute(f"SELECT COUNT(*) FROM {tbl}")
            results[tbl] = cursor.fetchone()[0]
        except Exception as e:
            logging.warning(f"Teradata count failed for {tbl}: {e}")
            results[tbl] = None
            
        try:
            table_name=tbl.split(".")[1]
            dataset_name=tbl.split(".")[0]
            cursor.execute(f"SELECT COUNT(*) FROM {dataset_name}_nogsampriv_v.{table_name}")
            result_priv[tbl] = cursor.fetchone()[0]
        except Exception as e:
            logging.warning(f"Teradata count failed for {tbl}: {e}")
            result_priv[tbl]=None
    cursor.close()
    conn.close()
    return results,result_priv

def process_data(tables, ple_counts, ple_ng_counts, ple_cols,
                 prd_counts, prd_ng_counts, prd_cols, td_counts,td_priv_count):

    result = []
    for tbl in tables:
        schema, name = tbl.split(".")
        ple_cnt = ple_counts.get(tbl)
        ple_ng_cnt = ple_ng_counts.get(tbl)
        prd_cnt = prd_counts.get(tbl)
        prd_ng_cnt = prd_ng_counts.get(tbl)
        td_cnt = td_counts.get(tbl)
        td_priv_cnt = td_priv_count.get(tbl)

        ple_col_df = ple_cols[ple_cols['table'] == tbl]
        prd_col_df = prd_cols[prd_cols['table'] == tbl]

        ple_cols_list = ple_col_df['column_name'].tolist()
        prd_cols_list = prd_col_df['column_name'].tolist()

        ple_types = ple_col_df['data_type'].tolist()
        prd_types = prd_col_df['data_type'].tolist()

        # col_intersection = len(set(ple_cols_list) & set(prd_cols_list))
        bq_col_sync = f"{len(ple_cols_list)}/{len(prd_cols_list)}"
        ple_prd_col_sync = f"{len(ple_cols_list)}/{len(prd_cols_list)}"

        # bcs_res = 'Y' if ple_cols_list == prd_cols_list else 'N'
        # pcs_res = 'Y' if len(ple_cols_list) == len(prd_cols_list) else 'N'
        col_type_match_res = 'Y' if ple_types == prd_types else 'N'

        result.append({
            "TABLE_NAME": tbl,
            "PLE_BQ_COUNT": ple_cnt,
            "PLE_BQ_PRIV_COUNT": ple_ng_cnt,
            "PRD_BQ_COUNT": prd_cnt,
            "PRD_BQ_PRIV_COUNT": prd_ng_cnt,
            "TD_COUNT": td_cnt,
            "TD_PRIV_COUNT":td_priv_cnt,
            "PLE_PRD_COL_SYNC": bq_col_sync,
            "BQ_PLE_PRD_DATATYPE": col_type_match_res
        })

    return pd.DataFrame(result)

def generate_report(**context):
    conf = context['dag_run'].conf
    tables = conf['tables']
    email_list =conf['email']

    ple_project = "vz-it-pr-gk1v-pleedwdo-0"
    prd_project = "vz-it-pr-gk1v-edwdo-0"
    ple_conn = "sa-vz-it-gk1v-pleedwdo-0-app"
    prd_conn = "sa-vz-it-gk1v-edwdo-0-app"

    ple_counts = get_bigquery_table_count(ple_project, ple_conn, tables)
    prd_counts = get_bigquery_table_count(prd_project, prd_conn, tables)
    ple_ng_counts = get_bigquery_table_count(ple_project, ple_conn, tables, is_view=True)
    prd_ng_counts = get_bigquery_table_count(prd_project, prd_conn, tables, is_view=True)

    ple_cols = get_bigquery_columns(ple_project, ple_conn, tables)
    prd_cols = get_bigquery_columns(prd_project, prd_conn, tables)

    td_counts,td_priv_count = get_teradata_counts(tables)

    df = process_data(tables, ple_counts, ple_ng_counts, ple_cols,
                      prd_counts, prd_ng_counts, prd_cols, td_counts,td_priv_count)

    html_table = df.to_html(index=False, classes='dataframe', border=0)

    html_email = f"""
    <html>
        <head>
            <style>
                .dataframe {{
                    border-collapse: collapse;
                    width: 100%;
                }}
                .dataframe th, .dataframe td {{
                    border: 1px solid #dddddd;
                    text-align: center;
                    padding: 8px;
                }}
                .dataframe tr:nth-child(even) {{
                    background-color: #f2f2f2;
                }}
                th {{
                    background-color: #4CAF50;
                    color: white;
                }}
            </style>
        </head>
        <body>
            <h3>GCP and Teradata Table Audit Report</h3>
            {html_table}
        </body>
    </html>
    """

    context['ti'].xcom_push(key='html_report', value=html_email)
    context['ti'].xcom_push(key='email_list', value=email_list)

with DAG(
    dag_id="dag_table_counts_all_env_audit_report",
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
        to="{{ ti.xcom_pull(task_ids='generate_report', key='email_list') }}",
        subject="GCP & Teradata Table Audit Report",
        html_content="{{ ti.xcom_pull(task_ids='generate_report', key='html_report') }}",
        mime_subtype='html'
    )

    generate_report_task >> send_email_task
