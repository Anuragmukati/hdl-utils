from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime
import pandas as pd

# Configuration
EMAIL_TO = 'abhijit.mahajan@verizon.com'

# The query to fetch SUCCESS/COMPLETED and Failed entries + table sizes
BQ_QUERY = """
WITH unioned_success AS (
  SELECT 
    BQDataset,
    BQTable,
    UPPER(Status) AS final_status,
    Start_time AS start_time,
    End_time AS end_time
  FROM `vz-it-pr-gk1v-edwpr-0.udm_prdstg_tbls.historical_load_audit_table`
  WHERE DATE(Start_time) = CURRENT_DATE()
    AND UPPER(Status) IN ('SUCCESS', 'COMPLETED')

  UNION ALL

  SELECT 
    tgt_schema AS BQDataset,
    tgt_table AS BQTable,
    UPPER(job_status) AS final_status,
    start_time,
    end_time
  FROM `vz-it-pr-gk1v-pleedwdo-0.edw_data_migration_audit_tbls.historical_load_audit_table`
  WHERE DATE(start_time) = CURRENT_DATE()
    AND UPPER(job_status) IN ('SUCCESS', 'COMPLETED')
),

failed_candidates AS (
  SELECT 
    BQDataset,
    BQTable
  FROM (
    SELECT 
      BQDataset,
      BQTable,
      ARRAY_AGG(DISTINCT UPPER(Status)) AS all_statuses
    FROM `vz-it-pr-gk1v-edwpr-0.udm_prdstg_tbls.historical_load_audit_table`
    WHERE DATE(Start_time) = CURRENT_DATE()
    GROUP BY BQDataset, BQTable

    UNION ALL

    SELECT 
      tgt_schema AS BQDataset,
      tgt_table AS BQTable,
      ARRAY_AGG(DISTINCT UPPER(job_status)) AS all_statuses
    FROM `vz-it-pr-gk1v-pleedwdo-0.edw_data_migration_audit_tbls.historical_load_audit_table`
    WHERE DATE(start_time) = CURRENT_DATE()
    GROUP BY tgt_schema, tgt_table
  )
  WHERE NOT ('SUCCESS' IN UNNEST(all_statuses) OR 'COMPLETED' IN UNNEST(all_statuses))
    AND ('INPROGRESS' IN UNNEST(all_statuses) OR 'STARTED' IN UNNEST(all_statuses))
),

table_sizes AS (
  SELECT
    BQDataset AS dataset_id,
    BQTable as table_name,
    32.32 table_size_tb
    from failed_candidates
  GROUP BY BQDataset,BQTable)

SELECT
  s.BQDataset,
  s.BQTable,
  s.final_status,
  s.start_time,
  s.end_time,
  ts.table_size_tb
FROM unioned_success s
LEFT JOIN table_sizes ts
  ON ts.dataset_id = s.BQDataset AND ts.table_name = s.BQTable

UNION ALL

SELECT
  f.BQDataset,
  f.BQTable,
  'FAILED' AS final_status,
  NULL AS start_time,
  NULL AS end_time,
  NULL AS table_size_tb
FROM failed_candidates f
"""

default_args = {
    'start_date': datetime(2024, 1, 1),
}

with DAG('dag_daily_hdl_report',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:

    def run_bq_and_prepare_report(**context):
        hook = BigQueryHook(gcp_conn_id='sa-vz-it-gk1v-pleedwdo-0-app', use_legacy_sql=False)
        df = hook.get_pandas_df(sql=BQ_QUERY, dialect='standard')

        total_success = df[df['final_status'].isin(['SUCCESS', 'COMPLETED'])].shape[0]
        total_failed = df[df['final_status'] == 'FAILED'].shape[0]
        total_size = df['table_size_tb'].sum(skipna=True).round(2)

        rows = ""
        for _, row in df.iterrows():
            color = "#e8f5e9" if row['final_status'] != "FAILED" else "#ffebee"
            rows += f"""
            <tr style='background-color:{color};'>
                <td style='padding:8px; border:1px solid #ccc'>{row['BQDataset']}</td>
                <td style='padding:8px; border:1px solid #ccc'>{row['BQTable']}</td>
                <td style='padding:8px; border:1px solid #ccc'>{row['final_status']}</td>
                <td style='padding:8px; border:1px solid #ccc'>{row['start_time'] or '-'}</td>
                <td style='padding:8px; border:1px solid #ccc'>{row['end_time'] or '-'}</td>
                <td style='padding:8px; border:1px solid #ccc'>{row['table_size_tb'] or '-'}</td>
            </tr>
            """

        html = f"""
        <html>
        <body style="font-family: Arial, sans-serif;">
        <h2>📄 BigQuery Load Report for {datetime.today().date()}</h2>
        <p><strong>Total Success:</strong> {total_success}</p>
        <p><strong>Total Failed:</strong> {total_failed}</p>
        <p><strong>Total Table Size (MB):</strong> {total_size}</p>

        <table style="border-collapse: collapse; width: 100%;">
            <tr style="background-color: #1976d2; color: white;">
                <th style='padding:8px; border:1px solid #ccc'>Dataset</th>
                <th style='padding:8px; border:1px solid #ccc'>Table</th>
                <th style='padding:8px; border:1px solid #ccc'>Status</th>
                <th style='padding:8px; border:1px solid #ccc'>Start Time</th>
                <th style='padding:8px; border:1px solid #ccc'>End Time</th>
                <th style='padding:8px; border:1px solid #ccc'>Size (MB)</th>
            </tr>
            {rows}
        </table>
        </body>
        </html>
        """

        context['ti'].xcom_push(key='report_html', value=html)

    generate_report = PythonOperator(
        task_id='generate_bq_report',
        python_callable=run_bq_and_prepare_report,
        provide_context=True
    )

    send_email = EmailOperator(
        task_id='send_report_email',
        to=EMAIL_TO,
        subject='[BQ Report] Daily Status - {{ ds }}',
        html_content="{{ ti.xcom_pull(task_ids='generate_bq_report', key='report_html') }}"
    )

    generate_report >> send_email
