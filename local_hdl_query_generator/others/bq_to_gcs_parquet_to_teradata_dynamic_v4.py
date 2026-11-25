from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python import PythonOperator
from airflow.providers.teradata.operators.teradata import TeradataOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

DAG_ID = "bq_to_gcs_parquet_to_teradata_dynamic_v3"
BQ_CONN_ID = "sa-vz-it-gk1v-edwdo-0-app"
GCS_BUCKET = "gk1v-prod-edwpr-0-usre-nos-inbound"
TD_CONN_ID = "P2-UWBDTSGCPBQ1"
PROJECT_ID="vz-it-pr-gk1v-edwdo-0"

def build_select_query_and_columns(dataset_id, table_id):

    dataset_table=dataset_id+"."+table_id
    sql = f"""
    WITH td_cols_raw AS (
    SELECT
      DatabaseName AS td_db,
      TableName AS td_table,
      ColumnName AS td_column,
      ColumnType,
      ColumnLength,
      DecimalTotalDigits,
      DecimalFractionalDigits,
      ColumnFormat,
      columnid,
      ROW_NUMBER() OVER (
          PARTITION BY DatabaseName, TableName
          ORDER BY columnid
      ) AS ordinal_position,
      IFNULL(
          REGEXP_EXTRACT(ColumnFormat, r"S\\(([0-9]+)\\)"),
          "0"
      ) AS ts_fmt
    FROM `vz-it-pr-gk1v-pleedwdo-0.edw_data_migration_ops_tbls.dbc_columnsv`
    WHERE LOWER(CONCAT(TRIM(DatabaseName), '.', TRIM(TableName)))
          IN ('{dataset_table.lower()}')
    ),

    bq_cols_raw AS (
    SELECT
      table_schema AS bq_dataset,
      table_name AS bq_table,
      column_name AS bq_column,
      ordinal_position,
      data_type AS bq_data_type
    FROM `vz-it-pr-gk1v-edwdo-0.region-us.INFORMATION_SCHEMA.COLUMNS`
    WHERE LOWER(CONCAT(table_schema, '.', table_name))
          IN ('{dataset_table.lower()}')
    ),

    td_cols AS (
    SELECT
      t.td_db,
      t.td_table,
      t.td_column,
      b.bq_column,
      t.ordinal_position,
      t.ColumnType,
      t.ColumnLength,
      t.DecimalTotalDigits,
      t.DecimalFractionalDigits,
      t.ColumnFormat,
      b.bq_data_type
    FROM td_cols_raw t
    JOIN bq_cols_raw b
      ON LOWER(b.bq_dataset) = LOWER(TRIM(t.td_db))
     AND LOWER(b.bq_table) = LOWER(TRIM(t.td_table))
     AND b.ordinal_position = t.ordinal_position
    ),

    td_cols_norm AS (
    SELECT
      td_db,
      td_table,
      td_column,
      bq_column,
      ordinal_position,
      CASE
          WHEN UPPER(ColumnType) IN ('CF') THEN 'CHAR'
          WHEN UPPER(ColumnType) IN ('CV') THEN 'VARCHAR'
          WHEN UPPER(ColumnType) IN ('D') THEN 'DECIMAL'
          WHEN UPPER(ColumnType) IN ('DA') THEN 'DATE'
          WHEN UPPER(ColumnType) IN ('TS','SZ') THEN 'TIMESTAMP'
          WHEN UPPER(ColumnType) IN ('I','I1','I2','I8') THEN 'INTEGER'
          ELSE UPPER(ColumnType)
      END AS td_data_type,
      ColumnLength AS td_length,
      DecimalFractionalDigits AS td_scale,
      bq_data_type,
      CASE
          WHEN UPPER(ColumnType) IN ('TS','SZ') THEN
            CAST(IFNULL(REGEXP_EXTRACT(ColumnFormat, r"S\\(([0-9]+)\\)"),"0") AS INT64)
          ELSE NULL
      END AS ts_precision
    FROM td_cols
    ),

    finalized AS (
    SELECT
      td_db,
      td_table,
    'select '||
      STRING_AGG(
        CASE
            WHEN td_data_type IN ('CHAR','VARCHAR') THEN
                FORMAT('CAST(%s AS STRING) AS %s', bq_column, td_column)
            WHEN td_data_type = 'DECIMAL' THEN
                FORMAT('CAST(%s AS NUMERIC) AS %s',
                       bq_column, td_column)
            WHEN td_data_type = 'DATE' THEN
                FORMAT('CAST(%s AS DATE) AS %s', bq_column, td_column)
            WHEN td_data_type LIKE 'TIMESTAMP%%' AND ts_precision = 0 THEN
                FORMAT("FORMAT_TIMESTAMP('%%Y-%%m-%%d %%H:%%M:%%S', %s) AS %s",
                       bq_column, td_column)
            WHEN td_data_type LIKE 'TIMESTAMP%%'
                 AND ts_precision IS NOT NULL
                 AND ts_precision <> 0 THEN
                FORMAT("FORMAT_TIMESTAMP('%%Y-%%m-%%d %%H:%%M:%%E6S', %s) AS %s",
                       bq_column, td_column)
            WHEN td_data_type = 'INTEGER' THEN
                FORMAT('CAST(%s AS INT64) AS %s', bq_column, td_column)
            ELSE
                FORMAT('CAST(%s AS STRING) AS %s', bq_column, td_column)
        END
        ORDER BY ordinal_position
      )||' from '|| lower(td_db) ||'.'||lower(td_table) AS parquet_expr,

      STRING_AGG(
        FORMAT(
          'CAST(%s AS %s) AS %s',
          td_column,
          CASE
              WHEN td_data_type IN ('CHAR','VARCHAR') THEN
                  FORMAT('%s(%d)', td_data_type, td_length)
              WHEN td_data_type = 'DECIMAL' THEN
                  FORMAT('DECIMAL(%d,%d)', td_length, td_scale)
              WHEN td_data_type = 'DATE' THEN
                  'DATE'
              WHEN td_data_type LIKE 'TIMESTAMP%%' THEN
                  FORMAT('TIMESTAMP(%d)', IFNULL(ts_precision, 6))
              WHEN td_data_type = 'INTEGER' THEN
                  'INTEGER'
              ELSE
                  td_data_type
          END,
          td_column
        )
        ORDER BY ordinal_position
      ) AS td_cast_exprs,

      STRING_AGG(td_column ORDER BY ordinal_position) AS column_list

    FROM td_cols_norm
    GROUP BY 1, 2
    )

    SELECT * FROM finalized;
    """

    hook = BigQueryHook(gcp_conn_id="sa-vz-it-gk1v-edwdo-0-app", use_legacy_sql=False)
    client = hook.get_client(project_id=PROJECT_ID)

    query_job = client.query(sql)
    rows = list(query_job.result())

    if not rows:
        raise ValueError(" No metadata returned for table: " + dataset_table)

    row = rows[0]

    return {
        "parquet_expr": row.parquet_expr,
        "td_cast_exprs": row.td_cast_exprs,
        "column_list": row.column_list,
    }



default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 10, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args
) as dag:
    def export_bq_to_gcs_callable(**context):
        ti=context['ti']
        conf = context.get("dag_run").conf or {}
        source_table = conf.get("bq_table")
        BQ_CONN_ID = "sa-vz-it-gk1v-edwdo-0-app"
        if not source_table:
            raise ValueError("Missing 'bq_table' in dag_run.conf")
        hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID, use_legacy_sql=False,location='US')
        client = hook.get_client(project_id=PROJECT_ID) 
        tmp_table=f"{source_table}_tmp"
        tmp_table_del=f"drop {source_table}_tmp"
        res=build_select_query_and_columns(source_table.split(".")[1], source_table.split(".")[2])
        query_export=f"create or replace table {tmp_table} as {res['parquet_expr']} "
        print(query_export)
        hook.run_query(sql=query_export)
        path_source_table = source_table.replace(".", "_")
        dt=context['execution_date'].strftime('%Y%m%dT%H%M%S')
        dest_prefix = f"bq2td/{path_source_table}/{dt}"
        BQ_CONN_ID = "sa-vz-it-gk1v-edwdo-0-app"
        GCS_BUCKET = "gk1v-prod-edwpr-0-usre-nos-inbound"

        gcs_path = f"{GCS_BUCKET}/{dest_prefix}/"
        destination_uri = f"gs://{gcs_path}*.PARQUET"

        export_task = BigQueryToGCSOperator(
            task_id="export_bq_to_gcs_parquet_dynamic",
            source_project_dataset_table=tmp_table,
            destination_cloud_storage_uris=[destination_uri],
            export_format="PARQUET",
            gcp_conn_id=BQ_CONN_ID,
            compression="SNAPPY"
        )
        export_task.execute(context=context)
        client.query(tmp_table_del)
        ti.xcom_push(key="gcs_path", value=gcs_path)
        print(f"Exported {source_table} => {destination_uri}")
        
    export_bq_to_gcs = PythonOperator(
        task_id="export_bq_to_gcs_dynamic",
        python_callable=export_bq_to_gcs_callable,
        provide_context=True,
    )

    def generate_nos_query(**context):
        ti=context['ti']
        conf = context.get("dag_run").conf or {}
        target_table = conf.get("td_table","")
        source_table = conf.get("bq_table")
        gcs_path=ti.xcom_pull(task_ids='export_bq_to_gcs_dynamic', key='gcs_path')
        if not target_table:
            raise ValueError("Missing 'td_table' in dag_run.conf")
        
        hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID, use_legacy_sql=False,location='US')
        project=source_table.split(".")[0]
        table=source_table.split(".")[2]
        dataset=source_table.split(".")[1]
        stg_tabl=target_table.split(".")[1]
        res=build_select_query_and_columns(dataset,table)
        stage_table=f"wireless_writeback.{stg_tabl}"
        stage_query='Delete from ' + stage_table + ' ALL '
        truncate_query='Delete from ' + target_table + ' ALL '
        nos_query = f"""
        INSERT INTO {stage_table}({res['column_list']}) 
        SELECT 
            {res['td_cast_exprs']}
        FROM READ_NOS (
            USING 
                LOCATION ('/gs/storage-vzit.p.googleapis.com/{gcs_path}')
                AUTHORIZATION (TD_SERVER_DB.NOS_AUTH_PRD_GK1V_EDWPR)
                STOREDAS ('PARQUET')
        ) AS d;"""
        ti.xcom_push(key="nos_query", value=nos_query)
        ti.xcom_push(key="stage_query", value=stage_query)
        ti.xcom_push(key="truncate_query", value=truncate_query)

    generate_query_task = PythonOperator(
        task_id="generate_query_task",
        python_callable=generate_nos_query,
        provide_context=True,
    )

    load_to_td_from_gcs=TeradataOperator(
        task_id="load_data_to_td_from_gcs_using_nos",
        conn_id=TD_CONN_ID,
        sql="{{ ti.xcom_pull(task_ids='generate_query_task', key='nos_query') }}",
        autocommit=True,
    )

    truncate_td_stage_data=TeradataOperator(
        task_id="truncate_td_stage_data",
        conn_id=TD_CONN_ID,
        sql="{{ ti.xcom_pull(task_ids='generate_query_task', key='stage_query') }}",
        autocommit=True,
    )

    truncate_td_tbl_data=TeradataOperator(
        task_id="truncate_data_from_td_tbl",
        conn_id=TD_CONN_ID,
        sql="{{ ti.xcom_pull(task_ids='generate_query_task', key='truncate_query') }}",
        autocommit=True,
    )
    export_bq_to_gcs >> generate_query_task >> truncate_td_stage_data >> truncate_td_tbl_data >> load_to_td_from_gcs