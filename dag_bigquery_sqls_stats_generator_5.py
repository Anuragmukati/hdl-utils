
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.email import EmailOperator
from datetime import datetime
import pandas as pd
import io
import uuid
import json
from airflow.utils import timezone


PROJECT_ID = "vz-nonit-pr-k65v-plefpardo-0"
BQ_CONN_ID = "sa-vz-nonit-k65v-fpardo-0-app"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 0
}

def run_sqls_from_gcs(**context):
    gcs_paths = context["dag_run"].conf.get("gcs_sql_files", [])
    dest_gcs_sql_dir = context["dag_run"].conf.get("dest_gcs_sql_dir","")
    if dest_gcs_sql_dir !="":
        dest_bucket, dest_blob = dest_gcs_sql_dir.replace("gs://", "").split("/", 1)
    if not gcs_paths:
        raise ValueError("No SQL file paths provided in DAG run config.")

    bq_hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID, location="US")
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')

    processed_prefix = "processed/"  # Folder inside the same bucket to move successful files

    job_records = []

    for gcs_path in gcs_paths:
        bucket, blob = gcs_path.replace("gs://", "").split("/", 1)
        filename = blob.split("/")[-1]
        blob_2="/".join(blob.split("/")[:-1])+"/"
        sql_content = gcs_hook.download(bucket_name=bucket, object_name=blob).decode("utf-8")

        try:
            job = bq_hook.insert_job(
                configuration={
                    "query": {
                        "query": sql_content,
                        "useLegacySql": False
                    }
                }
            )
            job_id = job.job_id
            job.result()
            status = "SUCCESS"

            
            if dest_gcs_sql_dir:
                dest_bucket=dest_bucket
                new_blob_name = dest_blob+filename
            else:
                dest_bucket=bucket
                new_blob_name = blob_2+processed_prefix + filename
                
            gcs_hook.copy(
                source_bucket =bucket,
                source_object =blob,
                destination_bucket =dest_bucket,
                destination_object =new_blob_name
            )
            gcs_hook.delete(bucket_name=bucket, object_name=blob)

        except Exception as e:
            print(e)
            job_id = f"FAILED_{uuid.uuid4().hex[:8]}"
            status = "FAILED"

        job_records.append({
            "parent_job_id": job_id,
            "filename": gcs_path,
            "status": status
        })

    context["ti"].xcom_push(key="job_records", value=job_records)


def run_stats_query(**context):
    job_records = context["ti"].xcom_pull(key="job_records", task_ids="run_sqls_from_gcs")

    successful_jobs = [j for j in job_records if not j["parent_job_id"].startswith("FAILED_")]

    if successful_jobs:
        job_ids_clause = ", ".join([f"'{j['parent_job_id']}'" for j in successful_jobs])
    else:
        job_ids_clause = "''"

    filename_map = {j["parent_job_id"]: j["filename"] for j in job_records}

    stats_query = f"""
    SELECT
coalesce(a.parent_job_id,a.job_id) as parent_job_id,
        a.job_id as job_id,
        a.start_time as start_time,
        a.end_time as end_time,
        dml_statistics.inserted_row_count AS inserted_row_count,
        dml_statistics.deleted_row_count AS deleted_row_count,
        dml_statistics.updated_row_count AS updated_row_count,
         case when dml_statistics.inserted_row_count is not null or dml_statistics.deleted_row_count is not null or dml_statistics.updated_row_count is not null then 
         null else s.records_written end as select_row_count,
         current_date() as job_run_date
    FROM
        `{PROJECT_ID}.region-us.INFORMATION_SCHEMA.JOBS`a,UNNEST(job_stages) as s
    WHERE  a.parent_job_id IN ({job_ids_clause}) or job_id IN ({job_ids_clause}) 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY a.job_id,a.parent_job_id ORDER BY s.id DESC) = 1;
    """
    print(stats_query)

    bq_hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID, location="US")

    if successful_jobs:
        df = bq_hook.get_pandas_df(sql=stats_query, dialect="standard", location="US")
    else:
        schema = {
        'parent_job_id': str,
        'job_id': str,
        'start_time': str,
        'end_time': str,
        'inserted_row_count':str,
        'deleted_row_count': str,
        'updated_row_count': str,
        'filename': str,
        'select_row_count':str,
        'job_run_date':'datetime64[ns]',
        'status':str
        
        }
        df = pd.DataFrame({col: pd.Series(dtype=typ) for col, typ in schema.items()})

    df["filename"] = df["parent_job_id"].map(filename_map).fillna("UNKNOWN")
    df["status"] = "SUCCESS"

    failed_jobs = [j for j in job_records if j["status"] == "FAILED"]
    print(failed_jobs)
    if failed_jobs:
        failed_df = pd.DataFrame({
            "parent_job_id": [j["parent_job_id"] for j in failed_jobs],
            "job_id": ["NA"]*len(failed_jobs),
            "start_time": ["NA"]*len(failed_jobs),
            "end_time": ["NA"]*len(failed_jobs),
            "inserted_row_count": ["NA"]*len(failed_jobs),
            "deleted_row_count": ["NA"]*len(failed_jobs),
            "updated_row_count": ["NA"]*len(failed_jobs),
            "filename": [j["filename"] for j in failed_jobs],
            "select_row_count":["NA"]*len(failed_jobs),
            "job_run_date":[str(datetime.utcnow().date())]*len(failed_jobs),
            "status": ["FAILED"]*len(failed_jobs)
        })
        print(df)
        print(failed_df)
        df = pd.concat([df, failed_df], ignore_index=True)
    
    

    # Save CSV for attachment
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_content = csv_buffer.getvalue()
    attachment_path = "/tmp/dml_stats.csv"
    with open(attachment_path, "w") as f:
        f.write(csv_content)
    # df=df.replace("NA",None)
    # records = df.to_dict(orient='records')
    # bq_hook.insert_all(
    #     dataset_id='edw_data_migration_audit_tbls',
    #     table_id='daily_bq_job_status_util',
    #     rows=records,
    #     project_id=PROJECT_ID
    # )
        
    

    # Build attractive HTML for email body
    html_content = """
    <html>
    <head>
      <style>
        body {
          font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
          font-size: 9px;
          color: #333;
          margin: 0;
          padding: 20px;
          background-color: #f9f9f9;
        }
        h2 {
          color: #2c3e50;
        }
        table {
          border-collapse: collapse;
          width: 100%;
          max-width: 900px;
          margin: 10px auto;
          background-color: #fff;
          box-shadow: 0 2px 6px rgba(0,0,0,0.1);
          border-radius: 6px;
          overflow: hidden;
          font-size: 9px;
        }
        thead {
          background: linear-gradient(90deg, #4a90e2, #357ABD);
          color: white;
          text-align: center;
          font-weight: bold;
        }
        th, td {
          padding: 6px 8px;
          text-align: center;
          border-bottom: 1px solid #ddd;
          max-width: 120px;
          overflow: hidden;
          text-overflow: hidden;
          white-space:nowrap;
        }
        tbody tr:hover {
          background-color: #f1faff;
        }
        tbody tr:nth-child(even) {
          background-color: #f6f8fa;
        }
        .status-success {
          color: #155724;
          background-color: #d4edda;
          font-weight: 600;
          border-radius: 4px;
          padding: 4px 10px;
          display: inline-block;
        }
        .status-failed {
          color: #721c24;
          background-color: #f8d7da;
          font-weight: 600;
          border-radius: 4px;
          padding: 4px 10px;
          display: inline-block;
        }
      </style>
    </head>
    <body>
      <h2>BigQuery SQLs Statistics Report: vz-nonit-pr-k65v-plefpardo-0</h2>
      <p><b>The report below displays the stats and details of the bigquery sqls of <b>vz-nonit-pr-k65v-plefpardo-0</b> project you have submitted to utility. Refer attached csv for more details.</b></p>
      <table>
        <thead>
          <tr>
            <th>Parent Job ID</th>
            <th>Job ID</th>
            <th>Start Time</th>
            <th>End Time</th>
            <th>Inserted Rows</th>
            <th>Deleted Rows</th>
            <th>Updated Rows</th>
            <th>Select Rows</th>
            <th>Filename</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>
    """

    for _, row in df.iterrows():
        status_class = "status-success" if row["status"] == "SUCCESS" else "status-failed"
        job_id = row["job_id"] if pd.notnull(row["job_id"]) else "â€”"
        start_time = row["start_time"] if pd.notnull(row["start_time"]) else "â€”"
        end_time = row["end_time"] if pd.notnull(row["end_time"]) else "â€”"
        inserted = row["inserted_row_count"] if pd.notnull(row["inserted_row_count"]) else "â€”"
        deleted = row["deleted_row_count"] if pd.notnull(row["deleted_row_count"]) else "â€”"
        updated = row["updated_row_count"] if pd.notnull(row["updated_row_count"]) else "â€”"
        select=row["select_row_count"] if pd.notnull(row["select_row_count"]) else "â€”"

        html_content += f"""
          <tr>
            <td>{row['parent_job_id']}</td>
            <td>{job_id}</td>
            <td>{start_time}</td>
            <td>{end_time}</td>
            <td>{inserted}</td>
            <td>{deleted}</td>
            <td>{updated}</td>
            <td>{select}</td>
            <td>{row['filename']}</td>
            <td><span class="{status_class}">{row['status']}</span></td>
          </tr>
        """

    html_content += """
        </tbody>
      </table>
    </body>
    </html>
    """

    context["ti"].xcom_push(key="stats_html", value=html_content)


with DAG(
    "dag_bigquery_sqls_stats_generator_5",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    run_sqls = PythonOperator(
        task_id="run_sqls_from_gcs",
        python_callable=run_sqls_from_gcs,
        provide_context=True
    )

    stats = PythonOperator(
        task_id="run_stats_query",
        python_callable=run_stats_query,
        provide_context=True
    )

    send_email = EmailOperator(
        task_id="send_email",
        to="{{ dag_run.conf.get('mail_ids', '') }}",
        subject="BigQuery SQLs Stats Report: vz-nonit-pr-k65v-plefpardo-0",
        html_content="{{ ti.xcom_pull(task_ids='run_stats_query', key='stats_html') }}",
        files=["/tmp/dml_stats.csv"]
    )

    run_sqls >> stats >> send_email
