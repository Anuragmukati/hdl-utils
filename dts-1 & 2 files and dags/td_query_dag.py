from airflow import DAG
from airflow.operators.python import PythonOperator
import teradatasql
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
from datetime import datetime
import pandas as pd
import os
from airflow.providers.google.cloud.hooks.gcs import GCSHook # Import GCSHook here

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'td_prod_query_fix',
    default_args=default_args,
    description='Execute Teradata SQL and upload to GCS, storing query and result. SQL loaded from config or GCS.',
    schedule_interval=None,
    catchup=False
)

GCP_CONN_ID = "sa-vz-it-gk1v-pleedwdo-0-app"
GCS_BUCKET = "gk1v-ple-edwdo-0-usre-beamx-code"

# Paths for the output results file
CSV_LOCAL_PATH = f"/tmp/td_results_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
GCS_OBJECT_PATH = f"teradataexecute/output/td_results_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"

def run_td_sql_and_save_and_upload(**context):
    """
    Connects to Teradata, executes SQL statements. The SQL script can be provided
    either directly in the dag_run.conf under the 'sql' key, or by providing
    a GCS path to the SQL file under the 'sql_gcs_path' key.
    Captures each SELECT query and its results, saves them directly to a local CSV (incrementally),
    and then uploads the CSV to Google Cloud Storage.
    This version optimizes memory by avoiding storing all results in memory.
    """
    dag_conf = context['dag_run'].conf or {}
    sql_script = ""

    # --- NEW LOGIC: Check for 'sql' directly in conf first ---
    if "sql" in dag_conf and dag_conf["sql"].strip():
        sql_script = dag_conf["sql"].strip()
        print("SQL script retrieved directly from dag_run.conf.")
    elif "sql_gcs_path" in dag_conf and dag_conf["sql_gcs_path"].strip():
        sql_gcs_path = dag_conf["sql_gcs_path"].strip()
        print(f"Attempting to download SQL script from gs://{GCS_BUCKET}/{sql_gcs_path}")
        gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
        
        try:
            # --- FIX: Use gcs_hook.download() which returns bytes, then decode ---
            sql_script_bytes = gcs_hook.download(
                bucket_name=GCS_BUCKET,
                object_name=sql_gcs_path
            )
            sql_script = sql_script_bytes.decode('utf-8')
            print("SQL script downloaded and decoded successfully from GCS.")
        except Exception as e:
            # Re-raise as ConnectionError for consistency
            raise ConnectionError(f"Failed to download SQL script from GCS path '{sql_gcs_path}': {e}")
    else:
        raise ValueError("Missing 'sql' or 'sql_gcs_path' in dag_run.conf. Please provide either the SQL script directly or the GCS path to your SQL script.")

    # Process the SQL script (from config or GCS)
    statements = [stmt.strip() for stmt in sql_script.split(";") if stmt.strip()]

    if not statements:
        raise ValueError("No valid SQL statements found after splitting (from config or GCS).")

    # Get Teradata connection details from Airflow connection
    try:
        conn_td = BaseHook.get_connection("UGCPDTSNOS5")
    except Exception as e:
        raise ConnectionError(f"Could not retrieve Airflow connection 'UGCPDTSNOS5': {e}")

    conn = None
    cursor = None
    csv_file = None # Initialize file handle outside try block

    try:
        conn = teradatasql.connect(
            host='tddp.tdc.vzwcorp.com',
            user=conn_td.login,
            password=conn_td.password
        )
        cursor = conn.cursor()

        # Ensure the directory exists for the temporary file
        os.makedirs(os.path.dirname(CSV_LOCAL_PATH), exist_ok=True)

        # Open the CSV file once for writing, to append results incrementally
        csv_file = open(CSV_LOCAL_PATH, "w")
        print(f"Opened local file for writing: {CSV_LOCAL_PATH}")

        select_statements_executed = 0

        for i, stmt in enumerate(statements, 1):
            print(f"Executing statement {i}/{len(statements)}: {stmt[:100]}...") # Log beginning of long query

            try:
                cursor.execute(stmt)

                if stmt.lower().startswith("select"):
                    select_statements_executed += 1
                    # Fetch all rows and column descriptions
                    rows = cursor.fetchall()
                    cols = [desc[0] for desc in cursor.description]

                    # Create a pandas DataFrame
                    df = pd.DataFrame(rows, columns=cols)

                    # Include the query in the section header
                    query_header = f"-- QUERY START --\n{stmt}\n-- QUERY END --"
                    # Convert DataFrame to CSV string (without index)
                    csv_data = df.to_csv(index=False)

                    # Write the query and its CSV result directly to the file
                    if select_statements_executed > 1: # Add newline separator only after the first block
                        csv_file.write("\n\n")
                    csv_file.write(f"{query_header}\n{csv_data}")

                else:
                    print(f"Statement {i} is not a SELECT statement, skipping result capture.")

            except teradatasql.Error as td_err:
                print(f"Error executing statement {i}: {stmt}\nError: {td_err}")
                # Log the error to the file as well
                if select_statements_executed > 0: # Add newline separator if some data was already written
                    csv_file.write("\n\n")
                csv_file.write(f"-- ERROR EXECUTING STATEMENT {i} --\n{stmt}\nERROR: {td_err}")
            except Exception as e:
                print(f"An unexpected error occurred for statement {i}: {stmt}\nError: {e}")
                # Log the error to the file as well
                if select_statements_executed > 0: # Add newline separator if some data was already written
                    csv_file.write("\n\n")
                csv_file.write(f"-- UNEXPECTED ERROR FOR STATEMENT {i} --\n{stmt}\nERROR: {e}")

        if select_statements_executed == 0:
            print("No SELECT statements found or no results were captured to write to file.")
            # If no SELECT statements were processed and no errors logged, no file will be effectively created
            # It's better to clean up an empty file if one was opened
            if csv_file:
                csv_file.close()
                csv_file = None # Mark as closed
            if os.path.exists(CSV_LOCAL_PATH) and os.path.getsize(CSV_LOCAL_PATH) == 0:
                os.remove(CSV_LOCAL_PATH)
                print(f"Removed empty local file: {CSV_LOCAL_PATH}")
            return # Exit if nothing to write

        # Close the local CSV file after all statements are processed
        if csv_file:
            csv_file.close()
            csv_file = None # Mark as closed
        print(f"All results written to local file: {CSV_LOCAL_PATH}")

        # --- UPLOAD FINAL RESULT FILE TO GCS ---
        print(f"Attempting to upload {CSV_LOCAL_PATH} to gs://{GCS_BUCKET}/{GCS_OBJECT_PATH}")
        gcs_hook_upload = GCSHook(gcp_conn_id=GCP_CONN_ID) # Use a new hook instance for clarity
        gcs_hook_upload.upload(
            bucket_name=GCS_BUCKET,
            object_name=GCS_OBJECT_PATH,
            filename=CSV_LOCAL_PATH
        )
        print(f"File {CSV_LOCAL_PATH} successfully uploaded to gs://{GCS_BUCKET}/{GCS_OBJECT_PATH}")

    except Exception as global_e:
        print(f"An error occurred during the overall process: {global_e}")
        raise # Re-raise the exception to fail the Airflow task
    finally:
        # Ensure file, cursor, and connection are closed even if errors occur
        if csv_file: # Make sure to close if it was opened but not explicitly closed due to an error
            csv_file.close()
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        # Optional: Clean up local file after upload, even if errors occurred during upload
        if os.path.exists(CSV_LOCAL_PATH):
            os.remove(CSV_LOCAL_PATH)
            print(f"Cleaned up local file: {CSV_LOCAL_PATH}")


run_and_upload_task = PythonOperator(
    task_id='run_td_and_upload_to_gcs',
    python_callable=run_td_sql_and_save_and_upload,
    provide_context=True,
    dag=dag,
)
