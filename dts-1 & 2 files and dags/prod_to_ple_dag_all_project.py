from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryHook
from google.cloud.exceptions import NotFound
import json
from datetime import datetime
import sys

BASE_DIR = "/home/airflow/gcs/dags/vz-it-gk1v-edwpr-0/"
sys.path.append(f"{BASE_DIR}/")

def read_config():
    """Reads configuration from a JSON file."""
    config_file_path = f"{BASE_DIR}/history_load_config_prod.json"
    try:
        with open(config_file_path, 'r') as f:
            confid_details = json.load(f)
        print(f"Successfully read configuration from: {config_file_path}")
        return confid_details
    except FileNotFoundError:
        print(f"Error: Configuration file not found at {config_file_path}")
        raise
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {config_file_path}. Check file format.")
        raise

config = read_config()

def create_project_specs(**kwargs):
    """Determines project and connection details based on DAG run configuration."""
    project = kwargs['dag_run'].conf.get('project', '').lower()
    
    bq_prod_project = None
    GCP_CONN_DO_ID_PLE = None
    bq_ple_project = None
    GCP_CONN_DO_ID_PROD = None
    
    if project:
        project_name = project
        print(f"Setting up for project: {project_name}")
        if project_name == 'gk1v':
            bq_prod_project = config.get("bq_prod_project_gk1v", "vz-it-pr-gk1v-edwdo-0")
            GCP_CONN_DO_ID_PLE = "sa-vz-it-gk1v-pleedwdo-0-app"
            bq_ple_project = "vz-it-pr-gk1v-pleedwdo-0"
            GCP_CONN_DO_ID_PROD = "sa-vz-it-gk1v-edwdo-0-app"
        elif project_name == 'fpardo':
            bq_prod_project = "vz-nonit-pr-k65v-fpardo-0"
            GCP_CONN_DO_ID_PLE = "sa-vz-nonit-k65v-plefpardo-0-app"
            bq_ple_project = "vz-nonit-pr-k65v-plefpardo-0"
            GCP_CONN_DO_ID_PROD = "sa-vz-nonit-k65v-fpardo-0-app"
        elif project_name == 'enabdo':
            bq_prod_project = "vz-nonit-pr-k45v-enabdo-0"
            GCP_CONN_DO_ID_PLE = "sa-vz-nonit-k45v-pleenabdo-0-app"
            bq_ple_project = "vz-nonit-pr-k45v-pleenabdo-0"
            GCP_CONN_DO_ID_PROD = "sa-vz-nonit-k45v-enabdo-0-app"
        elif project_name == 'pblsdo':
            bq_prod_project = "vz-nonit-pr-k45v-pblsdo-0"
            GCP_CONN_DO_ID_PLE = "sa-vz-nonit-k45v-plepblsdo-0-app"
            bq_ple_project = "vz-nonit-pr-k45v-plepblsdo-0"
            GCP_CONN_DO_ID_PROD = "sa-vz-nonit-k45v-pblsdo-0-app"
        elif project_name == 'msgcdo':
            bq_prod_project = "vz-nonit-pr-k45v-msgcdo-0"
            GCP_CONN_DO_ID_PLE = "sa-vz-nonit-k45v-plemsgcdo-0-app"
            bq_ple_project = "vz-nonit-pr-k45v-plemsgcdo-0"
            GCP_CONN_DO_ID_PROD = "sa-vz-nonit-k45v-msgcdo-0-app"
        elif project_name == 'vzendo':
            bq_prod_project = "vz-nonit-pr-k45v-vzendo-0"
            GCP_CONN_DO_ID_PLE = "sa-vz-nonit-k45v-plevzendo-0-app"
            bq_ple_project = "vz-nonit-pr-k45v-plevzendo-0"
            GCP_CONN_DO_ID_PROD = "sa-vz-nonit-k45v-vzendo-0-app"
        elif project_name == 'zdwv':
            bq_prod_project = "vz-it-pr-zdwv-edwdo-0"
            GCP_CONN_DO_ID_PLE = "sa-vz-it-zdwv-pleedwdo-0-app"
            bq_ple_project = "vz-it-pr-zdwv-pleedwdo-0"
            GCP_CONN_DO_ID_PROD = "sa-vz-it-zdwv-edwdo-0-app"
        else:
            print(f"Warning: Project '{project_name}' not recognized. Using default/empty values.")
    else:
        print("Warning: 'project' key not found in DAG run configuration. Using default/empty values.")
    
    if any(v is None for v in [bq_prod_project, GCP_CONN_DO_ID_PLE, bq_ple_project, GCP_CONN_DO_ID_PROD]):
        print("Error: One or more project/connection details could not be determined.")
    return bq_prod_project, GCP_CONN_DO_ID_PLE, bq_ple_project, GCP_CONN_DO_ID_PROD

def check_table_exists(table_name, bq_prod_project, gcp_conn_id_prod, bq_ple_project, gcp_conn_id_ple):
    """
    Checks for the existence of a BigQuery table in both PLE and PROD projects
    using the correct service account for each.
    """
    if not all([bq_prod_project, gcp_conn_id_prod, bq_ple_project, gcp_conn_id_ple]):
        print("Error: Project or connection details are missing for table existence check.")
        return 'False', 'False'
        
    ple_hook = BigQueryHook(gcp_conn_id=gcp_conn_id_ple, use_legacy_sql=False)
    ple_client = ple_hook.get_client()
    
    prod_hook = BigQueryHook(gcp_conn_id=gcp_conn_id_prod, use_legacy_sql=False)
    prod_client = prod_hook.get_client()
    
    ple_full_table_name = f'{bq_ple_project}.{table_name}'
    prod_full_table_name = f'{bq_prod_project}.{table_name}'
    
    ple_status = 'False'
    prod_status = 'False'
    
    # Check PLE table existence
    try:
        ple_client.get_table(ple_full_table_name)
        print(f"Table exists in PLE: {ple_full_table_name}")
        ple_status = 'True'
    except NotFound:
        print(f"Table does NOT exist in PLE: {ple_full_table_name}")
    except Exception as e:
        print(f"Error checking PLE table {ple_full_table_name}: {e}")
        
    # Check PROD table existence
    try:
        prod_client.get_table(prod_full_table_name)
        print(f"Table exists in PROD: {prod_full_table_name}")
        prod_status = 'True'
    except NotFound:
        print(f"Table does NOT exist in PROD: {prod_full_table_name}")
    except Exception as e:
        print(f"Error checking PROD table {prod_full_table_name}: {e}")
        
    return ple_status, prod_status

def prod_to_ple(**kwargs):
    """
    Copies data from PROD tables to PLE tables for a given list of tables.
    """
    table_list = kwargs['dag_run'].conf.get('Table_List', [])
    if not table_list:
        print("No tables specified in 'Table_List' in DAG run configuration. Exiting.")
        return
        
    bq_prod_project, gcp_conn_do_id_ple, bq_ple_project, gcp_conn_do_id_prod = create_project_specs(**kwargs)
    
    if not all([bq_prod_project, gcp_conn_do_id_ple, bq_ple_project, gcp_conn_do_id_prod]):
        print("Critical Error: Project or connection details are incomplete. Cannot proceed with table operations.")
        raise ValueError("Incomplete BigQuery project configuration.")
        
    for table_name in table_list:
        print(f"\n--- Processing table: {table_name} ---")
        
        # Pass both connection IDs to the check_table_exists function
        ple_status, prd_status = check_table_exists(table_name, bq_prod_project, gcp_conn_do_id_prod, bq_ple_project, gcp_conn_do_id_ple)
        
        if ple_status == 'True' and prd_status == 'True':
            sql = f"""
            BEGIN TRANSACTION;
            TRUNCATE TABLE `{bq_ple_project}.{table_name}`;
            INSERT INTO `{bq_ple_project}.{table_name}`
            SELECT * FROM `{bq_prod_project}.{table_name}`;
            COMMIT TRANSACTION;
            """
            print(f"Executing SQL for {table_name}:\n{sql}")
            
            # This hook uses the PLE connection ID for the write operation
            hook = BigQueryHook(gcp_conn_id=gcp_conn_do_id_ple, use_legacy_sql=False)
            
            try:
                conn = hook.get_conn()
                cursor = conn.cursor()
                cursor.execute(sql)
                print(f"Successfully copied data for {table_name} from PROD to PLE.")
            except Exception as e:
                print(f'Error occurred during data copy for {table_name}: {e}')
                print(f'PROD to PLE copy for {table_name} FAILED!!!!!')
                raise
        else:
            print("#######################################")
            if ple_status != 'True' and prd_status != 'True':
                print(f"Table not found in both PLE and PROD: {table_name}")
            elif ple_status != 'True':
                print(f"Table not found in PLE: {table_name}")
            elif prd_status != 'True':
                print(f"Table not found in PROD: {table_name}")
            print("Skipping copy operation for this table.")
            print("#######################################")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 9),
    'retries': 0
}

dag = DAG(
    dag_id='load_prod_to_ple_all',
    default_args=default_args,
    description='DAG to copy specified BigQuery tables from PROD to PLE environments based on project config.',
    schedule_interval=None,
    max_active_runs=1,
    catchup=False
)

gcs_to_bq_ple_load = PythonOperator(
    task_id='copy_prod_to_ple_tables',
    python_callable=prod_to_ple,
    provide_context=True,
    dag=dag
)
