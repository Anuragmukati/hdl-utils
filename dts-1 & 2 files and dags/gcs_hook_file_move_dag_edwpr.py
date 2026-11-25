from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime
import logging


def validate_and_prepare_pairs(**context):
    """
    Validate the configuration and prepare the list of source-destination pairs.
    """
    conf = context['dag_run'].conf
    if "pairs" not in conf or not isinstance(conf["pairs"], list):
        raise ValueError("The configuration must contain a 'pairs' key with a list of source-destination objects.")
   
    for idx, pair in enumerate(conf["pairs"]):
        if not isinstance(pair, dict) or "src_gcs_path" not in pair or "dest_gcs_path" not in pair:
            raise ValueError(f"Each pair must be a dictionary with 'src_gcs_path' and 'dest_gcs_path' keys. Error at index {idx}.")
   
    logging.info(f"Validated configuration: {conf['pairs']}")
    return conf["pairs"]


def move_files_with_gcs_hook(**context):
    """
    Move files from the source GCS path to the destination GCS path for all pairs using GCSHook.
    """
    pairs = context['ti'].xcom_pull(task_ids='validate_and_prepare_pairs')
    if not pairs:
        raise ValueError("No valid pairs found in the configuration.")

    gcs_hook = GCSHook(gcp_conn_id='sa-vz-it-gk1v-edwpr-0-app')

    for idx, pair in enumerate(pairs):
        src_path = pair["src_gcs_path"]
        dest_path = pair["dest_gcs_path"]

        # Parse bucket and blob names
        src_bucket_name, src_blob_name = src_path.split("/", 1)
        dest_bucket_name, dest_blob_name = dest_path.split("/", 1)

        # Copy the blob from source to destination
        gcs_hook.copy(
            source_bucket=src_bucket_name,
            source_object=src_blob_name,
            destination_bucket=dest_bucket_name,
            destination_object=dest_blob_name,
        )

        # Delete the source blob
        gcs_hook.delete(src_bucket_name, src_blob_name)

        logging.info(f"Moved file {idx + 1}/{len(pairs)}: {src_path} -> {dest_path}")


with DAG(
    dag_id="gcs_hook_file_move_dag_edwpr",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Triggered DAG, no schedule
    catchup=False,
) as dag:
   
    # Task 1: Validate and prepare configuration
    validate_config_task = PythonOperator(
        task_id="validate_and_prepare_pairs",
        python_callable=validate_and_prepare_pairs,
        provide_context=True,
    )

    # Task 2: Move files in batch using GCSHook
    move_files_task = PythonOperator(
        task_id="move_files_with_gcs_hook",
        python_callable=move_files_with_gcs_hook,
        provide_context=True,
    )

    validate_config_task >> move_files_task