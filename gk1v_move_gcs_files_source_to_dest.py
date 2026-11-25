from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime
import logging


def validate_and_prepare_pairs(**context):

    conf = context['dag_run'].conf
    if "pairs" not in conf or not isinstance(conf["pairs"], list):
        raise ValueError("The configuration must contain a 'pairs' key with a list of source-destination objects.")
   
    for idx, pair in enumerate(conf["pairs"]):
        if not isinstance(pair, dict) or "src_gcs_path" not in pair or "dest_gcs_path" not in pair:
            raise ValueError(f"Each pair must be a dictionary with 'src_gcs_path' and 'dest_gcs_path' keys. Error at index {idx}.")
   
    logging.info(f"Validated configuration: {conf['pairs']}")
    return conf["pairs"]


def move_files_with_gcs_hook(**context):
    pairs = context['ti'].xcom_pull(task_ids='validate_and_prepare_pairs')
    if not pairs:
        raise ValueError("No valid pairs found in the configuration.")

    gcs_hook = GCSHook(gcp_conn_id='sa-vz-it-gk1v-edwpr-0-app')

    for idx, pair in enumerate(pairs):
        src_path = pair["src_gcs_path"]
        dest_path = pair["dest_gcs_path"]
        src_bucket_name, src_blob_name = src_path.split("/", 1)
        dest_bucket_name, dest_blob_name = dest_path.split("/", 1)

        gcs_hook.copy(
            source_bucket=src_bucket_name,
            source_object=src_blob_name,
            destination_bucket=dest_bucket_name,
            destination_object=dest_blob_name,
        )

        gcs_hook.delete(src_bucket_name, src_blob_name)

        logging.info(f"Moved file {idx + 1}/{len(pairs)}: {src_path} -> {dest_path}")


with DAG(
    dag_id="gk1v_move_gcs_files_source_to_dest",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
   
    validate_config_task = PythonOperator(
        task_id="validate_and_prepare_pairs",
        python_callable=validate_and_prepare_pairs,
        provide_context=True,
    )

    move_files_task = PythonOperator(
        task_id="move_files",
        python_callable=move_files_with_gcs_hook,
        provide_context=True,
    )

    validate_config_task >> move_files_task
    
    
# Dag run config:
#     {
#     "pairs": [
#         {
#             "src_gcs_path": "gk1v-ple-edwpr-0-usre-inbound/OB_FEEDS_FROM_ONPREM/FIOS_DV_MXU_DISCONNECTS_DAY_09162026_09172026.txt.gz",
#             "dest_gcs_path": "gk1v-ple-cwlndo-0-usre-outbound/OB_FEEDS_FROM_ONPREM/FIOS_DV_MXU_DISCONNECTS_DAY_09162026_09172026.txt.gz",
#             "mode": "move"
#         },
#         {
#             "src_gcs_path": "gk1v-ple-edwpr-0-usre-inbound/OB_FEEDS_FROM_ONPREM/FIOS_INTERNET_MXU_DISCONNECTS_DAY_09162026_09172026.txt.gz",
#             "dest_gcs_path": "gk1v-ple-cwlndo-0-usre-outbound/OB_FEEDS_FROM_ONPREM/FIOS_INTERNET_MXU_DISCONNECTS_DAY_09162026_09172026.txt.gz",
#             "mode": "move"
#         },
#         {
#             "src_gcs_path": "gk1v-ple-edwpr-0-usre-inbound/OB_FEEDS_FROM_ONPREM/FIOS_TV_MXU_DISCONNECTS_DAY_09162026_09172026.txt.gz",
#             "dest_gcs_path": "gk1v-ple-cwlndo-0-usre-outbound/OB_FEEDS_FROM_ONPREM/FIOS_TV_MXU_DISCONNECTS_DAY_09162026_09172026.txt.gz",
#             "mode": "move"
#         },
#         {
#             "src_gcs_path": "gk1v-ple-edwpr-0-usre-inbound/OB_FEEDS_FROM_ONPREM/FIOS_DV_MXU_LIS_DAY_09162026_09172026.txt.gz",
#             "dest_gcs_path": "gk1v-ple-cwlndo-0-usre-outbound/OB_FEEDS_FROM_ONPREM/FIOS_DV_MXU_LIS_DAY_09162026_09172026.txt.gz",
#             "mode": "move"
#         },
#         {
#             "src_gcs_path": "gk1v-ple-edwpr-0-usre-inbound/OB_FEEDS_FROM_ONPREM/FIOS_INTERNET_MXU_LIS_DAY_09162026_09172026.txt.gz",
#             "dest_gcs_path": "gk1v-ple-cwlndo-0-usre-outbound/OB_FEEDS_FROM_ONPREM/FIOS_INTERNET_MXU_LIS_DAY_09162026_09172026.txt.gz",
#             "mode": "move"
#         },
#         {
#             "src_gcs_path": "gk1v-ple-edwpr-0-usre-inbound/OB_FEEDS_FROM_ONPREM/FIOS_TV_MXU_LIS_DAY_09162026_09172026.txt.gz",
#             "dest_gcs_path": "gk1v-ple-cwlndo-0-usre-outbound/OB_FEEDS_FROM_ONPREM/FIOS_TV_MXU_LIS_DAY_09162026_09172026.txt.gz",
#             "mode": "move"
#         }
#     ]
# }