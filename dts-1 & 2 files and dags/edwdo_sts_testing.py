import random
import pendulum
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import (
    CloudDataTransferServiceGCSToGCSOperator,
)
local_tz = pendulum.timezone("America/New_York")
DAG_ID = "edwdo_sts_testing"
BQ_CONN_ID = "sa-vz-it-gk1v-edwdo-0-app"
BQ_CONN_ID_PR = "sa-vz-it-gk1v-edwpr-0-app"
TD_CONN_ID_P2 = ["P2-GCPDTSNOS1","P2-GCPDTSNOS2","P2-GCPDTSNOS3","P2-GCPDTSNOS4","P2-GCPDTSNOS5"]
TD_CONN_ID = random.choice(TD_CONN_ID_P2)
PROJECT_ID = "vz-it-pr-gk1v-edwdo-0"
PROJECT_ID_PR = "vz-it-pr-gk1v-edwpr-0"
Table_Name = "udm_prd_tbls.rev_sum_fact_bl" 


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 4, tzinfo=local_tz),
    'retries': 0
}


with DAG(DAG_ID, schedule_interval=None, catchup=False, default_args=default_args) as dag:

    nos_gcs_archival_deletion_cf_triggercloudfunction = CloudDataTransferServiceGCSToGCSOperator(
        task_id="nos_gcs_archival_deletion_cf_triggercloudfunction",
        source_bucket="gk1v-prod-edwdo-0-usre-nos-inbound",
        source_path="email_suppress/",
        destination_bucket="gk1v-prod-edwdo-0-usre-nos-inbound",
        destination_path="archive/email_suppress/",
        project_id=PROJECT_ID,
        gcp_conn_id=BQ_CONN_ID,
        transfer_options={
            "deleteObjectsFromSourceAfterTransfer": True,
            "overwriteObjectsAlreadyExistingInSink": True,
        },
        description="Archive files from data to archive location",
        wait=True,
        timeout=7200,
    )
nos_gcs_archival_deletion_cf_triggercloudfunction