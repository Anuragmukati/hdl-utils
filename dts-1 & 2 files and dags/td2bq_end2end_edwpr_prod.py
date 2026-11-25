import airflow
import datetime
from airflow import models
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from kubernetes.client import models as k8s
from airflow.providers.cncf.kubernetes.secret import Secret

#Teradata details
TERADATA_HOSTNAME = "tddp.tdc.vzwcorp.com"
TERADATA_USERNAME = "UDBABQGCPDTS"

SELECT_STATEMENT = "SELECT * FROM udm_prd_tbls.pos_install_loan;"

#gcs details
GCS_BUCKET = "gk1v-prod-edwpr-0-usre-inbound"
GCS_PREFIX = "td2bq"
GCS_OBJECT_NAME = "pos.csv"
GCS_MAX_OBJECT_SIZE = "2000M"
GCS_CONNECTION_COUNT = "16"
GCS_BUFFER_COUNT=2 * int(GCS_CONNECTION_COUNT)

NUM_READ_INSTANCES = 3
NUM_WRITE_INSTANCES = 14


TERADATA_PASSWORD = Secret(
    deploy_type="env",
    deploy_target="TERADATA_PASSWORD",
    secret="tpt-secrets",
    key="TERADATA_PASSWORD",
)

GCS_ACCESS_KEY = Secret(
    deploy_type="env",
    deploy_target="GCS_ACCESS_KEY_ID",
    secret="tpt-secrets",
    key="GCS_ACCESS_KEY_ID",
)

GCS_SECRET_ACCESS_KEY = Secret(
    deploy_type="env",
    deploy_target="GCS_SECRET_ACCESS_KEY",
    secret="tpt-secrets",
    key="GCS_SECRET_ACCESS_KEY",
)



def read_export_tpt():
    with open("/home/airflow/gcs/dags/vz-it-gk1v-edwpr-0/export.tpt", "r") as f:
        return f.read().replace("$", r"\$")


with models.DAG(
        dag_id="td2bq_end_to_end_prod",
        schedule_interval=None,
        start_date=airflow.utils.dates.days_ago(1),
        max_active_tasks=2,
        default_args={
            "retries": 1,
            "retry_delay": datetime.timedelta(seconds=10),
        },
) as dag:
    for x in range(1):
        task_id = f"tpt{x}"
        tpt = KubernetesPodOperator(
            task_id=task_id,
            name="bq-dts-hist-load",
            cmds=["bash"],
            arguments=[
                "-c",
                rf"""
                set -e && \
                echo "{read_export_tpt()}" > export.tpt && \
                more export.tpt && \
                tbuild -f export.tpt -u "\
                  jobvar_tdpid='{TERADATA_HOSTNAME}', \
                  jobvar_username='{TERADATA_USERNAME}', \
                  jobvar_password='$TERADATA_PASSWORD', \
                  jobvar_num_read_instances={NUM_READ_INSTANCES}, \
                  jobvar_num_write_instances={NUM_WRITE_INSTANCES}, \
                  jobvar_selectstmt='{SELECT_STATEMENT}', \
                  jobvar_accessmoduleinitstr='\
                  Bucket={GCS_BUCKET} \
                  Prefix={GCS_PREFIX}/task_id={task_id}/try_number=1/pos_install_loan/ \
                  Object={GCS_OBJECT_NAME} \
                  MaxObjectSize={GCS_MAX_OBJECT_SIZE} \
                  ConnectionCount={GCS_CONNECTION_COUNT}'"
                """,
            ],
            #env_vars={"AIRFLOW_RETRY_NUMBER": "{{ task_instance.try_number }}","GCS_ACCESS_KEY_ID": GCS_ACCESS_KEY_ID,"GCS_SECRET_ACCESS_KEY": GCS_SECRET_ACCESS_KEY},

            container_resources=k8s.V1ResourceRequirements(
                requests={
                    "cpu": "8000m",
                    "memory": "16384Mi",
                },
                limits={
                    "cpu": "8000m",
                    "memory": "16384Mi",
                },
            ),
            log_events_on_failure=True,
            namespace="composer-2-6-6-airflow-2-7-3-c3c3cb26",
            secrets=[TERADATA_PASSWORD, GCS_ACCESS_KEY, GCS_SECRET_ACCESS_KEY],
            image="teradata/tpt:latest",
            config_file="/home/airflow/composer_kube_config",
            kubernetes_conn_id="kubernetes_default",
        )
    load_to_bq = GCSToBigQueryOperator(
        task_id='load_to_bq',
        bucket='gk1v-prod-edwpr-0-usre-inbound',
        source_objects='td2bq/task_id=tpt0/try_number=1/pos_install_loan/pos*.csv',
        destination_project_dataset_table='vz-it-pr-gk1v-edwpr-0.udm_prdstg_tbls.pos_install_loan_22thJuly',
        gcp_conn_id='sa-vz-it-gk1v-edwpr-0-app',
        field_delimiter='|',
        autodetect=True,
        skip_leading_rows=1,
        create_disposition='CREATE_IF_NEEDED',
    )
    tpt >> load_to_bq
