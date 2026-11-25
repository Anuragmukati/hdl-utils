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

#Tables to export
#TABLES_TO_EXPORT = [
#    {"table_name": "lineitem", "select_stmt": "SELECT * FROM tpch.lineitem;"},
#    {"table_name": "biglineitem", "select_stmt": "SELECT * FROM tpch.biglineitem;"},
#    {"table_name": "nation", "select_stmt": "SELECT * FROM tpch.nation;"},
#]
TABLES_TO_EXPORT = [
    {"table_name":"DCX_AEM_FORMS", "select_stmt":"select * from edw_sum.dcx_aem_forms;"},
    {"table_name":"DCX_METRICS", "select_stmt":"select * from edw_sum.dcx_metrics;"},
]

#SELECT_STATEMENT = "SELECT * FROM udm_prd_tbls.pos_install_loan;"

#gcs details
GCS_BUCKET = "gk1v-prod-edwpr-0-usre-inbound"
GCS_PREFIX = "td2bq"
GCS_OBJECT_NAME = "pos.csv"
GCS_MAX_OBJECT_SIZE = "500M"
GCS_CONNECTION_COUNT = "8"
GCS_BUFFER_COUNT=2 * int(GCS_CONNECTION_COUNT)

NUM_READ_INSTANCES = 3
NUM_WRITE_INSTANCES = 3


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
    key="GK1V_GCS_ACCESS_KEY_ID",
)

GCS_SECRET_ACCESS_KEY = Secret(
    deploy_type="env",
    deploy_target="GCS_SECRET_ACCESS_KEY",
    secret="tpt-secrets",
    key="GK1V_GCS_SECRET_ACCESS_KEY",
)

def create_kpo_args():
    kpo_args=[]
    for table in TABLES_TO_EXPORT:
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
                jobvar_selectstmt='{table.get('select_stmt')}', \
                jobvar_accessmoduleinitstr='\
                Bucket={GCS_BUCKET} \
                Prefix={GCS_PREFIX}/table_name={table.get('table_name')}/ \
                Object={table.get('table_name')} \
                MaxObjectSize={GCS_MAX_OBJECT_SIZE} \
                ConnectionCount={GCS_CONNECTION_COUNT}'" && \
                echo \"{{ "table_name": \"$TABLE_NAME\" }}\" > /airflow/xcom/return.json
            """,
        ]
        env_vars = {
            "AIRFLOW_RETRY_NUMBER": "{{ task_instance.try_number }}",
            "TABLE_NAME": table.get("table_name"),
        }

        kpo_args.append({"arguments": arguments, "env_vars": env_vars})

    return kpo_args

def read_export_tpt():
    with open("/home/airflow/gcs/dags/vz-it-gk1v-edwpr-0/export.tpt", "r") as f:
        return f.read().replace("$", r"\$")


with models.DAG(
        dag_id="wrapper_td2bq_end_to_end_prod",
        schedule_interval=None,
        start_date=airflow.utils.dates.days_ago(1),
        max_active_tasks=10,
        default_args={
            "retries": 0,
            "retry_delay": datetime.timedelta(seconds=10),
        },
) as dag:

    tpt = KubernetesPodOperator.partial(
        task_id="tpt",
        name="bq-dts-hist-load",
        cmds=["bash"],
        container_resources=k8s.V1ResourceRequirements(
            requests={
                "cpu": "500m",
                "memory": "1024Mi",
            },
            limits={
                "cpu": "2000m",
                "memory": "4096Mi",
            },
        ),
        log_events_on_failure=True,
        do_xcom_push=True,
        termination_grace_period=60,
        namespace="composer-user-workloads",
        secrets=[TERADATA_PASSWORD, GCS_ACCESS_KEY, GCS_SECRET_ACCESS_KEY],
        image="teradata/tpt:latest",
        config_file="/home/airflow/composer_kube_config",
        kubernetes_conn_id="kubernetes_default",
    ).expand_kwargs(create_kpo_args())

    load_to_bq = GCSToBigQueryOperator.partial(
        task_id='load_to_bq',
        bucket='gk1v-prod-edwpr-0-usre-inbound',
        gcp_conn_id='sa-vz-it-gk1v-edwpr-0-app',
        field_delimiter='|',
        autodetect=True,
        skip_leading_rows=1,
        create_disposition='CREATE_IF_NEEDED',
        ).expand_kwargs(tpt.output.map(lambda tpt_output: {
            "source_objects": f"{GCS_PREFIX}/table_name={tpt_output.get('table_name')}/*.csv",
            "destination_project_dataset_table": f"vz-it-pr-gk1v-edwpr-0.udm_prdstg_tbls.{tpt_output.get('table_name')}"
            } ))
    
    tpt >> load_to_bq



