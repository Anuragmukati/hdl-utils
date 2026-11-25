import airflow
import datetime
from airflow import models
from airflow.models.taskinstance import TaskInstance as TI

from airflow.utils.state import State

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


from airflow.decorators import dag, task, task_group
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from kubernetes.client import models as k8s
from airflow.providers.cncf.kubernetes.secret import Secret

#Teradata details
TERADATA_HOSTNAME = "tddp.tdc.vzwcorp.com"
TERADATA_USERNAME = "UDBABQGCPDTS"

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

#gcs details
GCS_BUCKET = "gk1v-prod-edwpr-0-usre-inbound"
GCS_PREFIX = "td2bq"
GCS_OBJECT_NAME = "data.csv"
GCS_MAX_OBJECT_SIZE = "500M"
GCS_CONNECTION_COUNT = "8"
GCS_BUFFER_SIZE = "16M"
GCS_BUFFER_COUNT=2 * int(GCS_CONNECTION_COUNT)


NUM_READ_INSTANCES = 2
NUM_WRITE_INSTANCES = 6


#Tables to export
#TABLES_TO_EXPORT = [
#    {"table_name": "lineitem", "select_stmt": "SELECT * FROM tpch.lineitem;"},
#    {"table_name": "biglineitem", "select_stmt": "SELECT * FROM tpch.biglineitem;"},
#    {"table_name": "nation", "select_stmt": "SELECT * FROM tpch.nation;"},
#]

TABLES_TO_EXPORT = []

def createJsonData(rows):

    LOFtableDict = []
    for row in rows:
        tableDict = {}
        print("Table working on...")
        tableDict['table_name']  = row[1]
        tableDict['select_stmt'] = row[4]
        tableDict['bqDataset'] = row[2]
        
        print(tableDict)
        LOFtableDict.append(tableDict)

    print("List of tables to run for... ")
    print(LOFtableDict)

    return LOFtableDict

def getMetadata():
    print("Getting Metadata...")
    # Function to query BigQuery and retrieve metadata values
    hook = BigQueryHook(gcp_conn_id='sa-vz-it-gk1v-edwpr-0-app',use_legacy_sql=False)
    sql = """
    SELECT
    *
    FROM `vz-it-pr-gk1v-edwpr-0.udm_prdstg_tbls.hist_metadata` where status="INCOMPLETE" and AppName="MFT" limit 5;
    """
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    
    #print(rows)
    
    
    TABLES_TO_EXPORT = createJsonData(rows)

    return TABLES_TO_EXPORT

#TABLES_TO_EXPORT = [
#    {"table_name":"DCX_AEM_FORMS", "select_stmt":"select * from edw_sum.dcx_aem_forms;"},
#    {"table_name":"DCX_METRICS", "select_stmt":"select * from edw_sum.dcx_metrics;"},
#]


#SELECT_STATEMENT = "SELECT * FROM udm_prd_tbls.pos_install_loan;"


@dag(
    dag_id="Wrapper_td2bq_end_to_end_prod_task_groups",
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(1),
    max_active_tasks=4,
    default_args={
        "retries": 1,
        "retry_delay": datetime.timedelta(seconds=10),
    },
    render_template_as_native_obj=True,
)
def dynamic_task_group_tpt():
    def read_export_tpt():
        with open("/home/airflow/gcs/dags/vz-it-gk1v-edwpr-0/export.tpt", "r") as f:
            return f.read().replace("$", r"\$")

    def audit_logging():
        # Insert audit logging logic here
        pass

    @task
    def get_tables_to_export():
        audit_logging()
        TABLES_TO_EXPORT = getMetadata()
        return TABLES_TO_EXPORT

    @task_group
    def extract_table(table):
        @task(multiple_outputs=True)
        def create_kpo_args(table_to_extract):
            arguments = [
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
                    jobvar_selectstmt='{table_to_extract.get('select_stmt')}', \
                    jobvar_accessmoduleinitstr='\
                    Bucket={GCS_BUCKET} \
                    Prefix={GCS_PREFIX}/table_name={table_to_extract.get('table_name')}/try_number=$AIRFLOW_RETRY_NUMBER/ \
                    Object={GCS_OBJECT_NAME} \
                    MaxObjectSize={GCS_MAX_OBJECT_SIZE} \
                    BufferSize={GCS_BUFFER_SIZE} \
                    BufferCount={GCS_BUFFER_COUNT} \
                    ConnectionCount={GCS_CONNECTION_COUNT}'" \
                """,
            ]
            return {"arguments": arguments}

        kpo_args = create_kpo_args(table)

        tpt = KubernetesPodOperator(
            task_id="tpt",
            name="tpt",
            cmds=["bash"],
            container_resources=k8s.V1ResourceRequirements(
                requests={
                    "cpu": "1000m",
                    "memory": "2000Mi",
                },
                limits={
                    "cpu": "2000m",
                    "memory": "4000Mi",
                },
            ),
            # Increase pod startup timeout to 10 minutes
            startup_timeout_seconds=600,
            log_events_on_failure=True,
            namespace="composer-user-workloads",
            secrets=[TERADATA_PASSWORD, GCS_ACCESS_KEY, GCS_SECRET_ACCESS_KEY],
            image="teradata/tpt:latest",
            config_file="/home/airflow/composer_kube_config",
            kubernetes_conn_id="kubernetes_default",
            env_vars={"AIRFLOW_RETRY_NUMBER": "'{{ task_instance.try_number }}'"},
            arguments=kpo_args["arguments"],
        )

        @task(trigger_rule="all_done", multiple_outputs=True)
        def get_tpt_output(tpt_output, **kwargs):
            ti = kwargs['ti']

            print(ti)
            print(ti.dag_id)
            print(tpt_output)
            print(ti.map_index)
            
            TABLES_TO_EXPORT = getMetadata()
            print(TABLES_TO_EXPORT)
            tableName=TABLES_TO_EXPORT[ti.map_index]['table_name']
            #print(TI.get_previous_ti(self,state=State.SUCCESS))

            #prev_ti = TI.get_task_instance(ti.dag_id, ti.run_id, "extract_table.tpt", ti.map_index)


            return {
                "source_objects": f"{GCS_PREFIX}/table_name="+tableName+"/try_number=1/*.csv",
                "destination_project_dataset_table": f'vz-it-pr-gk1v-edwpr-0.udm_prdstg_tbls.'+tableName+'_14thAug'
            }

        gcs_input = get_tpt_output(tpt.output)

        GCSToBigQueryOperator(
            task_id="gcs_to_bq",
            bucket=GCS_BUCKET,
            create_disposition="CREATE_IF_NEEDED",
            source_format="CSV",
            gcp_conn_id='sa-vz-it-gk1v-edwpr-0-app',
            write_disposition="WRITE_TRUNCATE",
            field_delimiter="|",
            autodetect=True,
            skip_leading_rows=1,
            source_objects=gcs_input["source_objects"],
            destination_project_dataset_table=gcs_input[
                "destination_project_dataset_table"
            ],
        ) 

    #extract_table.expand(table=get_tables_to_export())


    @task(trigger_rule="all_done", multiple_outputs=True)
    def update_metadata():
        TABLES_TO_EXPORT = getMetadata()
        
        hook = BigQueryHook(gcp_conn_id='sa-vz-it-gk1v-edwpr-0-app',use_legacy_sql=False)
        
        for i in range(len(TABLES_TO_EXPORT)):
            tableName = TABLES_TO_EXPORT[i]['table_name']
            datasetName = TABLES_TO_EXPORT[i]['bqDataset']
            sql = """
            UPDATE `vz-it-pr-gk1v-edwpr-0.udm_prdstg_tbls.hist_metadata` SET status="COMPLETE" WHERE BQTable='"""+tableName+"""' and BQDataset='"""+datasetName+"""';
            """
            print(sql)
            conn = hook.get_conn()
            cursor = conn.cursor()
            cursor.execute(sql)

    updateTableStatus = update_metadata()

    extract_table.expand(table=get_tables_to_export()) >> updateTableStatus
# Instantiate the DAG
dynamic_task_group_tpt()

