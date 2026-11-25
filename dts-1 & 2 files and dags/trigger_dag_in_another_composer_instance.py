from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.cloud_composer import (
    CloudComposerRunAirflowCLICommandOperator,
)
from airflow.providers.google.cloud.sensors.cloud_composer import CloudComposerDAGRunSensor

DAG_ID = "trigger_dag_in_another_composer_instance"

TARGET_PROJECT_ID = "vz-it-pr-wdwg-aidcom-0"
TARGET_REGION = "us-east4"
TARGET_ENV_ID = "vz-it-pr-wdwg-aidcom-0-cc-edw-dts-2"

TARGET_DAG = "dg_adhoc_csv_fpardo_ple"
COMMAND = f"dags trigger -- {TARGET_DAG}"

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "composer"],
) as dag:

    run_airflow_cli_cmd = CloudComposerRunAirflowCLICommandOperator(
        task_id="run_airflow_cli_cmd",
        project_id=TARGET_PROJECT_ID,
        environment_id=TARGET_ENV_ID,
        region=TARGET_REGION,
        command=COMMAND,
        # You can run this operator in the deferrable mode:
        # deferrable=True
    )

    dag_run_sensor = CloudComposerDAGRunSensor(
        task_id="dag_run_sensor",
        project_id=TARGET_PROJECT_ID,
        environment_id=TARGET_ENV_ID,
        region=TARGET_REGION,
        composer_dag_id=TARGET_DAG,
        allowed_states=["success"],
        execution_range=timedelta(minutes=5),
        # You can run this sensor in the deferrable mode:
        # deferrable=True
    )

    run_airflow_cli_cmd >> dag_run_sensor
