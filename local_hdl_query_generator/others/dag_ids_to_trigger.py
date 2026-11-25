from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.decorators import task
from airflow.models.param import Param # Optional but recommended for UI form

# Define the DAG ID list key in the config
DAG_LIST_KEY = "dag_ids_to_trigger"

with DAG(
    dag_id="parent_trigger_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["trigger"],
    # Define an example parameter for the UI trigger form
    params={
        DAG_LIST_KEY: Param(
            ["example_dag_1", "example_dag_2"], # Default list
            type="array",
            title="DAG IDs to Trigger",
            description="A list of DAG IDs that the parent DAG should trigger.",
        )
    }
) as dag:
    # ----------------------------------------------------
    # 1. Fetch the list of DAG IDs from the dag_run.conf
    # ----------------------------------------------------
    @task
    def get_dag_ids_from_config(**kwargs):
        """Pulls the list of DAG IDs from the dag_run.conf (or params)"""
        # Access dag_run.conf (which includes params) and get the list.
        dag_ids = kwargs["dag_run"].conf.get(DAG_LIST_KEY)
        if not dag_ids:
            # Fallback to the default if nothing is passed, or raise an error
            raise ValueError(f"'{DAG_LIST_KEY}' must be provided in the DAG run configuration.")
        return dag_ids # Automatically pushed to XCom

    dag_ids_list = get_dag_ids_from_config()

    # ----------------------------------------------------
    # 2. Dynamically map the TriggerDagRunOperator
    # ----------------------------------------------------
    trigger_downstream_dags = TriggerDagRunOperator.partial(
        task_id="trigger_child_dag",
        # Pass an empty dictionary for conf if no config is needed in the child DAGs
        conf={}, 
        # Optional: wait for each child DAG to complete before the next step
        wait_for_completion=False, 
    ).expand(
        # The 'trigger_dag_id' parameter is dynamically set for each item
        trigger_dag_id=dag_ids_list # Takes the XCom value from the upstream task
    )

    # Set the dependency
    dag_ids_list >> trigger_downstream_dags
