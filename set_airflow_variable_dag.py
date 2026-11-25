from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

def set_airflow_variable(**kwargs):
    # Read variable name and value from dag_run.conf
    variable_name = kwargs['dag_run'].conf.get('variable_name')
    variable_value = kwargs['dag_run'].conf.get('variable_value')

    if not variable_name or not variable_value:
        raise ValueError("Both 'variable_name' and 'variable_value' must be provided.")

    Variable.set(variable_name, variable_value)
    print(f"Airflow variable '{variable_name}' set to '{variable_value}'")

default_args = {
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='set_airflow_variable_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="DAG to set an Airflow variable from runtime config"
) as dag:

    set_variable_task = PythonOperator(
        task_id='set_variable',
        python_callable=set_airflow_variable,
        provide_context=True
    )
# dag run config-{
#     "variable_name": "db_vision5_ftr_raw_tbls",
#     "variable_value": "vision5_ftr_raw_tbls"
# }