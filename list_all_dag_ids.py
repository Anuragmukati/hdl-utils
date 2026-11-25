from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models.serialized_dag import SerializedDagModel

from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 0,
}

def list_dag_ids():
    dag_ids = [dag.dag_id for dag in SerializedDagModel.read_all_dags()]
    print("Available DAG IDs in the environment:")
    for dag_id in dag_ids:
        print(dag_id)

with DAG(
    dag_id='list_all_dag_ids',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Print all DAG IDs in the Airflow environment',
    tags=['admin', 'debug'],
) as dag:

    list_dags = PythonOperator(
        task_id='print_dag_ids',
        python_callable=list_dag_ids
    )

