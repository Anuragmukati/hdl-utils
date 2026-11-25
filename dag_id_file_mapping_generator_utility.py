
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from airflow.models.serialized_dag import SerializedDagModel

default_args = {
    'start_date': days_ago(1),
}

def generate_dag_details(**context):
    all_dags = SerializedDagModel.read_all_dags()
    lines = []

    for dag in all_dags:
        details = {
            "DAG ID": dag.dag_id,
            "File Location": dag.fileloc,
            "Schedule Interval": str(dag.schedule_interval),
            "Tags": ", ".join(dag.tags or []),
            "Is Paused": str(dag.is_paused),
            "Description": dag.description or "",
        }

        entry = "\n".join([f"{k}: {v}" for k, v in details.items()])
        lines.append(entry + "\n" + "-"*50)

    full_output = "\n\n".join(lines)
    context['ti'].xcom_push(key='dag_details', value=full_output)

def get_email(**context):
    email = context['dag_run'].conf.get('email')
    if not email:
        raise ValueError("Email address not provided in dag_run.conf['email']")
    context['ti'].xcom_push(key='email', value=email)

with DAG(
    dag_id='dag_id_file_mapping_generator_utility',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['utility', 'metadata'],
) as dag:

    gather_dag_info = PythonOperator(
        task_id='generate_dag_details',
        python_callable=generate_dag_details,
        provide_context=True
    )

    extract_email = PythonOperator(
        task_id='get_email',
        python_callable=get_email,
        provide_context=True
    )

    send_email = EmailOperator(
        task_id='send_email',
        to="{{ ti.xcom_pull(task_ids='get_email', key='email') }}",
        subject='Airflow DAG Metadata Report (Serialized)',
        html_content="<pre>{{ ti.xcom_pull(task_ids='generate_dag_details', key='dag_details') }}</pre>"
    )

    [gather_dag_info, extract_email] >> send_email

