from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from airflow.models import DagBag

def list_dags_and_tasks(**kwargs):
    dagbag = DagBag()
    result = []
    for dag_id, dag in dagbag.dags.items():
        for task_id, task in dag.task_dict.items():
            result.append({
                "dag_id": dag_id,
                "task_id": task.task_id,
                "operator": task.__class__.__name__,
            })

    html_table = """<h3>DAGs and Tasks Report</h3>
    <table border="1" cellspacing="0" cellpadding="4">
    <tr><th>DAG ID</th><th>Task ID</th><th>Operator</th></tr>"""
    for r in result:
        html_table += f"<tr><td>{r['dag_id']}</td><td>{r['task_id']}</td><td>{r['operator']}</td></tr>"
    html_table += "</table>"

    return html_table

with DAG(
    dag_id="list_dags_and_tasks_mail",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["utility"],
) as dag:

    list_dags = PythonOperator(
        task_id="list_dags",
        python_callable=list_dags_and_tasks,
    )

    send_email = EmailOperator(
        task_id="send_email",
        to="{{ dag_run.conf['email'] }}",   # take email from dag_run.conf
        subject="Airflow DAGs & Tasks Report",
        html_content="{{ ti.xcom_pull(task_ids='list_dags') }}",
    )

    list_dags >> send_email

