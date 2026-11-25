import os
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils import timezone
from airflow.models import DagRun, DagModel
from airflow.utils.session import provide_session

ENV_NAME = "vz-it-pr-wdwg-aidcom-0-cc-edw-wls-1"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

@provide_session
def generate_last_31_days_status(session=None, **kwargs):
    # 1. Last 31 days
    end_date = timezone.utcnow().date()
    start_date = end_date - timedelta(days=44)
    dates_31 = [start_date + timedelta(days=i) for i in range(45)]
    date_strs = [d.strftime('%Y-%m-%d') for d in dates_31]
    dagrun_conf=kwargs.get('dag_run').conf if kwargs.get('dag_run') else {}
    EMAIL_TO=dagrun_conf.get("mail_id","lalit.sakure@onixnet.com,abhijit.mahajan@onixnet.com,nitesh.verma@verizon.com,jignesh.dhandhukia@verizon.com,td2bq_rm_deploy@onixnet.com")
    date_strs=date_strs[::-1]



    # 2. Fetch all DAG runs from session
    all_runs = session.query(DagRun).all()  # fetch all
    # filter by last 31 days in Python
    filtered_runs = [
        dr for dr in all_runs
        if dr.start_date and start_date <= dr.start_date.date() <= end_date
    ]
    # optional: sort by start_date ascending
    filtered_runs.sort(key=lambda x: x.start_date)

    # 3. Convert to DataFrame
    all_runs_data = []
    for dr in filtered_runs:
        run_date_str = dr.start_date.date().strftime('%Y-%m-%d')
        if dr.run_id:
            mode=dr.run_id
            mode=mode.split("_")[0]
        else:
            mode=""
        all_runs_data.append({
            "dag_id": dr.dag_id,
            "run_date": run_date_str,
            "run_state": f"{mode}:{dr.state}"
        })

    df_all_runs = pd.DataFrame(all_runs_data)

    # 4. Pivot
    if not df_all_runs.empty:
        df_last_31_days = df_all_runs.pivot_table(
            index='dag_id',
            columns='run_date',
            values='run_state',
            aggfunc='first'
        )
    else:
        df_last_31_days = pd.DataFrame(columns=date_strs)

    # 5. Ensure all 31 date columns exist
    for date in date_strs:
        if date not in df_last_31_days.columns:
            df_last_31_days[date] = 'no_run'

    # 6. Add DAG schedule and composer info
    all_dags = session.query(DagModel).all()
    dag_info = {d.dag_id: {"schedule_interval": str(d.schedule_interval)} for d in all_dags}

    df_last_31_days = df_last_31_days.reset_index()
    df_last_31_days["dag_schedule"] = df_last_31_days["dag_id"].apply(lambda x: dag_info.get(x, {}).get("schedule_interval", "N/A"))
    df_last_31_days["composer"] = ENV_NAME

    # 7. Reorder columns
    cols = ["dag_id", "dag_schedule", "composer"] + date_strs
    df_last_31_days = df_last_31_days[cols]

    # 8. Save CSV
    tmp_dir = "/tmp/airflow_report"
    os.makedirs(tmp_dir, exist_ok=True)
    csv_path = os.path.join(tmp_dir, "dags_last_31_days_status.csv")
    df_last_31_days.to_csv(csv_path, index=False)

    # Push to XCom for email

    EmailOperator(
        task_id="send_last_31_days_report_email",
        to=EMAIL_TO,
        subject=f"{ENV_NAME}-Airflow Last 31 Days DAG Status Report - {timezone.utcnow().date()}",
        html_content=f"""
            <p>Please find attached the DAG status report for the last 31 days.</p>
            <p>Composer: {ENV_NAME}</p>
        """,
        files=[csv_path],
    ).execute(context=kwargs)


with DAG(
    dag_id="dag_airflow_monitoring_report_45_consolidated_days",
    default_args=default_args,
    description="Generate last 31 days DAG status report and send via email",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["report"]
) as dag:

    generate_report_task = PythonOperator(
        task_id="generate_last_45_days_status",
        python_callable=generate_last_31_days_status,
        provide_context=True,
    )
    generate_report_task
