import os
import zipfile
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import DagModel, DagRun, TaskInstance
from airflow.utils.session import provide_session
from airflow.utils import timezone
from sqlalchemy import desc
import pandas as pd
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

ENV_NAME="vz-it-pr-wdwg-aidcom-0-cc-edw-1(PROD)"

def generate_html_table(df, table_title, header_bg="#1976d2"):
    if df.empty:
        return f"<h3>{table_title}</h3><p>No data available.</p>"
    df=df.head(100)
    rows_html = ""
    for idx, (_, row) in enumerate(df.iterrows()):
        bg_color = "#f9f9f9" if idx % 2 == 0 else "white"
        rows_html += "<tr style='background:{};'>".format(bg_color) + "".join(
            f"<td style='border:1px solid #ccc; padding:8px;'>{row[col]}</td>" for col in df.columns
        ) + "</tr>"

    header_html = "<tr>" + "".join(
        f"<th style='border:1px solid #1976d2; padding:10px; background:{header_bg}; color:white; text-align:left;'>{col}</th>"
        for col in df.columns
    ) + "</tr>"

    table_html = f"""
    <h3 style="color:#1976d2; border-bottom: 2px solid #1976d2; padding-bottom: 5px;">{table_title}</h3>
    <table style='border-collapse: collapse; border:1px solid #ccc; width: 100%; margin-bottom:30px; font-family: Arial, sans-serif; font-size: 14px;'>
        {header_html}
        {rows_html}
    </table>
    """
    return table_html


def create_html_bar_chart(data_dict, title, max_width=400, bar_height=22):
    if not data_dict:
        return f"<p>No data for {title}</p>"

    max_value = max(data_dict.values())
    bars_html = ""

    base_colors = {
        "upstream_failed": "#fbc02d",  # yellow
        "failed": "#d32f2f",  # red
        "success": "#388e3c",  # green
        "running": "#1976d2",  # blue
        "queued": "#7b1fa2",  # purple
        "skipped": "#757575",  # grey
    }

    default_colors = ["#1976d2", "#d32f2f", "#388e3c", "#fbc02d", "#7b1fa2", "#00796b"]

    for i, (label, value) in enumerate(data_dict.items()):
        width = int((value / max_value) * max_width) if max_value > 0 else 0
        color = base_colors.get(label.lower(), default_colors[i % len(default_colors)])
        bars_html += f"""
        <div style="margin-bottom:6px; display:flex; align-items:center;">
            <span style="display:inline-block; width:140px; font-weight:bold; color:#333; font-family: Arial, sans-serif;">{label} ({value})</span>
            <div style="background:{color}; width:{width}px; height:{bar_height}px; border-radius: 4px;"></div>
        </div>
        """

    html = f"""
    <h4 style="color:#1976d2; font-family: Arial, sans-serif; border-bottom: 1px solid #ccc; padding-bottom: 4px; margin-bottom: 12px;">{title}</h4>
    <div>{bars_html}</div>
    """
    return html


def generate_stats_html(dag_runs_df, task_instances_df):
    total_dag_runs = len(dag_runs_df)
    total_unique_dag_ids_len=dag_runs_df['dag_id'].nunique()
    dag_run_states = dag_runs_df['state'].value_counts().to_dict() if not dag_runs_df.empty else {}

    total_tasks = len(task_instances_df)
    task_states = task_instances_df['state'].value_counts().to_dict() if not task_instances_df.empty else {}

    dag_run_details = f"""
    <p><strong>Total DAGs:</strong> {total_unique_dag_ids_len}</p>
    <p><strong>Total DAG runs:</strong> {total_dag_runs}</p>
    <ul>
        {''.join(f"<li>{state}: {count}</li>" for state, count in dag_run_states.items())}
    </ul>
    """

    task_instance_details = f"""
    <p><strong>Total Task Instances:</strong> {total_tasks}</p>
    <ul>
        {''.join(f"<li>{state}: {count}</li>" for state, count in task_states.items())}
    </ul>
    """

    dag_run_chart_html = create_html_bar_chart(dag_run_states, "DAG Runs State Distribution")
    task_state_chart_html = create_html_bar_chart(task_states, "Task Instances State Distribution")

    stats_html = f"""
    <div style="font-family: Arial, sans-serif; max-width: 700px;">
        <h2 style="color:#0d47a1; border-bottom: 3px solid #0d47a1; padding-bottom: 6px;">📊 Summary Statistics</h2>

        <section style="margin-bottom: 30px;">
            <h3 style="color:#1565c0;">DAG Runs Today</h3>
            {dag_run_details}
            {dag_run_chart_html}
        </section>

        <section style="margin-bottom: 30px;">
            <h3 style="color:#1565c0;">Task Instances Today</h3>
            {task_instance_details}
            {task_state_chart_html}
        </section>

        <hr style="border: none; border-top: 1px solid #ccc; margin-top: 40px;" />
    </div>
    """
    return stats_html


@provide_session
def generate_daily_report(session=None, **kwargs):
    dagrun_conf=kwargs.get('dag_run').conf if kwargs.get('dag_run') else {}
    EMAIL_TO=dagrun_conf.get("mail_id","sachin.jakhar@onixnet.com,varunesh.k@onixnet.com,lalit.sakure@onixnet.com,lalit.sakure@verizon.com,TD2BQArch-TPM@datametica.com,td2bq_rm_deploy@onixnet.com,abhijit.mahajan@onixnet.com,abhijit.mahajan@verizon.com")
    today_utc = timezone.utcnow().date()
    dag_run_conf = kwargs.get('dag_run').conf if kwargs.get('dag_run') else {}
    dag_ids_param = dag_run_conf.get("dag_ids") if dag_run_conf else None
    hrs=dagrun_conf.get("hours","24")
    hrs=int(hrs)

    # Normalize dag_ids_param to a list if it is string comma separated
    if dag_ids_param:
        if isinstance(dag_ids_param, str):
            dag_ids = [d.strip() for d in dag_ids_param.split(",") if d.strip()]
        elif isinstance(dag_ids_param, list):
            dag_ids = dag_ids_param
        else:
            dag_ids = []
    else:
        dag_ids = None  # means all dags

    dag_runs_data = []
    task_instances_data = []

    all_dags = session.query(DagModel).all()
    active_dags = [dag for dag in all_dags if dag.is_active]
    paused_dags = [dag for dag in all_dags if not dag.is_active]

    # Filter active dags if dag_ids specified
    if dag_ids:
        active_dags = [dag for dag in active_dags if dag.dag_id in dag_ids]
    ct = timezone.utcnow() - timedelta(hours=hrs)

    for dag in active_dags:
        dag_runs = (
            session.query(DagRun)
            .filter(DagRun.dag_id == dag.dag_id)
            .order_by(desc(DagRun.execution_date))
            .all()
        )
        for dr in dag_runs:
            # if dr.execution_date and dr.execution_date.date() == today_utc:
            if dr.start_date and dr.start_date >= ct:
                dag_runs_data.append({
                    "dag_id": dr.dag_id,
                    "run_id": dr.run_id,
                    "execution_date": dr.execution_date.strftime('%Y-%m-%d %H:%M'),
                    "start_date": dr.start_date.strftime('%Y-%m-%d %H:%M') if dr.start_date else None,
                    "end_date": dr.end_date.strftime('%Y-%m-%d %H:%M') if dr.end_date else None,
                    "state": dr.state
                })

                task_instances = (
                    session.query(TaskInstance)
                    .filter(TaskInstance.dag_id == dr.dag_id, TaskInstance.run_id == dr.run_id)
                    .all()
                )
                for ti in task_instances:
                    task_instances_data.append({
                        "dag_id": ti.dag_id,
                        "run_id": ti.run_id,
                        "task_id": ti.task_id,
                        "start_date": ti.start_date.strftime('%Y-%m-%d %H:%M') if ti.start_date else None,
                        "end_date": ti.end_date.strftime('%Y-%m-%d %H:%M') if ti.end_date else None,
                        "state": ti.state
                    })

    def dags_to_dict_list(dags_list):
        return [
            {
                "dag_id": d.dag_id,
                "is_active": d.is_active,
                "owners": d.owners,
                "schedule_interval": str(d.schedule_interval),
                "description": d.description if hasattr(d, 'description') else ""
            }
            for d in dags_list
        ]

    # For CSV attachments: if dag_ids filter applied, restrict CSVs to those dags only
    if dag_ids:
        filtered_all_dags = [d for d in all_dags if d.dag_id in dag_ids]
        filtered_active_dags = [d for d in active_dags if d.dag_id in dag_ids]
        filtered_paused_dags = [d for d in paused_dags if d.dag_id in dag_ids]
    else:
        filtered_all_dags = all_dags
        filtered_active_dags = active_dags
        filtered_paused_dags = paused_dags

    df_all_dags = pd.DataFrame(dags_to_dict_list(filtered_all_dags))
    df_active_dags = pd.DataFrame(dags_to_dict_list(filtered_active_dags))
    df_paused_dags = pd.DataFrame(dags_to_dict_list(filtered_paused_dags))

    if not dag_runs_data:
        dag_runs_data.append({"dag_id": "N/A", "run_id": "N/A", "execution_date": "N/A",
                              "start_date": "N/A", "end_date": "N/A", "state": "N/A"})
    if not task_instances_data:
        task_instances_data.append({"dag_id": "N/A", "run_id": "N/A", "task_id": "N/A",
                                    "start_date": "N/A", "end_date": "N/A", "state": "N/A"})

    df_dag_runs = pd.DataFrame(dag_runs_data)
    df_task_instances = pd.DataFrame(task_instances_data)

    stats_html = generate_stats_html(df_dag_runs, df_task_instances)
    dag_runs_html = generate_html_table(df_dag_runs, "Today's DAG Runs")
    task_instances_html = generate_html_table(df_task_instances, "Today's Task Instances")
    heading=f"📋 Airflow Daily Report - {timezone.utcnow().date()} :{ENV_NAME}"
    email_html_content = f"""
    <html>
      <body style="font-family: Arial, sans-serif; background:#f5f7fa; color:#333; padding:20px;">
        <div style="max-width: 900px; margin: auto; background: white; padding: 25px; border-radius: 8px; box-shadow: 0 0 12px rgba(0,0,0,0.1);">
          <h1 style="color:#0d47a1; border-bottom: 4px solid #0d47a1; padding-bottom: 8px; margin-bottom: 15px;">{heading}</h1>
          <p>This is the Airflow DAG run report for the <b>last 24 hours</b>.</p>
          <p>The report below displays the first few DAG runs. For the complete details, please refer to the <b>dag_runs.csv</b> and <b>task_instances.csv</b> CSV files in the attached ZIP.  </p>
          {stats_html}
          {dag_runs_html}
          {task_instances_html}
          <p style="font-size:14px; color:#666;">Find attached CSV files for full DAG lists and detailed data.</p>
        </div>
      </body>
    </html>
    """

    # Prepare CSV files and zip them
    tmp_dir = "/tmp/airflow_report"
    os.makedirs(tmp_dir, exist_ok=True)

    files = {
        "dag_runs.csv": df_dag_runs,
        "task_instances.csv": df_task_instances,
        "all_dags.csv": df_all_dags,
        "active_dags.csv": df_active_dags,
        "paused_dags.csv": df_paused_dags,
    }

    csv_paths = []
    for fname, df in files.items():
        path = os.path.join(tmp_dir, fname)
        df.to_csv(path, index=False)
        csv_paths.append(path)

    zip_path = "/tmp/airflow_report.zip"
    with zipfile.ZipFile(zip_path, 'w') as z:
        for file in csv_paths:
            z.write(file, arcname=os.path.basename(file))

    ti = kwargs.get('ti')
    if ti:
        ti.xcom_push(key='email_val', value=EMAIL_TO)
        ti.xcom_push(key='email_html', value=email_html_content)
        ti.xcom_push(key='file_path', value=zip_path)


@provide_session
def generate_long_running_report(session=None, **kwargs):
    dagrun_conf=kwargs.get('dag_run').conf if kwargs.get('dag_run') else {}
    EMAIL_TO=dagrun_conf.get("mail_id","sachin.jakhar@onixnet.com,varunesh.k@onixnet.com,lalit.sakure@onixnet.com,lalit.sakure@verizon.com,TD2BQArch-TPM@datametica.com,td2bq_rm_deploy@onixnet.com,abhijit.mahajan@onixnet.com,abhijit.mahajan@verizon.com")
    now_utc = timezone.utcnow()
    cutoff_time = now_utc - timedelta(days=7)

    dag_run_conf = kwargs.get('dag_run').conf if kwargs.get('dag_run') else {}
    dag_ids_param = dag_run_conf.get("dag_ids") if dag_run_conf else None

    # Normalize dag_ids_param to list
    if dag_ids_param:
        if isinstance(dag_ids_param, str):
            dag_ids = [d.strip() for d in dag_ids_param.split(",") if d.strip()]
        elif isinstance(dag_ids_param, list):
            dag_ids = dag_ids_param
        else:
            dag_ids = []
    else:
        dag_ids = None  # means all dags

    # Query long running DAG runs, optionally filtered by dag_ids
    query = session.query(DagRun).join(DagModel,DagRun.dag_id==DagModel.dag_id).filter(
        DagRun.start_date <= cutoff_time,
        DagRun.state == 'running',
        DagModel.is_active==True,
        DagModel.is_paused==False
    )
    if dag_ids:
        query = query.filter(DagRun.dag_id.in_(dag_ids))
    long_running_dag_runs = query.order_by(DagRun.start_date.asc()).all()

    dag_runs_data = []
    for dr in long_running_dag_runs:
        dag_runs_data.append({
            "dag_id": dr.dag_id,
            "run_id": dr.run_id,
            "execution_date": dr.execution_date.strftime('%Y-%m-%d %H:%M') if dr.execution_date else None,
            "start_date": dr.start_date.strftime('%Y-%m-%d %H:%M') if dr.start_date else None,
            "state": dr.state
        })

    # Query long running task instances optionally filtered by dag_ids
    task_query = session.query(TaskInstance).filter(
        TaskInstance.start_date <= cutoff_time,
        TaskInstance.state == 'running'
    )
    if dag_ids:
        task_query = task_query.filter(TaskInstance.dag_id.in_(dag_ids))
    long_running_tasks = task_query.order_by(TaskInstance.start_date.asc()).all()

    task_instances_data = []
    for ti in long_running_tasks:
        task_instances_data.append({
            "dag_id": ti.dag_id,
            "run_id": ti.run_id,
            "task_id": ti.task_id,
            "start_date": ti.start_date.strftime('%Y-%m-%d %H:%M') if ti.start_date else None,
            "state": ti.state
        })

    df_dag_runs = pd.DataFrame(dag_runs_data)
    df_task_instances = pd.DataFrame(task_instances_data)

    dag_runs_html = generate_html_table(df_dag_runs, "Long Running DAG Runs (>7 days, still running)", header_bg="#d9534f")
    task_instances_html = generate_html_table(df_task_instances, "Long Running Task Instances (>7 days, still running)", header_bg="#d9534f")
    heading=f"🚨 Airflow Long Running DAGs & Tasks Report - {timezone.utcnow().date()} :{ENV_NAME}"
    email_html_content = f"""
    <html>
      <body style="font-family: Arial, sans-serif; background:#fff3e0; color:#333; padding:20px;">
        <div style="max-width: 900px; margin: auto; background: #fff8e1; padding: 25px; border-radius: 8px; box-shadow: 0 0 12px rgba(251, 192, 45, 0.3);">
          <h1 style="color:#ef6c00; border-bottom: 4px solid #ef6c00; padding-bottom: 8px; margin-bottom: 30px;"> {heading}</h1>
          {dag_runs_html}
          {task_instances_html}
          <p style="font-size:14px; color:#666;">Please investigate these long-running executions.</p>
        </div>
      </body>
    </html>
    """

    ti = kwargs.get('ti')
    if ti:
        ti.xcom_push(key='email_val', value=EMAIL_TO)
        ti.xcom_push(key='email_html', value=email_html_content)


with DAG(
    dag_id="daily_airflow_monitoring_report_with_custom_date",
    default_args=default_args,
    description="Send daily and long-running DAGs/tasks reports with optional filtering by dag_ids",
    schedule_interval="30 1 * * *", #ist 7 am
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["report", "combined"]
) as dag:

    daily_report = PythonOperator(
        task_id="generate_daily_report",
        python_callable=generate_daily_report,
        provide_context=True,
    )

    send_daily_email = EmailOperator(
        task_id="send_daily_report_email",
        to="{{ ti.xcom_pull(task_ids='generate_daily_report', key='email_val') }}",
        subject=f"Airflow Daily DAGs & Task Instances Report {timezone.utcnow().date()} :{ENV_NAME} ",
        html_content="{{ ti.xcom_pull(task_ids='generate_daily_report', key='email_html') }}",
        files=["{{ ti.xcom_pull(task_ids='generate_daily_report', key='file_path') }}"],
    )

    long_running_report = PythonOperator(
        task_id="generate_long_running_report",
        python_callable=generate_long_running_report,
        provide_context=True,
    )

    send_long_running_email = EmailOperator(
        task_id="send_long_running_report_email",
        to="{{ ti.xcom_pull(task_ids='generate_long_running_report', key='email_val') }}",
        subject=f"Airflow Long Running DAGs & Tasks Report {timezone.utcnow().date()} :{ENV_NAME}",
        html_content="{{ ti.xcom_pull(task_ids='generate_long_running_report', key='email_html') }}",
    )

    daily_report >> send_daily_email >> long_running_report >> send_long_running_email


