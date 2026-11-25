import os
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils import timezone
from airflow.models import DagRun, DagModel
from airflow.utils.session import provide_session
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.models.dag import get_last_dagrun
from google.cloud import bigquery


ENV_NAME = "dts-2"  # set composer/env name
BQ_CONN_ID = "sa-vz-it-gk1v-edwdo-0-app"  # Airflow connection to GCP
BQ_PROJECT_ID = "vz-it-pr-gk1v-edwdo-0"
BQ_DATASET = "edw_data_migration_opsstg_tbls"
BQ_TABLE = f"{BQ_PROJECT_ID}.{BQ_DATASET}.airflow_dags_info_"+ ENV_NAME.replace("-","_") # Target table for second sheet
BQ_RUN_TABLE = f"{BQ_PROJECT_ID}.{BQ_DATASET}.airflow_dagrun_info_"+ ENV_NAME.replace("-","_") # Target table for second sheet

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

import re
from typing import List, Tuple, Optional

MONTH_NAMES = {
    "1": "January", "2": "February", "3": "March", "4": "April",
    "5": "May", "6": "June", "7": "July", "8": "August",
    "9": "September", "10": "October", "11": "November", "12": "December",
    "JAN": "January", "FEB": "February", "MAR": "March", "APR": "April",
    "MAY": "May", "JUN": "June", "JUL": "July", "AUG": "August",
    "SEP": "September", "OCT": "October", "NOV": "November", "DEC": "December"
}

WEEKDAY_NAMES = {
    "0": "Sunday", "1": "Monday", "2": "Tuesday", "3": "Wednesday",
    "4": "Thursday", "5": "Friday", "6": "Saturday",
    "SUN": "Sunday", "MON": "Monday", "TUE": "Tuesday", "WED": "Wednesday",
    "THU": "Thursday", "FRI": "Friday", "SAT": "Saturday"
}

ALIASES = {
    "@yearly": "0 0 1 1 *",   # once a year at midnight Jan 1
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",  # once a month at midnight on the 1st
    "@weekly": "0 0 * * 0",   # once a week at midnight on Sunday
    "@daily": "0 0 * * *",    # once a day at midnight
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *"    # every hour on the hour
}

_token_re = re.compile(r'^\s*([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s*$')

def _normalize_token(tok: str) -> str:
    """Uppercase month/day names and strip whitespace."""
    tok = tok.strip()
    # convert names to uppercase so mapping keys match (JAN, MON etc.)
    tok = re.sub(r'[A-Za-z]+', lambda m: m.group(0).upper(), tok)
    return tok

def _split_list(token: str) -> List[str]:
    return token.split(',')

def _describe_numeric_token(token: str, names_map: Optional[dict], minv: int, maxv: int, unit_singular: str, unit_plural: str) -> str:
    """
    Handle tokens like '*', '*/n', 'a-b', 'a-b/n', 'x,y,z', 'N'
    names_map: optional mapping to convert numeric or name to readable name (months, weekdays)
    """
    token = _normalize_token(token)
    if token == '*' or token == '?':
        return f"every {unit_plural}"
    # step: */n or a-b/n
    if '/' in token:
        left, step = token.split('/', 1)
        step = step.strip()
        if left == '*' or left == '':
            return f"every {step} {unit_plural}" if step != '1' else f"every {unit_plural}"
        # a-b/n
        if '-' in left:
            a, b = left.split('-', 1)
            a = a.strip(); b = b.strip()
            left_name = names_map.get(a, a) if names_map else a
            right_name = names_map.get(b, b) if names_map else b
            return f"every {step} {unit_plural} from {left_name} through {right_name}"
        # fallback
        return f"every {step} {unit_plural}"
    # range: a-b
    if '-' in token:
        a, b = token.split('-', 1)
        a = a.strip(); b = b.strip()
        a_name = names_map.get(a, a) if names_map else a
        b_name = names_map.get(b, b) if names_map else b
        return f"{unit_singular} from {a_name} through {b_name}"
    # list: x,y,z
    if ',' in token:
        parts = [p.strip() for p in token.split(',')]
        display = []
        for p in parts:
            display.append(names_map.get(p, p) if names_map else p)
        if len(display) == 2:
            return f"{unit_singular}s {display[0]} and {display[1]}"
        return f"{unit_singular}s " + ", ".join(display[:-1]) + f", and {display[-1]}"
    # single value
    if names_map:
        return f"{unit_singular} {names_map.get(token, token)}"
    return f"{unit_singular} {token}"

def _format_time(hour_token: str, minute_token: str) -> str:
    hour_token = _normalize_token(hour_token)
    minute_token = _normalize_token(minute_token)
    # both single numbers
    def is_single(t): return re.fullmatch(r'\d+', t) is not None
    if is_single(hour_token) and is_single(minute_token):
        h = int(hour_token); m = int(minute_token)
        return f"At {h:02d}:{m:02d}"
    # minute single, hour '*'
    if is_single(minute_token) and (hour_token == '*' or hour_token == '?'):
        return f"At minute {int(minute_token)} of every hour"
    # minute '*' and hour single
    if is_single(hour_token) and (minute_token == '*' or minute_token == '?'):
        return f"Every minute during hour {int(hour_token)}"
    # both wildcards
    if (minute_token == '*' or minute_token == '?') and (hour_token == '*' or hour_token == '?'):
        return "Every minute"
    # steps and lists: more generic fallback descriptions
    mdesc = _describe_numeric_token(minute_token, None, 0, 59, "minute", "minutes")
    hdesc = _describe_numeric_token(hour_token, None, 0, 23, "hour", "hours")
    # try to compress common pattern "every N minutes"
    if minute_token.startswith('*/') and (hour_token == '*' or hour_token == '?'):
        step = minute_token.split('/',1)[1]
        return f"Every {step} minutes"
    if hour_token.startswith('*/') and (minute_token == '*' or minute_token == '?'):
        step = hour_token.split('/',1)[1]
        return f"Every {step} hours"
    return f"{mdesc} {hdesc}"

def cron_to_description(expr: str) -> str:
    """
    Convert a 5-field cron expression (or common '@' alias) to a human-friendly English description.
    Supported fields: minute hour day-of-month month day-of-week
    Examples:
      "0 0 * * *" -> "At 00:00, every day"
      "*/15 9-17 * * MON-FRI" -> "Every 15 minutes from 9 through 17, weekdays Monday through Friday"
    """
    if expr in [None,"None"]:
      return "None"
    expr = expr.strip()
    # alias handling
    if expr.lower() in ALIASES:
        expr = ALIASES[expr.lower()]

    m = _token_re.match(expr)
    if not m:
        return "Invalid cron expression (expected 5 fields)."

    minute_tok, hour_tok, dom_tok, month_tok, dow_tok = m.group(1,2,3,4,5)

    # describe time
    time_part = _format_time(hour_tok, minute_tok)

    # day-of-month
    dom_desc = _describe_numeric_token(dom_tok, None, 1, 31, "day", "days")
    if dom_tok == '*' or dom_tok == '?':
        dom_part = "every day"
    else:
        # if both dom and dow specified, we will mention both later
        dom_part = dom_desc

    # month
    month_desc = _describe_numeric_token(month_tok, MONTH_NAMES, 1, 12, "month", "months")
    if month_tok == '*' or month_tok == '?':
        month_part = ""  # don't repeat "every month"
    else:
        # phrasing better if single month
        if re.fullmatch(r'\d+|[A-Z]{3}', _normalize_token(month_tok)):
            # single-month like 3 or MAR
            month_part = month_desc
        else:
            month_part = month_desc

    # day-of-week
    dow_desc = _describe_numeric_token(dow_tok, WEEKDAY_NAMES, 0, 6, "day", "days")
    if dow_tok == '*' or dow_tok == '?':
        dow_part = ""
    else:
        dow_part = dow_desc.replace("day ", "")  # "day Monday" -> "Monday"

    # Combine day-of-month / dow logic:
    # If both are wildcards -> every day
    if (dom_tok == '*' or dom_tok == '?') and (dow_tok == '*' or dow_tok == '?'):
        date_part = "every day"
    else:
        parts = []
        if dom_tok != '*' and dom_tok != '?':
            parts.append(dom_part)
        if dow_tok != '*' and dow_tok != '?':
            parts.append("on " + dow_part)
        if month_part:
            parts.append("in " + month_part.replace("month ", ""))
        date_part = ", ".join(parts)

    # polish small wording issues:
    date_part = date_part.replace("day day", "day").replace("days ", "days ")

    # Final assembly:
    description = f"{time_part}, {date_part}"
    # minor cleaning (avoid awkward commas)
    description = re.sub(r'\s+,', ',', description)
    description = description.replace("every day, in", "every day in")
    return description

@provide_session
def generate_last_45_days_status(session=None, **kwargs):
    # 1. Last 45 days
    end_date = timezone.utcnow().date()
    start_date = end_date - timedelta(days=44)
    dates_45 = [start_date + timedelta(days=i) for i in range(45)]
    date_strs = [d.strftime('%Y-%m-%d') for d in dates_45][::-1]  # latest first

    # email ids
    dagrun_conf = kwargs.get('dag_run').conf if kwargs.get('dag_run') else {}
    EMAIL_TO = dagrun_conf.get("mail_id", "team@example.com")

    # 2. Fetch all DAG runs
    all_runs = session.query(DagRun).all()
    filtered_runs = [
        dr for dr in all_runs
        if dr.end_date and start_date <= dr.end_date.date() <= end_date
    ]
    filtered_runs.sort(key=lambda x: x.end_date or datetime.min)

    # 3. Convert to DataFrame
    all_runs_data = []
    for dr in filtered_runs:
        run_date_str = dr.end_date.date().strftime('%Y-%m-%d')
        start_dt = str(dr.start_date) if dr.start_date else ""
        end_dt = str(dr.end_date) if dr.end_date else ""
        if dr.run_id:
            mode = dr.run_id.split("_")[0]
        else:
            mode = ""
        all_runs_data.append({
            "dag_id": dr.dag_id,
            "run_date": run_date_str,
            "run_state": f"{mode}:{dr.state}({start_dt},{end_dt})"
        })

    df_all_runs = pd.DataFrame(all_runs_data)

    # 4. Pivot
    if not df_all_runs.empty:
        df_last_45_days = df_all_runs.pivot_table(
            index='dag_id',
            columns='run_date',
            values='run_state',
            aggfunc='first'
        )
    else:
        df_last_45_days = pd.DataFrame(columns=date_strs)

    # 5. Ensure all 45 date columns exist
    for date in date_strs:
        if date not in df_last_45_days.columns:
            df_last_45_days[date] = 'no_run'

    # 6. Add DAG schedule and composer info
    all_dags = session.query(DagModel).all()
    dag_info = {
        d.dag_id: {
            "schedule_interval": str(d.schedule_interval),
            "cron_description":str(cron_to_description(str(d.schedule_interval))),
            "is_active": d.is_active,
            "is_paused": d.is_paused,
            "description": d.description or ""
        } for d in all_dags
    }

    df_last_45_days = df_last_45_days.reset_index()
    df_last_45_days["dag_schedule"] = df_last_45_days["dag_id"].apply(lambda x: dag_info.get(x, {}).get("schedule_interval", "N/A"))
    df_last_45_days["cron_description"] = df_last_45_days["dag_id"].apply(lambda x: dag_info.get(x, {}).get("cron_description", None))
    df_last_45_days["composer"] = ENV_NAME
    df_last_45_days["is_active"] = df_last_45_days["dag_id"].apply(lambda x: dag_info.get(x, {}).get("is_active", None))
    df_last_45_days["is_paused"] = df_last_45_days["dag_id"].apply(lambda x: dag_info.get(x, {}).get("is_paused",None))

    # Reorder columns
    cols = ["dag_id", "dag_schedule","cron_description", "is_active", "is_paused", "composer"] + date_strs
    df_last_45_days = df_last_45_days[cols]

    # 7. Prepare second CSV (all DAG info + last_dagrun + current_dagrun + next_dagrun)
    df_dags_info = []
    for d in all_dags:
        # last_dagrun = d.get_last_dagrun(session=session)
        # current_dagrun = d.get_current(session=session)
        last_run = get_last_dagrun(dag_id=d.dag_id,session=session,include_externally_triggered=False)
        try:
          sample={
              "dag_id": d.dag_id,
              "schedule_interval": str(d.schedule_interval),
              "cron_description":str(cron_to_description(str(d.schedule_interval))),
              "is_active": d.is_active,
              "is_paused": d.is_paused,
              "description": d.description or "",
              "last_run_id": str(last_run.run_id) if last_run else None,
              "last_execution_date": str(last_run.execution_date) if last_run else None,
              "last_execution_sate": str(last_run.state) if last_run else None,
              "last_execution_int_start": str(last_run.data_interval_start) if last_run else None,
              "last_execution_int_end": str(last_run.data_interval_end) if last_run else None,
              # "current_dagrun": str(current_dagrun.execution_date) if current_dagrun else None,
              "next_dagrun": str(d.next_dagrun) if d.next_dagrun else None
          }
          df_dags_info.append(sample)
        except Exception as e:
          print(f"boom:{e}")

    df_dags_info = pd.DataFrame(df_dags_info)

    # 8. Save both CSVs
    tmp_dir = "/tmp/airflow_report"
    os.makedirs(tmp_dir, exist_ok=True)

    csv_path1 = os.path.join(tmp_dir, "dags_last_45_days_status.csv")
    csv_path2 = os.path.join(tmp_dir, "all_dags_info.csv")

    df_last_45_days.to_csv(csv_path1, index=False)
    df_dags_info.to_csv(csv_path2, index=False)

    EmailOperator(
        task_id="send_last_45_days_report_email",
        to=EMAIL_TO,
        subject=f"{ENV_NAME}-Airflow Last 45 Days DAG Status Report - {timezone.utcnow().date()}",
        html_content=f"""
            <p>Please find attached the DAG status report for the last 45 days.</p>
            <p>Also attached is the list of all DAGs in the environment.</p>
            <p>Composer: {ENV_NAME}</p>
        """,
        files=[csv_path1, csv_path2],
    ).execute(context=kwargs)

    hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID, use_legacy_sql=False)
    client = hook.get_client(project_id=BQ_PROJECT_ID)
    # Load Sheet 1 → dag_metadata
    job_config1 = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE"
    )
    client.load_table_from_dataframe(df_last_45_days, BQ_TABLE, job_config=job_config1).result()

    # Load Sheet 2 → dag_list
    job_config2 = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE"
    )
    client.load_table_from_dataframe(df_dags_info, BQ_RUN_TABLE, job_config=job_config2).result()


with DAG(
    dag_id="dag_airflow_monitoring_report_45_days",
    default_args=default_args,
    description="Generate last 45 days DAG status report, send via email, and load DAG metadata to BQ",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["report"]
) as dag:

    generate_report_task = PythonOperator(
        task_id="generate_last_45_days_status",
        python_callable=generate_last_45_days_status,
        provide_context=True,
    )

    generate_report_task

