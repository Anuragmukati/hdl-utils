from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
from datetime import datetime
import teradatasql

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    dag_id='td_backup_dynamic_multi_table_with_email',
    default_args=default_args,
    description='Backs up fully-qualified Teradata tables to ONX_MIGTMP_TBLS with email report',
    schedule_interval=None,
    catchup=False
)

BACKUP_DB = "ONX_MIGTMP_TBLS"
EMAIL_RECIPIENTS = [
    "atharva.gokhare@verizon.com",
    "anurag.mukati@verizon.com",
    "vaibhav.mittal@verizon.com"
]

def backup_tables(**context):
    conf = context['dag_run'].conf or {}
    tables = conf.get("tables")

    if not tables or not isinstance(tables, list):
        raise ValueError("Please provide a list of tables in 'tables'. Format: ['db1.table1', 'db2.table2']")

    try:
        conn_td = BaseHook.get_connection("UGCPDTSNOS5")
    except Exception as e:
        raise ConnectionError(f"Teradata connection fetch failed: {e}")

    conn = None
    cursor = None
    summary = []

    try:
        conn = teradatasql.connect(
            host='tddp.tdc.vzwcorp.com',
            user=conn_td.login,
            password=conn_td.password
        )
        cursor = conn.cursor()

        for full_name in tables:
            if '.' not in full_name:
                summary.append({
                    "source_db": "-",
                    "source_table": full_name,
                    "backup_db": BACKUP_DB,
                    "backup_table": "-",
                    "row_count": "-",
                    "created_at": "-",
                    "status": "❌ Invalid format"
                })
                continue

            source_db, source_table = full_name.split('.', 1)
            backup_table = f"{source_db}_{source_table}"
            created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            row_count = "N/A"
            status = "✅ Success"

            try:
                # Check source table exists
                check_sql = f"""
                    SELECT COUNT(*) FROM dbc.tables
                    WHERE databasename = '{source_db}' AND tablename = '{source_table.upper()}'
                """
                cursor.execute(check_sql)
                exists = cursor.fetchone()[0]

                if not exists:
                    status = "❌ Source table missing"
                    created_at = "-"
                else:
                    # Drop old backup if it exists
                    try:
                        cursor.execute(f"DROP TABLE {BACKUP_DB}.{backup_table};")
                    except:
                        pass  # Fine if it doesn't exist

                    # Create backup
                    cursor.execute(f"""
                        CREATE TABLE {BACKUP_DB}.{backup_table}
                        AS {source_db}.{source_table} WITH DATA;
                    """)
                    cursor.execute(f"SELECT COUNT(*) FROM {BACKUP_DB}.{backup_table}")
                    row_count = cursor.fetchone()[0]

            except Exception as e:
                status = f"❌ Error during backup"
                row_count = "FAILED"

            summary.append({
                "source_db": source_db,
                "source_table": source_table,
                "backup_db": BACKUP_DB,
                "backup_table": backup_table,
                "row_count": row_count,
                "created_at": created_at,
                "status": status
            })

    finally:
        if cursor: cursor.close()
        if conn: conn.close()

    context['ti'].xcom_push(key='summary', value=summary)

def email_report(**context):
    summary = context['ti'].xcom_pull(key='summary', task_ids='backup_td_tables')

    # Start HTML for a simple table
    html_table = """
    <h3>✅ Teradata Table Backup Summary</h3>
    <table border="1" style="border-collapse: collapse; width: 100%;">
        <thead>
            <tr style="background-color: #f2f2f2;">
                <th style="padding: 8px; text-align: left;">Source DB</th>
                <th style="padding: 8px; text-align: left;">Source Table</th>
                <th style="padding: 8px; text-align: left;">Backup DB</th>
                <th style="padding: 8px; text-align: left;">Backup Table</th>
                <th style="padding: 8px; text-align: left;">Row Count</th>
                <th style="padding: 8px; text-align: left;">Created At</th>
                <th style="padding: 8px; text-align: left;">Status</th>
            </tr>
        </thead>
        <tbody>
    """

    for row in summary:
        html_table += f"""
            <tr>
                <td style="padding: 8px;">{row['source_db']}</td>
                <td style="padding: 8px;">{row['source_table']}</td>
                <td style="padding: 8px;">{row['backup_db']}</td>
                <td style="padding: 8px;">{row['backup_table']}</td>
                <td style="padding: 8px;">{row['row_count']}</td>
                <td style="padding: 8px;">{row['created_at']}</td>
                <td style="padding: 8px;">{row['status']}</td>
            </tr>
        """
    
    # Close HTML table tags
    html_table += """
        </tbody>
    </table>
    """

    email_op = EmailOperator(
        task_id='send_email',
        to=EMAIL_RECIPIENTS,
        subject='Teradata Backup Summary Report',
        html_content=html_table, # Use the generated HTML table here
        dag=dag
    )
    email_op.execute(context=context)

# TASK DEFINITIONS
backup_task = PythonOperator(
    task_id='backup_td_tables',
    python_callable=backup_tables,
    provide_context=True,
    dag=dag
)

email_task = PythonOperator(
    task_id='generate_email_report',
    python_callable=email_report,
    provide_context=True,
    dag=dag
)

backup_task >> email_task
