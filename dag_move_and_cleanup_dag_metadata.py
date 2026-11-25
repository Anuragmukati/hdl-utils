from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import DagRun, TaskInstance, XCom, Log
from airflow.utils.session import provide_session
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import os
import logging
import shutil

SOURCE_BUCKET = "wdwg-ple-aidcom-0-usre-composer-edw-1"
DEST_BUCKET = "gk1v-ple-edwpr-0-usre-beamx-code"
GCS_DAG_FOLDER = "dags/"
LOCAL_TMP_DIR = "/tmp/dag_backup/"

@dag(schedule=None, start_date=days_ago(1), catchup=False, tags=["dag_cleaner"])
def dag_move_and_cleanup_dag_metadata():

    @task
    def get_dag_ids_from_conf(dag_run=None):
        return dag_run.conf.get("dag_ids", [])

    @task
    def move_file_and_cleanup_metadata(dag_id: str):
        result = {"dag_id": dag_id, "status": "success"}
        try:
            # 1. Search .py file in source GCS
            src_hook = GCSHook(gcp_conn_id="google_cloud_default")
            all_files = src_hook.list(bucket_name=SOURCE_BUCKET, prefix=GCS_DAG_FOLDER)
            py_file_path = next((f for f in all_files if f.endswith(f"{dag_id}.py")), None)

            if not py_file_path:
                raise FileNotFoundError(f"DAG file for {dag_id} not found.")

            # 2. Download file locally
            local_path = os.path.join(LOCAL_TMP_DIR, py_file_path)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            src_hook.download(bucket_name=SOURCE_BUCKET, object_name=py_file_path, filename=local_path)

            # 3. Upload to destination GCS
            dest_hook = GCSHook(gcp_conn_id="sa-vz-it-gk1v-edwpr-0-app")
            dest_hook.upload(bucket_name=DEST_BUCKET, object_name=py_file_path, filename=local_path)
            src_hook.delete(bucket_name=SOURCE_BUCKET,object_name=py_file_path)
            print("file deleted from source bucket")

            # Optional: Remove local copy
            os.remove(local_path)
        except Exception as e:
            logging.error(f"Deletion and move Failed for {dag_id}: {e}")

            # 4. Print metadata before deletion
        print_dag_metadata(dag_id, prefix="BEFORE")

        # 5. Delete metadata
        delete_metadata(dag_id)

        # 6. Print metadata after deletion
        print_dag_metadata(dag_id, prefix="AFTER")

        

        return result

    @provide_session
    def print_dag_metadata(dag_id, session=None, prefix=""):
        runs = session.query(DagRun).filter(DagRun.dag_id == dag_id).count()
        tasks = session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).count()
        xcoms = session.query(XCom).filter(XCom.dag_id == dag_id).count()
        logs = session.query(Log).filter(Log.dag_id == dag_id).count()

        print(f"[{prefix}] DAG: {dag_id}, DagRuns: {runs}, TaskInstances: {tasks}, XComs: {xcoms}, Logs: {logs}")

    @provide_session
    def delete_metadata(dag_id, session=None):
        session.query(DagRun).filter(DagRun.dag_id == dag_id).delete()
        session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).delete()
        session.query(XCom).filter(XCom.dag_id == dag_id).delete()
        session.query(Log).filter(Log.dag_id == dag_id).delete()
        session.commit()

    dag_ids = get_dag_ids_from_conf()
    results = move_file_and_cleanup_metadata.expand(dag_id=dag_ids)

dag_move_and_cleanup_dag_metadata()

