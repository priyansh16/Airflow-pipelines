import json
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

sys.path.insert(0, "/opt/airflow/include")

GCP_PROJECT = "nodal-triumph-495809-s2"
GCS_BUCKET  = "nodal-triumph-495809-s2-raw-data"
BQ_DATASET  = "raw"
BQ_TABLE    = "ecommerce_events"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def notify_on_failure(context):
    """Called automatically when any task in this DAG fails."""
    dag_id  = context["dag"].dag_id
    task_id = context["task"].task_id
    exec_dt = context["logical_date"]
    print(
        f"ALERT: Task failed | DAG: {dag_id} | "
        f"Task: {task_id} | Date: {exec_dt}"
    )
    # In Phase 5 we will replace this print with a real Slack webhook


@dag(
    dag_id="ecommerce_elt",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 0 * * *",   # Every day at midnight
    catchup=False,
    tags=["elt", "ecommerce", "dbt"],
    on_failure_callback=notify_on_failure,
    doc_md="""
    ## E-commerce ELT Pipeline

    **Pattern**: ELT — data lands raw in BigQuery first,
    then dbt transforms it inside the warehouse using SQL.

    **Why ELT here vs ETL for the news pipeline?**
    The transformation logic is pure SQL aggregations — BigQuery
    is far more efficient at this than Python. dbt also gives us
    version-controlled, tested business logic.

    **Steps**:
    1. Generate synthetic e-commerce events
    2. Upload raw JSON to GCS
    3. Load raw JSON into BigQuery unchanged
    4. dbt transforms raw → staging → marts inside BigQuery
    5. dbt tests validate the transformed data
    """,
)
def ecommerce_elt():

    @task
    def generate_and_upload(ds=None) -> str:
        """
        Generate events and upload raw JSON to GCS.
        ds is the execution date injected by Airflow (e.g. 2024-01-15).
        Returns the GCS path.
        """
        from generate_events import generate_events
        from google.cloud import storage
        from google.oauth2 import service_account

        events = generate_events(500)

        # Save to tmp file
        tmp_path = f"/tmp/events_{ds}.json"
        with open(tmp_path, "w") as f:
            for event in events:
                f.write(json.dumps(event) + "\n")

        print(f"Generated {len(events)} events → {tmp_path}")

        # Upload to GCS — partitioned by date
        credentials = service_account.Credentials.from_service_account_file(
            "/opt/airflow/include/gcp_keyfile.json"
        )
        client = storage.Client(
            project=GCP_PROJECT, credentials=credentials
        )
        bucket   = client.bucket(GCS_BUCKET)
        gcs_path = f"events/{ds}/events.json"
        blob     = bucket.blob(gcs_path)
        blob.upload_from_filename(tmp_path)

        print(f"Uploaded to gs://{GCS_BUCKET}/{gcs_path}")
        return gcs_path

    load_raw_to_bq = GCSToBigQueryOperator(
        task_id="load_raw_to_bigquery",
        bucket=GCS_BUCKET,
        source_objects=["events/{{ ds }}/events.json"],
        destination_project_dataset_table=(
            f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
        ),
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_APPEND",
        autodetect=True,
        project_id=GCP_PROJECT,
        gcp_conn_id="google_cloud_default",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/dbt/ecommerce_dbt && "
            "dbt run --profiles-dir /opt/airflow/dbt --target prod"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/dbt/ecommerce_dbt && "
            "dbt test --profiles-dir /opt/airflow/dbt --target prod"
        ),
    )

    # ── Wire up dependencies ───────────────────────────────────────────────
    gcs_path = generate_and_upload()
    gcs_path >> load_raw_to_bq >> dbt_run >> dbt_test


ecommerce_elt()