from datetime import datetime, timedelta, timezone
import os
import sys

from google.cloud import bigquery

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.backfill import INCREMENTAL_WATERMARK_PROCESS, run_initial_backfill
from modules.delete_sync import sync_hard_deletes
from modules.extract import extract_from_supabase
from modules.load import get_control_timestamp, init_bigquery_tables, load_to_bigquery, upsert_control_state
from modules.transform import transform_articles


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _to_utc(value):
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def bootstrap_warehouse_wrapper(**context):
    init_bigquery_tables(bigquery.Client())
    return "Warehouse bootstrap completed."


def historical_backfill_wrapper(**context):
    rows_loaded = run_initial_backfill()
    return f"Historical backfill completed or skipped. Rows loaded: {rows_loaded}."


def incremental_etl_wrapper(**context):
    """
    Runs hourly incremental ETL using a persisted updated_at watermark.
    This avoids missing data if the scheduler is down while keeping the DAG hourly.
    """
    client = init_bigquery_tables(bigquery.Client())
    interval_start = _to_utc(context["data_interval_start"])
    interval_end = _to_utc(context["data_interval_end"])
    watermark = get_control_timestamp(client, INCREMENTAL_WATERMARK_PROCESS) or interval_start

    if watermark >= interval_end:
        print(f"Incremental watermark {watermark} is already at or after interval end {interval_end}.")
        return "No incremental window to process."

    print(f"Incremental ETL window: [{watermark}, {interval_end})")
    raw_df = extract_from_supabase(watermark, interval_end)

    if raw_df.empty:
        upsert_control_state(
            client,
            INCREMENTAL_WATERMARK_PROCESS,
            "completed",
            watermark=interval_end,
            details={"rows_loaded": 0, "window_start": watermark, "window_end": interval_end},
        )
        return "No updated articles found."

    transformed_df = transform_articles(raw_df)
    load_to_bigquery(transformed_df)
    upsert_control_state(
        client,
        INCREMENTAL_WATERMARK_PROCESS,
        "completed",
        watermark=interval_end,
        details={
            "rows_loaded": len(transformed_df),
            "window_start": watermark,
            "window_end": interval_end,
        },
    )

    return f"Successfully processed {len(transformed_df)} updated articles."


def delete_sync_wrapper(**context):
    affected_rows = sync_hard_deletes()
    return f"Hard-delete reconciliation completed. Rows affected: {affected_rows}."


with DAG(
    dag_id="kumparan_articles_etl",
    default_args=default_args,
    description="Hourly article ETL from Supabase/Postgres to BigQuery with backfill and hard-delete sync.",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=12),
    tags=["kumparan", "data-engineering", "articles"],
) as dag:
    bootstrap_warehouse = PythonOperator(
        task_id="bootstrap_warehouse_task",
        python_callable=bootstrap_warehouse_wrapper,
    )

    historical_backfill = PythonOperator(
        task_id="historical_backfill_task",
        python_callable=historical_backfill_wrapper,
    )

    incremental_etl = PythonOperator(
        task_id="incremental_etl_task",
        python_callable=incremental_etl_wrapper,
    )

    reconcile_deletes = PythonOperator(
        task_id="hard_delete_sync_task",
        python_callable=delete_sync_wrapper,
    )

    bootstrap_warehouse >> historical_backfill >> incremental_etl >> reconcile_deletes
