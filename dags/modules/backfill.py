import os
from datetime import datetime, timedelta, timezone

from google.cloud import bigquery

from modules.extract import iter_articles_created_between
from modules.load import (
    get_control_record,
    get_control_timestamp,
    init_bigquery_tables,
    load_to_bigquery,
    upsert_control_state,
)
from modules.transform import transform_articles


BACKFILL_PROCESS = "articles_historical_backfill"
INCREMENTAL_WATERMARK_PROCESS = "articles_incremental_watermark"


def _utc_datetime(year, month, day):
    return datetime(year, month, day, tzinfo=timezone.utc)


def _windows(start_time, end_time, chunk_days):
    cursor = start_time
    while cursor < end_time:
        next_cursor = min(cursor + timedelta(days=chunk_days), end_time)
        yield cursor, next_cursor
        cursor = next_cursor


def run_initial_backfill():
    """
    Loads all historical article rows from 2016 onward exactly once per warehouse.
    Data is processed in bounded windows and batches, then future hourly runs use
    updated_at watermarking for incremental changes.
    """
    client = init_bigquery_tables(bigquery.Client())
    existing_backfill = get_control_record(client, BACKFILL_PROCESS)

    if existing_backfill and existing_backfill.state == "completed":
        if not get_control_timestamp(client, INCREMENTAL_WATERMARK_PROCESS):
            upsert_control_state(
                client,
                INCREMENTAL_WATERMARK_PROCESS,
                "completed",
                watermark=existing_backfill.watermark,
                details={"source": "historical_backfill_existing_state"},
            )
        print("Historical backfill already completed. Skipping.")
        return 0

    chunk_days = int(os.getenv("BACKFILL_CHUNK_DAYS", "31"))
    batch_size = int(os.getenv("BACKFILL_BATCH_SIZE", "5000"))
    start_time = _utc_datetime(2016, 1, 1)
    backfill_started_at = datetime.now(timezone.utc)

    upsert_control_state(
        client,
        BACKFILL_PROCESS,
        "running",
        watermark=backfill_started_at,
        details={"chunk_days": chunk_days, "batch_size": batch_size},
    )

    total_rows = 0
    total_batches = 0

    for window_start, window_end in _windows(start_time, backfill_started_at, chunk_days):
        window_rows = 0
        print(f"Backfill window: {window_start} to {window_end}")
        for raw_df in iter_articles_created_between(window_start, window_end, batch_size=batch_size):
            transformed_df = transform_articles(raw_df)
            load_to_bigquery(transformed_df)
            batch_count = len(transformed_df)
            total_rows += batch_count
            window_rows += batch_count
            total_batches += 1
        print(f"Backfill window completed with {window_rows} rows.")

    upsert_control_state(
        client,
        BACKFILL_PROCESS,
        "completed",
        watermark=backfill_started_at,
        details={
            "rows_loaded": total_rows,
            "batches_loaded": total_batches,
            "source_start": start_time,
            "source_end": backfill_started_at,
        },
    )
    upsert_control_state(
        client,
        INCREMENTAL_WATERMARK_PROCESS,
        "completed",
        watermark=backfill_started_at,
        details={"source": "historical_backfill_completion"},
    )

    print(f"Historical backfill completed: {total_rows} rows in {total_batches} batches.")
    return total_rows
