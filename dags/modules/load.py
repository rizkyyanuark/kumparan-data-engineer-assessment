import json

import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


DATASET_ID = "dwh_kumparan"
FACT_TABLE = "fact_articles"
DIM_DATES_TABLE = "dim_dates"
DIM_AUTHORS_TABLE = "dim_authors"
CONTROL_TABLE = "etl_control"


def _dataset_ref(client):
    return bigquery.DatasetReference(client.project, DATASET_ID)


def _table_id(client, table_name):
    return f"{client.project}.{DATASET_ID}.{table_name}"


def fact_article_schema():
    return [
        bigquery.SchemaField("article_key", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("article_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("author_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("published_date_key", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("created_date_key", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("content", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("word_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("char_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("published_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("deleted_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("is_deleted", "BOOLEAN", mode="NULLABLE"),
    ]


def date_dimension_schema():
    return [
        bigquery.SchemaField("date_key", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("date_actual", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("year", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("quarter", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("month", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("month_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("day", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("day_of_week", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("day_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("is_weekend", "BOOLEAN", mode="NULLABLE"),
    ]


def _ensure_table(client, table_name, schema, description=None, partition_field=None, clustering_fields=None):
    table_id = _table_id(client, table_name)
    try:
        table = client.get_table(table_id)
        existing_columns = {field.name for field in table.schema}
        missing_columns = [field for field in schema if field.name not in existing_columns]
        if missing_columns:
            table.schema = list(table.schema) + missing_columns
            table = client.update_table(table, ["schema"])
            print(f"Updated BigQuery table schema: {table_id}")
        return table
    except NotFound:
        table = bigquery.Table(table_id, schema=schema)
        table.description = description
        if partition_field:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field,
            )
        if clustering_fields:
            table.clustering_fields = clustering_fields
        table = client.create_table(table)
        print(f"Created BigQuery table: {table_id}")
        return table


def init_bigquery_tables(client=None):
    """Ensure warehouse tables exist."""
    client = client or bigquery.Client()
    dataset_ref = _dataset_ref(client)

    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "asia-southeast1"
        dataset.description = "Data warehouse for the Kumparan data engineering assessment."
        client.create_dataset(dataset)
        print(f"Created BigQuery dataset: {client.project}.{DATASET_ID}")

    _ensure_table(
        client,
        FACT_TABLE,
        fact_article_schema(),
        description="Article fact table. Grain: one row per article_id.",
        partition_field="created_at",
        clustering_fields=["author_id", "article_id"],
    )

    date_table = _ensure_table(
        client,
        DIM_DATES_TABLE,
        date_dimension_schema(),
        description="Calendar lookup dimension for article dates.",
    )
    if date_table.num_rows == 0:
        seed_dim_dates(client)

    author_schema = [
        bigquery.SchemaField("author_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("author_key", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("author_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("first_article_created_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("last_article_updated_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("total_article_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("active_article_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
    ]
    _ensure_table(
        client,
        DIM_AUTHORS_TABLE,
        author_schema,
        description="Author dimension derived from distinct article authors in the source table.",
    )

    control_schema = [
        bigquery.SchemaField("process_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("state", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("watermark", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("last_success_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("details", "STRING", mode="NULLABLE"),
    ]
    _ensure_table(
        client,
        CONTROL_TABLE,
        control_schema,
        description="ETL state table for idempotent backfill and incremental watermarks.",
    )

    return client


def seed_dim_dates(client=None, start_date="2015-01-01", end_date="2030-12-31"):
    client = client or bigquery.Client()
    table_id = _table_id(client, DIM_DATES_TABLE)
    print(f"Seeding {table_id} from {start_date} to {end_date}...")

    date_range = pd.date_range(start=start_date, end=end_date)
    df = pd.DataFrame({"date_actual": date_range})
    df["date_key"] = df["date_actual"].apply(lambda value: int(value.strftime("%Y%m%d")))
    df["year"] = df["date_actual"].dt.year
    df["quarter"] = df["date_actual"].dt.quarter
    df["month"] = df["date_actual"].dt.month
    df["month_name"] = df["date_actual"].dt.strftime("%B")
    df["day"] = df["date_actual"].dt.day
    df["day_of_week"] = df["date_actual"].dt.dayofweek + 1
    df["day_name"] = df["date_actual"].dt.strftime("%A")
    df["is_weekend"] = df["day_of_week"].isin([6, 7])
    df["date_actual"] = df["date_actual"].dt.date

    job_config = bigquery.LoadJobConfig(
        schema=date_dimension_schema(),
        write_disposition="WRITE_TRUNCATE",
    )
    client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
    print(f"Seeded {len(df)} rows into {table_id}.")


def refresh_dim_authors(client=None):
    """Refresh the derived author dimension."""
    client = client or bigquery.Client()
    fact_table_id = _table_id(client, FACT_TABLE)
    dim_table_id = _table_id(client, DIM_AUTHORS_TABLE)

    query = f"""
    CREATE OR REPLACE TABLE `{dim_table_id}` AS
    SELECT
      author_id,
      CAST(author_id AS STRING) AS author_key,
      CONCAT('author_', CAST(author_id AS STRING)) AS author_name,
      MIN(created_at) AS first_article_created_at,
      MAX(updated_at) AS last_article_updated_at,
      COUNT(1) AS total_article_count,
      COUNTIF(is_deleted = FALSE) AS active_article_count,
      CURRENT_TIMESTAMP() AS updated_at
    FROM `{fact_table_id}`
    WHERE author_id IS NOT NULL
    GROUP BY author_id
    """
    client.query(query).result()
    print(f"Refreshed author dimension: {dim_table_id}")


def load_to_bigquery(df, dataset_id=DATASET_ID, table_id=FACT_TABLE):
    """Merge transformed rows into the fact table."""
    if df.empty:
        print("Load: DataFrame is empty. Skipping fact table load.")
        return

    client = init_bigquery_tables()
    project_id = client.project
    stg_table_id = f"{project_id}.{dataset_id}.stg_{table_id}_temp"
    dest_table_id = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        schema=fact_article_schema(),
        write_disposition="WRITE_TRUNCATE",
    )
    print(f"Uploading {len(df)} rows to staging table {stg_table_id}...")
    client.load_table_from_dataframe(df, stg_table_id, job_config=job_config).result()

    merge_query = f"""
    MERGE `{dest_table_id}` T
    USING `{stg_table_id}` S
    ON T.article_id = S.article_id
    WHEN MATCHED THEN
      UPDATE SET
        author_id = S.author_id,
        published_date_key = S.published_date_key,
        created_date_key = S.created_date_key,
        title = S.title,
        content = S.content,
        word_count = S.word_count,
        char_count = S.char_count,
        published_at = S.published_at,
        created_at = S.created_at,
        updated_at = S.updated_at,
        deleted_at = S.deleted_at,
        is_deleted = S.is_deleted
    WHEN NOT MATCHED THEN
      INSERT (
        article_key, article_id, author_id, published_date_key, created_date_key,
        title, content, word_count, char_count, published_at, created_at,
        updated_at, deleted_at, is_deleted
      )
      VALUES (
        S.article_key, S.article_id, S.author_id, S.published_date_key,
        S.created_date_key, S.title, S.content, S.word_count, S.char_count,
        S.published_at, S.created_at, S.updated_at, S.deleted_at, S.is_deleted
      )
    """
    print(f"Merging staging rows into {dest_table_id}...")
    client.query(merge_query).result()
    client.delete_table(stg_table_id, not_found_ok=True)
    refresh_dim_authors(client)
    print("Fact MERGE completed.")


def get_control_record(client, process_name):
    init_bigquery_tables(client)
    table_id = _table_id(client, CONTROL_TABLE)
    query = f"""
    SELECT process_name, state, watermark, last_success_at, details
    FROM `{table_id}`
    WHERE process_name = @process_name
    LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("process_name", "STRING", process_name),
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())
    return rows[0] if rows else None


def get_control_timestamp(client, process_name):
    record = get_control_record(client, process_name)
    return record.watermark if record else None


def upsert_control_state(client, process_name, state, watermark=None, details=None):
    init_bigquery_tables(client)
    table_id = _table_id(client, CONTROL_TABLE)
    details_value = json.dumps(details or {}, default=str)
    query = f"""
    MERGE `{table_id}` T
    USING (
      SELECT
        @process_name AS process_name,
        @state AS state,
        @watermark AS watermark,
        CURRENT_TIMESTAMP() AS last_success_at,
        @details AS details
    ) S
    ON T.process_name = S.process_name
    WHEN MATCHED THEN
      UPDATE SET
        state = S.state,
        watermark = S.watermark,
        last_success_at = S.last_success_at,
        details = S.details
    WHEN NOT MATCHED THEN
      INSERT (process_name, state, watermark, last_success_at, details)
      VALUES (S.process_name, S.state, S.watermark, S.last_success_at, S.details)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("process_name", "STRING", process_name),
            bigquery.ScalarQueryParameter("state", "STRING", state),
            bigquery.ScalarQueryParameter("watermark", "TIMESTAMP", watermark),
            bigquery.ScalarQueryParameter("details", "STRING", details_value),
        ]
    )
    client.query(query, job_config=job_config).result()
    print(f"Updated ETL control state for {process_name}: {state}")
