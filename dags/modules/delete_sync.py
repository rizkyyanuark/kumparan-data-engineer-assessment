import os

from google.cloud import bigquery

from modules.extract import iter_article_ids
from modules.load import DATASET_ID, FACT_TABLE, init_bigquery_tables, refresh_dim_authors


def sync_hard_deletes(conn_string=None):
    """
    Detects rows that disappeared from the source table and soft-deletes them in the DWH.
    Source IDs are staged in BigQuery so the comparison is handled by the warehouse.
    """
    client = init_bigquery_tables(bigquery.Client())
    project_id = client.project
    fact_table_id = f"{project_id}.{DATASET_ID}.{FACT_TABLE}"
    staging_table_id = f"{project_id}.{DATASET_ID}.stg_source_article_ids"
    batch_size = int(os.getenv("DELETE_SYNC_BATCH_SIZE", "10000"))

    schema = [bigquery.SchemaField("article_id", "STRING", mode="REQUIRED")]
    client.delete_table(staging_table_id, not_found_ok=True)
    client.create_table(bigquery.Table(staging_table_id, schema=schema))

    source_count = 0
    try:
        for id_df in iter_article_ids(batch_size=batch_size, conn_string=conn_string):
            batch_df = id_df.rename(columns={"id": "article_id"})
            batch_df["article_id"] = batch_df["article_id"].astype(str)
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                write_disposition="WRITE_APPEND",
            )
            client.load_table_from_dataframe(
                batch_df,
                staging_table_id,
                job_config=job_config,
            ).result()
            source_count += len(batch_df)

        print(f"Staged {source_count} source article IDs for hard-delete reconciliation.")

        sync_query = f"""
        MERGE `{fact_table_id}` T
        USING `{staging_table_id}` S
        ON T.article_id = S.article_id
        WHEN NOT MATCHED BY SOURCE AND T.is_deleted = FALSE THEN
          UPDATE SET
            is_deleted = TRUE,
            deleted_at = COALESCE(T.deleted_at, CURRENT_TIMESTAMP()),
            updated_at = CURRENT_TIMESTAMP()
        """

        query_job = client.query(sync_query)
        query_job.result()
        affected_rows = query_job.num_dml_affected_rows or 0
        print(f"Hard-delete sync completed. Marked {affected_rows} missing articles as deleted.")
        if affected_rows:
            refresh_dim_authors(client)
        return affected_rows
    finally:
        client.delete_table(staging_table_id, not_found_ok=True)
