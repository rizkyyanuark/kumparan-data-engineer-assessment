import os

import pandas as pd
from sqlalchemy import create_engine, text


ARTICLE_COLUMNS = """
    id,
    title,
    content,
    published_at,
    author_id,
    created_at,
    updated_at,
    deleted_at
"""


def get_source_engine(conn_string=None):
    conn_string = conn_string or os.getenv("SUPABASE_CONN_STRING")
    if not conn_string:
        raise ValueError("SUPABASE_CONN_STRING environment variable is not set.")
    return create_engine(conn_string)


def extract_from_supabase(start_time, end_time, conn_string=None):
    """Read rows updated in one watermark window."""
    engine = get_source_engine(conn_string)
    query = text(f"""
        SELECT {ARTICLE_COLUMNS}
        FROM articles
        WHERE updated_at >= :start_time
          AND updated_at < :end_time
        ORDER BY updated_at ASC, id ASC
    """)

    print(f"Extracting updated articles from {start_time} to {end_time}...")
    with engine.connect() as conn:
        df = pd.read_sql_query(
            query,
            conn,
            params={"start_time": start_time, "end_time": end_time},
        )

    print(f"Extracted {len(df)} updated articles.")
    return df


def iter_articles_created_between(start_time, end_time, batch_size=5000, conn_string=None):
    """Yield historical rows in keyset-paginated batches."""
    engine = get_source_engine(conn_string)
    last_created_at = start_time
    last_id = 0

    query = text(f"""
        SELECT {ARTICLE_COLUMNS}
        FROM articles
        WHERE created_at >= :window_start
          AND created_at < :window_end
          AND (
            created_at > :last_created_at
            OR (created_at = :last_created_at AND id > :last_id)
          )
        ORDER BY created_at ASC, id ASC
        LIMIT :batch_size
    """)

    while True:
        with engine.connect() as conn:
            df = pd.read_sql_query(
                query,
                conn,
                params={
                    "window_start": start_time,
                    "window_end": end_time,
                    "last_created_at": last_created_at,
                    "last_id": last_id,
                    "batch_size": batch_size,
                },
            )

        if df.empty:
            break

        yield df
        last_row = df.iloc[-1]
        last_created_at = last_row["created_at"]
        last_id = int(last_row["id"])


def iter_article_ids(batch_size=10000, conn_string=None):
    """Yield source article IDs in batches."""
    engine = get_source_engine(conn_string)
    last_id = 0
    query = text("""
        SELECT id
        FROM articles
        WHERE id > :last_id
        ORDER BY id ASC
        LIMIT :batch_size
    """)

    while True:
        with engine.connect() as conn:
            df = pd.read_sql_query(
                query,
                conn,
                params={"last_id": last_id, "batch_size": batch_size},
            )

        if df.empty:
            break

        yield df
        last_id = int(df["id"].iloc[-1])
