import pandas as pd


LOCAL_TIMEZONE = "Asia/Jakarta"


def _date_key(value):
    if pd.isna(value):
        return None
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(timestamp):
        return None
    return int(timestamp.tz_convert(LOCAL_TIMEZONE).strftime("%Y%m%d"))


def _timestamp_utc(series):
    return pd.to_datetime(series, utc=True, errors="coerce")


def transform_articles(df):
    """
    Transforms raw source articles into the DWH fact schema.
    """
    if df.empty:
        print("Transform: input DataFrame is empty. Skipping.")
        return df

    df = df.copy()
    print(f"Transforming {len(df)} articles...")

    df["title"] = df["title"].fillna("")
    df["content"] = df["content"].fillna("")
    df["word_count"] = df["content"].apply(lambda value: len(str(value).split()))
    df["char_count"] = df["content"].apply(lambda value: len(str(value)))

    df["published_at"] = _timestamp_utc(df["published_at"])
    df["created_at"] = _timestamp_utc(df["created_at"])
    df["updated_at"] = _timestamp_utc(df["updated_at"])
    df["deleted_at"] = _timestamp_utc(df["deleted_at"])

    df["is_deleted"] = df["deleted_at"].notna()
    df["published_date_key"] = df["published_at"].apply(_date_key)
    df["created_date_key"] = df["created_at"].apply(_date_key)

    df["article_id"] = df["id"].astype(str)
    df["article_key"] = df["article_id"]
    df["author_id"] = pd.to_numeric(df["author_id"], errors="coerce").astype("Int64")
    df["published_date_key"] = pd.to_numeric(df["published_date_key"], errors="coerce").astype("Int64")
    df["created_date_key"] = pd.to_numeric(df["created_date_key"], errors="coerce").astype("Int64")
    df["word_count"] = pd.to_numeric(df["word_count"], errors="coerce").fillna(0).astype("Int64")
    df["char_count"] = pd.to_numeric(df["char_count"], errors="coerce").fillna(0).astype("Int64")

    final_cols = [
        "article_key",
        "article_id",
        "author_id",
        "published_date_key",
        "created_date_key",
        "title",
        "content",
        "word_count",
        "char_count",
        "published_at",
        "created_at",
        "updated_at",
        "deleted_at",
        "is_deleted",
    ]

    print("Transformation completed.")
    return df[final_cols]
