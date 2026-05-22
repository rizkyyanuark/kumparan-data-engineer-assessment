# Kumparan Data Engineer Assessment V3.1

This repository is my submission for the Kumparan Data Engineer internship skill test.

The assessment asks for an ETL/ELT pipeline that moves `articles` data from a database into a data warehouse, runs every hour, and optionally explains or implements:

1. a dimensional data warehouse schema,
2. handling historical data that has existed since 2016,
3. synchronization when the source uses hard deletes.

This solution implements the required hourly pipeline and covers the three bonus topics in code and documentation.

## Solution Summary

| Area | Implementation |
| --- | --- |
| Source database | Supabase PostgreSQL `articles` table |
| Data pipeline tool | Apache Airflow in Docker |
| Data warehouse | Google BigQuery dataset `dwh_kumparan` |
| Schedule | Airflow DAG `kumparan_articles_etl` with `@hourly` schedule |
| Load strategy | BigQuery staging table plus idempotent `MERGE` |
| Historical strategy | Chunked backfill from `2016-01-01` |
| Incremental strategy | Persisted `updated_at` watermark in BigQuery `etl_control` |
| Delete strategy | BigQuery-side hard-delete reconciliation from staged source IDs |

## Assessment Coverage

| Assessment requirement | How this repository answers it | Proof in repository |
| --- | --- | --- |
| Create ETL/ELT from a database to a data warehouse | Extract from Supabase PostgreSQL, transform in Python, load into BigQuery | [dags/modules/extract.py](dags/modules/extract.py), [dags/modules/transform.py](dags/modules/transform.py), [dags/modules/load.py](dags/modules/load.py) |
| Provide code, not only GUI screenshots | Pipeline, schema bootstrap, source initialization, and Docker runtime are defined as code | [dags/](dags/), [init_supabase.sql](init_supabase.sql), [docker-compose.yml](docker-compose.yml) |
| ETL/ELT runs each hour every day | Airflow DAG uses `schedule="@hourly"` | [dags/kumparan_articles_etl.py](dags/kumparan_articles_etl.py) |
| Bonus: Data Warehouse Schema using dimensional modelling | BigQuery creates `fact_articles`, `dim_dates`, and `dim_authors` | [dags/modules/load.py](dags/modules/load.py) |
| Bonus: Consider data that already exists since 2016 | A bounded historical backfill starts from `2016-01-01`, processes windows and batches, then stores completion state | [dags/modules/backfill.py](dags/modules/backfill.py), [dags/modules/extract.py](dags/modules/extract.py) |
| Bonus: Hard-delete source rows must stay synchronized with DWH | Current source IDs are staged in BigQuery and missing active fact rows are soft-deleted with warehouse SQL | [dags/modules/delete_sync.py](dags/modules/delete_sync.py) |

## Source Data

The assessment defines an `articles` dataset with these fields:

| Source column | Usage in this solution |
| --- | --- |
| `id` | Natural article identifier and fact merge key |
| `title` | Fact descriptive attribute |
| `content` | Fact descriptive attribute and metric input |
| `published_at` | Publication timestamp and publication date key |
| `author_id` | Author dimension relationship |
| `created_at` | Historical backfill range and fact partition timestamp |
| `updated_at` | Incremental extraction watermark |
| `deleted_at` | Soft-delete signal from source |

The provided [init_supabase.sql](init_supabase.sql) file:

- creates the PostgreSQL `articles` table,
- creates indexes for incremental extraction, historical backfill, and ID reconciliation,
- adds a trigger that refreshes `updated_at` on row update,
- seeds test data from 2016, 2020, recent active rows, and one soft-deleted row.

## Architecture

```text
                 +-----------------------------+
                 | Supabase PostgreSQL         |
                 | articles                    |
                 +--------------+--------------+
                                |
              historical load   |   hourly updated rows
              by created_at      |   by updated_at watermark
                                v
                 +-----------------------------+
                 | Apache Airflow              |
                 | kumparan_articles_etl       |
                 |                             |
                 | 1. bootstrap warehouse      |
                 | 2. initial backfill         |
                 | 3. incremental ETL          |
                 | 4. hard-delete sync         |
                 +--------------+--------------+
                                |
                  transform     |   BigQuery staging + MERGE
                                v
                 +-----------------------------+
                 | Google BigQuery             |
                 | dwh_kumparan                |
                 |                             |
                 | fact_articles               |
                 | dim_dates                   |
                 | dim_authors                 |
                 | etl_control                 |
                 +-----------------------------+
```

## DAG Flow

The Airflow DAG is defined in [dags/kumparan_articles_etl.py](dags/kumparan_articles_etl.py).

```text
bootstrap_warehouse_task
        |
historical_backfill_task
        |
incremental_etl_task
        |
hard_delete_sync_task
```

### 1. `bootstrap_warehouse_task`

Creates BigQuery objects if they do not exist:

- dataset `dwh_kumparan`,
- fact table `fact_articles`,
- date dimension `dim_dates`,
- author dimension `dim_authors`,
- ETL state table `etl_control`.

This keeps the warehouse setup reproducible and avoids manual table creation.

### 2. `historical_backfill_task`

Handles the assessment question about source data that already exists since 2016.

The task:

1. checks `etl_control` to see whether the initial backfill already completed,
2. starts from `2016-01-01` when no completed backfill state exists,
3. divides the history into bounded `created_at` windows,
4. paginates each window with `(created_at, id)` keyset ordering,
5. transforms and merges each batch into BigQuery,
6. stores a completion watermark for future hourly incremental loads.

This design avoids one large operational database read and avoids creating tens of thousands of hourly Airflow catchup runs from 2016.

### 3. `incremental_etl_task`

Processes new or updated source rows every hour.

The incremental query shape is:

```sql
WHERE updated_at >= previous_watermark
  AND updated_at < current_interval_end
```

The watermark is kept in `etl_control`. This is more resilient than only using the current Airflow run interval because a delayed scheduler can continue from the last successful processed boundary.

### 4. `hard_delete_sync_task`

Handles the assessment scenario where the source row disappears completely.

A pure `updated_at` incremental pipeline cannot observe a hard-deleted source row because there is no source row left to extract. This pipeline therefore:

1. streams current source article IDs,
2. loads those IDs into a BigQuery staging table,
3. runs a BigQuery `MERGE` with `WHEN NOT MATCHED BY SOURCE`,
4. marks missing active warehouse rows as deleted with `is_deleted = TRUE` and `deleted_at`.

The comparison is executed in the warehouse rather than building the full source and DWH ID sets in Python memory.

## Transform and Load Logic

### Transform

[dags/modules/transform.py](dags/modules/transform.py) prepares article rows for analytics:

- normalizes nullable text fields,
- derives `word_count` and `char_count`,
- derives `published_date_key` and `created_date_key`,
- flags soft-deleted source rows with `is_deleted`,
- normalizes timestamps to UTC before BigQuery loading,
- builds `article_key` and `article_id` for fact loading.

Date keys are calculated using `Asia/Jakarta` calendar dates so publication and creation dates remain aligned with the business timezone represented by the seeded source timestamps.

### Idempotent Load

[dags/modules/load.py](dags/modules/load.py) loads transformed rows through a temporary BigQuery staging table and merges them into the fact table by `article_id`.

This makes retries and repeated windows safe:

- existing article IDs are updated,
- new article IDs are inserted,
- duplicate fact rows are avoided for the same `article_id`.

## Data Warehouse Schema

The warehouse is modeled around article analytics.

### `fact_articles`

Grain: one row per article.

| Column group | Columns |
| --- | --- |
| Keys | `article_key`, `article_id`, `author_id`, `published_date_key`, `created_date_key` |
| Descriptive attributes | `title`, `content` |
| Metrics | `word_count`, `char_count` |
| Timestamps | `published_at`, `created_at`, `updated_at`, `deleted_at` |
| Delete state | `is_deleted` |

Physical design:

- partitioned by `created_at`,
- clustered by `author_id` and `article_id`.

### `dim_dates`

Calendar dimension seeded from 2015 to 2030.

Columns include:

- `date_key`,
- `date_actual`,
- `year`,
- `quarter`,
- `month`,
- `month_name`,
- `day`,
- `day_of_week`,
- `day_name`,
- `is_weekend`.

### `dim_authors`

Author dimension derived from distinct `author_id` values in `fact_articles`.

The assessment source schema provides `author_id` but does not provide an author name table. For that reason:

- `author_id` is the business identifier,
- `author_key` is a string form of the identifier,
- `author_name` is a stable placeholder such as `author_101`,
- article activity summaries are exposed through `first_article_created_at`, `last_article_updated_at`, `total_article_count`, and `active_article_count`.

If a real author master table is available, this derived dimension can be replaced or enriched from that source.

### `etl_control`

Operational metadata table for restartable ETL state.

It stores:

- `process_name`,
- `state`,
- `watermark`,
- `last_success_at`,
- JSON `details`.

Current process names:

- `articles_historical_backfill`,
- `articles_incremental_watermark`.

## Design Decisions

### Why separate backfill and incremental logic?

Historical completeness and hourly freshness have different extraction patterns:

- the initial load needs the full history from 2016,
- recurring hourly loads only need rows that changed since the last processed watermark.

Using separate paths makes the pipeline both bounded for history and efficient for recurring runs.

### Why use BigQuery `MERGE`?

Airflow retries, manual reruns, and delayed runs are expected operational scenarios. A merge-based load keeps repeated processing idempotent at the article grain.

### Why stage source IDs for hard deletes?

Hard deletes require reconciliation because the incremental extractor cannot read a deleted row that no longer exists. Staging source IDs lets BigQuery perform the missing-row comparison close to the DWH data.

## Repository Structure

```text
.
|-- dags/
|   |-- kumparan_articles_etl.py
|   `-- modules/
|       |-- backfill.py
|       |-- delete_sync.py
|       |-- extract.py
|       |-- load.py
|       `-- transform.py
|-- docker-compose.yml
|-- init_supabase.sql
|-- .env.example
`-- README.md
```

## Setup and Run

### Prerequisites

- Docker Desktop with Docker Compose
- Supabase PostgreSQL project
- Google Cloud project with BigQuery enabled
- GCP service account key that can create and query the BigQuery dataset/tables

### 1. Initialize Source PostgreSQL

Run [init_supabase.sql](init_supabase.sql) in the Supabase SQL Editor.

### 2. Configure Environment

Create a local `.env` file from [.env.example](.env.example).

Minimum values:

```bash
AIRFLOW_UID=50000
SUPABASE_CONN_STRING=postgresql://...
```

Place the local Google Cloud service account file at:

```text
gcp-key.json
```

The Docker Compose stack mounts it into Airflow through `GOOGLE_APPLICATION_CREDENTIALS`.

### 3. Start Airflow

```bash
docker compose up airflow-init
docker compose up -d
```

Open the local Airflow UI:

```text
http://localhost:8089
```

Default local credentials:

- username: `admin`
- password: `admin`

Unpause:

```text
kumparan_articles_etl
```

The DAG is scheduled hourly. A manual DAG run can also be triggered from Airflow for review.

## Reviewer Verification

### Quick code and DAG checks

```bash
docker compose config --quiet
docker compose exec -T airflow-scheduler python -m compileall -q /opt/airflow/dags
docker compose exec -T airflow-scheduler airflow dags list-import-errors
docker compose exec -T airflow-scheduler airflow tasks list kumparan_articles_etl --tree
```

Expected Airflow task tree:

```text
bootstrap_warehouse_task
    historical_backfill_task
        incremental_etl_task
            hard_delete_sync_task
```

### Evidence from the verified seeded run

The repository was verified locally on **May 22, 2026** with the seeded source dataset from [init_supabase.sql](init_supabase.sql).

Verification results:

| Check | Result |
| --- | --- |
| Airflow DAG import errors | none |
| Manual verification DAG run | `codex_submission_check_20260522` succeeded |
| Bootstrap warehouse task | success |
| Historical backfill task | success; completed backfill state already detected on rerun |
| Incremental ETL task | success |
| Hard-delete sync task | success |
| Source `articles` row count | 8 |
| BigQuery `fact_articles` row count | 8 |
| Soft-deleted article count | 1 |
| BigQuery `dim_dates` row count | 5844 |
| BigQuery `dim_authors` row count | 4 |
| BigQuery `etl_control` records | historical backfill and incremental watermark |

These values are expected for the included mock seed. With a different source dataset, the row counts will change while the pipeline flow remains the same.

### Manual behavior checks

The included seed already proves historical coverage because it contains rows created in 2016 and 2020. The following checks can be used when reviewing update and delete behavior.

#### Incremental update check

Update one source article in Supabase:

```sql
UPDATE articles
SET title = 'Updated title for incremental verification'
WHERE id = 1;
```

Run the hourly DAG manually or wait for the next schedule. The `updated_at` trigger makes the changed row eligible for `incremental_etl_task`, and the BigQuery `MERGE` updates the matching `fact_articles.article_id`.

#### Hard-delete check

Delete one source article in Supabase:

```sql
DELETE FROM articles
WHERE id = 2;
```

Run the DAG manually. After `hard_delete_sync_task`, verify the warehouse state:

```sql
SELECT article_id, is_deleted, deleted_at
FROM `project_id.dwh_kumparan.fact_articles`
WHERE article_id = '2';
```

Expected result: the warehouse fact row remains available for analytical history, while `is_deleted` becomes `TRUE` and `deleted_at` is populated.

## Runtime Tuning

Optional `.env` parameters:

| Variable | Default | Purpose |
| --- | --- | --- |
| `BACKFILL_CHUNK_DAYS` | `31` | Size of each historical `created_at` window |
| `BACKFILL_BATCH_SIZE` | `5000` | Maximum source rows per historical batch |
| `DELETE_SYNC_BATCH_SIZE` | `10000` | Maximum source IDs loaded per reconciliation batch |

The DAG uses `max_active_runs=1` so historical bootstrap and hourly work do not overlap in this assessment environment.

## Security and Submission Notes

Do not submit real credentials or local runtime artifacts.

The following are intentionally ignored:

- `.env`,
- `gcp-key.json`,
- Airflow `logs/`,
- Airflow `plugins/`,
- Python `__pycache__/` and `.pyc` files.

The assessment submission should contain source code, SQL initialization, Docker configuration, `.env.example`, and this README.

The supplied assessment PDF is intentionally kept out of the GitHub repository because the reviewer already has the source prompt and the repository should contain the solution artifacts.
