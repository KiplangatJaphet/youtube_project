"""
youtube_dag.py
────────────────────────────────────────────────────────────────────────────────
Airflow DAG — YouTube Channel Analytics Pipeline

Architecture  : thin DAG — all logic lives in the imported modules
Schedule      : daily at 06:00 UTC
Tasks         : extract_youtube_data  →  process_youtube_data  →  load_to_postgres

Imports:
  youtube_data_api   → run_youtube_api_extraction()
  youtube_data_spark → run_pyspark_processing()
────────────────────────────────────────────────────────────────────────────────
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Import callables from sibling scripts ─────────────────────────────────────
# Both files must be on the PYTHONPATH / in the same dags/ or scripts/ folder
from youtube_data_api   import run_youtube_api_extraction
from youtube_data_spark import run_pyspark_processing


# ── Load task (defined here — lightweight, no extra module needed) ─────────────
def run_load_to_postgres():
    """
    Read the processed CSV written by Spark and load it into PostgreSQL.
    Connection details come from the Airflow 'postgres_youtube' Connection
    configured in the Airflow UI (Admin → Connections).
    """
    import glob
    import pandas as pd
    from sqlalchemy import create_engine
    from airflow.hooks.base import BaseHook

    OUTPUT_PATH = "/opt/airflow/data/youtube_processed"
    files = glob.glob(f"{OUTPUT_PATH}/part-*.csv")

    if not files:
        raise FileNotFoundError(
            f"No processed CSV found in {OUTPUT_PATH}. "
            "Did the process_youtube_data task succeed?"
        )

    df = pd.read_csv(files[0])
    print(f"Rows to load: {len(df)}")

    conn   = BaseHook.get_connection("postgres_youtube")
    engine = create_engine(
        f"postgresql+psycopg2://{conn.login}:{conn.password}"
        f"@{conn.host}:{conn.port or 5432}/{conn.schema}"
    )
    df.to_sql("youtube_video_stats", engine, if_exists="replace", index=False, chunksize=500)
    print(f"Loaded {len(df)} rows → PostgreSQL table 'youtube_video_stats'")


# ── Default args ──────────────────────────────────────────────────────────────
default_args = {
    "owner":            "airflow",
    "depends_on_past":  False,
    "start_date":       datetime(2025, 10, 4),
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}

# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="youtube_data_pipeline",
    default_args=default_args,
    description="YouTube Data ETL: API → PySpark → PostgreSQL",
    schedule_interval="0 6 * * *",   # daily at 06:00 UTC (same as @daily)
    catchup=False,
    tags=["youtube", "pyspark", "data-engineering"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_youtube_data",
        python_callable=run_youtube_api_extraction,
        doc_md="Fetch channel + video stats from YouTube API → save to CSV.",
    )

    process_task = PythonOperator(
        task_id="process_youtube_data",
        python_callable=run_pyspark_processing,
        doc_md="Run PySpark transformations on raw CSV → write processed CSV.",
    )

    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=run_load_to_postgres,
        doc_md="Load processed CSV into PostgreSQL youtube_video_stats table.",
    )

    # ── Pipeline order ─────────────────────────────────────────────────────────
    extract_task >> process_task >> load_task
