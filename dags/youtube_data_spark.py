"""
youtube_data_spark.py
────────────────────────────────────────────────────────────────────────────────
PySpark Transformation Module

Exposes run_pyspark_processing() which is imported and called by the
Airflow DAG (youtube_dag.py).

Pipeline:
  1. Read  → Youtube_data.csv
  2. Clean → fill nulls
  3. Transform → timestamp, day/hour features, engagement rate
  4. Write → youtube_processed/ (single CSV part file)
────────────────────────────────────────────────────────────────────────────────
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, dayofweek, hour, date_format

# ── Paths (must match youtube_data_api.py and docker-compose volume mounts) ───
INPUT_CSV    = "/opt/airflow/data/Youtube_data.csv"
OUTPUT_PATH  = "/opt/airflow/data/youtube_processed"


# ── Core transformation logic ─────────────────────────────────────────────────
def transform(df):
    """
    Apply all cleaning and feature-engineering steps to a raw Spark DataFrame.
    Returns the enriched DataFrame.
    """
    # 1. Fill numeric nulls
    df = df.na.fill({"views": 0, "likes": 0, "comments": 0})

    # 2. Fill string nulls
    df = df.na.fill({"title": "unknown", "video_id": "unknown"})

    # 3. Parse ISO timestamp
    df = df.withColumn("publishedAt", to_timestamp("publishedAt"))

    # 4. Time-based features
    df = df.withColumn("publish_day_name", date_format("publishedAt", "EEEE"))
    df = df.withColumn("publish_day",      dayofweek("publishedAt"))
    df = df.withColumn("publish_hour",     hour("publishedAt"))

    # 5. Engagement rate = (likes + comments) / views — guard against div/0
    df = df.withColumn(
        "engagement_rate",
        ((col("likes") + col("comments")) / col("views").cast("double")).cast("double")
    )
    df = df.na.fill({"engagement_rate": 0.0})

    return df


# ── Airflow callable ──────────────────────────────────────────────────────────
def run_pyspark_processing():
    """
    Entry point called by the Airflow PythonOperator.
    Initialises Spark, runs the transformation, and writes output CSV.
    """
    print("Starting PySpark processing...")

    spark = (
        SparkSession.builder
        .appName("YoutubeTransform")
        .master("local[*]")           # uses all cores; swap to spark://spark:7077 in Docker
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Read
    df = spark.read.csv(INPUT_CSV, header=True, inferSchema=True)
    print(f"Rows loaded: {df.count()}")

    # Transform
    df = transform(df)

    # Preview
    df.select(
        "video_id", "title", "views", "likes", "comments",
        "publish_day_name", "publish_hour", "engagement_rate"
    ).show(10, truncate=False)

    # Write — coalesce(1) produces a single CSV file for easy downstream loading
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(OUTPUT_PATH)
    print(f"Processed data saved → {OUTPUT_PATH}/")

    spark.stop()
    print("PySpark processing complete.")


# ── Standalone test ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_pyspark_processing()
