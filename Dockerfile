# ──────────────────────────────────────────────────────────────────────────────
# Dockerfile — YouTube Analytics Pipeline
# Base image: Apache Airflow 2.9 with Java (for PySpark) + project dependencies
# ──────────────────────────────────────────────────────────────────────────────

FROM apache/airflow:2.9.1-python3.11

# Switch to root to install system packages
USER root

# ── System dependencies ───────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        curl \
        wget \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME so PySpark can find it
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ── Python dependencies ───────────────────────────────────────────────────────
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# ── Copy project files into the image ─────────────────────────────────────────
COPY --chown=airflow:root dags/youtube_dag.py          /opt/airflow/dags/
COPY --chown=airflow:root dags/youtube_data_api.py     /opt/airflow/dags/
COPY --chown=airflow:root dags/youtube_data_spark.py   /opt/airflow/dags/
COPY --chown=airflow:root scripts/youtube_extractor.py /opt/airflow/scripts/

# ── Create data directory for CSV files ───────────────────────────────────────
RUN mkdir -p /opt/airflow/data

# Default command (overridden by docker-compose per service)
CMD ["airflow", "standalone"]
