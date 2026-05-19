**End-to-End YouTube Channel Analytics Pipeline
A complete data engineering pipeline that collects data from the YouTube Data API v3, processes it with PySpark, orchestrates workflows using Apache Airflow, visualizes insights in Grafana, and runs entirely inside Docker containers.
---
Project Overview
This project analyzes a Football/Sports Highlights YouTube channel — collecting video performance data, transforming it, and surfacing insights through interactive dashboards.

---
Architecture
```
YouTube Data API v3
        │
        ▼
  youtube_data_api.py        ← Extract: channel + video stats
        │
        ▼
  youtube_data_spark.py      ← Transform: PySpark cleaning + feature engineering
        │
        ▼
    PostgreSQL                ← Load: youtube_video_stats table
        │
        ▼
     Grafana                  ← Visualize: 6 insight panels
        
All orchestrated by Apache Airflow DAG (youtube_data_pipeline)
All services containerized with Docker
```
---
Project Structure
```
youtube_project/
├── Dockerfile                          # Airflow + Java + PySpark image
├── docker-compose.yml                  # All 6 services
├── requirements.txt                    # Python dependencies
├── init_db.sql                         # Creates youtube_analytics DB
│
├── dags/
│   ├── youtube_dag.py                  # Airflow DAG: extract → process → load
│   ├── youtube_data_api.py             # YouTube API extraction module
│   └── youtube_data_spark.py          # PySpark transformation module
│
├── scripts/
│   └── youtube_extractor.py           # Standalone extraction script
│
├── data/                               # Runtime data (git-ignored)
│   ├── Youtube_data.csv               # Raw video stats
│   └── youtube_processed/             # Spark output
│
└── grafana/
    ├── dashboards/
    │   └── grafana_dashboard.json     # 6-panel dashboard
    └── provisioning/
        ├── datasources/
        │   └── postgres.yaml          # Auto-wires PostgreSQL
        └── dashboards/
            └── dashboard.yaml         # Auto-loads dashboard
```
---
Tech Stack
Tool	Version	Purpose
Python	3.11	Core language
YouTube Data API v3	—	Data source
PySpark	3.5.1	Data transformation
Apache Airflow	2.9.1	Workflow orchestration
PostgreSQL	15	Data storage
Grafana	10.4.0	Visualization
Docker	—	Containerization
---
Pipeline Tasks
Task 1 — Data Ingestion (`youtube_data_api.py`)
Connects to YouTube Data API v3
Fetches channel-level stats: subscribers, total views, video count
Fetches video-level stats: title, publish date, views, likes, comments
Saves raw data to `Youtube_data.csv`
Task 2 — Data Processing (`youtube_data_spark.py`)
Loads raw CSV into PySpark DataFrame
Cleans nulls in numeric and string columns
Parses `publishedAt` ISO timestamp
Engineers new features:
`publish_day_name` (Monday, Tuesday …)
`publish_day` (1–7)
`publish_hour` (0–23)
`engagement_rate` = (likes + comments) / views
Writes processed data to `youtube_processed/`
Task 3 — Workflow Orchestration (`youtube_dag.py`)
DAG ID: `youtube_data_pipeline`
Schedule: daily at 06:00 UTC
Tasks: `extract_youtube_data` → `process_youtube_data` → `load_to_postgres`
Retries: 1 retry with 5-minute delay
Task 4 — Visualization (Grafana)
Six dashboard panels:
Panel	Type	Insight
KPI Cards	Stat	Total videos, views, likes, engagement
Top 10 Videos	Bar chart	Most viewed videos
Engagement Rate Over Time	Time series	Engagement trends
Cumulative Views	Time series	Channel growth
Publishing Heatmap	Heatmap	Best day × hour to publish
Average Views by Day	Bar chart	Best day of week
Full Stats Table	Table	All video metrics
---
Key Findings
Most viewed video: Barcelona 5 x 0 Real Madrid — 34.1 Million views
Best day to publish: Friday and Saturday get the highest average views
Peak engagement: Ronaldinho highlights have the highest engagement rate (2.94%)
Channel growth: Steady cumulative view growth since 2005
---

Prerequisites
Docker and Docker Compose installed
YouTube Data API v3 key
1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/youtube_project.git
cd youtube_project
```
2. Set your API credentials (optional — defaults are set)
```bash
export YT_API_KEY="your_api_key_here"
export YT_CHANNEL_ID="your_channel_id_here"
```
3. Fix data folder permissions
```bash
chmod -R 777 data/
```
4. Start all services
```bash
docker-compose up --build -d
```
5. Add PostgreSQL connection in Airflow
```bash
docker exec youtube_airflow_scheduler airflow connections add 'postgres_youtube' \
    --conn-type 'postgres' \
    --conn-host 'postgres' \
    --conn-login 'airflow' \
    --conn-password 'airflow' \
    --conn-schema 'airflow' \
    --conn-port '5432'
```
6. Access the services
Service	URL	Login
Airflow	http://localhost:8082	admin / admin
Grafana	http://localhost:3002	admin / admin
Spark UI	http://localhost:8081	—
PostgreSQL	localhost:5434	airflow / airflow
7. Trigger the DAG
Open Airflow at http://localhost:8082
Find `youtube_data_pipeline`
Click the ▶ Run button
8. Import Grafana Dashboard
Open Grafana at http://localhost:3002
Go to Dashboards → Import
Upload `grafana/dashboards/grafana_dashboard.json`
---
Data Schema
Table: `youtube_video_stats`
Column	Type	Description
video_id	text	YouTube video ID
title	text	Video title
publishedAt	text	ISO publish timestamp
views	bigint	View count
likes	bigint	Like count
comments	bigint	Comment count
publish_day_name	text	Day name (Monday…)
publish_day	bigint	Day number (1–7)
publish_hour	bigint	Hour of day (0–23)
engagement_rate	float	(likes + comments) / views
published_ts	timestamp	Parsed timestamp for Grafana
---
**
