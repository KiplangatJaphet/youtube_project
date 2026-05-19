-- Runs automatically on first postgres container start
CREATE DATABASE youtube_analytics;
GRANT ALL PRIVILEGES ON DATABASE youtube_analytics TO airflow;
