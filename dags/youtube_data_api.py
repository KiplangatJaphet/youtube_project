"""
youtube_data_api.py
────────────────────────────────────────────────────────────────────────────────
YouTube Data API v3 — Extraction Module

Exposes run_youtube_api_extraction() which is imported and called by the
Airflow DAG (youtube_dag.py).  All other functions are internal helpers.
────────────────────────────────────────────────────────────────────────────────
"""

import requests
import pandas as pd

# ── Configuration ─────────────────────────────────────────────────────────────
API_KEY    = "AIzaSyCppoUV06LXKMzj7x7LoLGA7IOo5RqHnFM"
CHANNEL_ID = "UCzdsjuqqfd4-f7gtqcLBOyg"

# Linux path — works both locally (WSL/Linux) and inside Docker containers
OUTPUT_CSV = "/opt/airflow/data/Youtube_data.csv"


# ── Helper: channel-level stats ───────────────────────────────────────────────
def channel_stats(channel_id=CHANNEL_ID, api_key=API_KEY):
    """Return a dict of channel metadata including the uploads playlist ID."""
    url = (
        "https://www.googleapis.com/youtube/v3/channels"
        f"?part=snippet,statistics,contentDetails&id={channel_id}&key={api_key}"
    )
    data = requests.get(url).json()

    if "items" not in data:
        raise Exception(f"Channel not found or API error: {data}")

    item = data["items"][0]
    return {
        "channel_title":       item["snippet"]["title"],
        "subscribers":         int(item["statistics"].get("subscriberCount", 0)),
        "total_views":         int(item["statistics"].get("viewCount",       0)),
        "total_videos":        int(item["statistics"].get("videoCount",      0)),
        "uploads_playlist_id": item["contentDetails"]["relatedPlaylists"]["uploads"],
    }


# ── Helper: collect all video IDs from uploads playlist ───────────────────────
def all_video_ids(playlist_id, api_key=API_KEY):
    """Page through a playlist and return every video ID as a list."""
    video_ids = []
    params    = {
        "part":       "snippet",
        "playlistId": playlist_id,
        "maxResults": 50,
        "key":        api_key,
    }
    while True:
        resp  = requests.get(
            "https://www.googleapis.com/youtube/v3/playlistItems", params=params
        ).json()
        for item in resp.get("items", []):
            video_ids.append(item["snippet"]["resourceId"]["videoId"])
        token = resp.get("nextPageToken")
        if not token:
            break
        params["pageToken"] = token

    return video_ids


# ── Helper: video-level stats (batched 50 at a time) ─────────────────────────
def video_details(video_ids, api_key=API_KEY):
    """Return a list of dicts — one row per video — with views/likes/comments."""
    videos = []
    for i in range(0, len(video_ids), 50):
        batch = ",".join(video_ids[i : i + 50])
        res   = requests.get(
            f"https://www.googleapis.com/youtube/v3/videos"
            f"?part=snippet,statistics&id={batch}&key={api_key}"
        ).json()
        for item in res.get("items", []):
            snippet = item["snippet"]
            stats   = item.get("statistics", {})
            videos.append({
                "video_id":    item["id"],
                "title":       snippet["title"],
                "publishedAt": snippet["publishedAt"],
                "views":       int(stats.get("viewCount",    0)),
                "likes":       int(stats.get("likeCount",    0)),
                "comments":    int(stats.get("commentCount", 0)),
            })
    return videos


# ── Airflow callable ──────────────────────────────────────────────────────────
def run_youtube_api_extraction():
    """
    Entry point called by the Airflow PythonOperator.
    Orchestrates channel → video IDs → video details → CSV.
    """
    import os
    print("Starting YouTube API extraction...")

    channel_data = channel_stats()
    print(f"Channel : {channel_data['channel_title']}")
    print(f"Videos  : {channel_data['total_videos']}")

    video_ids = all_video_ids(channel_data["uploads_playlist_id"])
    print(f"Video IDs fetched: {len(video_ids)}")

    videos = video_details(video_ids)
    print(f"Video details fetched: {len(videos)}")

    if not videos:
        raise ValueError("No video data retrieved — aborting task.")

    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    pd.DataFrame(videos).to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"YouTube data saved → {OUTPUT_CSV}")


# ── Standalone test ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_youtube_api_extraction()
