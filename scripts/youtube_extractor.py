import requests
import json
import csv
import pandas as pd

# ── Configuration ────────────────────────────────────────────────────────────
CHANNEL_ID = "UCzdsjuqqfd4-f7gtqcLBOyg"
API_KEY    = "AIzaSyCppoUV06LXKMzj7x7LoLGA7IOo5RqHnFM"


# ── 1. Channel Stats ─────────────────────────────────────────────────────────
def channel_stats(channel_id=CHANNEL_ID, api_key=API_KEY):
    """
    Fetch channel-level statistics and return them as a dictionary.
    Also returns the uploads playlist ID needed to list all videos.
    """
    url = (
        f"https://www.googleapis.com/youtube/v3/channels"
        f"?part=snippet,statistics,contentDetails"
        f"&id={channel_id}&key={api_key}"
    )
    response = requests.get(url)
    data = response.json()

    if "items" not in data:
        raise Exception(f"Channel not found or API error: {data}")

    item = data["items"][0]
    return {
        "channel_title":       item["snippet"]["title"],
        "subscribers":         int(item["statistics"].get("subscriberCount", 0)),
        "total_views":         int(item["statistics"].get("viewCount", 0)),
        "total_videos":        int(item["statistics"].get("videoCount", 0)),
        "uploads_playlist_id": item["contentDetails"]["relatedPlaylists"]["uploads"],
    }


# ── 2. All Video IDs from Uploads Playlist ───────────────────────────────────
def all_video_ids(playlist_id, api_key=API_KEY):
    """
    Page through a channel's uploads playlist and collect every video ID.
    Returns a list of video ID strings.
    """
    video_ids = []
    base_url  = "https://www.googleapis.com/youtube/v3/playlistItems"
    params    = {
        "part":       "snippet",
        "playlistId": playlist_id,
        "maxResults": 50,
        "key":        api_key,
    }

    while True:
        response = requests.get(base_url, params=params).json()

        for item in response.get("items", []):
            video_ids.append(item["snippet"]["resourceId"]["videoId"])

        next_page_token = response.get("nextPageToken")
        if next_page_token:
            params["pageToken"] = next_page_token
        else:
            break

    return video_ids


# ── 3. Video Details (views, likes, comments …) ──────────────────────────────
def video_details(video_ids, api_key=API_KEY):
    """
    Fetch snippet + statistics for every video ID (in batches of 50).
    Returns a list of dicts, one per video.
    """
    videos = []

    for i in range(0, len(video_ids), 50):
        batch_ids = ",".join(video_ids[i : i + 50])
        url = (
            f"https://www.googleapis.com/youtube/v3/videos"
            f"?part=snippet,statistics&id={batch_ids}&key={api_key}"
        )
        res = requests.get(url).json()

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


# ── 4. Save to CSV ───────────────────────────────────────────────────────────
def save_to_csv(data, filename="Youtube_data.csv"):
    """
    Accept a list of dicts (or a DataFrame) and write it to CSV.
    """
    df = pd.DataFrame(data) if not isinstance(data, pd.DataFrame) else data
    df.to_csv(filename, index=False, encoding="utf-8")
    print(f"Saved {len(df)} rows → {filename}")


# ── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":

    # 1. Channel stats
    stats = channel_stats()
    print("\n── Channel Stats ──────────────────────────")
    print(stats)
    df_channel = pd.DataFrame([stats])
    print(df_channel.head())

    # 2. All video IDs
    playlist_id  = stats["uploads_playlist_id"]
    video_ids    = all_video_ids(playlist_id)
    df_video_ids = pd.DataFrame(video_ids, columns=["video_id"])
    print(f"\n── Video IDs fetched: {len(video_ids)} ──────────────────────────")
    print(df_video_ids.head(5))

    # 3. Video details
    video_id_list = df_video_ids["video_id"].tolist()
    video_data    = video_details(video_id_list)
    df_videos     = pd.DataFrame(video_data)
    print("\n── Video Details (first 6 rows) ───────────")
    print(df_videos.head(6))

    # 4. Save to CSV
    save_to_csv(df_videos)
