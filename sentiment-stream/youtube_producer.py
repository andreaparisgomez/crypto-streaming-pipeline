import os
import json
import time
import requests
from datetime import datetime, UTC
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

KAFKA_TOPIC = "youtube_raw_comments"
FETCH_INTERVAL = 60

COMMENTS_URL = "https://www.googleapis.com/youtube/v3/commentThreads"


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"

SEARCH_QUERIES = [
    "bitcoin crypto",
    "ethereum crypto",
    "altcoin news"
]

def search_crypto_videos(query, max_results=5):
    params = {
        "part": "snippet",
        "q": query,
        "type": "video",
        "maxResults": max_results,
        "key": YOUTUBE_API_KEY,
    }

    response = requests.get(SEARCH_URL, params=params)
    data = response.json()

    if response.status_code != 200:
        print("YouTube search error:", data)
        return []

    videos = []

    for item in data.get("items", []):
        videos.append({
            "video_id": item["id"]["videoId"],
            "video_title": item["snippet"]["title"],
            "channel_title": item["snippet"]["channelTitle"],
            "source_query": query,
        })

    return videos


def fetch_youtube_comments(video_id, max_results=10):
    params = {
        "part": "snippet",
        "videoId": video_id,
        "maxResults": max_results,
        "textFormat": "plainText",
        "key": YOUTUBE_API_KEY,
    }

    response = requests.get(COMMENTS_URL, params=params)
    data = response.json()

    if response.status_code != 200:
        error_reason = (
            data.get("error", {})
                .get("errors", [{}])[0]
                .get("reason")
       )

        if error_reason == "commentsDisabled":
            print(f"Skipping video {video_id}: comments disabled")
            return []

        print("YouTube API error:", data)
        return []

    return data.get("items", [])


def build_comment_event(item, video):
    comment = item["snippet"]["topLevelComment"]
    snippet = comment["snippet"]

    return {
        "platform": "youtube",
        "content_type": "comment",
        "video_id": video["video_id"],
        "video_title": video["video_title"],
        "channel_title": video["channel_title"],
        "comment_id": comment["id"],
        "author": snippet.get("authorDisplayName"),
        "comment_text": snippet.get("textDisplay"),
        "like_count": snippet.get("likeCount"),
        "published_at": snippet.get("publishedAt"),
        "ingested_at": datetime.now(UTC).isoformat(),
        "source_query": video["source_query"],
    }

seen_comment_ids = set()
skipped_video_ids = set()

while True:
    for query in SEARCH_QUERIES:
        videos = search_crypto_videos(query)

        for video in videos:
            if video["video_id"] in skipped_video_ids:
                continue

            comments = fetch_youtube_comments(video["video_id"])
            
            if comments == []:
                skipped_video_ids.add(video["video_id"])
                continue

            for item in comments:
                event = build_comment_event(item, video)

                if event["comment_id"] not in seen_comment_ids:
                    producer.send(KAFKA_TOPIC, value=event)
                    print(f"Sent comment: {event['comment_text'][:80]}")
                    seen_comment_ids.add(event["comment_id"])
                    print("Message sent to Kafka:", event["comment_id"])

    producer.flush()

    print("Waiting for next fetch cycle...")
    time.sleep(FETCH_INTERVAL)
