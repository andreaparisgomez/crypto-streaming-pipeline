import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("YOUTUBE_API_KEY")

SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"

params = {
    "part": "snippet",
    "q": "bitcoin crypto",
    "type": "video",
    "maxResults": 5,
    "key": API_KEY,
}

response = requests.get(SEARCH_URL, params=params)

print("Status code:", response.status_code)

data = response.json()

for item in data.get("items", []):
    video_id = item["id"]["videoId"]
    title = item["snippet"]["title"]
    channel = item["snippet"]["channelTitle"]

    print(video_id, "|", channel, "|", title)
