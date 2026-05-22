import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("YOUTUBE_API_KEY")

COMMENTS_URL = "https://www.googleapis.com/youtube/v3/commentThreads"

VIDEO_ID = "mpcYojohzBU"

params = {
    "part": "snippet",
    "videoId": VIDEO_ID,
    "maxResults": 10,
    "textFormat": "plainText",
    "key": API_KEY,
}

response = requests.get(COMMENTS_URL, params=params)

print("Status code:", response.status_code)

data = response.json()

if "error" in data:
    print(data["error"])
else:
    for item in data.get("items", []):
        comment_snippet = item["snippet"]["topLevelComment"]["snippet"]

        comment_id = item["snippet"]["topLevelComment"]["id"]
        author = comment_snippet["authorDisplayName"]
        text = comment_snippet["textDisplay"]
        like_count = comment_snippet["likeCount"]
        published_at = comment_snippet["publishedAt"]

        print("-" * 80)
        print("Comment ID:", comment_id)
        print("Author:", author)
        print("Likes:", like_count)
        print("Published:", published_at)
        print("Text:", text)
