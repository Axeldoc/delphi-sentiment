import os, requests
from datetime import datetime
from nltk.sentiment.vader import SentimentIntensityAnalyzer

YT_API_KEY = os.environ.get("YT_API_KEY")
VIDEO_ID = os.environ.get("YT_VIDEO_ID")

def yt_comments(video_id, page_token=None):
    url = 'https://www.googleapis.com/youtube/v3/commentThreads'
    params = {
        'part': 'snippet',
        'videoId': video_id,
        'maxResults': 50,
        'key': YT_API_KEY
    }
    if page_token:
        params['pageToken'] = page_token
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()

def main():
    if not YT_API_KEY or not VIDEO_ID:
        print("❌ Missing YT_API_KEY or YT_VIDEO_ID")
        return

    sia = SentimentIntensityAnalyzer()
    counts = {"positive":0,"neutral":0,"negative":0}
    total = 0
    token = None
    pages = 0
    while True and pages < 3:
        data = yt_comments(VIDEO_ID, token)
        for item in data.get('items', []):
            s = item['snippet']['topLevelComment']['snippet']
            text = s.get('textDisplay') or ""
            score = sia.polarity_scores(text)['compound']
            sentiment = 'positive' if score > 0.2 else 'negative' if score < -0.2 else 'neutral'
            counts[sentiment] += 1
            total += 1
        token = data.get('nextPageToken')
        pages += 1
        if not token:
            break

    print(f"✅ Total comments analyzed: {total}")
    for k,v in counts.items():
        print(f"{k.capitalize()}: {v}")

if __name__ == "__main__":
    main()
