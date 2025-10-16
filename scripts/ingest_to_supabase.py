import os
import re
import yaml
import requests
import psycopg2
from datetime import datetime
from nltk.sentiment.vader import SentimentIntensityAnalyzer

DB_URL = os.environ.get("SUPABASE_DB_URL")
YT_API_KEY = os.environ.get("YT_API_KEY")
VIDEO_ID = os.environ.get("YT_VIDEO_ID")

# ---------- Load topic rules ----------
def load_topics_map(path="config/topics.yml"):
    try:
        with open(path, "r") as f:
            raw = yaml.safe_load(f) or {}
        compiled = {}
        for topic, kws in raw.items():
            pats = [re.compile(rf"\b{re.escape(kw)}\\b", re.IGNORECASE) for kw in kws]
            compiled[topic] = pats
        return compiled
    except FileNotFoundError:
        return {}

TOPIC_PATTERNS = load_topics_map()

print("Loaded topics map:", {k: len(v) for k, v in TOPIC_PATTERNS.items()})

def extract_topics(text: str):
    """Return a list of topic names that appear in text."""
    if not text:
        return []
    hits = []
    for topic, patterns in TOPIC_PATTERNS.items():
        if any(p.search(text) for p in patterns):
            hits.append(topic)
    return hits

# ---------- YouTube API ----------
def fetch_comments(video_id, page_token=None):
    url = "https://www.googleapis.com/youtube/v3/commentThreads"
    params = {
        "part": "snippet",
        "videoId": video_id,
        "maxResults": 100,
        "key": YT_API_KEY,
    }
    if page_token:
        params["pageToken"] = page_token
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

# ---------- Main ----------
def main():
    if not (DB_URL and YT_API_KEY and VIDEO_ID):
        print("❌ Missing one of: SUPABASE_DB_URL, YT_API_KEY, YT_VIDEO_ID")
        return

    sia = SentimentIntensityAnalyzer()
    conn = psycopg2.connect(DB_URL, sslmode="require")
    conn.autocommit = True
    cur = conn.cursor()

    count_comments = count_analyses = 0
    token = None

    for _ in range(5):  # fetch up to ~500 comments
        data = fetch_comments(VIDEO_ID, token)
        for item in data.get("items", []):
            snippet = item["snippet"]["topLevelComment"]["snippet"]
            text = snippet.get("textDisplay", "")
            author = snippet.get("authorDisplayName")
            published_at = snippet.get("publishedAt")
            comment_id = item["id"]

            # sentiment
            score = sia.polarity_scores(text)["compound"]
            sentiment = "positive" if score > 0.2 else "negative" if score < -0.2 else "neutral"
        
        # topic extraction
        topics = extract_topics(text) or None

        if topics:
            print("MATCH ->", topics, "|", text[:80].replace("\n", " ") + ("…" if len(text) > 80 else ""))



            # insert comment
            cur.execute("""
                INSERT INTO comments (video_id, author, body, published_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                RETURNING id
            """, (VIDEO_ID, author, text, published_at))
            row = cur.fetchone()
            if row is None:
                cur.execute("SELECT id FROM comments WHERE video_id=%s AND body=%s LIMIT 1", (VIDEO_ID, text))
                row = cur.fetchone()
            cid = row[0]
            count_comments += 1

            # insert/update analysis
            cur.execute("""
                INSERT INTO analyses (comment_id, sentiment, sentiment_score, topics, last)
                VALUES (%s, %s, %s, %s, TRUE)
                ON CONFLICT (comment_id) DO UPDATE
                SET sentiment = EXCLUDED.sentiment,
                    sentiment_score = EXCLUDED.sentiment_score,
                    topics = EXCLUDED.topics,
                    last = TRUE;
            """, (cid, sentiment, score, topics))
            count_analyses += 1

        token = data.get("nextPageToken")
        if not token:
            break

    conn.close()
    print(f"✅ Upserted comments: {count_comments}, analyses created: {count_analyses}")

if __name__ == "__main__":
    main()

