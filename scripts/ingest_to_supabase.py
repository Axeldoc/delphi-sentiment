import os, requests, psycopg2
from datetime import datetime, timezone
from nltk.sentiment.vader import SentimentIntensityAnalyzer

DB_URL     = os.environ.get("SUPABASE_DB_URL")
YT_API_KEY = os.environ.get("YT_API_KEY")
VIDEO_ID   = os.environ.get("YT_VIDEO_ID")
GAME_SLUG  = "james-bond-first-light"

def yt_comments(video_id, page_token=None):
    url = 'https://www.googleapis.com/youtube/v3/commentThreads'
    params = {'part':'snippet','videoId':video_id,'maxResults':100,'key':YT_API_KEY}
    if page_token: params['pageToken'] = page_token
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()

def upsert_game_and_source(cur):
    cur.execute("""
        insert into games (slug, title)
        values (%s, %s)
        on conflict (slug) do nothing
        returning id
    """, (GAME_SLUG, "James Bond: First Light"))
    row = cur.fetchone()
    if row is None:
        cur.execute("select id from games where slug=%s", (GAME_SLUG,))
        row = cur.fetchone()
    game_id = row[0]

    video_url = f"https://www.youtube.com/watch?v={VIDEO_ID}"
    cur.execute("""
        insert into sources (game_id, platform, url, external_id)
        values (%s, 'youtube', %s, %s)
        on conflict do nothing
        returning id
    """, (game_id, video_url, VIDEO_ID))
    row = cur.fetchone()
    if row is None:
        cur.execute("select id from sources where game_id=%s and platform='youtube' and external_id=%s", (game_id, VIDEO_ID))
        row = cur.fetchone()
    source_id = row[0]
    return game_id, source_id

def main():
    if not (DB_URL and YT_API_KEY and VIDEO_ID):
        print("❌ Missing one of: SUPABASE_DB_URL, YT_API_KEY, YT_VIDEO_ID")
        return

    conn = psycopg2.connect(DB_URL, sslmode="require")
    conn.autocommit = True
    cur = conn.cursor()
    sia = SentimentIntensityAnalyzer()

    game_id, source_id = upsert_game_and_source(cur)

    inserted, analyzed = 0, 0
    token = None
    pages = 0
    while True and pages < 5:  # ~500 comments max
        data = yt_comments(VIDEO_ID, token)
        for item in data.get('items', []):
            s = item['snippet']['topLevelComment']['snippet']
            text = s.get('textDisplay') or ""
            author = s.get('authorDisplayName')
            published = s.get('publishedAt')
            published_at = datetime.fromisoformat(published.replace('Z','+00:00')) if published else None
            ext_cid = item['snippet']['topLevelComment']['id']

            # Insert comment (ignore if exists)
            cur.execute("""
                insert into comments (source_id, external_comment_id, author, body, published_at)
                values (%s,%s,%s,%s,%s)
                on conflict (source_id, external_comment_id) do nothing
                returning id
            """, (source_id, ext_cid, author, text, published_at))
            row = cur.fetchone()
            if row is None:
                cur.execute("select id from comments where source_id=%s and external_comment_id=%s", (source_id, ext_cid))
                row = cur.fetchone()
            comment_id = row[0]
            inserted += 1 if row else 0

            # Analyze & store
            score = sia.polarity_scores(text)['compound']
            sentiment = 'positive' if score > 0.2 else 'negative' if score < -0.2 else 'neutral'
            cur.execute("""
                insert into analyses (comment_id, sentiment, sentiment_score, topics, last)
                values (%s,%s,%s,%s,true)
            """, (comment_id, sentiment, score, None))
            analyzed += 1

        token = data.get('nextPageToken')
        pages += 1
        if not token:
            break

    cur.close(); conn.close()
    print(f"✅ Upserted comments: {inserted}, analyses created: {analyzed}")

if __name__ == "__main__":
    main()
