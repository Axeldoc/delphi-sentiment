import os, re, yaml, psycopg2

DB_URL = os.environ.get("SUPABASE_DB_URL")

# --- load topic rules (same as ingest) ---
def load_topics_map(path="config/topics.yml"):
    with open(path, "r") as f:
        raw = yaml.safe_load(f) or {}
    compiled = {}
    for topic, kws in raw.items():
        compiled[topic] = [re.compile(rf"\b{re.escape(kw)}\b", re.IGNORECASE) for kw in kws]
    return compiled

def extract_topics(text: str, patterns) -> list[str]:
    if not text:
        return []
    hits = []
    for topic, pats in patterns.items():
        if any(p.search(text) for p in pats):
            hits.append(topic)
    return hits

def main():
    if not DB_URL:
        raise SystemExit("Missing SUPABASE_DB_URL")
    patterns = load_topics_map()
    print("Loaded topics:", {k: len(v) for k,v in patterns.items()})

    conn = psycopg2.connect(DB_URL, sslmode="require")
    conn.autocommit = True
    cur = conn.cursor()

    # fetch comments + analyses that need topics (NULL)
    cur.execute("""
        select a.comment_id, c.body
        from analyses a
        join comments c on c.id = a.comment_id
        where a.last = true and (a.topics is null or cardinality(a.topics)=0)
    """)
    rows = cur.fetchall()
    print(f"Found {len(rows)} analyses without topics")

    updated = 0
    for comment_id, body in rows:
        topics = extract_topics(body or "", patterns)
        if topics:
            cur.execute(
                "update analyses set topics=%s where comment_id=%s and last=true",
                (topics, comment_id)
            )
            updated += 1
    print(f"✅ Backfill complete. Updated {updated} analyses with topics")

    cur.close(); conn.close()

if __name__ == "__main__":
    main()

