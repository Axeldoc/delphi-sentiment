import os
import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

# ----------------- Page setup -----------------
st.set_page_config(page_title="Delphi Sentiment", page_icon="🎮", layout="wide")

# We read the DB URL from Streamlit secrets (preferred) or environment (fallback)
DB_URL = st.secrets.get("SUPABASE_DB_URL") or os.getenv("SUPABASE_DB_URL")

# --- OPTIONAL: force IPv4 to avoid IPv6 DNS issues on some networks ---
def force_ipv4_in_url(url: str) -> str:
    try:
        from urllib.parse import urlparse
        import socket
        p = urlparse(url)
        host = p.hostname
        ipv4 = socket.gethostbyname(host)  # resolve to IPv4
        return url.replace(host, ipv4)
    except Exception:
        return url

DB_URL = force_ipv4_in_url(DB_URL)

if not DB_URL:
    st.error("No database URL found. Add SUPABASE_DB_URL to .streamlit/secrets.toml")
    st.stop()

# ----------------- Small helper to run queries -----------------
@st.cache_data(ttl=300)
def query_df(sql: str, params=None) -> pd.DataFrame:
    conn = psycopg2.connect(DB_URL, sslmode="require")
    try:
        df = pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()
    return df

# ----------------- UI -----------------
st.title("🎮 Delphi Sentiment Dashboard")
st.caption("YouTube comments → VADER sentiment → Supabase")

# Sidebar filters
st.sidebar.header("Filters")
days = st.sidebar.slider("Days back", min_value=3, max_value=90, value=30, step=1)
sentiment_filter = st.sidebar.multiselect(
    "Sentiment",
    options=["positive", "neutral", "negative"],
    default=["positive", "neutral", "negative"],
)

# ----------------- Data -----------------
# This view should exist in your DB. If not, see the note below to create it.
summary_sql = """
select day::date, total, pos, neu, neg
from daily_sentiment_summary
where day >= now() - interval %s
order by day asc;
"""
summary = query_df(summary_sql, params=(f"{days} days",))

comments_sql = """
select c.published_at, c.author, c.body as text, a.sentiment, a.sentiment_score as score
from comments c
join analyses a on a.comment_id = c.id
where a.last = true
  and c.published_at >= now() - interval %s
  and a.sentiment = any(%s)
order by c.published_at desc
limit 500;
"""
sentiments = sentiment_filter or ["positive", "neutral", "negative"]
comments = query_df(comments_sql, params=(f"{days} days", sentiments))

import os
import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

# ----------------- Page setup -----------------
st.set_page_config(page_title="Delphi Sentiment", page_icon="🎮", layout="wide")

# We read the DB URL from Streamlit secrets (preferred) or environment (fallback)
DB_URL = st.secrets.get("SUPABASE_DB_URL") or os.getenv("SUPABASE_DB_URL")

# --- OPTIONAL: force IPv4 to avoid IPv6 DNS issues on some networks ---
def force_ipv4_in_url(url: str) -> str:
    try:
        from urllib.parse import urlparse
        import socket
        p = urlparse(url)
        host = p.hostname
        ipv4 = socket.gethostbyname(host)  # resolve to IPv4
        return url.replace(host, ipv4)
    except Exception:
        return url

DB_URL = force_ipv4_in_url(DB_URL)

if not DB_URL:
    st.error("No database URL found. Add SUPABASE_DB_URL to .streamlit/secrets.toml")
    st.stop()

# ----------------- Small helper to run queries -----------------
@st.cache_data(ttl=300)
def query_df(sql: str, params=None) -> pd.DataFrame:
    conn = psycopg2.connect(DB_URL, sslmode="require")
    try:
        df = pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()
    return df

# ----------------- UI -----------------
st.title("🎮 Delphi Sentiment Dashboard")
st.caption("YouTube comments → VADER sentiment → Supabase")

# Sidebar filters
st.sidebar.header("Filters")
days = st.sidebar.slider("Days back", min_value=3, max_value=90, value=30, step=1)
sentiment_filter = st.sidebar.multiselect(
    "Sentiment",
    options=["positive", "neutral", "negative"],
    default=["positive", "neutral", "negative"],
)

# ----------------- Data -----------------
# This view should exist in your DB. If not, see the note below to create it.
summary_sql = """
select day::date, total, pos, neu, neg
from daily_sentiment_summary
where day >= now() - interval %s
order by day asc;
"""
summary = query_df(summary_sql, params=(f"{days} days",))

comments_sql = """
select c.published_at, c.author, c.body as text, a.sentiment, a.sentiment_score as score
from comments c
join analyses a on a.comment_id = c.id
where a.last = true
  and c.published_at >= now() - interval %s
  and a.sentiment = any(%s)
order by c.published_at desc
limit 500;
"""
sentiments = sentiment_filter or ["positive", "neutral", "negative"]
comments = query_df(comments_sql, params=(f"{days} days", sentiments))

# ----------------- KPIs -----------------
col1, col2, col3, col4 = st.columns(4)
total = int(summary["total"].sum()) if not summary.empty else 0
pos = int(summary["pos"].sum()) if not summary.empty else 0
neu = int(summary["neu"].sum()) if not summary.empty else 0
neg = int(summary["neg"].sum()) if not summary.empty else 0

col1.metric("Total comments", total)
col2.metric("Positive", pos)
col3.metric("Neutral", neu)
col4.metric("Negative", neg)

st.divider()

# ----------------- Chart -----------------
st.subheader("Daily sentiment counts")
if summary.empty:
    st.info("No data in the selected date range yet. Try increasing 'Days back' or wait for the next ingest run.")
else:
    plot_df = summary.melt(
        id_vars="day",
        value_vars=["pos", "neu", "neg"],
        var_name="sentiment",
        value_name="count",
    )
    plot_df["sentiment"] = pd.Categorical(plot_df["sentiment"], ["pos", "neu", "neg"])
    fig = px.area(plot_df, x="day", y="count", color="sentiment", title=None)
    st.plotly_chart(fig, use_container_width=True)

# ----------------- Table -----------------
st.subheader("Recent comments")
if comments.empty:
    st.write("No comments match your filters yet.")
else:
    q = st.text_input("Search text (press Enter to apply)")
    filtered = comments if not q else comments[comments["text"].str.contains(q, case=False, na=False)]
    st.dataframe(filtered, use_container_width=True, height=420)

