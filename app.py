import streamlit as st
import pandas as pd
import psycopg2
import altair as alt


# PostgreSQL Connection Configuration
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432",
}


def get_data():
    """Fetch top 10 trending videos based on hours trending."""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    query = """
    SELECT 
        title, 
        channel, 
        views, 
        likes, 
        comments, 
        published_at, 
        EXTRACT(EPOCH FROM (MAX(last_trending_at) - MIN(trending_start))) / 3600 AS hours_trending
    FROM youtube_trending3
    GROUP BY title, channel, views, likes, comments, published_at
    ORDER BY hours_trending DESC
    """

    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()

    df = pd.DataFrame(rows, columns=[
                      "title", "channel", "views", "likes", "comments", "published_at", "hours_trending"])
    df["engagement"] = df["likes"] + df["comments"]
    return df


st.title("YouTube Trending Videos Dashboard 📊")
df = get_data()

top_videos = df.nlargest(10, "views")[
    ["title", "channel", "views", "engagement", "likes", "comments"]
].dropna(subset=["channel", "title", "views", "engagement", "likes", "comments"])

# Bar Chart: Most Viewed Videos
st.subheader("Top 10 Most Viewed Videos")

bar_chart = alt.Chart(top_videos).mark_bar().encode(
    x=alt.X("views:Q", title="Views"),
    y=alt.Y("title:N", sort="-x", title="Video Title"),
    color=alt.Color("channel:N", title="Channel"),
    tooltip=["title", "channel", "views"]
).properties(
    height=400
)

st.altair_chart(bar_chart, use_container_width=True)

# Bar Chart: Most Hours Trending Videos
st.subheader("Top 10 Most Hours Trending Videos")

hours_chart = alt.Chart(df.nlargest(10, "hours_trending")).mark_bar().encode(
    x=alt.X("hours_trending:Q", title="Hours Trending"),
    y=alt.Y("title:N", sort="-x", title="Video Title"),
    color=alt.Color("channel:N", title="Channel"),
    tooltip=["title", "channel", "hours_trending"]
).properties(
    height=400
)
st.altair_chart(hours_chart, use_container_width=True)

# Scatter Plot: Engagement vs Views
st.subheader(
    "Views vs. Engagement (Likes + Comments) - Top 10 Most Viewed Videos")

scatter_chart = alt.Chart(top_videos).mark_circle(size=100).encode(
    x=alt.X("views", title="Views"),
    y=alt.Y("engagement", title="Engagement (Likes + Comments)"),
    size="engagement",
    color="title",
    tooltip=["title", "views", "engagement", "likes", "comments"]
).interactive()

st.altair_chart(scatter_chart, use_container_width=True)