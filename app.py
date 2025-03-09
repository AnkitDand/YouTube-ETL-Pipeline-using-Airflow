import streamlit as st
import pandas as pd
import psycopg2

# PostgreSQL Connection Configuration
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",  
    "port": "5432",
}

def get_data():
    conn = psycopg2.connect(**DB_CONFIG)
    query = """
    SELECT title, channel, views, likes, comments, published_at 
    FROM youtube_trending;
    """
    df = pd.read_sql(query, conn)
    conn.close()

    df = df.sort_values(by="views", ascending=False).head(10)
    return df


st.title("YouTube Trending Videos Dashboard 📊")

df = get_data()

st.subheader("Top 10 Most Viewed Videos")
st.dataframe(df)

st.subheader("Top 10 Videos by Views")
st.bar_chart(df.set_index("title")["views"])

st.subheader("Likes vs. Comments")
st.scatter_chart(df.set_index("title")[["likes", "comments"]])