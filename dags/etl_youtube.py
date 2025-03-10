from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import json

import os
from dotenv import load_dotenv

# Constants
POSTGRES_CONN_ID = "postgres_default"
API_CONN_ID = "youtube_api"
REGION_CODE = "IN"

load_dotenv()
API_KEY = os.getenv("YOUTUBE_API_KEY")

# Default Arguments
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

# DAG Definition
with DAG(
    dag_id="youtube_trending_etl",
    default_args=default_args,
    schedule_interval="@hourly",
    catchup=False,
) as dags:

    @task()
    def extract_trending_videos():
        """Extract trending YouTube videos using the API."""
        http_hook = HttpHook(method="GET", http_conn_id=API_CONN_ID)

        # API Endpoint
        # https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&chart=mostPopular&regionCode={IN}&maxResults=50&key={API_KEY}

        endpoint = f"/youtube/v3/videos?part=snippet,statistics&chart=mostPopular&regionCode={REGION_CODE}&maxResults=50&key={API_KEY}"

        # Fetch data
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            data = response.json()
            return data.get("items", [])
        else:
            raise Exception(f"Failed to fetch data: {response.status_code}")

    @task()
    def transform_videos(video_data):
        """Transform YouTube trending video data."""
        transformed = []
        for video in video_data:
            snippet = video["snippet"]
            stats = video["statistics"]
            transformed.append({
                "video_id": video["id"],
                "title": snippet["title"],
                "channel": snippet["channelTitle"],
                "views": int(stats.get("viewCount", 0)),
                "likes": int(stats.get("likeCount", 0)),
                "comments": int(stats.get("commentCount", 0)),
                "published_at": snippet["publishedAt"],
            })
        return transformed

    @task()
    def load_videos_to_postgres(transformed_data):
        """Load transformed data into PostgreSQL."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS youtube_trending3 (
            video_id VARCHAR PRIMARY KEY,
            title TEXT,
            channel TEXT,
            views BIGINT,
            likes BIGINT,
            comments BIGINT,
            published_at TIMESTAMP,
            trending_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  
            last_trending_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  
        );
        """)

        # Insert or update data
        insert_query = """
        INSERT INTO youtube_trending3 (video_id, title, channel, views, likes, comments, published_at, trending_start, last_trending_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (video_id) 
        DO UPDATE 
        SET views = EXCLUDED.views, 
            likes = EXCLUDED.likes, 
            comments = EXCLUDED.comments,
            last_trending_at = CURRENT_TIMESTAMP;
        """

        records = [
            (
                video["video_id"],
                video["title"],
                video["channel"],
                video["views"],
                video["likes"],
                video["comments"],
                video["published_at"],
            )
            for video in transformed_data
        ]

        cursor.executemany(insert_query, records)
        conn.commit()  # Commit changes
        cursor.close()
        conn.close()

    # DAG Workflow
    video_data = extract_trending_videos()
    transformed_data = transform_videos(video_data)
    load_videos_to_postgres(transformed_data)
