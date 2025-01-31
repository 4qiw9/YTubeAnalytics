import csv
import logging

from googleapiclient.discovery import build
import os
from dotenv import load_dotenv
from common_logging import setup_logging

setup_logging()

load_dotenv()
API_KEY = os.getenv("GOOGLE_API_KEY")

def get_youtube_service():
    if not API_KEY:
        raise ValueError("YOUTUBE_API_KEY is not set. Check your .env file.")

    return build("youtube", "v3", developerKey=API_KEY)

def save_to_csv(file_name, data, headers=None, skip_duplicates=True):
    file_exists = os.path.isfile(file_name)

    existing_ids = set()
    if skip_duplicates and file_exists:
        with open(file_name, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                existing_ids.add(row["video_id"])

    new_data = [row for row in data if row["video_id"] not in existing_ids]

    if not new_data:
        logging.info(f"No new video. Skipping save to {file_name}.")
        return

    with open(file_name, mode="a" if file_exists else "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=headers)

        if not file_exists:
            writer.writeheader()

        writer.writerows(new_data)

    logging.info(f"Saved {len(new_data)} new records to {file_name}")


def get_channel_id_by_name(channel_name):
    youtube = get_youtube_service()
    request = youtube.search().list(
        part="snippet",
        q=channel_name,
        type="channel",
        maxResults=1
    )
    response = request.execute()

    if 'items' in response and len(response['items']) > 0:
        return response['items'][0]['snippet']['channelId']
    else:
        raise ValueError(f"Channel with name '{channel_name}' not found.")
