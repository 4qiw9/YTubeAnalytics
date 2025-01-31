import logging

from common import get_youtube_service, save_to_csv, get_channel_id_by_name
from common_logging import setup_logging
from common_cache import get_last_fetched_date, update_last_fetched_date

setup_logging()

def fetch_videos_from_channel(channel_identifier, output_file):
    youtube = get_youtube_service()
    videos = []

    channel_id = channel_identifier
    channel_name = None

    last_fetched_date = get_last_fetched_date("channels", channel_id)
    logging.info(f"Last fetching was for {channel_name} ({channel_id}): {last_fetched_date}")

    if not channel_identifier.startswith("UC"):
        logging.info(f"Looking up channel ID for name: {channel_identifier}")
        channel_id = get_channel_id_by_name(channel_identifier)
        channel_name = channel_identifier
        logging.info(f"Found channel ID: {channel_id}")
    else:
        request = youtube.channels().list(
            part="snippet",
            id=channel_id
        )
        response = request.execute()
        if "items" in response and response["items"]:
            channel_name = response["items"][0]["snippet"]["title"]
        else:
            raise ValueError(f"Channel with ID '{channel_id}' not found.")

    request = youtube.search().list(
        part="snippet",
        channelId=channel_id,
        maxResults=50,
        type="video",
        order="date",
        publishedAfter=last_fetched_date if last_fetched_date else None  # Download only newest videos to save API limits
    )
    while request:
        response = request.execute()
        for item in response['items']:
            video_data = {
                "video_id": item['id']['videoId'],
                "title": item['snippet']['title'],
                "description": item['snippet']['description'],
                "published_at": item['snippet']['publishedAt'],
                "channel_id": channel_id,
                "channel_name": channel_name,
            }
            videos.append(video_data)
        request = youtube.search().list_next(request, response)

    headers = ["video_id", "title", "description", "published_at", "channel_id", "channel_name"]
    save_to_csv(output_file, videos, headers=headers)
    logging.info(f"Fetched {len(videos)} videos from channel: {channel_name} ({channel_id})")

    # if videos exists, then at least one new was added, so cache update required
    if videos:
        newest_date = max(video["published_at"] for video in videos)
        update_last_fetched_date("channels", channel_id, newest_date)
        logging.info(f"📝 Cache updated for {channel_name}: {newest_date}")


if __name__ == "__main__":
    channel_identifier = input("Enter channel name or ID: ")
    output_file = "channel_videos.csv"
    fetch_videos_from_channel(channel_identifier, output_file)
