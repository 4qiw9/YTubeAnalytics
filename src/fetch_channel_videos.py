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

    # Find ID of hidden "Uploads" playlist
    # cannot catch data directly from channel because it's limited to ~500 records
    request = youtube.channels().list(
        part="contentDetails",
        id=channel_id
    )
    response = request.execute()

    if "items" not in response or not response["items"]:
        raise ValueError(f"Failed to retrieve 'Uploads' playlist for {channel_name}")

    uploads_playlist_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    logging.info(f"Uploads playlist ID for {channel_name}: {uploads_playlist_id}")

    # Fetching all videos for "Uploads" playlist
    request = youtube.playlistItems().list(
        part="snippet",
        playlistId=uploads_playlist_id,
        maxResults=50
    )

    while request:
        response = request.execute()
        for item in response.get("items", []):
            snippet = item["snippet"]
            video_data = {
                "video_id": snippet["resourceId"]["videoId"],
                "title": snippet["title"],
                "description": snippet["description"],
                "published_at": snippet["publishedAt"],
                "channel_id": channel_id,
                "channel_name": channel_name,
            }
            videos.append(video_data)
        request = youtube.playlistItems().list_next(request, response)

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
