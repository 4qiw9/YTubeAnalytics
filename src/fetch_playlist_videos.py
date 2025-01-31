import logging

from common import get_youtube_service, save_to_csv
from common_logging import setup_logging
from common_cache import get_last_fetched_date, update_last_fetched_date

setup_logging()

def fetch_videos_from_playlist(playlist_id, output_file):
    youtube = get_youtube_service()
    videos = []

    playlist_info = youtube.playlists().list(
        part="snippet",
        id=playlist_id
    ).execute()

    if "items" in playlist_info and playlist_info["items"]:
        channel_id = playlist_info["items"][0]["snippet"]["channelId"]
        channel_name = playlist_info["items"][0]["snippet"]["channelTitle"]
    else:
        raise ValueError(f"Playlist with ID '{playlist_id}' not found.")

    last_fetched_date = get_last_fetched_date("playlists", playlist_id)
    logging.info(f"Last fetching for playlist {playlist_id} ({channel_name}): {last_fetched_date}")

    request = youtube.playlistItems().list(
        part="snippet",
        playlistId=playlist_id,
        maxResults=50
    )
    while request:
        response = request.execute()
        for item in response['items']:
            video_id = item['snippet']['resourceId']['videoId']
            published_at = item['snippet']['publishedAt']

            if last_fetched_date and published_at <= last_fetched_date:
                continue  # skip because (by cache data) it has been already downloaded

            video_data = {
                "video_id": video_id,
                "title": item['snippet']['title'],
                "description": item['snippet']['description'],
                "published_at": published_at,
                "channel_id": channel_id,
                "channel_name": channel_name,
            }
            videos.append(video_data)

        request = youtube.playlistItems().list_next(request, response)

    headers = ["video_id", "title", "description", "published_at", "channel_id", "channel_name"]
    save_to_csv(output_file, videos, headers=headers)
    logging.info(f"Fetched {len(videos)} videos from playlist: {playlist_id} (Channel: {channel_name} - {channel_id})")

    if videos:
        newest_date = max(video["published_at"] for video in videos)
        update_last_fetched_date("playlists", playlist_id, newest_date)
        logging.info(f"Cache updated for playlist {playlist_id}: {newest_date}")

if __name__ == "__main__":
    playlist_id = input("Enter playlist ID: ")
    output_file = "playlist_videos.csv"
    fetch_videos_from_playlist(playlist_id, output_file)
