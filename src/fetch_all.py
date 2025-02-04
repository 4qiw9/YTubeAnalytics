import logging
import os
from fetch_channel_videos import fetch_videos_from_channel
from fetch_playlist_videos import fetch_videos_from_playlist
from common_logging import setup_logging

setup_logging()

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

CHANNELS_FILE = os.path.join(BASE_DIR, "data", "channels.csv")
PLAYLISTS_FILE = os.path.join(BASE_DIR, "data", "playlists.csv")


def load_ids(file_path):
    if not os.path.exists(file_path):
        logging.warning(f"File {file_path} does not exist. Skipping.")
        return []

    with open(file_path, mode="r", encoding="utf-8") as file:
        ids = []
        for line in file:
            line = line.strip()
            if not line or line.startswith("#"):  # Ignored lines
                continue
            clean_id = line.split("#")[0].strip()  # Ignore comments
            if clean_id:
                ids.append(clean_id)
        return ids

def fetch_all_channels():
    channels = load_ids(CHANNELS_FILE)
    for channel_id in channels:
        logging.info(f"Fetching videos from channel: {channel_id}")
        fetch_videos_from_channel(channel_id, os.path.join(OUTPUT_DIR, "midel.csv"))

def fetch_all_playlists():
    playlists = load_ids(PLAYLISTS_FILE)
    for playlist_id in playlists:
        logging.info(f"Fetching videos from playlist: {playlist_id}")
        fetch_videos_from_playlist(playlist_id, os.path.join(OUTPUT_DIR, "playlist_videos.csv"))

if __name__ == "__main__":
    logging.info("Start fetching...")
    fetch_all_channels()
    fetch_all_playlists()
    logging.info("Fetching ended!")
