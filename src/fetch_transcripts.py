import os
import logging
import time

import pandas as pd
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound, TooManyRequests
from common_logging import setup_logging

from common_cache import should_retry, record_failed_attempt, record_successful_attempt, load_failed_cache


setup_logging(script_name="fetch_transcripts")

TRANSCRIPTS_DIR = os.path.join(os.path.dirname(__file__), "../output/transcripts")
VIDEO_CSV_PATH = os.path.join(os.path.dirname(__file__), "../output/analyze_list.csv")

LANGUAGE_CODES = ['pl']
MAX_WORKERS = 5

os.makedirs(TRANSCRIPTS_DIR, exist_ok=True)

# Load cache with failed download to reduce I/O
load_failed_cache()

def sanitize_filename(name):
    sanitized = name.replace(' ', '_')
    return re.sub(r'[<>:"/\\|?*]', '', sanitized)[:50]


def load_video_data(csv_path):
    if not os.path.exists(csv_path):
        logging.error(f"Input CSV file does not exist: {csv_path}")
        return []

    df = pd.read_csv(csv_path)
    return list(df[["video_id", "channel_id", "channel_name", "published_at"]].itertuples(index=False, name=None))


def download_transcript(video_id, language_codes=LANGUAGE_CODES):
    for lang in language_codes:
        try:
            transcript = YouTubeTranscriptApi.get_transcript(video_id, languages=[lang])
            logging.debug(f"✅ Transcript found for video {video_id} in language '{lang}'")
            return transcript
        except TranscriptsDisabled:
            logging.debug(f"❌ Transcripts are disabled for video {video_id}.")
            return None
        except NoTranscriptFound:
            logging.debug(f"❌ No transcript available for video {video_id}.")
            return None
        except TooManyRequests:
            logging.debug(f"🚨 Too many requests! YouTube API is blocking requests for video {video_id}.")
            time.sleep(10)  # wait 10s for next request (but only on this thread!)
            return None
        except Exception as e:
            logging.error(f"❌❌ Unexpected error while fetching transcript for {video_id}: {e}")
            return None
    logging.debug(f"❌ No transcripts available for video {video_id} in {language_codes}")
    return None


def save_transcript(channel_id, video_id, transcript):
    if transcript is None:
        return False

    channel_dir = os.path.join(TRANSCRIPTS_DIR, sanitize_filename(channel_id))
    os.makedirs(channel_dir, exist_ok=True)
    transcript_path = os.path.join(channel_dir, f"{video_id}.txt")

    lines = []
    for entry in transcript:
        start_time = entry['start']
        minutes, seconds = divmod(int(start_time), 60)
        formatted_time = f"[{minutes}:{seconds:02d}]"
        lines.append(f"{formatted_time} {entry['text']}")

    try:
        with open(transcript_path, "w", encoding="utf-8") as file:
            file.write("\n".join(lines))
        logging.debug(f"Transcript saved for video {video_id} -> {transcript_path}")
        return True
    except Exception as e:
        logging.error(f"Failed to save transcript for {video_id}: {e}")
        return False


def process_video(video):
    video_id, channel_id, channel_name, published_at = video

    if not should_retry(video_id): # failed download caching: check
        return channel_name, False, f"🚫 Skipping {video_id} from {channel_name} ({channel_id}) published at {published_at} - too many failed attempts."

    transcript_path = os.path.join(TRANSCRIPTS_DIR, sanitize_filename(channel_id), f"{video_id}.txt")

    if os.path.exists(transcript_path):
        return channel_name, True, f"⏩ Already exists: {video_id} from {channel_name} ({channel_id}) published at {published_at}"

    transcript = download_transcript(video_id, LANGUAGE_CODES)
    if transcript:
        success = save_transcript(channel_id, video_id, transcript)
        if success:
            record_successful_attempt(video_id) # failed download caching: removal
            return channel_name, True, f"✅ Downloaded transcript: {video_id} from {channel_name} ({channel_id}) published at {published_at}"
        else:
            return channel_name, False, f"❌❌ Failed to save transcript: {video_id} from {channel_name} ({channel_id}) published at {published_at}"
    else:
        record_failed_attempt(video_id) # failed download caching: update
        return channel_name, False, f"❌ No transcript available: {video_id} from {channel_name} ({channel_id}) published at {published_at}"


def fetch_transcripts():
    video_data = load_video_data(VIDEO_CSV_PATH)
    logging.info(f"🎥 Found {len(video_data)} videos to process.")

    downloaded = {}
    missing_transcripts = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_video, video): video for video in video_data}

        for future in as_completed(futures):
            try:
                channel_name, success, message = future.result()
                logging.info(message)
                if success:
                    downloaded[channel_name] = downloaded.get(channel_name, 0) + 1
                else:
                    missing_transcripts[channel_name] = missing_transcripts.get(channel_name, 0) + 1
            except Exception as e:
                video = futures[future]
                logging.error(f"❌ Error processing video {video[0]}: {e}")

    logging.info("📌 Transcript download process finished")
    logging.info(f"✅ Downloaded transcripts: {downloaded}")
    logging.info(f"❌ Missing transcripts: {missing_transcripts}")


if __name__ == "__main__":
    fetch_transcripts()
