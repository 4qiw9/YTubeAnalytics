import os
import pandas as pd
import logging
import argparse
from datetime import datetime
from src.common_logging import setup_logging

setup_logging()

# Default paths
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CHANNEL_VIDEOS_CSV = os.path.join(BASE_DIR, "output", "channel_videos.csv")
PLAYLIST_VIDEOS_CSV = os.path.join(BASE_DIR, "output", "playlist_videos.csv")
OUTPUT_CSV = os.path.join(BASE_DIR, "output", "analyze_list.csv")

DEFAULT_KEYWORDS = None
DEFAULT_CHANNELS = None
DEFAULT_START_DATE = "2024-01-01"
DEFAULT_END_DATE = None

def load_video_data():
    logging.info("🔄 Loading video lists...")
    df_channels = pd.read_csv(CHANNEL_VIDEOS_CSV, dtype=str) if os.path.exists(CHANNEL_VIDEOS_CSV) else pd.DataFrame()
    df_playlists = pd.read_csv(PLAYLIST_VIDEOS_CSV, dtype=str) if os.path.exists(
        PLAYLIST_VIDEOS_CSV) else pd.DataFrame()

    df = pd.concat([df_channels, df_playlists], ignore_index=True)
    if df.empty:
        logging.warning("⚠️ No video data found!")
        return None

    logging.info(f"📂 Loaded {len(df)} video records.")
    return df


def filter_videos(df, keywords, channels, start_date, end_date):
    conditions = []

    # Filter by keywords (title OR description)
    if keywords:
        keyword_pattern = "|".join(keywords)
        keyword_condition = (
                df["title"].str.contains(keyword_pattern, case=False, na=False) |
                df["description"].str.contains(keyword_pattern, case=False, na=False)
        )
        conditions.append(keyword_condition)

    # Filter by channel names
    if channels:
        channel_condition = df["channel_name"].isin(channels)
        conditions.append(channel_condition)

    # Filter by date range
    if start_date:
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce").dt.tz_localize(None)
        conditions.append(df["published_at"] >= start_date)

    if end_date:
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
        conditions.append(df["published_at"] <= end_date)

    # Apply all filters
    if conditions:
        df_filtered = df.loc[pd.concat(conditions, axis=1).all(axis=1)]
    else:
        df_filtered = df

    logging.info(f"✅ After filtering, {len(df_filtered)} videos remain.")
    return df_filtered


def generate_analyze_list(keywords, channels, start_date, end_date, output_csv):
    df = load_video_data()
    if df is None:
        return

    df_filtered = filter_videos(df, keywords, channels, start_date, end_date)
    df_filtered.to_csv(output_csv, index=False, encoding="utf-8")

    logging.info(f"✅ Saved {len(df_filtered)} videos to {output_csv}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Filter video list for analysis")

    parser.add_argument("--keywords", nargs="+",
                        default=DEFAULT_KEYWORDS,
                        help="List of keywords to filter videos by (title or description)")
    parser.add_argument("--channels", nargs="+",
                        default=DEFAULT_CHANNELS,
                        help="List of channel names to filter videos by")
    parser.add_argument("--start-date", type=str,
                        default=DEFAULT_START_DATE,
                        help="Start date for filtering videos (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str,
                        default=DEFAULT_END_DATE,
                        help="End date for filtering videos (YYYY-MM-DD, optional)")
    parser.add_argument("--output", type=str,
                        default=OUTPUT_CSV,
                        help="Output CSV file path")

    args = parser.parse_args()

    generate_analyze_list(args.keywords, args.channels, args.start_date, args.end_date, args.output)
