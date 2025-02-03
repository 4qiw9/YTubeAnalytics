import logging
import os
import re
import time

import pandas as pd

from src.common_logging import setup_logging

setup_logging()


class BaseAnalyzer:
    def __init__(self, analyze_list_csv, transcripts_dir):
        self.analyze_list_csv = analyze_list_csv
        self.transcripts_dir = transcripts_dir

    def load_transcripts(self):
        if not os.path.exists(self.analyze_list_csv):
            logging.error(f"🚨 File {self.analyze_list_csv} does not exist!")
            return []

        analyze_list = pd.read_csv(self.analyze_list_csv, dtype=str)
        transcripts = []

        total_files = len(analyze_list)
        logging.info(f"📂 Found {total_files} files for analysis.")

        start_time = time.time()
        for idx, row in analyze_list.iterrows():
            video_id = row["video_id"]
            channel_id = row["channel_id"]
            transcript_path = os.path.join(self.transcripts_dir, channel_id, f"{video_id}.txt")

            if os.path.exists(transcript_path):
                with open(transcript_path, "r", encoding="utf-8") as f:
                    text = f.read().lower()
                    text = re.sub(r"\[\d+:\d+\]", "", text).strip()
                    transcripts.append((video_id, row.get("published_at"), text))
            else:
                logging.warning(f"⚠️ Missing transcript for  {video_id} ({channel_id})")

            if (idx + 1) % 10 == 0 or idx + 1 == total_files:
                elapsed_time = time.time() - start_time
                logging.info(f"📊 Processesd {idx + 1}/{total_files} filed ({elapsed_time:.2f} s)")

        return transcripts






