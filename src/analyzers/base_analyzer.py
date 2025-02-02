import os
import re
import logging
import time
import pandas as pd
import stanza
from stopwordsiso import stopwords
from src.common_logging import setup_logging

setup_logging()

# Initialize NLP Stanza
stanza.download("pl")
nlp = stanza.Pipeline("pl", processors="tokenize,mwt,pos,lemma", use_gpu=True)

class BaseAnalyzer:
    def __init__(self, analyze_list_csv, transcripts_dir):
        self.analyze_list_csv = analyze_list_csv
        self.transcripts_dir = transcripts_dir
        self.stopwords = self.load_stopwords()

    def load_stopwords(self):
        # Loading stopwords from `stopwordsiso` + `config/stopwords_pl.txt`
        stopwords_set = set(stopwords("pl"))
        stopwords_file = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../config/stopwords_pl.txt"))

        if os.path.exists(stopwords_file):
            with open(stopwords_file, "r", encoding="utf-8") as f:
                stopwords_set.update(line.strip().lower() for line in f if line.strip())

        logging.info(f"📌 Loaded {len(stopwords_set)} stopwords.")
        return stopwords_set

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

    def clean_text(self, texts):
        # Lemmatization and remove stop words
        if not texts:
            return []

        logging.info("🔄 Starting NLP...")
        docs = nlp("\n".join(texts))

        processed_words = []
        for sentence in docs.sentences:
            for word in sentence.words:
                lemma = word.lemma.lower()
                if lemma not in self.stopwords and len(lemma) > 2:
                    processed_words.append(lemma)

        logging.info(f"✅ Ended NLP analysis. Found {len(processed_words)} words.")
        return processed_words


    def split_text_into_chunks(self, text, max_chunk_size=5000):
        chunks = []
        start = 0
        while start < len(text):
            end = min(start + max_chunk_size, len(text))

            # Find last space in chunk
            if end < len(text) and text[end] != " ":
                end = text.rfind(" ", start, end)

            # If not space, thim as is
            if end == -1 or end == start:
                end = min(start + max_chunk_size, len(text))

            chunks.append(text[start:end].strip())
            start = end

        return chunks

