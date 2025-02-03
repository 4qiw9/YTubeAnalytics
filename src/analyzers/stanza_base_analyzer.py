import logging
import os
import time

import stanza
from stopwordsiso import stopwords

from src.analyzers.base_analyzer import BaseAnalyzer
from src.common_logging import setup_logging

setup_logging()

class StanzaBaseAnalyzer(BaseAnalyzer):

    def __init__(self, analyze_list_csv, transcripts_dir):
        super().__init__(analyze_list_csv, transcripts_dir)
        self.stopwords = self.load_stopwords()

        # ✅ Initialize Stanza only once
        start_time = time.time()
        stanza.download("pl")
        self.nlp = stanza.Pipeline("pl", processors="tokenize,mwt,pos,lemma", use_gpu=True)
        logging.info(f"✅ Stanza NLP loaded in {time.time() - start_time:.2f}s")

    def load_stopwords(self):
        # Loading stopwords from `stopwordsiso` + `config/stopwords_pl.txt`
        stopwords_set = set(stopwords("pl"))
        stopwords_file = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../config/stopwords_pl.txt"))

        if os.path.exists(stopwords_file):
            with open(stopwords_file, "r", encoding="utf-8") as f:
                stopwords_set.update(line.strip().lower() for line in f if line.strip())

        logging.info(f"📌 Loaded {len(stopwords_set)} stopwords.")
        return stopwords_set


    def clean_text(self, texts):
        # Lemmatization and remove stop words
        if not texts:
            return []

        logging.info("🔄 Starting NLP...")
        docs = self.nlp("\n".join(texts))

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