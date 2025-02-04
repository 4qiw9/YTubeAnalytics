import logging
import os
import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter
from wordcloud import WordCloud
from concurrent.futures import ThreadPoolExecutor
import time
from src.analyzers.stanza_base_analyzer import StanzaBaseAnalyzer

class WordFrequencyAnalyzer(StanzaBaseAnalyzer):
    def __init__(self,
                 analyze_list_csv, transcripts_dir,
                 output_csv,
                 top_n=50,
                 min_length=3,
                 output_plots="/output/plots",
                 num_threads=4,  # ✅ Number of threads for parallel processing
                 cache_nlp_results=True  # ✅ Cache NLP results
                 ):
        super().__init__(analyze_list_csv, transcripts_dir)
        self.output_csv = output_csv
        self.top_n = top_n
        self.min_length = min_length
        self.output_plots = os.path.abspath(output_plots)
        self.num_threads = num_threads  # ✅ Store the number of threads
        self.cache_nlp_results = cache_nlp_results
        self.nlp_cache_file = output_csv.replace(".csv", "_nlp.csv")  # ✅ Cached NLP data file

        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
        if output_plots.startswith("/"):
            self.output_plots = os.path.join(base_dir, output_plots.lstrip("/"))  # convert to `output/plots`
        else:
            self.output_plots = output_plots

        os.makedirs(self.output_plots, exist_ok=True)

    def analyze(self):
        if self.cache_nlp_results and os.path.exists(self.nlp_cache_file):
            logging.info(f"✅ Loading cached NLP results from {self.nlp_cache_file}")
            df = pd.read_csv(self.nlp_cache_file)
        else:
            transcripts = self.load_transcripts()
            if not transcripts:
                logging.error("Missing all transcripts!")
                return

            texts = [t[2] for t in transcripts]
            logging.info(f"🔄 Starting NLP with {self.num_threads} threads...")
            start_time = time.time()
            all_words = self.parallel_clean_text(texts)  # ✅ Parallel processing
            total_time = time.time() - start_time
            logging.info(f"✅ Finished NLP processing in {total_time:.2f}s. Found {len(all_words)} words.")

            word_counts = Counter(all_words)
            df = pd.DataFrame(word_counts.items(), columns=["word", "count"])
            df.to_csv(self.nlp_cache_file, index=False, encoding="utf-8")
            logging.info(f"✅ Cached NLP results saved to {self.nlp_cache_file}")

        # ✅ Now filter for top_n words only for visualization
        df_sorted = df.sort_values(by="count", ascending=False)
        df_sorted.to_csv(self.output_csv, index=False, encoding="utf-8")
        logging.info(f"✅ Word frequency analysis saved to {self.output_csv}")

        word_counts = dict(zip(df_sorted["word"], df_sorted["count"]))
        self.generate_wordcloud(word_counts)
        self.plot_top_words(df_sorted.head(self.top_n).values.tolist())

    def parallel_clean_text(self, texts):
        """Splits texts into chunks and processes them in parallel."""
        chunk_size = max(1, len(texts) // self.num_threads)
        chunks = [texts[i:i + chunk_size] for i in range(0, len(texts), chunk_size)]

        total_chunks = len(chunks)
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            results = []
            for i, result in enumerate(executor.map(self.clean_text, chunks), 1):
                results.append(result)
                elapsed_time = time.time() - start_time
                estimated_total_time = (elapsed_time / i) * total_chunks
                remaining_time = estimated_total_time - elapsed_time
                logging.info(f"🔄 Processed {i}/{total_chunks} chunks ({(i/total_chunks)*100:.2f}%) | Elapsed: {elapsed_time:.2f}s | ETA: {remaining_time:.2f}s")

        # Flatten the results
        return [word for sublist in results for word in sublist]

    def generate_wordcloud(self, word_counts):
        wordcloud = WordCloud(width=800, height=400, background_color="white").generate_from_frequencies(word_counts)
        plt.figure(figsize=(10, 5))
        plt.imshow(wordcloud, interpolation="bilinear")
        plt.axis("off")
        plt.title("Word Map")

        # save to file
        wordcloud_path = os.path.join(self.output_plots, "wordcloud.png")
        plt.savefig(wordcloud_path, bbox_inches="tight")

        # also show
        plt.show()

        # then close
        plt.close()

    def plot_top_words(self, most_common_words):
        words, counts = zip(*most_common_words)
        plt.figure(figsize=(12, 6))
        plt.bar(words, counts, color="blue")
        plt.xticks(rotation=45, ha="right")
        plt.xlabel("Word")
        plt.ylabel("Number of occurrences")
        plt.title(f"Most popular words (TOP {self.top_n})")

        # save to file
        top_words_path = os.path.join(self.output_plots, "top_words.png")
        plt.savefig(top_words_path, bbox_inches="tight")

        # also show
        plt.show()

        # then close
        plt.close()
