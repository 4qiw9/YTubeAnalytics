import os
import time
import logging
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.cm as cm
from collections import defaultdict
from src.analyzers.base_analyzer import BaseAnalyzer
from concurrent.futures import ThreadPoolExecutor, as_completed


class WordTrendAnalyzer(BaseAnalyzer):
    def __init__(self,
                 analyze_list_csv, transcripts_dir,
                 output_csv,
                 min_length=3,
                 output_dir=None,
                 n_top_words=15,
                 max_workers=8,
                 chunk_size=5000
                 ):
        super().__init__(analyze_list_csv, transcripts_dir)
        self.output_csv = output_csv
        self.min_length = min_length
        self.top_n = n_top_words  # dynamic for n of words on chart
        self.max_workers = max_workers  # n of threads
        self.chunk_size = chunk_size  # size of chunk for single thread

        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
        self.output_dir = output_dir if output_dir else os.path.join(base_dir, "output")
        self.output_plots_dir = os.path.join(base_dir, "output", "plots")
        os.makedirs(self.output_plots_dir, exist_ok=True)

    def process_chunk(self, chunk, date):
        words = self.clean_text([chunk])

        local_trends = defaultdict(int)
        for word in words:
            if len(word) >= self.min_length:  # skip too short words
                local_trends[(word, date)] += 1  # store with date (for multithread)

        return local_trends

    def process_single_file(self, video_id, published_at, text):
        if not published_at:
            return {}

        date = published_at[:10] # take YYYY-MM-DD

        # split file into chunks for parallel processing
        chunks = self.split_text_into_chunks(text, self.chunk_size)

        local_trends = defaultdict(int)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_chunk = {executor.submit(self.process_chunk, chunk, date): chunk for chunk in chunks}

            for future in as_completed(future_to_chunk):
                try:
                    chunk_result = future.result()
                    for (word, date), count in chunk_result.items():
                        local_trends[(word, date)] += count  # collecting results

                except Exception as e:
                    logging.error(f"🚨 Chunk processing error: {e}")

        return local_trends

    def analyze(self):
        transcripts = self.load_transcripts()
        if not transcripts:
            logging.warning("⚠️ No transcripts for analysis!")
            return

        word_trends = defaultdict(lambda: defaultdict(int))  # {word: {date: n_occurs}}

        total_files = len(transcripts)
        start_time = time.time()

        for idx, (video_id, published_at, text) in enumerate(transcripts):
            logging.info(f"📄 Processing video: {video_id} ({idx + 1}/{total_files})")

            if not published_at:
                continue
            date = published_at[:10]  # take: YYYY-MM-DD
            file_start_time = time.time()

            local_trends = self.process_single_file(video_id, published_at, text)

            for (word, date), count in local_trends.items():
                word_trends[word][date] += count  # collecting

            # logging time
            elapsed_file_time = time.time() - file_start_time
            elapsed_total_time = time.time() - start_time
            remaining_time = (elapsed_total_time / (idx + 1)) * (total_files - (idx + 1))

            logging.info(
                f"📄 Processed {idx + 1}/{total_files} | Time: {elapsed_file_time:.2f}s | Eta: {remaining_time:.2f}s")

        trend_data = [{"word": word, "date": date, "count": count}
                      for word, date_counts in word_trends.items()
                      for date, count in date_counts.items()]

        df = pd.DataFrame(trend_data)
        df["date"] = pd.to_datetime(df["date"])  # convert to datetime!!
        df.sort_values("date", inplace=True)  # increasing dates (timeline)
        df.to_csv(self.output_csv, index=False, encoding="utf-8")

        total_time = time.time() - start_time
        logging.info(f"✅ Saved trend analysis to {self.output_csv} | Total time: {total_time:.2f}s")

        self.plot_word_trends(df)

    def plot_word_trends(self, df):
        top_words = df.groupby("word")["count"].sum().nlargest(self.top_n).index.tolist()
        df_filtered = df[df["word"].isin(top_words)]

        plt.figure(figsize=(12, 6))

        color_map = cm.get_cmap('tab20', len(top_words))
        colors = [color_map(i) for i in range(len(top_words))]

        for idx, word in enumerate(top_words):
            subset = df_filtered[df_filtered["word"] == word]
            plt.plot(
                pd.to_datetime(subset["date"]),
                subset["count"],
                marker="o",
                label=word,
                color=colors[idx]
            )

        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.gca().xaxis.set_major_locator(mdates.DayLocator())

        plt.xlabel("Date")
        plt.ylabel("Number of words")
        plt.title(f"Word occurrence over time (TOP {self.top_n})")
        plt.legend()
        plt.xticks(rotation=45)

        trends_path = os.path.join(self.output_plots_dir, "word_trends.png")
        plt.savefig(trends_path, bbox_inches="tight")
        logging.info(f"✅ Saved tred chart to {trends_path}")

        plt.show()
        plt.close()

        self.export_matrix_to_csv(df)


    def export_matrix_to_csv(self, df):
        pivot = df.pivot_table(index="word", columns="date", values="count", aggfunc="sum", fill_value=0)

        # columns format date -> YYYY-MM-DD
        pivot.columns = pivot.columns.strftime("%Y-%m-%d")

        # add 'total' column
        pivot["total"] = pivot.sum(axis=1)

        # export to csv
        matrix_path = os.path.join(self.output_dir, "word_trends_matrix.csv")
        pivot.to_csv(matrix_path, encoding="utf-8")
        logging.info(f"✅ Saved trend matrix to {matrix_path}")

