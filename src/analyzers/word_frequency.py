import logging
import os

import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter
from wordcloud import WordCloud
from src.analyzers.stanza_base_analyzer import StanzaBaseAnalyzer

class WordFrequencyAnalyzer(StanzaBaseAnalyzer):
    def __init__(self,
                 analyze_list_csv, transcripts_dir,
                 output_csv,
                 top_n=50,
                 min_length=3,
                 output_plots="/output/plots"
                 ):
        super().__init__(analyze_list_csv, transcripts_dir)
        self.output_csv = output_csv
        self.top_n = top_n
        self.min_length = min_length
        self.output_plots = os.path.abspath(output_plots)

        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
        if output_plots.startswith("/"):
            self.output_plots = os.path.join(base_dir, output_plots.lstrip("/"))  # convert to `output/plots`
        else:
            self.output_plots = output_plots

        os.makedirs(self.output_plots, exist_ok=True)

    def analyze(self):
        transcripts = self.load_transcripts()
        if not transcripts:
            logging.error("Missing all transcripts!")
            return

        texts = [t[2] for t in transcripts]
        all_words = self.clean_text(texts)

        word_counts = Counter(all_words)
        most_common_words = word_counts.most_common(self.top_n)

        df = pd.DataFrame(most_common_words, columns=["word", "count"])
        df.to_csv(self.output_csv, index=False, encoding="utf-8")

        logging.info(f"Word frequency analysis saved to {self.output_csv}")

        self.generate_wordcloud(word_counts)
        self.plot_top_words(most_common_words)

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




