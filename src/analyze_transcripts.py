import os
import argparse
import logging
from src.common_logging import setup_logging
from src.analyzers.word_frequency import WordFrequencyAnalyzer
from src.analyzers.word_trend import WordTrendAnalyzer

# Logging
setup_logging()

# Script setup
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
DEFAULT_INPUT_CSV = os.path.join(OUTPUT_DIR, "analyze_list.csv")
DEFAULT_TRANSCRIPTS_DIR = os.path.join(OUTPUT_DIR, "transcripts")
DEFAULT_FREQ_CSV = os.path.join(OUTPUT_DIR, "word_frequencies.csv")
DEFAULT_TREND_CSV = os.path.join(OUTPUT_DIR, "word_trends.csv")
DEFAULT_MODE = "trend"
DEFAULT_TOP = 50
DEFAULT_MIN_LENGTH = 3

def main():
    parser = argparse.ArgumentParser(description="Starting transcripts analysis")

    # Base params
    parser.add_argument("--mode", choices=["frequency", "trend"],
                        default=DEFAULT_MODE,
                        help="Mode: 'frequency' (words freq) or 'trend' (words over time)")

    parser.add_argument("--input",
                        default=DEFAULT_INPUT_CSV,
                        help="Input CSV with videos for analysis (default: analyze_list.csv)")

    parser.add_argument("--transcripts",
                        default=DEFAULT_TRANSCRIPTS_DIR,
                        help="Transcripts directory (default: /output/transcripts/)")

    parser.add_argument("--output", help="Ścieżka do pliku wynikowego CSV")

    # Additional params
    parser.add_argument("--top", type=int,
                        default=DEFAULT_TOP,
                        help=f"Number of most popular words (default: {DEFAULT_TOP})")

    parser.add_argument("--min-length", type=int,
                        default=DEFAULT_MIN_LENGTH,
                        help=f"Minimal length for analysis (default: {DEFAULT_MIN_LENGTH})")

    args = parser.parse_args()

    logging.info(f"🚀 Starting analysis: {args.mode}")
    logging.info(f"📂 Input file: {args.input}")
    logging.info(f"📂 Transcripts directory: {args.transcripts}")

    if args.output:
        output_csv = args.output
    elif args.mode == "frequency":
        output_csv = DEFAULT_FREQ_CSV
    elif args.mode == "trend":
        output_csv = DEFAULT_TREND_CSV
    else:
        raise Exception("args.output problem")

    logging.info(f"📂 Output file: {output_csv}")

    if args.mode == "frequency":
        analyzer = WordFrequencyAnalyzer(args.input, args.transcripts, output_csv, args.top, args.min_length)
    elif args.mode == "trend":
        analyzer = WordTrendAnalyzer(args.input, args.transcripts, output_csv, args.min_length)
    else:
        raise Exception("args.mode problem")

    analyzer.analyze()
    logging.info("✅ Analysis ended!")


if __name__ == "__main__":
    main()
