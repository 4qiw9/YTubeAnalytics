import logging
import os
from datetime import datetime

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LOG_DIR = os.path.join(BASE_DIR, "log")

def setup_logging(script_name=None, level=logging.INFO):
    os.makedirs(LOG_DIR, exist_ok=True)

    # LOG file name: YYYY-MM-DD. One per day.
    timestamp = datetime.now().strftime("%Y-%m-%d")
    log_filename = f"{timestamp}.log" if script_name is None else f"{timestamp}_{script_name}.log"
    log_file_path = os.path.join(LOG_DIR, log_filename)

    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_file_path, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

    logging.info(f"✅ Logging configured for {'common_logging' if script_name is None else script_name}")
