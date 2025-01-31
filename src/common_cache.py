import json
import logging
import os
import time
import threading
import atexit  # for saving cache after script execution
from datetime import datetime

from common_logging import setup_logging

setup_logging(script_name="common_cache")

CACHE_FILE = os.path.join(os.path.dirname(__file__), "../output/cache.json")

FAILED_CACHE_FILE = os.path.join(os.path.dirname(__file__), "../output/failed_transcripts.json")
MAX_ATTEMPTS = 5
WAIT_TIME = [0, 60, 3600, 86400, 86400] # minimal seconds wait time between attempts (86400 = 24h)

# Global caching, only for failed download cache
_failed_cache = {}
_failed_cache_lock = threading.Lock()

######################
### DOWNLOAD CACHE ###
######################
def load_cache():
    if not os.path.exists(CACHE_FILE):
        return {"channels": {}, "playlists": {}}

    with open(CACHE_FILE, "r", encoding="utf-8") as file:
        return json.load(file)


def save_cache(cache):
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
    with open(CACHE_FILE, "w", encoding="utf-8") as file:
        json.dump(cache, file, indent=4)


def get_last_fetched_date(source_type, source_id):
    cache = load_cache()
    return cache[source_type].get(source_id, None)


def update_last_fetched_date(source_type, source_id, last_date):
    cache = load_cache()
    cache[source_type][source_id] = last_date
    save_cache(cache)

####################
### FAILED CACHE ###
####################
def load_failed_cache():
    global _failed_cache
    if not os.path.exists(FAILED_CACHE_FILE):
        _failed_cache = {}
    else:
        with open(FAILED_CACHE_FILE, "r", encoding="utf-8") as file:
            try:
                _failed_cache = json.load(file)
            except json.JSONDecodeError:
                _failed_cache = {}

def save_failed_cache():
    with _failed_cache_lock:
        with open(FAILED_CACHE_FILE, "w", encoding="utf-8") as file:
            json.dump(_failed_cache, file, indent=4)
            logging.info("📝 Failed transcripts downloads cache updated")

def save_failed_cache_periodically():
    while True:
        time.sleep(10) # in seconds
        save_failed_cache()

# save cache periodically while script is running
cache_saver_thread = threading.Thread(target=save_failed_cache_periodically, daemon=True)
cache_saver_thread.start()

# save cache when script ends
atexit.register(save_failed_cache)

def should_retry(video_id):
    with _failed_cache_lock:
        if video_id not in _failed_cache:
            return True

        attempts = _failed_cache[video_id]["attempts"]
        last_attempt_str = _failed_cache[video_id]["last_attempt"]
        last_attempt = datetime.strptime(last_attempt_str, "%Y-%m-%d %H:%M:%S")

        if attempts >= MAX_ATTEMPTS:
            return False

        wait_time = WAIT_TIME[min(attempts, len(WAIT_TIME) - 1)]
        return (datetime.now() - last_attempt).total_seconds() >= wait_time

def record_failed_attempt(video_id):
    with _failed_cache_lock:
        if video_id not in _failed_cache:
            _failed_cache[video_id] = {"attempts": 1, "last_attempt": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        else:
            _failed_cache[video_id]["attempts"] += 1
            _failed_cache[video_id]["last_attempt"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def record_successful_attempt(video_id):
    with _failed_cache_lock:
        if video_id in _failed_cache:
            del _failed_cache[video_id]
