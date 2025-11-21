# coding=utf-8

import os
import json
import time
import datetime
import requests
from dotenv import load_dotenv
from google.cloud import pubsub_v1

# Wczytanie zmiennych z .env (działa lokalnie; w Dockerze możesz użyć env albo env-file)
load_dotenv()

# --- GCP CONFIGURATION (z ENV, z sensownymi domyślnymi wartościami) ---
PROJECT_ID = os.getenv("PROJECT_ID", "big-data-crypto-sentiment")
TOPIC_ID = os.getenv("TOPIC_ID", "TwitterTopic")

# --- TWITTER API CONFIGURATION ---
API_KEY = os.getenv("API_KEY")
if not API_KEY:
    raise ValueError("Missing API_KEY! Set it as an environment variable API_KEY=...")

TWITTER_URL = "https://api.twitterapi.io/twitter/tweet/advanced_search"

# --- PUB/SUB CLIENT (global, wykorzysta GOOGLE_APPLICATION_CREDENTIALS z ENV) ---
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


def fetch_tweets_for_symbol(symbol: str, limit: int = 50):
    """
    Pobiera tweety dla danej kryptowaluty z API (np. 'SHIB', 'ETH').
    """
    query = f"#{symbol} lang:en -filter:retweets"

    headers = {
        "X-API-Key": API_KEY,
        "Content-Type": "application/json",
    }

    params = {
        "query": query,
        "limit": limit,
        "include_user_data": False,
    }

    try:
        response = requests.get(TWITTER_URL, params=params, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"[API ERROR] Failed to fetch tweets for {symbol}: {e}")
        return []

    data = response.json()
    tweets = data.get("tweets", [])
    print(f"Fetched {len(tweets)} tweets for {symbol}")
    return tweets


def publish_tweet_to_pubsub(tweet: dict, crypto: str):
    """
    Publikuje pojedynczy tweet do Pub/Sub.
    Dodaje simulated_crypto i timestamp jako atrybuty.
    """
    tweet["simulated_crypto"] = crypto

    try:
        message_data = json.dumps(tweet).encode("utf-8")
    except Exception as e:
        print(f"[SERIALIZATION ERROR] Could not serialize tweet: {e}")
        return

    current_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
    attributes = {
        "timestamp": current_time,
        "simulated_crypto": crypto,
    }

    future = publisher.publish(topic_path, message_data, **attributes)
    try:
        msg_id = future.result(timeout=10)
        print(f"Published tweet ({crypto}). Message ID: {msg_id}")
    except Exception as e:
        print(f"[PUBSUB ERROR] Failed to publish tweet: {e}")


def fetch_and_publish_once(crypto: str, limit: int = 50):
    """
    Jeden cykl:
      1. pobierz tweety dla danej kryptowaluty
      2. od razu opublikuj każdy tweet do Pub/Sub
    """
    tweets = fetch_tweets_for_symbol(crypto, limit=limit)

    for tweet in tweets:
        if isinstance(tweet, str):
            tweet = {"text": tweet}
        publish_tweet_to_pubsub(tweet, crypto)


if __name__ == "__main__":
    # Możesz też czytać listę symboli z ENV, np. SYMBOLS="SHIB,ETH,SOL"
    symbols_env = os.getenv("SYMBOLS")
    if symbols_env:
        symbols = [s.strip() for s in symbols_env.split(",") if s.strip()]
    else:
        symbols = ["SHIB", "ETH", "SOL", "FTM"]

    for symbol in symbols:
        print(f"\n--- Fetching and publishing tweets for {symbol} ---")
        fetch_and_publish_once(symbol, limit=20)
        time.sleep(5)
