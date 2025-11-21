# coding=utf-8

import os
import json
import time
import datetime
import requests
from dotenv import load_dotenv
from google.cloud import pubsub_v1

# --- GCP CONFIGURATION ---
PROJECT_ID = "big-data-crypto-sentiment"
TOPIC_ID = "TwitterTopic"

# --- TWITTER API CONFIGURATION ---
load_dotenv()
API_KEY = os.getenv("API_KEY")
if not API_KEY:
    raise ValueError("Missing API_KEY! Add it to your .env file as API_KEY=...")

TWITTER_URL = "https://api.twitterapi.io/twitter/tweet/advanced_search"

# --- PUB/SUB CLIENT (created once globally) ---
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


def fetch_tweets_for_symbol(symbol: str, limit: int = 50):
    """
    Fetches tweets for a given cryptocurrency symbol from the API.
    Example: symbol='SHIB', 'ETH', etc.
    """
    query = f"#{symbol} lang:en -filter:retweets"

    headers = {
        "X-API-Key": API_KEY,
        "Content-Type": "application/json"
    }

    params = {
        "query": query,
        "limit": limit,
        "include_user_data": False
    }

    try:
        response = requests.get(TWITTER_URL, params=params, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"[API ERROR] Failed to fetch tweets for {symbol}: {e}")
        return []

    data = response.json()

    # The API typically returns {'tweets': [...]}
    tweets = data.get("tweets", [])
    print(f"Fetched {len(tweets)} tweets for {symbol}")
    return tweets


def publish_tweet_to_pubsub(tweet: dict, crypto: str):
    """
    Publishes a single tweet to Pub/Sub.
    Adds simulated_crypto and timestamp attributes.
    """

    # Add cryptocurrency identifier
    tweet["simulated_crypto"] = crypto

    # Serialize to JSON
    try:
        message_data = json.dumps(tweet).encode("utf-8")
    except Exception as e:
        print(f"[SERIALIZATION ERROR] Could not serialize tweet: {e}")
        return

    # Add message attributes
    current_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
    attributes = {
        "timestamp": current_time,
        "simulated_crypto": crypto
    }

    # Publish to Pub/Sub
    future = publisher.publish(topic_path, message_data, **attributes)
    try:
        msg_id = future.result(timeout=10)
        print(f"Published tweet ({crypto}). Message ID: {msg_id}")
    except Exception as e:
        print(f"[PUBSUB ERROR] Failed to publish tweet: {e}")


def fetch_and_publish_once(crypto: str, limit: int = 50):
    """
    Single-cycle run:
      1. Fetch tweets for a specific crypto.
      2. Publish each tweet immediately to Pub/Sub.
    No local files are created.
    """
    tweets = fetch_tweets_for_symbol(crypto, limit=limit)

    for tweet in tweets:
        # Convert string tweets to dict if needed
        if isinstance(tweet, str):
            tweet = {"text": tweet}

        publish_tweet_to_pubsub(tweet, crypto)


if __name__ == "__main__":
    # Example: one-time ingestion for several cryptocurrencies
    symbols = ["SHIB", "ETH", "SOL", "FTM"]

    for symbol in symbols:
        print(f"\n--- Fetching and publishing tweets for {symbol} ---")
        fetch_and_publish_once(symbol, limit=20)
        # Small pause between API calls
        time.sleep(5)
