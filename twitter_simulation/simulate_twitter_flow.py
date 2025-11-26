import os
import json
import time
import random  # <--- NEW IMPORT
from google.cloud import pubsub_v1
from datetime import datetime, timezone

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "big-data-crypto-sentiment-test")
TOPIC_ID = os.getenv("PUBSUB_TOPIC_ID", "crypto-tweets-stream")

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def load_all_data_into_memory(cryptos_list: list) -> dict:
    """
    Loads ALL files into a dictionary structure BEFORE the loop starts.
    Returns: { 'ETH': [tweet1, tweet2], 'SOL': [...] }
    """
    data_cache = {}
    for symbol in cryptos_list:
        filename = f"simulation_data/{symbol}_tweets.json"
        
        if not os.path.exists(filename):
            print(f"[SKIP] File not found: {filename}")
            data_cache[symbol] = []
            continue

        try:
            with open(filename, "r", encoding="utf-8") as f:
                tweets = json.load(f)
                data_cache[symbol] = tweets
                print(f"[{symbol}] Pre-loaded {len(tweets)} tweets.")
        except Exception as e:
            print(f"[ERROR] Could not read file {filename}: {e}")
            data_cache[symbol] = []
            
    return data_cache

def publish_to_pubsub(tweet: dict):
    """
    Publishes a single tweet to Pub/Sub with valid current timestamp.
    """
    try:
        now = datetime.now(timezone.utc)
        tweet["created_at_iso"] = now.isoformat().replace("+00:00", "Z")
        tweet["timestamp_ms"] = int(now.timestamp() * 1000)
        
        # Encode AFTER updating timestamp
        message_data = json.dumps(tweet).encode("utf-8")
        
        attributes = {
            "crypto_key": tweet.get("crypto_key", "UNKNOWN"),
            "event_timestamp": tweet.get("created_at_iso", "")
        }

        future = publisher.publish(topic_path, message_data, **attributes)
        future.result(timeout=5)
        
    except Exception as e:
        print(f"[PUBSUB ERROR] Could not publish tweet {tweet.get('id')}: {e}")

def run_interleaved_simulation(cryptos_list: list):

    data_pool = load_all_data_into_memory(cryptos_list)
    i = 0

    print("Start infinite repeated stream from saved tweets")
    while True:
        symbol = random.choice(cryptos_list)
        
        tweets = data_pool.get(symbol, [])
        tweet = random.choice(tweets)
        publish_to_pubsub(tweet)
        time.sleep(random.uniform(0.5, 1))
        i = i + 1
        
        if i%100==0:
            print(f"Sent {i} tweets")

if __name__ == "__main__":
    crypto_list = ["ETH", "SOL", "FTM", "SHIB"]
    
    try:
        run_interleaved_simulation(crypto_list)
    except KeyboardInterrupt:
        print("\nSimulation stopped.")