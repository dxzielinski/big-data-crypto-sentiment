import os
import json
import time
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from google.cloud import pubsub_v1
from google.cloud import secretmanager

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TOPIC_ID = os.getenv("PUBSUB_TOPIC_ID")
SECRET_ID = "twitter_api_key"

def get_secret(project_id: str, secret_id: str, version_id: str = "latest") -> str:
    """
    Accesses the Secret Manager to retrieve the API Key.
    """
    
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    try:
        response = client.access_secret_version(request={"name": name})
        payload = response.payload.data.decode("UTF-8")
        print("Successfully loaded Secret Manager secret")
        return payload
    except Exception as e:
        raise RuntimeError(f"Failed to fetch secret {secret_id}: {e}")

API_KEY = get_secret(PROJECT_ID, SECRET_ID)

if not all([API_KEY, PROJECT_ID, TOPIC_ID]):
    raise ValueError("Missing environment variables. Check your .env file.")

TWITTER_URL = "https://api.twitterapi.io/twitter/tweet/advanced_search"
TWITTER_DATE_FORMAT = "%a %b %d %H:%M:%S %z %Y"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def normalize_tweet_data(tweet: dict, crypto_symbol: str) -> dict:
    """
    Cleans the tweet data and generates time formats for downstream systems
    """
    raw_date = tweet.get("createdAt")
    
    # Initialize default times (now) in case parsing fails
    now = datetime.now(timezone.utc)
    dt_obj = now

    try:
        if raw_date:
            dt_obj = datetime.strptime(raw_date, TWITTER_DATE_FORMAT)
    except (ValueError, TypeError):
        print(f"[WARN] Could not parse date: {raw_date}. Using current time.")
        dt_obj = now

    # Flexible time format
    # 1. For BigQuery (likes ISO strings) & Dataflow Attributes (RFC 3339)
    iso_format = dt_obj.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    
    # 2. For MongoDB (likes Int64 milliseconds)
    timestamp_ms = int(dt_obj.timestamp() * 1000)
    timestamp_sec = int(dt_obj.timestamp())

    return {
        # IDs & Content
        "id": tweet.get("id"),
        "text": tweet.get("text"),
        "author_id": tweet.get("author", {}).get("id"), # Safe access if author missing
        "crypto_key": crypto_symbol, # Partition key for Dataflow
        
        # Universal Time Formats
        "created_at_raw": raw_date,
        "created_at_iso": iso_format,  # <--- Use this for BigQuery
        "timestamp_ms": timestamp_ms,  # <--- Use this for MongoDB
        "timestamp_sec": timestamp_sec 
    }

def fetch_tweets_for_symbol(symbol: str, limit: int = 20) -> list:
    """
    Fetches tweets specifically for one cryptocurrency symbol.
    """
    # Time Window Logic (Last 60 seconds)
    now_ts = int(time.time())
    window_size = 60
    since_ts = now_ts - window_size

    # Query: symbol OR hashtag, English only, no retweets
    query = f"(${symbol} OR #{symbol}) lang:en since_time:{since_ts} -filter:retweets"

    headers = {
        "X-API-Key": API_KEY,
        "Content-Type": "application/json"
    }

    params = {
        "query": query,
        "limit": limit,
        "include_user_data": "true" 
    }

    print(f"[{symbol}] Fetching tweets since {since_ts}...")

    try:
        response = requests.get(TWITTER_URL, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        raw_tweets = data.get("tweets", [])
        
        if not raw_tweets:
            print(f"[{symbol}] No new tweets found.")
            return []

        # Process tweets using the normalizer function
        processed_tweets = [normalize_tweet_data(t, symbol) for t in raw_tweets]
        print(f"[{symbol}] Found {len(processed_tweets)} valid tweets.")
        return processed_tweets

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] API request failed for {symbol}: {e}")
        return []
    except Exception as e:
        print(f"[ERROR] Unexpected error processing {symbol}: {e}")
        return []

def publish_to_pubsub(tweet: dict):
    """
    Publishes a single tweet to Pub/Sub with strict attributes for Dataflow.
    """
    try:
        message_data = json.dumps(tweet).encode("utf-8")
        
        attributes = {
            "crypto_key": tweet["crypto_key"],
            "event_timestamp": tweet["created_at_iso"]
        }

        future = publisher.publish(topic_path, message_data, **attributes)
        
    except Exception as e:
        print(f"[PUBSUB ERROR] Could not publish tweet {tweet.get('id')}: {e}")

def run_pipeline(cryptos_list: list):

    print(f"--- Starting Cycle: {datetime.now().isoformat()} ---")
    for crypto in cryptos_list:
        tweets = fetch_tweets_for_symbol(crypto)
        
        for tweet in tweets:
            publish_to_pubsub(tweet)
            
    print("--- Cycle Finished ---\n")


if __name__ == "__main__":
    crypto_list = ["ETH", "SOL", "FTM", "SHIB"]
    

    run_pipeline(crypto_list)
    time.sleep(30)
