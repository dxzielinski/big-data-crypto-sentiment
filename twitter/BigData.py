import os
import requests
from dotenv import load_dotenv
import json
from datetime import datetime, timedelta, timezone
import time

# Wczytaj zmienne środowiskowe z pliku .env
load_dotenv()

API_KEY = os.getenv("API_KEY")

if not API_KEY:
    raise ValueError("Brak klucza API! Dodaj go do pliku .env jako API_KEY=...")

url = "https://api.twitterapi.io/twitter/tweet/advanced_search"

now_timestamp = int(time.time()) 

window_size = 60
since_timestamp = now_timestamp - window_size


headers = {
    "X-API-Key": API_KEY,
    "Content-Type": "application/json"
}

payload = {
    "query": f"($BTC or #BTC) since_time:{since_timestamp} -filter:retweets",
    "limit": 10,
    "include_user_data": False
}
times = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
parse_format_string = "%a %b %d %H:%M:%S %z %Y"

try:
    response = requests.get(url, params=payload, headers=headers, timeout=10)
    response.raise_for_status()
    
    print(f"Status Code: {response.status_code}")
    data = response.json()
    
    processed_tweets = []


    tweets_list = data.get("tweets", [])
    
    if not tweets_list:
        print("No tweets found in this window.")
    
    for tweet in tweets_list:
        tweet_created_at = tweet.get("createdAt")
        
        try:
            dt_object = datetime.strptime(tweet_created_at, parse_format_string)
            tweet_timestamp = int(dt_object.timestamp())
        except (ValueError, TypeError):
            tweet_timestamp = None

        processed_tweets.append({
            "id": tweet.get("id"),
            "text": tweet.get("text"),
            "created_at": tweet_created_at,
            "timestamp": tweet_timestamp,
            "crypto_key": "BTC"
        })

    output_data = {
        "tweets": processed_tweets,
        "query_info": {
            "since": since_timestamp,
            "query_time": now_timestamp
        }
    }
    
    filename = f"data/BTC_{times}_time_limited.json"
    
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=4, ensure_ascii=False)

    print(f"Success! {len(processed_tweets)} tweets saved to {filename}")

except requests.exceptions.RequestException as e:
    print(f"Network/API Error: {e}")
except Exception as e:
    print(f"Unexpected Error: {e}")