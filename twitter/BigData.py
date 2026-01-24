import os
import requests
from dotenv import load_dotenv
import json
from datetime import datetime, timedelta
import re

load_dotenv()

API_KEY = os.getenv("API_KEY")
if not API_KEY:
    raise ValueError("Brak klucza API! Dodaj go do pliku .env jako API_KEY=...")

url = "https://api.twitterapi.io/twitter/tweet/advanced_search"
crypto ="ETH"
hours_ago = 48

# obliczamy czas sprzed 24h w UTC
since_dt = datetime.utcnow() - timedelta(hours=hours_ago)
since_iso = since_dt.isoformat() + "Z"  # ISO format w UTC, np. "2026-01-24T15:30:00Z"


headers = {
    "X-API-Key": API_KEY,
    "Content-Type": "application/json"
}

payload = {
    "query": f"#{crypto} OR ${crypto} lang:en -filter:retweets",
    "limit": 100,
    "include_user_data": False,
    #"since_time":since_iso  
    "since":"2026-01-22"
}


times = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

try:
    response = requests.get(url, params=payload, headers=headers, timeout=10)
    response.raise_for_status()

    data = response.json()
    tweets = data.get("tweets", [])

    print(f"Pobrano {len(tweets)} tweetów.")

    transformed = []

    for t in tweets:
        text = t.get("text", "")
        created_raw = t.get("createdAt")

        # parsowanie daty z Twittera
        dt = datetime.strptime(created_raw, "%a %b %d %H:%M:%S %z %Y")

        transformed.append({
            "id": t.get("id"),
            "text": text,
            "author_id": t.get("authorId"),  
            "crypto_key": crypto,
            "created_at_raw": created_raw,
            "created_at_iso": dt.isoformat().replace("+00:00", "Z"),
            "timestamp_ms": int(dt.timestamp() * 1000),
            "timestamp_sec": int(dt.timestamp())
        })

    os.makedirs("data", exist_ok=True)
    output_path = f"data/{crypto}_{times}.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(transformed, f, indent=4, ensure_ascii=False)

    print(f"Wyniki zapisane w {output_path}")

except requests.exceptions.RequestException as e:
    print("Błąd połączenia:")
    print(e)
