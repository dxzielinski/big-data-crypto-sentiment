# coding=utf-8

import os
import json
import time
import datetime
import requests
from google.cloud import pubsub_v1

# --- GCP CONFIGURATION ---
PROJECT_ID = "big-data-crypto-sentiment"
TOPIC_ID = "CoinCapTopic" 

# --- COINCAP API CONFIGURATION ---
COINCAP_BASE_URL = "https://api.coincap.io/v2" ## NIE DZIAŁA

# Example: list of assets to track
ASSETS = ["bitcoin", "ethereum", "solana", "fantom"]  # CoinCap uses asset ids, not tickers


# --- PUB/SUB CLIENT (created once globally) ---
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


def fetch_asset_price(asset_id: str) -> dict | None:
    """
    Fetches current data for a single asset from CoinCap.
    asset_id example: 'bitcoin', 'ethereum', 'solana', 'fantom'.
    Returns a dictionary with asset data or None on error.
    """
    url = f"{COINCAP_BASE_URL}/assets/{asset_id}"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"[API ERROR] Failed to fetch data for {asset_id}: {e}")
        return None

    data = response.json()
    asset_data = data.get("data")

    if not asset_data:
        print(f"[API WARNING] No data field returned for {asset_id}: {data}")
        return None

    return asset_data


def publish_asset_to_pubsub(asset_data: dict):
    """
    Publishes single asset data from CoinCap to Pub/Sub.
    Adds timestamp and a couple of attributes for filtering.
    """

    # Some helpful attributes
    asset_id = asset_data.get("id", "unknown")
    symbol = asset_data.get("symbol", "UNKNOWN")

    # Add custom field for consistency with tweet messages if needed
    # ( can skip this if not needed )
    asset_data["source"] = "coincap"

    # Serialize to JSON
    try:
        message_data = json.dumps(asset_data).encode("utf-8")
    except Exception as e:
        print(f"[SERIALIZATION ERROR] Could not serialize asset {asset_id}: {e}")
        return

    # Add message attributes (Pub/Sub attributes are always strings)
    current_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
    attributes = {
        "timestamp": current_time,
        "asset_id": asset_id,
        "symbol": symbol,
        "source": "coincap",
    }

    # Publish to Pub/Sub
    future = publisher.publish(topic_path, message_data, **attributes)
    try:
        msg_id = future.result(timeout=10)
        print(f"Published asset {asset_id} ({symbol}). Message ID: {msg_id}")
    except Exception as e:
        print(f"[PUBSUB ERROR] Failed to publish asset {asset_id}: {e}")


def fetch_and_publish_once(assets: list[str]):
    """
    Single-cycle run:
      1. Fetches data for each asset.
      2. Immediately publishes each asset to Pub/Sub.
    """
    for asset_id in assets:
        print(f"\n--- Fetching and publishing data for {asset_id} ---")
        asset_data = fetch_asset_price(asset_id)
        if asset_data is None:
            continue
        publish_asset_to_pubsub(asset_data)


if __name__ == "__main__":
    # One-time run – good for testing
    fetch_and_publish_once(ASSETS)

    # If you want continuous streaming, change this into:
    #
    # while True:
    #     fetch_and_publish_once(ASSETS)
    #     print("\nCycle finished, sleeping for 60 seconds...\n")
    #     time.sleep(60)
