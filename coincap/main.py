import json
import time
import datetime
import requests
from google.cloud import pubsub_v1,secretmanager

client = secretmanager.SecretManagerServiceClient()
name = "projects/big-data-crypto-sentiment/secrets/COINCAP_API_KEY/versions/latest"
resp = client.access_secret_version(request={"name": name})
COINCAP_API_KEY = resp.payload.data.decode("utf-8")
PROJECT_ID = "big-data-crypto-sentiment"
TOPIC_PRICE_ID = "CoinCapPriceStream"
TOPIC_TA_ID = "CoinCapTAStream"
COINCAP_BASE_URL = "https://rest.coincap.io/v3"
SYMBOLS = ["ETH", "SOL", "SHIB"]
SLUGS = ["ethereum", "solana", "shiba-inu"]
publisher = pubsub_v1.PublisherClient()
topic_price_path = publisher.topic_path(PROJECT_ID, TOPIC_PRICE_ID)
topic_ta_path = publisher.topic_path(PROJECT_ID, TOPIC_TA_ID)

def get_real_prices():
    """
    Call CoinCap price API and return:
    {
      "timestamp": 1763818013693,
      "ETH": 2716.829999999999927240,
      "SOL": 125.677077839999995490,
      "SHIB": 0.000007612000000000
    }
    """
    symbols_str = ",".join(SYMBOLS)
    # Encode "ETH,SOL,SHIB" -> "ETH%2CSOL%2CSHIB"
    encoded_symbols = requests.utils.quote(symbols_str, safe="")
    url = f"{COINCAP_BASE_URL}/price/bysymbol/{encoded_symbols}"
    headers = {"accept": "application/json"}
    if COINCAP_API_KEY:
        headers["Authorization"] = f"Bearer {COINCAP_API_KEY}"
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    payload = resp.json()
    timestamp = payload["timestamp"]
    raw_prices = payload["data"]  # list of strings in same order as SYMBOLS
    result = {"timestamp": timestamp}
    for symbol, price_str in zip(SYMBOLS, raw_prices):
        result[symbol] = float(price_str)
    return result


def publish_price_message(price_message: dict):
    data_bytes = json.dumps(price_message).encode("utf-8")
    future = publisher.publish(topic_price_path, data=data_bytes)
    message_id = future.result()
    print(f"[{datetime.datetime.utcnow().isoformat()}Z] "
          f"Published REAL prices (message_id={message_id}): {price_message}")


def get_real_ta_for_slug(slug: str, symbol: str, fetch_interval: str = "h1") -> dict:
    """
    Fetch real TA from CoinCap for a single slug and return a normalized message:

    {
        "timestamp": ms,
        "symbol": "ETH",
        "sma": float,
        "rsi": float,
        "macd": float,
        "macd_signal": float,
        "macd_hist": float,
        "vwap24": float,
        "time": ms,
        "date": "ISO8601 UTC"
    }
    """
    url = f"{COINCAP_BASE_URL}/ta/{slug}/allLatest"
    params = {"fetchInterval": fetch_interval}
    headers = {"accept": "*/*"}
    if COINCAP_API_KEY:
        headers["Authorization"] = f"Bearer {COINCAP_API_KEY}"
    resp = requests.get(url, headers=headers, params=params, timeout=20)
    resp.raise_for_status()
    payload = resp.json()
    timestamp_ms = payload["timestamp"]
    iso = datetime.datetime.utcfromtimestamp(timestamp_ms / 1000.0).isoformat() + "Z"
    sma = float(payload["sma"]["sma"])
    rsi = float(payload["rsi"]["rsi"])
    macd = float(payload["macd"]["macd"])
    macd_signal = float(payload["macd"]["signal"])
    macd_hist = float(payload["macd"]["histogram"])
    time = payload["sma"]["time"]
    vwap = float(payload["vwap24"])

    return {
        "timestamp": timestamp_ms,
        "symbol": symbol,
        "sma": sma,
        "rsi": rsi,
        "macd": macd,
        "macd_signal": macd_signal,
        "macd_hist": macd_hist,
        "vwap24": vwap,
        "time": time,
        "date": iso,
    }

def get_real_ta(fetch_interval: str = "h1"):
    """
    Yield one TA message per symbol/slug pair.
    """
    for symbol, slug in zip(SYMBOLS, SLUGS):
        yield get_real_ta_for_slug(slug=slug, symbol=symbol, fetch_interval=fetch_interval)

def publish_ta_message(ta_message: dict):
    data_bytes = json.dumps(ta_message).encode("utf-8")
    future = publisher.publish(topic_ta_path, data=data_bytes)
    message_id = future.result()
    print(
        f"[{datetime.datetime.utcnow().isoformat()}Z] "
        f"Published REAL TA (message_id={message_id}): {ta_message}"
    )

def run_ta_parallel(fetch_interval: str = "h1"):
    """
    Fetch TA for all symbols in parallel and publish each message
    as soon as it is ready.
    """
    with ThreadPoolExecutor(max_workers=len(SLUGS)) as executor:
        future_to_symbol = {
            executor.submit(get_real_ta_for_slug, slug, symbol, fetch_interval): symbol
            for symbol, slug in zip(SYMBOLS, SLUGS)
        }

        for future in as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            try:
                ta_message = future.result()
                publish_ta_message(ta_message)
            except requests.exceptions.Timeout:
                print(f"[{datetime.datetime.utcnow().isoformat()}Z] "
                      f"TA request timed out for symbol {symbol}")
            except Exception as e:
                print(f"[{datetime.datetime.utcnow().isoformat()}Z] "
                      f"Error fetching/publishing TA for {symbol}: {e}")

def main():
    while True:
        price_message = get_real_prices()
        publish_price_message(price_message)
        for ta_message in get_real_ta(fetch_interval="h1"):
            publish_ta_message(ta_message)
        time.sleep(15)

if __name__ == "__main__":
    main()