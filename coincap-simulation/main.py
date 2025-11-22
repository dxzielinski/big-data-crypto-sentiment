import os
import random
import json
import time
import datetime
from google.cloud import pubsub_v1

PROJECT_ID = "big-data-crypto-sentiment"
TOPIC_PRICE_ID = "CoinCapPriceStreamSimulated"
TOPIC_TA_ID = "CoinCapTAStreamSimulated"

COINCAP_BASE_URL = "https://rest.coincap.io/v3"
COINCAP_API_KEY = os.getenv("COINCAP_API_KEY")
SYMBOLS = ["ETH", "SOL", "FTM", "SHIB"]
SLUGS = ["ethereum", "solana", "fantom", "shiba-inu"]

publisher = pubsub_v1.PublisherClient()
topic_price_path = publisher.topic_path(PROJECT_ID, TOPIC_PRICE_ID)
topic_ta_path = publisher.topic_path(PROJECT_ID, TOPIC_TA_ID)
real_prices = [
    2735.391034999999646971,
    127.530000000000001137,
    0.108200000000000003,
    0.000007810000000000,
]
real_ema = [
    2757.7870101656194,
    128.2927647443417,
    0.10588926400193995,
    0.000007878602253280224,
]
real_sma = [
    3001.137233928096,
    136.42378790808945,
    0.11972547918237494,
    0.000008597684091627225,
]
real_rsi = [43.07941169909985, 45.27223199737072, 43.10689947082265, 35.40015464980758]
real_macd = [
    -37.591591243286345,
    -1.9322619158150673,
    -0.003097579927973009,
    -1.3896534889595627e-7,
]
real_macd_signal = [
    -49.08549646354389,
    -2.3879750225057736,
    -0.0032616956468224842,
    -1.531006499115531e-7,
]
real_macd_hist = [
    11.493905220257545,
    0.4557131066907061,
    0.00016411571884947528,
    1.4135301015596835e-8,
]
real_vwap24 = [
    2775.7097698014663,
    129.78452069267738,
    0.10916873787174941,
    0.0000080309604145788,
]


def generate_simulated_prices(base_prices, spread=0.05):
    """
    Generate simulated prices within ±spread (default 5%) of the base prices.
    Each call produces a new random price in that range.
    """
    simulated = {}
    for symbol, base_price in zip(SYMBOLS, base_prices):
        factor = random.uniform(1 - spread, 1 + spread)
        simulated[symbol] = base_price * factor
    return simulated


def publish_price_message(simulated_prices):
    """
    Publish a message to Pub/Sub with example structure:
    {
      "timestamp": 1763753634750,
      "BTC": 84739.05,
      "ETH": 2770.14,
      "SOL": 129.18,
      "XRP": 1.96,
      "ADA": 0.41,
    }
    """
    message = {
        "timestamp": int(time.time() * 1000),  # ms
    }
    for symbol, price in simulated_prices.items():
        message[symbol] = price
    data_bytes = json.dumps(message).encode("utf-8")
    future = publisher.publish(topic_price_path, data=data_bytes)
    message_id = future.result()
    print(
        f"[{datetime.datetime.utcnow().isoformat()}Z] "
        f"Published simulated prices (message_id={message_id}): {message}"
    )


def generate_simulated_ta(spread=0.05):
    """
    Generate simulated TA values within ±spread (default 5%) of the base values.

    Yields one message per symbol, e.g.:

    {
      "timestamp": 1763753634750,
      "symbol": "BTC",
      "sma": ...,
      "rsi": ...,
      "macd": ...,
      "macd_signal": ...,
      "macd_hist": ...,
      "vwap24": ...,
      "time": ...,
      "date": "2025-11-21T19:00:08.753Z"
    }
    """
    if not all(
        len(lst) == len(SYMBOLS)
        for lst in [
            real_sma,
            real_rsi,
            real_macd,
            real_macd_signal,
            real_macd_hist,
            real_vwap24,
        ]
    ):
        raise ValueError("All TA base lists must be the same length as SYMBOLS")

    timestamp_ms = int(time.time() * 1000)
    iso = datetime.datetime.utcnow().isoformat() + "Z"

    for (
        symbol,
        base_sma,
        base_rsi,
        base_macd,
        base_macd_signal,
        base_macd_hist,
        base_vwap,
    ) in zip(
        SYMBOLS,
        real_sma,
        real_rsi,
        real_macd,
        real_macd_signal,
        real_macd_hist,
        real_vwap24,
    ):
        def jitter(value, clamp=None):
            val = value * random.uniform(1 - spread, 1 + spread)
            if clamp is not None:
                lo, hi = clamp
                val = max(lo, min(hi, val))
            return val
        sma = jitter(base_sma)
        rsi = jitter(base_rsi, clamp=(0.0, 100.0))  # RSI in [0, 100]
        macd = jitter(base_macd)
        macd_signal = jitter(base_macd_signal)
        macd_hist = jitter(base_macd_hist)
        vwap = jitter(base_vwap)
        yield {
            "timestamp": timestamp_ms,
            "symbol": symbol,
            "sma": sma,
            "rsi": rsi,
            "macd": macd,
            "macd_signal": macd_signal,
            "macd_hist": macd_hist,
            "vwap24": vwap,
            "time": timestamp_ms,
            "date": iso,
        }

def publish_ta_message(simulated_ta_message):
    """
    Publish TA message to Pub/Sub.
    """
    data_bytes = json.dumps(simulated_ta_message).encode("utf-8")
    future = publisher.publish(topic_ta_path, data=data_bytes)
    message_id = future.result()

    print(
        f"[{datetime.datetime.utcnow().isoformat()}Z] "
        f"Published simulated TA (message_id={message_id}): {simulated_ta_message}"
    )


def main():
    print(f"Base prices from CoinCap: {real_prices}")
    print("Starting simulated PRICE + TA stream...")

    while True:
        simulated_prices = generate_simulated_prices(real_prices, spread=0.05)
        publish_price_message(simulated_prices)
        for ta_message in generate_simulated_ta(spread=0.05):
            publish_ta_message(ta_message)
        time.sleep(15)


if __name__ == "__main__":
    main()
