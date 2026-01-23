"""
Dataflow without online ML - helper for MongoDB sanity checking.
"""

import argparse
import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam import window
from datetime import datetime, timezone
from typing import Optional

import pymongo


PROJECT_ID = "big-data-crypto-sentiment-test"
SUBSCRIPTION_TWEETS = f"projects/{PROJECT_ID}/subscriptions/crypto-tweets-stream-sub"
SUBSCRIPTION_PRICES = f"projects/{PROJECT_ID}/subscriptions/crypto-prices-stream-sub"


BQ_TABLE_ANALYSIS = f"{PROJECT_ID}:crypto_analysis.crypto_prices_with_tweets"
BQ_RAW_TWEETS = f"{PROJECT_ID}:crypto_analysis.raw_tweets"
BQ_RAW_PRICES = f"{PROJECT_ID}:crypto_analysis.raw_prices"


def _parse_rfc3339_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except ValueError:
        return None


def _parse_window_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        return parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            parsed = datetime.fromisoformat(value)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        except ValueError:
            return None


def _coerce_int(value: Optional[object]) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_float(value: Optional[object]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _raw_tweet_to_mongo(record: dict) -> dict:
    return {
        "id": record.get("id"),
        "text": record.get("text"),
        "author_id": record.get("author_id"),
        "crypto_key": record.get("crypto_key"),
        "created_at_raw": record.get("created_at_raw"),
        "created_at_iso": _parse_rfc3339_timestamp(record.get("created_at_iso")),
        "timestamp_ms": _coerce_int(record.get("timestamp_ms")),
        "timestamp_sec": _coerce_int(record.get("timestamp_sec")),
    }


def _raw_tweet_kv_to_mongo(element) -> dict:
    _, record = element
    return _raw_tweet_to_mongo(record)


def _raw_price_to_mongo(symbol: str, price_data: dict) -> dict:
    return {
        "symbol": symbol,
        "price": _coerce_float(price_data.get("price")),
        "timestamp": _coerce_int(price_data.get("timestamp")),
    }


def _raw_price_kv_to_mongo(element) -> dict:
    symbol, price_data = element
    return _raw_price_to_mongo(symbol, price_data)


def _windowed_metrics_to_mongo(record: dict) -> Optional[dict]:
    event_ts = _parse_window_timestamp(record.get("event_timestamp"))
    if event_ts is None:
        return None
    return {
        "event_timestamp": event_ts,
        "symbol": record.get("symbol"),
        "tweet_volume": _coerce_int(record.get("tweet_volume")),
        "avg_price": _coerce_float(record.get("avg_price")),
        "last_price": _coerce_float(record.get("last_price")),
        "tweet_texts": record.get("tweet_texts"),
    }


def _is_not_none(value) -> bool:
    return value is not None


def _write_to_mongo(
    pcoll,
    label_prefix: str,
    collection: str,
    mongo_uri: str,
    mongo_db: str,
    transform_fn,
):
    if not mongo_uri:
        return
    (
        pcoll
        | f"{label_prefix}ToMongoDoc" >> beam.Map(transform_fn)
        | f"{label_prefix}FilterMongoDoc" >> beam.Filter(_is_not_none)
        | f"{label_prefix}WriteMongo"
        >> beam.ParDo(MongoWriteFn(mongo_uri, mongo_db, collection))
    )


class MongoWriteFn(beam.DoFn):
    def __init__(self, mongo_uri: str, mongo_db: str, collection: str):
        self._mongo_uri = mongo_uri
        self._mongo_db = mongo_db
        self._collection_name = collection
        self._client = None
        self._collection = None

    def setup(self):
        if not self._mongo_uri:
            return
        self._client = pymongo.MongoClient(
            self._mongo_uri, serverSelectionTimeoutMS=5000
        )
        self._collection = self._client[self._mongo_db][self._collection_name]

    def teardown(self):
        if self._client:
            self._client.close()

    def process(self, doc):
        if doc is None or not self._collection:
            return
        try:
            self._collection.insert_one(doc)
        except Exception:
            logging.exception(
                "MongoDB insert failed for collection %s", self._collection_name
            )


class ParseTweetFn(beam.DoFn):
    """
    Parses raw Tweet Pub/Sub messages.
    Output: (Symbol, TweetDict)
    """

    def process(self, element):
        try:
            record = json.loads(element.decode("utf-8"))
            symbol = record.get("crypto_key")
            yield (symbol, record)
        except Exception:
            pass


class ParseAndExplodePriceFn(beam.DoFn):
    """
    Parses Price Pub/Sub messages and SPLITS them.
    Output: (Symbol, {'price': 123.45, 'timestamp': 17000...})
    """

    def process(self, element):
        try:
            record = json.loads(element.decode("utf-8"))
            ts = record.get("timestamp", 0)

            target_symbols = ["ETH", "SOL", "FTM", "SHIB"]

            for symbol in target_symbols:
                if symbol in record:
                    price_data = {"price": record[symbol], "timestamp": ts}
                    yield (symbol, price_data)

        except Exception:
            pass


class AnalyzeBatchFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        symbol, data = element
        tweets = data["tweets"]
        prices = data["prices"]

        window_end = window.end.to_utc_datetime().strftime("%Y-%m-%d %H:%M:%S")

        avg_price = 0.0
        last_price = 0.0

        if prices:
            sorted_prices = sorted(prices, key=lambda x: x["timestamp"])
            last_price = float(sorted_prices[-1]["price"])
            total_val = sum(p["price"] for p in prices)
            avg_price = float(total_val / len(prices))

        tweet_count = len(tweets)

        tweet_texts = [t.get("text") for t in tweets]

        yield {
            "event_timestamp": window_end,
            "symbol": symbol,
            "tweet_volume": tweet_count,
            "avg_price": avg_price,
            "last_price": last_price,
            "tweet_texts": tweet_texts,
        }


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mongo_uri",
        default=None,
        help="MongoDB connection string, e.g. mongodb://10.128.0.5:27017",
    )
    parser.add_argument(
        "--mongo_db",
        default="crypto_analysis",
        help="MongoDB database name to store the mirrored collections",
    )
    known_args, pipeline_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True
    mongo_uri = known_args.mongo_uri
    mongo_db = known_args.mongo_db

    with beam.Pipeline(options=pipeline_options) as p:
        # --- BRANCH 1: TWEETS ---
        tweets_kv = (
            p
            | "ReadTweets"
            >> beam.io.ReadFromPubSub(
                subscription=SUBSCRIPTION_TWEETS, timestamp_attribute="event_timestamp"
            )
            | "ParseTweets" >> beam.ParDo(ParseTweetFn())
        )

        # Path A: Write Raw Archive
        # We extract just the Dict (record) from the Tuple (Symbol, Record)
        (
            tweets_kv
            | "ExtractTweetRecord" >> beam.Map(lambda x: x[1])
            | "WriteRawTweets"
            >> beam.io.WriteToBigQuery(
                BQ_RAW_TWEETS,
                # No Schema needed -> Terraform manages it
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                method="STREAMING_INSERTS",
            )
        )

        _write_to_mongo(
            tweets_kv,
            "RawTweetsMongo",
            "raw_tweets",
            mongo_uri,
            mongo_db,
            _raw_tweet_kv_to_mongo,
        )

        # Path B: Window for Analysis
        windowed_tweets = tweets_kv | "WindowTweets" >> beam.WindowInto(
            window.FixedWindows(30)
        )

        # --- BRANCH 2: PRICES ---
        prices_kv = (
            p
            | "ReadPrices"
            >> beam.io.ReadFromPubSub(
                subscription=SUBSCRIPTION_PRICES, timestamp_attribute="event_timestamp"
            )
            | "ExplodePrices" >> beam.ParDo(ParseAndExplodePriceFn())
        )

        (
            prices_kv
            | "FlattenPriceRecord" >> beam.Map(lambda x: {"symbol": x[0], **x[1]})
            | "WriteRawPrices"
            >> beam.io.WriteToBigQuery(
                BQ_RAW_PRICES,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                method="STREAMING_INSERTS",
            )
        )

        _write_to_mongo(
            prices_kv,
            "RawPricesMongo",
            "raw_prices",
            mongo_uri,
            mongo_db,
            _raw_price_kv_to_mongo,
        )

        windowed_prices = prices_kv | "WindowPrices" >> beam.WindowInto(
            window.FixedWindows(30)
        )

        joined_data = (
            {"tweets": windowed_tweets, "prices": windowed_prices}
            | "JoinStreams" >> beam.CoGroupByKey()
            | "Analyze" >> beam.ParDo(AnalyzeBatchFn())
        )

        (
            joined_data
            | "WriteAnalysisToBQ"
            >> beam.io.WriteToBigQuery(
                BQ_TABLE_ANALYSIS,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                method="STREAMING_INSERTS",
            )
        )
        _write_to_mongo(
            joined_data,
            "WindowedMetricsMongo",
            "crypto_prices_with_tweets",
            mongo_uri,
            mongo_db,
            _windowed_metrics_to_mongo,
        )


if __name__ == "__main__":
    run()
