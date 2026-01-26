"""
Dataflow with ML models (Sentiment Analysis) - MongoDB output version.
ARIMA forecasting removed, but argument retained for compatibility.
"""

import argparse
import json
import logging
from datetime import datetime, timezone
from typing import Optional

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam import window

import pymongo
from transformers import pipeline


PROJECT_ID = "big-data-crypto-sentiment-test"
SUBSCRIPTION_TWEETS = f"projects/{PROJECT_ID}/subscriptions/crypto-tweets-stream-sub"
SUBSCRIPTION_PRICES = f"projects/{PROJECT_ID}/subscriptions/crypto-prices-stream-sub"

TARGET_SYMBOLS = ["ETH", "SOL", "FTM", "SHIB"]
# ARIMA model bundle stored in GCS (re-added constant)
ARIMA_MODELS_GCS_URI = (
    "gs://big-data-crypto-sentiment-test-arima-models/models/arima_models.joblib"
)
HF_MODEL_NAME = "Harsha901/tinybert-imdb-sentiment-analysis-model"


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


def _tweet_sentiment_to_mongo(record: dict) -> dict:
    return {
        "event_timestamp": record.get("event_timestamp"),
        "symbol": record.get("symbol"),
        "text": record.get("text"),
        "sentiment_score": _coerce_float(record.get("sentiment_score")),
        "sentiment_magnitude": _coerce_float(record.get("sentiment_magnitude")),
        "sentiment_label": record.get("sentiment_label"),
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
        if doc is None or self._collection is None:
            return []
        try:
            self._collection.insert_one(doc)
        except Exception:
            logging.exception(
                "MongoDB insert failed for collection %s", self._collection_name
            )
        return []


class ParseTweetFn(beam.DoFn):
    def process(self, element):
        try:
            record = json.loads(element.decode("utf-8"))
            symbol = record.get("crypto_key")
            if symbol:
                yield (symbol, record)
        except Exception:
            return


class ParseAndExplodePriceFn(beam.DoFn):
    def process(self, element):
        try:
            record = json.loads(element.decode("utf-8"))
            ts = record.get("timestamp", 0)

            for symbol in TARGET_SYMBOLS:
                if symbol in record:
                    price_data = {"price": float(record[symbol]), "timestamp": ts}
                    yield (symbol, price_data)
        except Exception:
            return


class HfSentimentFn(beam.DoFn):
    def __init__(self, model_name: str = HF_MODEL_NAME, max_length: int = 512):
        self._model_name = model_name
        self._max_length = max_length
        self._pipeline = None
        self._use_top_k = None

    def setup(self):
        self._pipeline = pipeline(
            "sentiment-analysis",
            model=self._model_name,
            tokenizer=self._model_name,
            device=-1,
        )

    @staticmethod
    def _normalize_label(label: str) -> Optional[str]:
        key = (label or "").strip().lower()
        if key in ("label_0", "negative"):
            return "NEGATIVE"
        if key in ("label_1", "neutral"):
            return "NEUTRAL"
        if key in ("label_2", "positive"):
            return "POSITIVE"
        return None

    def _extract_scores(self, result):
        if isinstance(result, dict):
            items = [result]
        elif not result:
            return {}
        elif isinstance(result[0], list):
            items = result[0]
        else:
            items = result

        scores = {}
        for item in items:
            label = self._normalize_label(item.get("label", ""))
            if label:
                scores[label] = float(item.get("score", 0.0))
        return scores

    def process(self, element, ts=beam.DoFn.TimestampParam):
        symbol, record = element
        text = (record.get("text") or "").strip()

        out = {
            "event_timestamp": ts.to_utc_datetime().strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": symbol,
            "text": text,
            "sentiment_score": None,
            "sentiment_magnitude": None,
            "sentiment_label": None,
        }

        if not text:
            yield out
            return

        if len(text) > 10000:
            text = text[:10000]
            out["text"] = text

        try:
            if self._pipeline is None:
                self.setup()
            if self._use_top_k is None:
                try:
                    result = self._pipeline(
                        text, truncation=True, max_length=self._max_length, top_k=None
                    )
                    self._use_top_k = True
                except TypeError:
                    result = self._pipeline(
                        text, truncation=True, max_length=self._max_length, return_all_scores=True
                    )
                    self._use_top_k = False
            elif self._use_top_k:
                result = self._pipeline(
                    text, truncation=True, max_length=self._max_length, top_k=None
                )
            else:
                result = self._pipeline(
                    text, truncation=True, max_length=self._max_length, return_all_scores=True
                )
        except Exception:
            yield out
            return

        scores = self._extract_scores(result)
        if not scores:
            yield out
            return

        neg = scores.get("NEGATIVE", 0.0)
        neu = scores.get("NEUTRAL", 0.0)
        pos = scores.get("POSITIVE", 0.0)
        total = neg + neu + pos
        if total <= 0:
            yield out
            return

        neg /= total
        neu /= total
        pos /= total

        out["sentiment_score"] = pos - neg
        out["sentiment_magnitude"] = pos + neg

        label_scores = {"NEGATIVE": neg, "NEUTRAL": neu, "POSITIVE": pos}
        out["sentiment_label"] = max(label_scores, key=label_scores.get)

        yield out


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
    # Re-added the ARIMA models GCS URI argument
    parser.add_argument("--arima_models_gcs_uri", default=ARIMA_MODELS_GCS_URI)
    parser.add_argument("--mongo_uri", default=None)
    parser.add_argument("--mongo_db", default="crypto_analysis")
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

        _write_to_mongo(
            tweets_kv,
            "RawTweetsMongo",
            "raw_tweets",
            mongo_uri,
            mongo_db,
            _raw_tweet_kv_to_mongo,
        )

        # Sentiment ML Branch
        tweet_sentiment = (
            tweets_kv
            | "TweetSentimentNLP" >> beam.ParDo(HfSentimentFn())
        )
        
        _write_to_mongo(
            tweet_sentiment,
            "TweetSentimentMongo",
            "tweet_sentiment",
            mongo_uri,
            mongo_db,
            _tweet_sentiment_to_mongo,
        )

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

        # --- JOIN & ANALYSIS ---
        joined_data = (
            {"tweets": windowed_tweets, "prices": windowed_prices}
            | "JoinStreams" >> beam.CoGroupByKey()
            | "Analyze" >> beam.ParDo(AnalyzeBatchFn())
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
    logging.getLogger().setLevel(logging.INFO)
    run()