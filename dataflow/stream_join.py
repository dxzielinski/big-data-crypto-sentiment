import argparse
import json
import logging
import io
from datetime import datetime, timezone
from typing import Optional

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam import window
from apache_beam.io.filesystems import FileSystems

import joblib
import pymongo
import pmdarima as pm

from transformers import pipeline


PROJECT_ID = "big-data-crypto-sentiment-test"
SUBSCRIPTION_TWEETS = f"projects/{PROJECT_ID}/subscriptions/crypto-tweets-stream-sub"
SUBSCRIPTION_PRICES = f"projects/{PROJECT_ID}/subscriptions/crypto-prices-stream-sub"

# EXISTING TABLES (DO NOT CHANGE)
BQ_TABLE_ANALYSIS = f"{PROJECT_ID}:crypto_analysis.crypto_prices_with_tweets"
BQ_RAW_TWEETS = f"{PROJECT_ID}:crypto_analysis.raw_tweets"
BQ_RAW_PRICES = f"{PROJECT_ID}:crypto_analysis.raw_prices"

# NEW TABLES (SAFE: do not affect existing schemas)
BQ_TWEET_SENTIMENT = f"{PROJECT_ID}:crypto_analysis.tweet_sentiment"
BQ_PRICE_FORECASTS = f"{PROJECT_ID}:crypto_analysis.price_forecasts"

TARGET_SYMBOLS = ["ETH", "SOL", "FTM", "SHIB"]

# ARIMA model bundle stored in GCS
# Bundle format: joblib dict like {"ETH": <pmdarima model>, "SOL": <pmdarima model>, ...}
ARIMA_MODELS_GCS_URI = (
    "gs://big-data-crypto-sentiment-test-arima-models/models/arima_models.joblib"
)
HF_MODEL_NAME = "cardiffnlp/twitter-roberta-base-sentiment-latest"


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


def _price_forecast_to_mongo(record: dict) -> dict:
    return {
        "event_timestamp": record.get("event_timestamp"),
        "symbol": record.get("symbol"),
        "price": _coerce_float(record.get("price")),
        "price_timestamp": _coerce_int(record.get("price_timestamp")),
        "arima_next_price_forecast": _coerce_float(
            record.get("arima_next_price_forecast")
        ),
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
    """
    Parses raw Tweet Pub/Sub messages.
    Output: (Symbol, TweetDict)
    """

    def process(self, element):
        try:
            record = json.loads(element.decode("utf-8"))
            symbol = record.get("crypto_key")
            if symbol:
                yield (symbol, record)
        except Exception:
            return


class ParseAndExplodePriceFn(beam.DoFn):
    """
    Parses Price Pub/Sub messages and SPLITS them.
    Output: (Symbol, {'price': 123.45, 'timestamp': 17000...})
    """

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
    """
    Hugging Face CardiffNLP sentiment per tweet.

    Writes to NEW table only (does not alter existing join/analysis output).
    Sentiment score is in [-1.0, 1.0], magnitude in [0.0, 1.0].
    """

    def __init__(self, model_name: str = HF_MODEL_NAME, max_length: int = 512):
        self._model_name = model_name
        self._max_length = max_length
        self._pipeline = None
        self._use_top_k = None

    def setup(self):
        # Load model/tokenizer once per worker
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

        # Keep it minimal; don’t mutate the original record (used elsewhere).
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

        # Defensive truncation for very long text
        if len(text) > 10000:
            text = text[:10000]
            out["text"] = text

        try:
            if self._pipeline is None:
                self.setup()
            if self._use_top_k is None:
                try:
                    result = self._pipeline(
                        text,
                        truncation=True,
                        max_length=self._max_length,
                        top_k=None,
                    )
                    self._use_top_k = True
                except TypeError:
                    result = self._pipeline(
                        text,
                        truncation=True,
                        max_length=self._max_length,
                        return_all_scores=True,
                    )
                    self._use_top_k = False
            elif self._use_top_k:
                result = self._pipeline(
                    text,
                    truncation=True,
                    max_length=self._max_length,
                    top_k=None,
                )
            else:
                result = self._pipeline(
                    text,
                    truncation=True,
                    max_length=self._max_length,
                    return_all_scores=True,
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


class ArimaForecastFn(beam.DoFn):
    """
    Loads pre-trained pmdarima models from GCS once per worker.
    Maintains a per-symbol model in-memory in this DoFn instance and updates it per observation.
    (No Beam state APIs used -> avoids ValueStateSpec import issues.)
    """

    def __init__(self, model_bundle_gcs_uri: str):
        self._model_bundle_gcs_uri = model_bundle_gcs_uri
        self._models_by_symbol = {}

    def setup(self):
        # Load dict(symbol -> model) from GCS
        with FileSystems.open(self._model_bundle_gcs_uri) as f:
            data = f.read()
        self._models_by_symbol = joblib.load(io.BytesIO(data))

    def process(self, element, ts=beam.DoFn.TimestampParam):
        symbol, price_data = element
        price = float(price_data["price"])

        model = self._models_by_symbol.get(symbol)
        forecast = None

        if model is not None:
            try:
                # Update model with the new observation
                model.update([price])
            except Exception:
                pass

            try:
                # One-step ahead forecast
                forecast = float(model.predict(n_periods=1)[0])
            except Exception:
                forecast = None

            # Keep the updated model in memory for next elements
            self._models_by_symbol[symbol] = model

        yield {
            "event_timestamp": ts.to_utc_datetime().strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": symbol,
            "price": price,
            "price_timestamp": price_data.get("timestamp"),
            "arima_next_price_forecast": forecast,
        }


class AnalyzeBatchFn(beam.DoFn):
    # IMPORTANT: UNCHANGED OUTPUT SCHEMA (no new columns)
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
    parser.add_argument("--arima_models_gcs_uri", default=ARIMA_MODELS_GCS_URI)
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
    parser.add_argument(
        "--nlp_language_hint",
        default=None,
        help="Optional BCP-47 language code, e.g. en",
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

        # Path A: Write Raw Archive (UNCHANGED)
        (
            tweets_kv
            | "ExtractTweetRecord" >> beam.Map(lambda x: x[1])
            | "WriteRawTweets"
            >> beam.io.WriteToBigQuery(
                BQ_RAW_TWEETS,
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

        # NEW: Sentiment branch -> NEW table only
        tweet_sentiment = (
            tweets_kv
            | "TweetSentimentNLP"
            >> beam.ParDo(HfSentimentFn())
        )
        (
            tweet_sentiment
            | "WriteTweetSentiment"
            >> beam.io.WriteToBigQuery(
                BQ_TWEET_SENTIMENT,
                schema="event_timestamp:STRING,symbol:STRING,text:STRING,sentiment_score:FLOAT,sentiment_magnitude:FLOAT,sentiment_label:STRING",
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                method="STREAMING_INSERTS",
            )
        )
        _write_to_mongo(
            tweet_sentiment,
            "TweetSentimentMongo",
            "tweet_sentiment",
            mongo_uri,
            mongo_db,
            _tweet_sentiment_to_mongo,
        )

        # Path B: Window for Analysis (UNCHANGED logic: uses original tweets_kv)
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

        # Raw archive (UNCHANGED)
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

        # NEW: ARIMA forecast branch -> NEW table only
        price_forecasts = (
            prices_kv
            | "ARIMAForecast"
            >> beam.ParDo(
                ArimaForecastFn(model_bundle_gcs_uri=known_args.arima_models_gcs_uri)
            )
        )
        (
            price_forecasts
            | "WritePriceForecasts"
            >> beam.io.WriteToBigQuery(
                BQ_PRICE_FORECASTS,
                schema="event_timestamp:STRING,symbol:STRING,price:FLOAT,price_timestamp:INTEGER,arima_next_price_forecast:FLOAT",
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                method="STREAMING_INSERTS",
            )
        )
        _write_to_mongo(
            price_forecasts,
            "PriceForecastsMongo",
            "price_forecasts",
            mongo_uri,
            mongo_db,
            _price_forecast_to_mongo,
        )

        # Window for Join/Analysis (UNCHANGED logic: uses original prices_kv)
        windowed_prices = prices_kv | "WindowPrices" >> beam.WindowInto(
            window.FixedWindows(30)
        )

        # JOIN + ANALYZE (UNCHANGED output schema -> writes to EXISTING table safely)
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
    logging.getLogger().setLevel(logging.INFO)
    run()
