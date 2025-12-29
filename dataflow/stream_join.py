import argparse
import json
import logging
import io
import time
import random
from typing import Optional

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam import window
from apache_beam.io.filesystems import FileSystems

import joblib
import pmdarima as pm

from google.cloud import language_v1


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


class GcpNlpSentimentFn(beam.DoFn):
    """
    Cloud Natural Language API sentiment per tweet.

    Writes to NEW table only (does not alter existing join/analysis output).
    Sentiment score is in [-1.0, 1.0], magnitude >= 0. :contentReference[oaicite:3]{index=3}
    """

    def __init__(
        self,
        language_hint: Optional[str] = None,
        max_retries: int = 3,
        timeout_s: float = 5.0,
    ):
        self._language_hint = language_hint
        self._max_retries = max_retries
        self._timeout_s = timeout_s
        self._client = None

    def setup(self):
        # Create client once per worker
        self._client = (
            language_v1.LanguageServiceClient()
        )  # :contentReference[oaicite:4]{index=4}

    @staticmethod
    def _label_from_score(score: float) -> str:
        # Simple thresholds; adjust if you want
        if score <= -0.25:
            return "NEGATIVE"
        if score >= 0.25:
            return "POSITIVE"
        return "NEUTRAL"

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

        document = language_v1.Document(
            content=text,
            type_=language_v1.Document.Type.PLAIN_TEXT,
            language=self._language_hint or "",
        )

        last_err = None
        for attempt in range(self._max_retries + 1):
            try:
                response = self._client.analyze_sentiment(
                    request={
                        "document": document,
                        "encoding_type": language_v1.EncodingType.UTF8,
                    },
                    timeout=self._timeout_s,
                )
                score = float(response.document_sentiment.score)
                magnitude = float(response.document_sentiment.magnitude)

                out["sentiment_score"] = score
                out["sentiment_magnitude"] = magnitude
                out["sentiment_label"] = self._label_from_score(score)
                yield out
                return
            except Exception as e:
                last_err = e
                if attempt < self._max_retries:
                    sleep_s = (2**attempt) * 0.2 + random.random() * 0.2
                    time.sleep(sleep_s)

        # On failure: emit row with null sentiment (pipeline continues)
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
        "--nlp_language_hint",
        default=None,
        help="Optional BCP-47 language code, e.g. en",
    )
    known_args, pipeline_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

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

        # NEW: Sentiment branch -> NEW table only
        (
            tweets_kv
            | "TweetSentimentNLP"
            >> beam.ParDo(GcpNlpSentimentFn(language_hint=known_args.nlp_language_hint))
            | "WriteTweetSentiment"
            >> beam.io.WriteToBigQuery(
                BQ_TWEET_SENTIMENT,
                schema="event_timestamp:STRING,symbol:STRING,text:STRING,sentiment_score:FLOAT,sentiment_magnitude:FLOAT,sentiment_label:STRING",
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                method="STREAMING_INSERTS",
            )
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

        # NEW: ARIMA forecast branch -> NEW table only
        (
            prices_kv
            | "ARIMAForecast"
            >> beam.ParDo(
                ArimaForecastFn(model_bundle_gcs_uri=known_args.arima_models_gcs_uri)
            )
            | "WritePriceForecasts"
            >> beam.io.WriteToBigQuery(
                BQ_PRICE_FORECASTS,
                schema="event_timestamp:STRING,symbol:STRING,price:FLOAT,price_timestamp:INTEGER,arima_next_price_forecast:FLOAT",
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                method="STREAMING_INSERTS",
            )
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


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
