import argparse
import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam import window
from datetime import datetime, timezone


PROJECT_ID = "big-data-crypto-sentiment-test"
SUBSCRIPTION_TWEETS = f"projects/{PROJECT_ID}/subscriptions/crypto-tweets-stream-sub"
SUBSCRIPTION_PRICES = f"projects/{PROJECT_ID}/subscriptions/crypto-prices-stream-sub"


BQ_TABLE_SPEC = f"{PROJECT_ID}:crypto_analysis.crypto_prices_with_tweets"

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
                    price_data = {
                        "price": record[symbol],
                        "timestamp": ts
                    }
                    yield (symbol, price_data)
                    
        except Exception:
            pass

class AnalyzeBatchFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        symbol, data = element
        tweets = data['tweets']
        prices = data['prices']

        window_end = window.end.to_utc_datetime().strftime("%Y-%m-%d %H:%M:%S")

        avg_price = 0.0
        last_price = 0.0
        
        if prices:
            sorted_prices = sorted(prices, key=lambda x: x['timestamp'])
            last_price = float(sorted_prices[-1]['price'])
            total_val = sum(p['price'] for p in prices)
            avg_price = float(total_val / len(prices))

        tweet_count = len(tweets)
        
        tweet_texts = [t.get("text") for t in tweets]

        yield {
            "event_timestamp": window_end,
            "symbol": symbol,
            "tweet_volume": tweet_count,
            "avg_price": avg_price,
            "last_price": last_price,
            "tweet_texts": tweet_texts
        }

def run():
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args()
    
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:

        tweets = (
            p 
            | "ReadTweets" >> beam.io.ReadFromPubSub(
                subscription=SUBSCRIPTION_TWEETS,
                timestamp_attribute="event_timestamp"
            )
            | "ParseTweets" >> beam.ParDo(ParseTweetFn())
            | "WindowTweets" >> beam.WindowInto(window.FixedWindows(30))
        )

        prices = (
            p
            | "ReadPrices" >> beam.io.ReadFromPubSub(
                subscription=SUBSCRIPTION_PRICES,
                timestamp_attribute="event_timestamp"
            )
            | "ExplodePrices" >> beam.ParDo(ParseAndExplodePriceFn())
            | "WindowPrices" >> beam.WindowInto(window.FixedWindows(30))
        )

        joined_data = (
            {'tweets': tweets, 'prices': prices}
            | "JoinStreams" >> beam.CoGroupByKey()
            | "Analyze" >> beam.ParDo(AnalyzeBatchFn())
        )

        (
            joined_data
            | "WriteToBQ" >> beam.io.WriteToBigQuery(
                BQ_TABLE_SPEC,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                method="STREAMING_INSERTS"
            )
        )

if __name__ == "__main__":
    run()