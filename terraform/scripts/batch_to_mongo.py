#!/usr/bin/env python3
import argparse
import json
import logging
import os
import operator
from functools import reduce
from typing import Dict, List, Tuple

from google.cloud import storage
from pymongo import MongoClient
from pyspark.sql import SparkSession, functions as F, types as T


LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"


def load_state(path: str) -> Dict[str, Dict[str, List[str]]]:
    if not os.path.exists(path):
        return {"processed_objects": {}}
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def save_state(path: str, state: Dict[str, Dict[str, List[str]]]) -> None:
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2, sort_keys=True)
    os.replace(tmp_path, path)


def list_new_blobs(bucket, prefix: str, processed: set) -> List:
    blobs = []
    for blob in bucket.list_blobs(prefix=prefix):
        if not blob.name.endswith(".avro"):
            continue
        if blob.name in processed:
            continue
        blobs.append(blob)
    blobs.sort(key=lambda b: b.name)
    return blobs


def download_blobs(blobs, staging_dir: str) -> Tuple[List[str], List[str]]:
    local_paths = []
    processed_names = []
    for blob in blobs:
        local_path = os.path.join(staging_dir, blob.name)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        blob.download_to_filename(local_path)
        local_paths.append(local_path)
        processed_names.append(blob.name)
    return local_paths, processed_names


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("batch-to-mongo")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def parse_pubsub_df(df):
    data_col = None
    for candidate in ("data", "payload"):
        if candidate in df.columns:
            data_col = candidate
            break
    if data_col is None:
        raise RuntimeError("No data column found in Avro records.")
    map_schema = T.MapType(T.StringType(), T.StringType(), True)
    df = df.withColumn("data_str", F.col(data_col).cast("string"))
    df = df.withColumn("data_map", F.from_json(F.col("data_str"), map_schema))
    return df.filter(F.col("data_map").isNotNull())


def extract_tweets(df):
    dm = F.col("data_map")
    tweets = df.filter(dm.getItem("crypto_key").isNotNull())
    if tweets.rdd.isEmpty():
        return None
    tweets = tweets.select(
        dm.getItem("id").alias("id"),
        dm.getItem("text").alias("text"),
        dm.getItem("author_id").alias("author_id"),
        dm.getItem("crypto_key").alias("crypto_key"),
        dm.getItem("created_at_raw").alias("created_at_raw"),
        dm.getItem("created_at_iso").alias("created_at_iso"),
        dm.getItem("timestamp_ms").cast("long").alias("timestamp_ms"),
        dm.getItem("timestamp_sec").cast("long").alias("timestamp_sec"),
    )
    created_at_iso_ts = F.coalesce(
        F.to_timestamp("created_at_iso"),
        F.to_timestamp(F.from_unixtime(F.col("timestamp_ms") / 1000)),
    )
    tweets = tweets.withColumn("created_at_iso_ts", created_at_iso_ts)
    tweets = tweets.withColumn(
        "event_time",
        F.coalesce(
            F.col("created_at_iso_ts"),
            F.to_timestamp(F.from_unixtime(F.col("timestamp_ms") / 1000)),
        ),
    )
    return tweets


def extract_prices(df, symbols: List[str]):
    if not symbols:
        return None
    dm = F.col("data_map")
    has_symbol = reduce(operator.or_, [dm.getItem(sym).isNotNull() for sym in symbols])
    prices = df.filter(dm.getItem("timestamp").isNotNull() & has_symbol)
    if prices.rdd.isEmpty():
        return None
    entries = F.array(
        [
            F.struct(
                F.lit(sym).alias("symbol"),
                dm.getItem(sym).cast("double").alias("price"),
            )
            for sym in symbols
        ]
    )
    prices_long = prices.select(
        dm.getItem("timestamp").cast("long").alias("timestamp_ms"),
        F.explode(entries).alias("kv"),
    )
    prices_long = prices_long.select(
        "timestamp_ms",
        F.col("kv.symbol").alias("symbol"),
        F.col("kv.price").alias("price"),
    ).filter(F.col("price").isNotNull())
    prices_long = prices_long.withColumn(
        "event_time", F.to_timestamp(F.from_unixtime(F.col("timestamp_ms") / 1000))
    )
    return prices_long


def build_windowed_metrics(tweets, prices, window_minutes: int):
    window_spec = f"{window_minutes} minutes"
    tweet_metrics = None
    price_metrics = None

    if tweets is not None:
        tweets = tweets.filter(F.col("event_time").isNotNull()).withColumn(
            "symbol", F.col("crypto_key")
        )
        tweet_metrics = tweets.groupBy(F.window("event_time", window_spec), "symbol").agg(
            F.count("*").alias("tweet_volume"),
            F.collect_list("text").alias("tweet_texts"),
        )

    if prices is not None:
        prices = prices.filter(F.col("event_time").isNotNull())
        price_metrics = prices.groupBy(F.window("event_time", window_spec), "symbol").agg(
            F.avg("price").alias("avg_price"),
            F.max(F.struct("event_time", "price")).alias("last_struct"),
        )
        price_metrics = price_metrics.select(
            "window",
            "symbol",
            "avg_price",
            F.col("last_struct.price").alias("last_price"),
        )

    if tweet_metrics is None and price_metrics is None:
        return None
    if tweet_metrics is None:
        metrics = price_metrics
    elif price_metrics is None:
        metrics = tweet_metrics
    else:
        metrics = tweet_metrics.join(price_metrics, ["window", "symbol"], "full_outer")

    metrics = metrics.withColumn("event_timestamp", F.col("window.start")).drop("window")
    return metrics


def prepare_raw_tweets(tweets):
    if tweets is None:
        return None
    return tweets.select(
        "id",
        "text",
        "author_id",
        "crypto_key",
        "created_at_raw",
        F.col("created_at_iso_ts").alias("created_at_iso"),
        "timestamp_ms",
        "timestamp_sec",
    )


def prepare_raw_prices(prices):
    if prices is None:
        return None
    return prices.select("symbol", "price", F.col("timestamp_ms").alias("timestamp"))


def write_df_to_mongo(df, mongo_uri: str, mongo_db: str, collection: str) -> None:
    if df is None:
        return
    if df.rdd.isEmpty():
        return

    def write_partition(rows):
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        coll = client[mongo_db][collection]
        batch = []
        try:
            for row in rows:
                doc = row.asDict(recursive=True)
                if doc:
                    batch.append(doc)
                if len(batch) >= 1000:
                    try:
                        coll.insert_many(batch, ordered=False)
                    except Exception:
                        logging.exception("MongoDB insert failed for %s", collection)
                    batch = []
            if batch:
                try:
                    coll.insert_many(batch, ordered=False)
                except Exception:
                    logging.exception("MongoDB insert failed for %s", collection)
        finally:
            client.close()

    df.foreachPartition(write_partition)


def cleanup_files(paths: List[str]) -> None:
    for path in paths:
        try:
            os.remove(path)
        except OSError:
            logging.warning("Failed to remove %s", path)


def parse_args():
    parser = argparse.ArgumentParser(description="Load batch Avro into MongoDB.")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--mongo-uri", required=True)
    parser.add_argument("--mongo-db", required=True)
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--staging-dir", required=True)
    parser.add_argument("--window-minutes", type=int, default=30)
    parser.add_argument("--symbols", default="ETH,SOL,FTM,SHIB")
    parser.add_argument("--prefixes", default="tweets,prices")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

    state_path = os.path.join(args.state_dir, "processed.json")
    os.makedirs(args.state_dir, exist_ok=True)
    os.makedirs(args.staging_dir, exist_ok=True)

    state = load_state(state_path)
    processed_objects = state.get("processed_objects", {})

    prefixes = [p.strip() for p in args.prefixes.split(",") if p.strip()]
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    storage_client = storage.Client()
    bucket = storage_client.bucket(args.bucket)

    local_paths = []
    newly_processed: Dict[str, List[str]] = {}

    for prefix in prefixes:
        processed = set(processed_objects.get(prefix, []))
        blobs = list_new_blobs(bucket, prefix, processed)
        if not blobs:
            continue
        new_paths, names = download_blobs(blobs, args.staging_dir)
        local_paths.extend(new_paths)
        newly_processed[prefix] = names

    if not local_paths:
        logging.info("No new avro files found.")
        return

    spark = None
    success = False
    try:
        spark = build_spark()
        df = spark.read.format("avro").load(local_paths)
        df = parse_pubsub_df(df)

        tweets = extract_tweets(df)
        prices = extract_prices(df, symbols)

        raw_tweets = prepare_raw_tweets(tweets)
        raw_prices = prepare_raw_prices(prices)
        metrics = build_windowed_metrics(tweets, prices, args.window_minutes)

        write_df_to_mongo(
            raw_tweets, args.mongo_uri, args.mongo_db, "raw_batch_tweets"
        )
        write_df_to_mongo(
            raw_prices, args.mongo_uri, args.mongo_db, "raw_batch_prices"
        )
        write_df_to_mongo(
            metrics,
            args.mongo_uri,
            args.mongo_db,
            "raw_batch_prices_with_tweets",
        )

        success = True
    finally:
        if spark is not None:
            spark.stop()

    if success:
        for prefix, names in newly_processed.items():
            existing = set(processed_objects.get(prefix, []))
            existing.update(names)
            processed_objects[prefix] = sorted(existing)
        state["processed_objects"] = processed_objects
        save_state(state_path, state)
        cleanup_files(local_paths)


if __name__ == "__main__":
    main()
