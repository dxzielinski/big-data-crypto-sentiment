import argparse
import io
import time
import warnings
import json
import os
import tempfile
from typing import Dict, Tuple

import joblib
import matplotlib
import numpy as np
import pandas as pd
import pmdarima as pm
import pymongo
try:
    from pyspark.sql import SparkSession, functions as F
except ImportError:
    SparkSession = None
    F = None
from google.cloud import bigquery
from google.cloud import storage

# Headless-friendly backend for plotting; set before importing pyplot
matplotlib.use("Agg")
import matplotlib.pyplot as plt

warnings.filterwarnings("ignore")


def parse_gs_uri(gs_uri: str) -> Tuple[str, str]:
    if not gs_uri.startswith("gs://"):
        raise ValueError(f"Expected gs:// URI, got: {gs_uri}")
    path = gs_uri[len("gs://") :]
    bucket, _, blob = path.partition("/")
    if not bucket or not blob:
        raise ValueError(f"Invalid gs:// URI: {gs_uri}")
    return bucket, blob


def load_prices_from_bq(
    project: str,
    table_fqn: str,
    symbols: list[str],
    lookback_days: int | None,
    lookback_hours: int | None,
) -> pd.DataFrame:
    client = bigquery.Client(project=project)

    time_filter = ""
    if lookback_hours is not None:
        time_filter = "AND TIMESTAMP_MILLIS(CAST(timestamp AS INT64)) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_hours HOUR)"
    elif lookback_days is not None:
        time_filter = "AND TIMESTAMP_MILLIS(CAST(timestamp AS INT64)) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_days DAY)"

    query = f"""
      SELECT symbol, timestamp, price
      FROM `{table_fqn}`
      WHERE symbol IN UNNEST(@symbols)
        AND price IS NOT NULL
        AND timestamp IS NOT NULL
        {time_filter}
      ORDER BY symbol, timestamp
    """

    params = [
        bigquery.ArrayQueryParameter("symbols", "STRING", symbols),
    ]
    if lookback_hours is not None:
        params.append(
            bigquery.ScalarQueryParameter("lookback_hours", "INT64", lookback_hours)
        )
    elif lookback_days is not None:
        params.append(
            bigquery.ScalarQueryParameter("lookback_days", "INT64", lookback_days)
        )

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    df = client.query(query, job_config=job_config).to_dataframe()
    return df


def load_prices_from_mongo(
    mongo_uri: str,
    mongo_db: str,
    mongo_collection: str,
    symbols: list[str],
    lookback_days: int | None,
    lookback_hours: int | None,
) -> pd.DataFrame:
    if not mongo_uri:
        raise ValueError("mongo_uri is required for Mongo source")
    client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    try:
        coll = client[mongo_db][mongo_collection]
        query: dict = {"symbol": {"$in": symbols}, "price": {"$ne": None}}
        if lookback_hours is not None:
            cutoff_ms = int(time.time() * 1000) - (lookback_hours * 3600 * 1000)
            query["timestamp"] = {"$gte": cutoff_ms}
        elif lookback_days is not None:
            cutoff_ms = int(time.time() * 1000) - (lookback_days * 86400 * 1000)
            query["timestamp"] = {"$gte": cutoff_ms}

        cursor = coll.find(query, {"symbol": 1, "timestamp": 1, "price": 1, "_id": 0})
        rows = list(cursor)
        return pd.DataFrame(rows)
    finally:
        client.close()


def normalize_timestamp_to_datetime(ts_series: pd.Series) -> pd.DatetimeIndex:
    """
    Normalize numeric millisecond timestamps to UTC datetimes.
    """
    ts = pd.to_numeric(ts_series, errors="coerce")
    if ts.isna().all():
        return pd.DatetimeIndex([])

    unit = "ms"
    return pd.to_datetime(ts.astype("Int64"), unit=unit, utc=True, errors="coerce")


def compute_metrics(actual: pd.Series, predicted: np.ndarray) -> Dict[str, float]:
    actual_values = actual.astype(float).values
    predicted_values = np.asarray(predicted, dtype=float)

    mae = float(np.mean(np.abs(actual_values - predicted_values)))
    mse = float(np.mean((actual_values - predicted_values) ** 2))
    mape = float(
        np.mean(
            np.abs(
                (actual_values - predicted_values) / np.clip(actual_values, 1e-8, None)
            )
        )
        * 100
    )
    return {"mae": mae, "mape": mape, "mse": mse}


def plot_forecast(
    train_index: pd.Index,
    train_actual: np.ndarray,
    train_predicted: np.ndarray,
    test_index: pd.Index,
    test_actual: np.ndarray,
    test_predicted: np.ndarray,
    conf_int: np.ndarray,
    symbol: str,
    output_dir: str | None,
) -> str | None:
    if not output_dir:
        return None

    os.makedirs(output_dir, exist_ok=True)
    plot_path = os.path.join(output_dir, f"{symbol}_forecast.png")

    plt.figure(figsize=(10, 4))
    plt.plot(
        train_index, train_actual, label="train actual", color="#1f77b4", linewidth=1.5
    )
    plt.plot(
        train_index,
        train_predicted,
        label="train predicted",
        color="#2ca02c",
        linewidth=1.2,
    )
    plt.axvline(
        x=test_index[0], color="#d62728", linestyle="--", linewidth=1.2, label="holdout"
    )
    plt.plot(
        test_index, test_actual, label="test actual", color="#1f77b4", linewidth=1.5
    )
    plt.plot(
        test_index,
        test_predicted,
        label="test forecast",
        color="#ff7f0e",
        linewidth=1.5,
    )
    plt.fill_between(
        test_index,
        conf_int[:, 0],
        conf_int[:, 1],
        color="#ff7f0e",
        alpha=0.2,
        label="95% CI",
    )
    plt.title(f"{symbol} holdout forecast")
    plt.xlabel("timestamp")
    plt.ylabel("price")
    plt.legend()
    plt.tight_layout()
    plt.savefig(plot_path, bbox_inches="tight")
    plt.close()
    return plot_path


def persist_metrics(metrics: Dict[str, Dict], output_dir: str | None) -> str | None:
    if not output_dir or not metrics:
        return None

    os.makedirs(output_dir, exist_ok=True)
    metrics_path = os.path.join(output_dir, "metrics.json")
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)
    return metrics_path


def train_models(
    df: pd.DataFrame,
    resample_rule: str = "30S",
    holdout_points: int = 30,
    train_plot_points: int = 60,
    eval_dir: str | None = None,
) -> Tuple[Dict[str, pm.ARIMA], Dict[str, Dict]]:
    models: Dict[str, pm.ARIMA] = {}
    eval_results: Dict[str, Dict] = {}

    for symbol, sdf in df.groupby("symbol"):
        sdf = sdf.copy()
        dt_index = normalize_timestamp_to_datetime(sdf["timestamp"])
        sdf["dt"] = dt_index
        sdf = sdf.dropna(subset=["dt", "price"]).sort_values("dt")
        if sdf.empty:
            continue

        # Regularize frequency to match your pipeline windowing cadence
        series = (
            sdf.set_index("dt")["price"]
            .astype(float)
            .resample(resample_rule)
            .last()
            .ffill()
        )

        if holdout_points > 0 and len(series) <= holdout_points:
            print(
                f"Skipping {symbol}: need more than {holdout_points} points, have {len(series)}"
            )
            continue

        train_series = series.iloc[:-holdout_points] if holdout_points > 0 else series
        test_series = series.iloc[-holdout_points:] if holdout_points > 0 else None

        if len(train_series) < 50:
            # Too little history for a stable ARIMA fit
            continue

        model = pm.auto_arima(
            train_series.values,
            seasonal=False,
            stepwise=True,
            suppress_warnings=True,
            error_action="ignore",
            max_p=5,
            max_q=5,
            # let auto_arima decide differencing
            d=None,
        )
        models[symbol] = model
        if test_series is not None and not test_series.empty:
            preds, conf_int = model.predict(
                n_periods=len(test_series), return_conf_int=True
            )
            metrics = compute_metrics(test_series, preds)
            train_plot_points = min(train_plot_points, len(train_series))
            train_start = len(train_series) - train_plot_points
            train_pred = model.predict_in_sample(
                start=train_start, end=len(train_series) - 1
            )
            plot_path = plot_forecast(
                train_series.index[train_start:],
                train_series.values[train_start:],
                train_pred,
                test_series.index,
                test_series.values,
                preds,
                conf_int,
                symbol,
                eval_dir,
            )
            eval_results[symbol] = {
                "metrics": metrics,
                "train_points": len(train_series),
                "test_points": len(test_series),
                "plot_path": plot_path,
            }
            print(
                f"[{symbol}] MAE={metrics['mae']:.6f}, MAPE={metrics['mape']:.3f}%, MSE={metrics['mse']:.6f}"
            )
        else:
            print(f"[{symbol}] trained with {len(train_series)} points (no holdout).")

    return models, eval_results


def _train_symbol_records(
    symbol: str,
    rows: list,
    resample_rule: str,
    holdout_points: int,
    train_plot_points: int,
    include_plot_data: bool,
) -> Dict:
    result = {
        "symbol": symbol,
        "model_bytes": None,
        "metrics": None,
        "train_points": 0,
        "test_points": 0,
        "status": "skipped",
        "error": None,
    }
    if not rows:
        result["status"] = "no_data"
        return result

    pdf = pd.DataFrame(rows)
    if pdf.empty:
        result["status"] = "no_data"
        return result

    pdf["timestamp"] = pd.to_numeric(pdf.get("timestamp"), errors="coerce")
    pdf["price"] = pd.to_numeric(pdf.get("price"), errors="coerce")
    pdf = pdf.dropna(subset=["timestamp", "price"])
    if pdf.empty:
        result["status"] = "no_data"
        return result

    dt_index = normalize_timestamp_to_datetime(pdf["timestamp"])
    pdf["dt"] = dt_index
    pdf = pdf.dropna(subset=["dt"]).sort_values("dt")
    if pdf.empty:
        result["status"] = "no_data"
        return result

    series = (
        pdf.set_index("dt")["price"]
        .astype(float)
        .resample(resample_rule)
        .last()
        .ffill()
    )

    if holdout_points > 0 and len(series) <= holdout_points:
        result["status"] = "not_enough_points"
        return result

    train_series = series.iloc[:-holdout_points] if holdout_points > 0 else series
    test_series = series.iloc[-holdout_points:] if holdout_points > 0 else None

    if len(train_series) < 50:
        result["status"] = "not_enough_points"
        return result

    try:
        model = pm.auto_arima(
            train_series.values,
            seasonal=False,
            stepwise=True,
            suppress_warnings=True,
            error_action="ignore",
            max_p=5,
            max_q=5,
            d=None,
        )
    except Exception as exc:
        result["status"] = "error"
        result["error"] = str(exc)
        return result

    result["train_points"] = int(len(train_series))
    result["test_points"] = int(len(test_series) if test_series is not None else 0)

    preds = None
    conf_int = None
    if test_series is not None and not test_series.empty:
        preds, conf_int = model.predict(
            n_periods=len(test_series), return_conf_int=True
        )
        result["metrics"] = compute_metrics(test_series, preds)

    if include_plot_data and test_series is not None and not test_series.empty:
        train_plot_points = min(train_plot_points, len(train_series))
        train_start = len(train_series) - train_plot_points
        train_pred = model.predict_in_sample(
            start=train_start, end=len(train_series) - 1
        )
        result["plot_payload"] = {
            "train_index": [t.isoformat() for t in train_series.index[train_start:]],
            "train_actual": train_series.values[train_start:].tolist(),
            "train_pred": np.asarray(train_pred, dtype=float).tolist(),
            "test_index": [t.isoformat() for t in test_series.index],
            "test_actual": test_series.values.tolist(),
            "test_pred": np.asarray(preds, dtype=float).tolist()
            if preds is not None
            else [],
            "conf_int": np.asarray(conf_int, dtype=float).tolist()
            if conf_int is not None
            else [],
        }

    buffer = io.BytesIO()
    joblib.dump(model, buffer, compress=3)
    result["model_bytes"] = buffer.getvalue()
    result["status"] = "trained"
    return result


def build_spark_session(master: str | None, packages: str | None, ivy_dir: str | None):
    if SparkSession is None:
        raise RuntimeError(
            "pyspark is required for spark training. Install pyspark or use --trainer pandas."
        )

    builder = SparkSession.builder.appName("arima-train")
    if master:
        builder = builder.master(master)
    if packages:
        builder = builder.config("spark.jars.packages", packages)
    if ivy_dir:
        builder = builder.config("spark.jars.ivy", ivy_dir)
    builder = builder.config("spark.sql.session.timeZone", "UTC")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_prices_with_spark(
    spark,
    project: str,
    table_fqn: str,
    symbols: list[str],
    lookback_days: int | None,
    lookback_hours: int | None,
):
    if F is None:
        raise RuntimeError("pyspark is required for spark training.")

    def apply_filters(df):
        df = df.select("symbol", "timestamp", "price")
        df = df.where(F.col("symbol").isin(symbols))
        df = df.where(F.col("price").isNotNull() & F.col("timestamp").isNotNull())
        if lookback_hours is not None:
            cutoff_ms = int(time.time() * 1000) - (lookback_hours * 3600 * 1000)
            df = df.where(F.col("timestamp").cast("long") >= F.lit(cutoff_ms))
        elif lookback_days is not None:
            cutoff_ms = int(time.time() * 1000) - (lookback_days * 86400 * 1000)
            df = df.where(F.col("timestamp").cast("long") >= F.lit(cutoff_ms))
        return df

    try:
        df = (
            spark.read.format("bigquery")
            .option("table", table_fqn)
            .option("parentProject", project)
            .load()
        )
        return apply_filters(df)
    except Exception as exc:
        print(
            f"[WARN] Spark BigQuery read failed ({exc}). Falling back to BigQuery client."
        )
        pdf = load_prices_from_bq(
            project, table_fqn, symbols, lookback_days, lookback_hours
        )
        df = spark.createDataFrame(pdf)
        return apply_filters(df)


def train_models_spark(
    df,
    resample_rule: str = "30S",
    holdout_points: int = 30,
    train_plot_points: int = 60,
    eval_dir: str | None = None,
) -> Tuple[Dict[str, pm.ARIMA], Dict[str, Dict]]:
    if F is None:
        raise RuntimeError("pyspark is required for spark training.")

    grouped = df.groupBy("symbol").agg(
        F.collect_list(F.struct("timestamp", "price")).alias("rows")
    )

    def train_row(row):
        symbol = row["symbol"]
        rows = [{"timestamp": r["timestamp"], "price": r["price"]} for r in row["rows"]]
        return _train_symbol_records(
            symbol,
            rows,
            resample_rule=resample_rule,
            holdout_points=holdout_points,
            train_plot_points=train_plot_points,
            include_plot_data=bool(eval_dir),
        )

    results = grouped.rdd.map(train_row).collect()

    models: Dict[str, pm.ARIMA] = {}
    eval_results: Dict[str, Dict] = {}

    for item in results:
        symbol = item.get("symbol")
        status = item.get("status")
        if status != "trained":
            if symbol:
                print(f"[WARN] {symbol}: {status}")
            continue
        model_bytes = item.get("model_bytes")
        if not model_bytes or not symbol:
            continue
        model = joblib.load(io.BytesIO(model_bytes))
        models[symbol] = model
        metrics = item.get("metrics")
        if metrics:
            eval_results[symbol] = {
                "metrics": metrics,
                "train_points": item.get("train_points"),
                "test_points": item.get("test_points"),
                "plot_path": None,
            }
        plot_payload = item.get("plot_payload")
        if eval_dir and plot_payload:
            try:
                train_index = pd.to_datetime(plot_payload["train_index"], utc=True)
                test_index = pd.to_datetime(plot_payload["test_index"], utc=True)
                plot_path = plot_forecast(
                    train_index,
                    np.asarray(plot_payload["train_actual"], dtype=float),
                    np.asarray(plot_payload["train_pred"], dtype=float),
                    test_index,
                    np.asarray(plot_payload["test_actual"], dtype=float),
                    np.asarray(plot_payload["test_pred"], dtype=float),
                    np.asarray(plot_payload["conf_int"], dtype=float),
                    symbol,
                    eval_dir,
                )
                if metrics:
                    eval_results[symbol]["plot_path"] = plot_path
            except Exception:
                print(f"[WARN] Failed to plot forecast for {symbol}")

    return models, eval_results


def upload_to_gcs(local_path: str, gs_uri: str, project: str):
    bucket_name, blob_name = parse_gs_uri(gs_uri)
    client = storage.Client(project=project)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_path)
    print(f"Uploaded model bundle to {gs_uri}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source",
        choices=["bq", "mongo"],
        default="mongo",
        help="Where to read prices from (default: mongo).",
    )
    parser.add_argument(
        "--project",
        default=None,
        help="GCP project id, required when --source bq is used.",
    )
    parser.add_argument(
        "--bq_table_fqn",
        default=None,
        help="BigQuery table FQN, required when --source bq is used.",
    )
    parser.add_argument(
        "--mongo_uri",
        default=os.getenv("MONGO_URI", "mongodb://localhost:27017"),
        help="MongoDB URI (default: mongodb://localhost:27017).",
    )
    parser.add_argument(
        "--mongo_db",
        default=os.getenv("MONGO_DB", "crypto_analysis"),
        help="MongoDB database name (default: crypto_analysis).",
    )
    parser.add_argument(
        "--mongo_collection",
        default=os.getenv("MONGO_COLLECTION", "raw_prices"),
        help="MongoDB collection for raw prices (default: raw_prices).",
    )
    parser.add_argument(
        "--symbols", default="ETH,SOL,FTM,SHIB", help="Comma-separated symbols"
    )
    parser.add_argument(
        "--lookback_days",
        type=int,
        default=None,
        help="How many days of history to train on (ignored if lookback_hours is set).",
    )
    parser.add_argument(
        "--lookback_hours",
        type=int,
        default=3,
        help="How many hours of history to train on (default 3).",
    )
    parser.add_argument(
        "--gcs_uri",
        required=True,
        help="Where to upload joblib bundle, e.g. gs://bucket/models/arima_models.joblib",
    )
    parser.add_argument(
        "--resample_rule", default="30S", help="Pandas resample rule, default 30S"
    )
    parser.add_argument(
        "--holdout_points",
        type=int,
        default=30,
        help="How many most recent resampled points to hold out per symbol for evaluation",
    )
    parser.add_argument(
        "--train_plot_points",
        type=int,
        default=60,
        help="How many most recent training points to include in evaluation plots",
    )
    parser.add_argument(
        "--eval_dir",
        default="arima_eval",
        help="Local directory to store evaluation artifacts (plots, metrics). Use empty string to skip saving.",
    )
    parser.add_argument(
        "--trainer",
        choices=["spark", "pandas"],
        default="spark",
        help="Training engine to use. Spark is the default.",
    )
    parser.add_argument(
        "--spark_master",
        default=os.getenv("SPARK_MASTER"),
        help="Optional Spark master URL. If unset, Spark decides.",
    )
    parser.add_argument(
        "--spark_packages",
        default=os.getenv("SPARK_PACKAGES", ""),
        help="Comma-separated Spark packages (e.g., BigQuery connector).",
    )
    parser.add_argument(
        "--spark_ivy_dir",
        default=os.getenv("SPARK_IVY_DIR", "/tmp/spark-ivy"),
        help="Where Spark should cache downloaded jars.",
    )
    args = parser.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    lookback_hours = args.lookback_hours
    lookback_days = args.lookback_days if lookback_hours is None else None
    eval_dir = args.eval_dir or None
    if args.source == "bq" and (not args.project or not args.bq_table_fqn):
        raise ValueError("--project and --bq_table_fqn are required for --source bq")

    if args.trainer == "spark":
        spark = build_spark_session(
            args.spark_master, args.spark_packages, args.spark_ivy_dir
        )
        try:
            if args.source == "bq":
                sdf = load_prices_with_spark(
                    spark,
                    args.project,
                    args.bq_table_fqn,
                    symbols,
                    lookback_days,
                    lookback_hours,
                )
            else:
                pdf = load_prices_from_mongo(
                    args.mongo_uri,
                    args.mongo_db,
                    args.mongo_collection,
                    symbols,
                    lookback_days,
                    lookback_hours,
                )
                if pdf.empty:
                    raise RuntimeError("No data returned from MongoDB.")
                sdf = spark.createDataFrame(pdf)
            models, eval_results = train_models_spark(
                sdf,
                resample_rule=args.resample_rule,
                holdout_points=args.holdout_points,
                train_plot_points=args.train_plot_points,
                eval_dir=eval_dir,
            )
        finally:
            spark.stop()
    else:
        if args.source == "bq":
            df = load_prices_from_bq(
                args.project, args.bq_table_fqn, symbols, lookback_days, lookback_hours
            )
        else:
            df = load_prices_from_mongo(
                args.mongo_uri,
                args.mongo_db,
                args.mongo_collection,
                symbols,
                lookback_days,
                lookback_hours,
            )
        models, eval_results = train_models(
            df,
            resample_rule=args.resample_rule,
            holdout_points=args.holdout_points,
            train_plot_points=args.train_plot_points,
            eval_dir=eval_dir,
        )
    if not models:
        raise RuntimeError("No models were trained (not enough data?)")

    metrics_path = persist_metrics(eval_results, eval_dir)
    if metrics_path:
        print(f"Saved evaluation metrics to {metrics_path}")

    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = os.path.join(tmpdir, "arima_models.joblib")
        joblib.dump(models, local_path, compress=3)
        upload_to_gcs(local_path, args.gcs_uri, args.project)


if __name__ == "__main__":
    main()
