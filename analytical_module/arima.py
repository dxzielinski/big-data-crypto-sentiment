import argparse
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
    project: str, table_fqn: str, symbols: list[str], lookback_days: int | None
) -> pd.DataFrame:
    client = bigquery.Client(project=project)

    time_filter = ""
    if lookback_days is not None:
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
    if lookback_days is not None:
        params.append(
            bigquery.ScalarQueryParameter("lookback_days", "INT64", lookback_days)
        )

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    df = client.query(query, job_config=job_config).to_dataframe()
    return df


def normalize_timestamp_to_datetime(ts_series: pd.Series) -> pd.DatetimeIndex:
    """
    Normalize numeric millisecond timestamps to UTC datetimes.
    """
    ts = pd.to_numeric(ts_series, errors="coerce").dropna().astype(np.int64)
    if ts.empty:
        return pd.DatetimeIndex([])

    unit = "ms"
    return pd.to_datetime(ts_series.astype(np.int64), unit=unit, utc=True)


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
        "--project",
        required=True,
        help="GCP project id, e.g. big-data-crypto-sentiment-test",
    )
    parser.add_argument(
        "--bq_table_fqn",
        required=True,
        help="BigQuery table FQN, e.g. project.dataset.raw_prices",
    )
    parser.add_argument(
        "--symbols", default="ETH,SOL,FTM,SHIB", help="Comma-separated symbols"
    )
    parser.add_argument(
        "--lookback_days",
        type=int,
        default=30,
        help="How many days of history to train on",
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
    args = parser.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    df = load_prices_from_bq(
        args.project, args.bq_table_fqn, symbols, args.lookback_days
    )

    eval_dir = args.eval_dir or None
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
