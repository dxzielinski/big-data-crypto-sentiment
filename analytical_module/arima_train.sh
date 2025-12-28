#!/bin/bash

PROJECT_ID="big-data-crypto-sentiment-test"
BUCKET_NAME="big-data-crypto-sentiment-test-arima-models"

python3 arima.py \
  --project "$PROJECT_ID" \
  --bq_table_fqn "$PROJECT_ID.crypto_analysis.raw_prices" \
  --gcs_uri "gs://$BUCKET_NAME/models/arima_models.joblib" \
  --train_plot_points 60 \
  --holdout_points 30

echo "ARIMA model batch training completed. Models saved to GCS."