#!/bin/bash

PROJECT_ID="big-data-crypto-sentiment-test"
BUCKET_NAME="big-data-crypto-sentiment-test-arima-models"

python3 arima.py \
  --source "mongo" \
  --mongo_uri "${MONGO_URI:-mongodb://localhost:27017}" \
  --mongo_db "${MONGO_DB:-crypto_analysis}" \
  --mongo_collection "${MONGO_COLLECTION:-raw_prices}" \
  --gcs_uri "gs://$BUCKET_NAME/models/arima_models.joblib" \
  --lookback_hours 3 \
  --train_plot_points 60 \
  --holdout_points 30

echo "ARIMA model batch training completed. Models saved to GCS."
