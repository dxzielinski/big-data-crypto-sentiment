#!/bin/bash

PROJECT_ID="big-data-crypto-sentiment-test"
BUCKET_NAME="big-data-crypto-sentiment-test-dataflow-bucket"
REGION="europe-west6"
JOB_NAME="crypto-sentiment-stream"
ARIMA_MODELS_GCS_URI="gs://big-data-crypto-sentiment-test-arima-models/models/arima_models.joblib"
PIPELINE_SCRIPT="${PIPELINE_SCRIPT:-stream_join_old.py}"
MONGO_URI="${MONGO_URI:-}"
MONGO_DB="${MONGO_DB:-crypto_analysis}"

echo "Deploying Dataflow Job: $JOB_NAME"

MONGO_ARGS=()
if [[ -n "$MONGO_URI" ]]; then
  MONGO_ARGS+=(--mongo_uri "$MONGO_URI" --mongo_db "$MONGO_DB")
fi

ARIMA_ARGS=(--arima_models_gcs_uri "$ARIMA_MODELS_GCS_URI")

python3 "$PIPELINE_SCRIPT" \
  --runner DataflowRunner \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --temp_location "gs://$BUCKET_NAME/temp" \
  --staging_location "gs://$BUCKET_NAME/staging" \
  --requirements_file requirements.txt \
  --job_name "$JOB_NAME" \
  --streaming \
  --experiments=use_runner_v2 \
  "${ARIMA_ARGS[@]}" \
  "${MONGO_ARGS[@]}"

echo "Job submitted. Check the Dataflow Console to monitor progress."
