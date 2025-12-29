#!/bin/bash

PROJECT_ID="big-data-crypto-sentiment-test"
BUCKET_NAME="big-data-crypto-sentiment-test-dataflow-bucket"
REGION="europe-west6"
JOB_NAME="crypto-sentiment-stream"
ARIMA_MODELS_GCS_URI="gs://big-data-crypto-sentiment-test-arima-models/models/arima_models.joblib"

echo "Deploying Dataflow Job: $JOB_NAME"

python3 stream_join.py \
  --runner DataflowRunner \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --temp_location "gs://$BUCKET_NAME/temp" \
  --staging_location "gs://$BUCKET_NAME/staging" \
  --requirements_file requirements.txt \
  --job_name "$JOB_NAME" \
  --streaming \
  --experiments=use_runner_v2 \
  --arima_models_gcs_uri "$ARIMA_MODELS_GCS_URI"

echo "Job submitted. Check the Dataflow Console to monitor progress."
