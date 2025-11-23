#!/bin/bash

PROJECT_ID="big-data-crypto-sentiment-test"
BUCKET_NAME="big-data-crypto-sentiment-test-dataflow-bucket"
REGION="europe-central2"
JOB_NAME="crypto-sentiment-stream"


echo "Deploying Dataflow Job: $JOB_NAME"

python3 stream_join.py \
  --runner DataflowRunner \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --temp_location "gs://$BUCKET_NAME/temp" \
  --staging_location "gs://$BUCKET_NAME/staging" \
  --requirements_file requirements.txt \
  --job_name "$JOB_NAME" \
  --streaming

echo "Job submitted. Check the Dataflow Console to monitor progress."