#!/usr/bin/env bash
set -euo pipefail

LOCK_FILE="/var/lock/batch_to_mongo.lock"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  exit 0
fi

PROJECT_ID="$(curl -s -H "Metadata-Flavor: Google" \
  http://metadata.google.internal/computeMetadata/v1/project/project-id)"
BATCH_BUCKET="${BATCH_BUCKET:-${PROJECT_ID}-batch-storage}"

MONGO_URI="${MONGO_URI:-mongodb://localhost:27017}"
MONGO_DB="${MONGO_DB:-crypto_analysis}"

STATE_DIR="${STATE_DIR:-/var/lib/batch_to_mongo}"
STAGING_DIR="${STAGING_DIR:-$STATE_DIR/staging}"
WINDOW_MINUTES="${WINDOW_MINUTES:-30}"
SYMBOLS="${SYMBOLS:-ETH,SOL,FTM,SHIB}"

mkdir -p "$STATE_DIR" "$STAGING_DIR"

export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
export PYSPARK_PYTHON=python3

python3 /opt/batch/batch_to_mongo.py \
  --bucket "$BATCH_BUCKET" \
  --mongo-uri "$MONGO_URI" \
  --mongo-db "$MONGO_DB" \
  --state-dir "$STATE_DIR" \
  --staging-dir "$STAGING_DIR" \
  --window-minutes "$WINDOW_MINUTES" \
  --symbols "$SYMBOLS"
