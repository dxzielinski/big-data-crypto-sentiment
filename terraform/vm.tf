resource "google_service_account" "crypto_streamer_sa" {
  account_id   = "crypto-streamer-sa"
  display_name = "Crypto Streamer Service Account"
}

resource "google_compute_instance" "vm" {
  name         = var.vm_name
  machine_type = var.machine_type
  zone         = var.zone

  # Make sure APIs + repo exist first
  depends_on = [
    google_project_service.compute,
    google_project_service.artifact_registry,
    google_project_service.cloudresourcemanager,
    google_artifact_registry_repository.docker_repo,
  ]

  boot_disk {
    initialize_params {
      # OS image (NOT docker image)
      image = "projects/ubuntu-os-cloud/global/images/family/ubuntu-2204-lts"
      size  = var.disk_size_gb
      type  = "pd-balanced"
    }
  }

  network_interface {
    network = "default"
    access_config {}
  }

  service_account {
    email = google_service_account.crypto_streamer_sa.email
    scopes = [
      "https://www.googleapis.com/auth/pubsub",
      "https://www.googleapis.com/auth/cloud-platform",
    ]
  }

  metadata_startup_script = replace(<<EOT
#!/bin/bash
set -euo pipefail

logger -t startup-script "Startup script: begin"

IMAGE_CRYPTO="${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.docker_repo.repository_id}/coincap-simulation:latest"
IMAGE_TWITTER="${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.docker_repo.repository_id}/twitter-simulation-data:latest"
REGISTRY_HOST="${var.region}-docker.pkg.dev"

logger -t startup-script "Startup script: using image $IMAGE_CRYPTO"

# -----------------------------
# Ensure Docker is installed
# -----------------------------
if ! command -v docker >/dev/null 2>&1; then
  logger -t startup-script "Docker not found on image, installing docker.io and dependencies"

  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y docker.io python3 ca-certificates curl

  systemctl enable docker
  systemctl start docker

  logger -t startup-script "Docker installed: $(docker --version)"
else
  logger -t startup-script "Docker found: $(docker --version)"
fi

# Ensure python3 is present (for JSON parsing), just in case
if ! command -v python3 >/dev/null 2>&1; then
  logger -t startup-script "python3 not found, installing"
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y python3
fi

# -----------------------------
# Run MongoDB locally (Docker)
# -----------------------------
MONGO_CONTAINER="mongodb"
MONGO_IMAGE="mongo:7.0"
MONGO_DATA_DIR="/var/lib/mongo"

logger -t startup-script "Preparing MongoDB data dir at $MONGO_DATA_DIR"
mkdir -p "$MONGO_DATA_DIR"

if docker ps -a --format '{{.Names}}' | grep -q "^${MONGO_CONTAINER}$"; then
  logger -t startup-script "Removing existing container $MONGO_CONTAINER"
  docker rm -f "$MONGO_CONTAINER" || true
fi

logger -t startup-script "Starting MongoDB container $MONGO_CONTAINER"
docker run -d \
  --name "$MONGO_CONTAINER" \
  -p 27017:27017 \
  -v "$MONGO_DATA_DIR:/data/db" \
  --restart unless-stopped \
  "$MONGO_IMAGE" || logger -t startup-script "Failed to start MongoDB container"

# Wait briefly for MongoDB to accept connections
for i in {1..20}; do
  if docker exec "$MONGO_CONTAINER" mongosh --quiet --eval "db.runCommand({ ping: 1 })" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

cat <<'EOF' >/tmp/mongo-init.js
const dbName = "crypto_analysis";
const database = db.getSiblingDB(dbName);

function ensureCollection(name, schema) {
  const options = {
    validator: schema,
    validationLevel: "strict",
    validationAction: "error",
  };

  const exists = database.getCollectionNames().includes(name);
  if (!exists) {
    database.createCollection(name, options);
  } else {
    database.runCommand(Object.assign({ collMod: name }, options));
  }
}

const rawTweetsSchema = {
  $jsonSchema: {
    bsonType: "object",
    additionalProperties: false,
    properties: {
      _id: { bsonType: "objectId" },
      id: { bsonType: ["string", "null"] },
      text: { bsonType: ["string", "null"] },
      author_id: { bsonType: ["string", "null"] },
      crypto_key: { bsonType: ["string", "null"] },
      created_at_raw: { bsonType: ["string", "null"] },
      created_at_iso: { bsonType: ["date", "null"] },
      timestamp_ms: { bsonType: ["long", "int", "null"] },
      timestamp_sec: { bsonType: ["long", "int", "null"] },
    },
  },
};

const rawPricesSchema = {
  $jsonSchema: {
    bsonType: "object",
    additionalProperties: false,
    required: ["symbol"],
    properties: {
      _id: { bsonType: "objectId" },
      symbol: { bsonType: "string" },
      price: { bsonType: ["double", "null"] },
      timestamp: { bsonType: ["long", "int", "null"] },
    },
  },
};

const windowedMetricsSchema = {
  $jsonSchema: {
    bsonType: "object",
    additionalProperties: false,
    required: ["event_timestamp", "symbol"],
    properties: {
      _id: { bsonType: "objectId" },
      event_timestamp: { bsonType: "date" },
      symbol: { bsonType: "string" },
      tweet_volume: { bsonType: ["long", "int", "null"] },
      avg_price: { bsonType: ["double", "null"] },
      last_price: { bsonType: ["double", "null"] },
      tweet_texts: {
        bsonType: ["array", "null"],
        items: { bsonType: ["string", "null"] },
      },
    },
  },
};

const tweetSentimentSchema = {
  $jsonSchema: {
    bsonType: "object",
    additionalProperties: false,
    properties: {
      _id: { bsonType: "objectId" },
      event_timestamp: { bsonType: ["string", "null"] },
      symbol: { bsonType: ["string", "null"] },
      text: { bsonType: ["string", "null"] },
      sentiment_score: { bsonType: ["double", "null"] },
      sentiment_magnitude: { bsonType: ["double", "null"] },
      sentiment_label: { bsonType: ["string", "null"] },
    },
  },
};

const priceForecastsSchema = {
  $jsonSchema: {
    bsonType: "object",
    additionalProperties: false,
    properties: {
      _id: { bsonType: "objectId" },
      event_timestamp: { bsonType: ["string", "null"] },
      symbol: { bsonType: ["string", "null"] },
      price: { bsonType: ["double", "null"] },
      price_timestamp: { bsonType: ["long", "int", "null"] },
      arima_next_price_forecast: { bsonType: ["double", "null"] },
    },
  },
};

ensureCollection("raw_tweets", rawTweetsSchema);
ensureCollection("raw_prices", rawPricesSchema);
ensureCollection("crypto_prices_with_tweets", windowedMetricsSchema);
ensureCollection("tweet_sentiment", tweetSentimentSchema);
ensureCollection("price_forecasts", priceForecastsSchema);
EOF

docker cp /tmp/mongo-init.js "$MONGO_CONTAINER":/tmp/mongo-init.js
docker exec "$MONGO_CONTAINER" mongosh --quiet /tmp/mongo-init.js || \
  logger -t startup-script "MongoDB init script failed"

# -----------------------------
# Authenticate to Artifact Registry using VM service account
# -----------------------------
logger -t startup-script "Fetching access token from metadata server"

TOKEN=$(curl -s -H "Metadata-Flavor: Google" \
  "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token" \
  | python3 -c "import sys, json; print(json.load(sys.stdin)['access_token'])")

logger -t startup-script "Logging in to Artifact Registry at $REGISTRY_HOST"

echo "$TOKEN" | docker login -u oauth2accesstoken --password-stdin "https://$REGISTRY_HOST" || {
  logger -t startup-script "Docker login to Artifact Registry failed"
  exit 1
}

# -----------------------------
# Pull and run the container
# -----------------------------
logger -t startup-script "Pulling image $IMAGE_CRYPTO"

if docker pull "$IMAGE_CRYPTO"; then
  logger -t startup-script "Pulled image $IMAGE_CRYPTO"

  # Remove existing container if present
  if docker ps -a --format '{{.Names}}' | grep -q '^crypto-simulation$'; then
    logger -t startup-script "Removing existing container crypto-simulation"
    docker rm -f crypto-simulation || true
  fi

  # Run container
  logger -t startup-script "Starting container crypto-simulation"
  docker run -d --name crypto-simulation -p 8080:8080 "$IMAGE_CRYPTO" || \
    logger -t startup-script "Failed to start container crypto-simulation"
else
  logger -t startup-script "Failed to pull image $IMAGE_CRYPTO"
fi

logger -t startup-script "Pulling image $IMAGE_TWITTER"

if docker pull "$IMAGE_TWITTER"; then
  logger -t startup-script "Pulled image $IMAGE_TWITTER"

  # Remove existing container if present
  if docker ps -a --format '{{.Names}}' | grep -q '^twitter-simulation$'; then
    logger -t startup-script "Removing existing container twitter-simulation"
    docker rm -f twitter-simulation || true
  fi

  # Run container
  logger -t startup-script "Starting container twitter-simulation"
  docker run -d --name twitter-simulation -e PROJECT_ID="${var.project_id}" -p 8081:8080 "$IMAGE_TWITTER" || \
    logger -t startup-script "Failed to start container twitter-simulation"
else
  logger -t startup-script "Failed to pull image $IMAGE_TWITTER"
fi

logger -t startup-script "Startup script: end"
EOT
  , "\r", "")
  tags = ["big-data-crypto-vm"]
}

resource "google_project_iam_member" "crypto_streamer_pubsub_publisher" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.crypto_streamer_sa.email}"
}

resource "google_project_iam_member" "crypto_streamer_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.crypto_streamer_sa.email}"
}

resource "google_project_iam_member" "crypto_streamer_artifact_reader" {
  project = var.project_id
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${google_service_account.crypto_streamer_sa.email}"
}
