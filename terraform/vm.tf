resource "google_service_account" "crypto_streamer_sa" {
  project = var.project_id
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

  # Simple startup script that runs a container in background
  metadata_startup_script = <<EOT
#!/bin/bash
set -euo pipefail

logger -t startup-script "Startup script: begin"

IMAGE="${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.docker_repo.repository_id}/crypto-simulation:latest"
REGISTRY_HOST="${var.region}-docker.pkg.dev"

logger -t startup-script "Startup script: using image $IMAGE"

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
logger -t startup-script "Pulling image $IMAGE"

if docker pull "$IMAGE"; then
  logger -t startup-script "Pulled image $IMAGE"

  # Remove existing container if present
  if docker ps -a --format '{{.Names}}' | grep -q '^crypto-simulation$'; then
    logger -t startup-script "Removing existing container crypto-simulation"
    docker rm -f crypto-simulation || true
  fi

  # Run container
  logger -t startup-script "Starting container crypto-simulation"
  docker run -d --name crypto-simulation -p 8080:8080 "$IMAGE" || \
    logger -t startup-script "Failed to start container crypto-simulation"
else
  logger -t startup-script "Failed to pull image $IMAGE"
fi

logger -t startup-script "Startup script: end"
EOT

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