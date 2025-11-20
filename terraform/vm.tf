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
      image = data.google_compute_image.cos.self_link
      size  = var.disk_size_gb
      type  = "pd-balanced"
    }
  }

  network_interface {
    network = "default"
    access_config {}
  }

  # Simple startup script that runs a container in background
  metadata_startup_script = <<EOT
#!/bin/bash
set -euo pipefail

logger -t startup-script "Startup script: begin"

IMAGE="${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.docker_repo.repository_id}/dummy-app:latest"

logger -t startup-script "Startup script: using image $IMAGE"

# Check docker
if ! command -v docker >/dev/null 2>&1; then
  logger -t startup-script "Docker not found on COS image"
else
  logger -t startup-script "Docker found: $(docker --version)"
fi

# Try to pull the image (will fail if you haven't pushed it yet)
if docker pull "$IMAGE"; then
  logger -t startup-script "Pulled image $IMAGE"

  # Remove existing container if present
  if docker ps -a --format '{{.Names}}' | grep -q '^dummy-app$'; then
    logger -t startup-script "Removing existing container dummy-app"
    docker rm -f dummy-app || true
  fi

  # Run container
  logger -t startup-script "Starting container dummy-app"
  docker run -d --name dummy-app -p 8080:8080 "$IMAGE" || \
    logger -t startup-script "Failed to start container dummy-app"
else
  logger -t startup-script "Failed to pull image $IMAGE"
fi

logger -t startup-script "Startup script: end"
EOT

  tags = ["big-data-crypto-vm"]
}
