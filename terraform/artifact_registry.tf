resource "google_artifact_registry_repository" "docker_repo" {
  location      = var.region
  repository_id = var.docker_repo_id
  description   = "Docker images for big-data-crypto-sentiment"
  format        = "DOCKER"
  depends_on = [
    google_project_service.compute,
    google_project_service.artifact_registry,
    google_artifact_registry_repository.docker_repo,
  ]
}
