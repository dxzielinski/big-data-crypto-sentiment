resource "google_project_service_identity" "pubsub_agent" {
  provider = google-beta
  project  = var.project_id
  service  = "pubsub.googleapis.com"
}

resource "google_storage_bucket_iam_member" "pubsub_gcs_writer" {
  bucket = google_storage_bucket.master_dataset_bucket.name
  role   = "roles/storage.admin"
  member = "serviceAccount:${resource.google_project_service_identity.pubsub_agent.email}"
}

resource "google_pubsub_topic" "data_topics" {
  for_each = var.pubsub_topics

  name = each.value.topic_name

  labels = {
    environment = "production"
    pipeline    = each.key
  }
}

resource "google_pubsub_subscription" "data_subs" {
  for_each = var.pubsub_topics

  topic = google_pubsub_topic.data_topics[each.key].name
  name  = "${each.value.topic_name}-sub"

  message_retention_duration = each.value.retention
  ack_deadline_seconds       = each.value.ack_deadline

  expiration_policy {
    ttl = "2678400s"
  }
}

resource "google_pubsub_subscription" "master_dataset" {
  for_each = var.pubsub_topics

  topic = google_pubsub_topic.data_topics[each.key].name
  name  = "${each.value.topic_name}-sub-for-master-dataset"

  cloud_storage_config {
    bucket = google_storage_bucket.master_dataset_bucket.name

    filename_prefix          = "raw/${each.value.topic_name}/"
    filename_suffix          = ".avro"
    filename_datetime_format = "YYYY-MM-DD/hh_mm_ssZ"

    max_bytes    = 100000000
    max_duration = "600s"
    avro_config {
      write_metadata   = true
      use_topic_schema = true
    }


  }
  depends_on = [google_storage_bucket_iam_member.pubsub_gcs_writer]
}


resource "google_project_iam_member" "transfer_sa_bq_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.crypto_streamer_sa.email}"
}

resource "google_project_iam_member" "transfer_sa_storage_viewer" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.crypto_streamer_sa.email}"
}
