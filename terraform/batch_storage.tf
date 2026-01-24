resource "google_storage_bucket" "batch_storage" {
  name                        = "${var.project_id}-batch-storage"
  location                    = var.region
  uniform_bucket_level_access = true
}
data "google_project" "current" {}

resource "google_storage_bucket_iam_member" "pubsub_bucket_reader" {
  bucket = google_storage_bucket.batch_storage.name
  role   = "roles/storage.legacyBucketReader"
  member = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_storage_bucket_iam_member" "pubsub_object_creator" {
  bucket = google_storage_bucket.batch_storage.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_storage_bucket_iam_member" "crypto_streamer_batch_reader" {
  bucket = google_storage_bucket.batch_storage.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.crypto_streamer_sa.email}"
}


# Pub/Sub → GCS: Prices (Avro)
resource "google_pubsub_subscription" "prices_to_gcs" {
  name  = "prices-to-gcs-batch"
  topic = "projects/${var.project_id}/topics/crypto-prices-stream"

  cloud_storage_config {
    bucket = google_storage_bucket.batch_storage.name

    # Files will land in: gs://batch-storage/prices/...
    filename_prefix          = "prices/"
    filename_suffix          = ".avro"
    filename_datetime_format = "YYYY/MM/DD/hh_mm_ss"

    # Batch controls: a file is finalized when ANY of these thresholds is hit
    max_duration = "300s"   # close file after 5 minutes
    max_bytes    = 10000000 # or ~10 MB

    avro_config {}
  }
  depends_on = [
    google_storage_bucket_iam_member.pubsub_bucket_reader,
    google_storage_bucket_iam_member.pubsub_object_creator,
  ]
}

# Pub/Sub → GCS: TA (Avro)
resource "google_pubsub_subscription" "ta_to_gcs" {
  name  = "ta-to-gcs-batch"
  topic = "projects/${var.project_id}/topics/crypto-ta-indicators"

  cloud_storage_config {
    bucket = google_storage_bucket.batch_storage.name

    # Files will land in: gs://batch-storage/ta/...
    filename_prefix          = "ta/"
    filename_suffix          = ".avro"
    filename_datetime_format = "YYYY/MM/DD/hh_mm_ss"

    max_duration = "300s"
    max_bytes    = 10000000

    avro_config {}
  }
  depends_on = [
    google_storage_bucket_iam_member.pubsub_bucket_reader,
    google_storage_bucket_iam_member.pubsub_object_creator,
  ]
}


# Pub/Sub → GCS: Tweets (Avro)
resource "google_pubsub_subscription" "tweets_to_gcs" {
  name  = "tweets-to-gcs-batch"
  topic = "projects/${var.project_id}/topics/crypto-tweets-stream"

  cloud_storage_config {
    bucket = google_storage_bucket.batch_storage.name

    # Files will land in: gs://batch-storage/tweets/...
    filename_prefix          = "tweets/"
    filename_suffix          = ".avro"
    filename_datetime_format = "YYYY/MM/DD/hh_mm_ss"

    max_duration = "300s"
    max_bytes    = 10000000

    avro_config {}
  }
  depends_on = [
    google_storage_bucket_iam_member.pubsub_bucket_reader,
    google_storage_bucket_iam_member.pubsub_object_creator,
  ]
}
