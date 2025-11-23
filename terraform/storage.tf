resource "google_storage_bucket" "dataflow_temp" {
  name          = "${var.project_id}-dataflow-bucket" # Name must be globally unique
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true
}

resource "google_bigquery_dataset" "crypto_analysis" {
  dataset_id                 = "crypto_analysis"
  friendly_name              = "Crypto Sentiment Analysis"
  description                = "Dataset for storing aggregated crypto price and tweet sentiment data"
  location                   = var.region
  delete_contents_on_destroy = true

  labels = {
    environment = "test"
    managed_by  = "terraform"
  }
}


resource "google_bigquery_table" "windowed_metrics" {
  dataset_id = google_bigquery_dataset.crypto_analysis.dataset_id
  table_id   = "crypto_prices_with_tweets"

  schema = <<EOF
[
  {
    "name": "event_timestamp",
    "type": "TIMESTAMP",
    "mode": "REQUIRED",
    "description": " The end time of the processing window"
  },
  {
    "name": "symbol",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Crypto ticker symbol (e.g., ETH, SOL)"
  },
  {
    "name": "tweet_volume",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "avg_price",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "last_price",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "tweet_texts",
    "type": "STRING",
    "mode": "REPEATED",
    "description": "List of raw tweet texts captured in this window"
  }
]
EOF

  time_partitioning {
    type  = "HOUR"
    field = "event_timestamp"
  }
  clustering = ["symbol"]
  depends_on = [google_bigquery_dataset.crypto_analysis]
}