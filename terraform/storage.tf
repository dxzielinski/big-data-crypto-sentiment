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

resource "google_bigquery_table" "raw_tweets" {
  dataset_id = google_bigquery_dataset.crypto_analysis.dataset_id
  table_id   = "raw_tweets"

  schema = <<EOF
[
  {
    "name": "id",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "text",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "author_id",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "crypto_key",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "The symbol found in the tweet, e.g. ETH"
  },
  {
    "name": "created_at_raw",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "created_at_iso",
    "type": "TIMESTAMP",
    "mode": "NULLABLE",
    "description": "Parsed event time used for partitioning"
  },
  {
    "name": "timestamp_ms",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "timestamp_sec",
    "type": "INTEGER",
    "mode": "NULLABLE"
  }
]
EOF

  time_partitioning {
    type  = "DAY"
    field = "created_at_iso"
  }

  # Cluster by symbol for query performance
  clustering = ["crypto_key"]

  depends_on = [google_bigquery_dataset.crypto_analysis]
}

resource "google_bigquery_table" "raw_prices" {
  dataset_id = google_bigquery_dataset.crypto_analysis.dataset_id
  table_id   = "raw_prices"

  schema = <<EOF
[
  {
    "name": "symbol",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "price",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "timestamp",
    "type": "INTEGER",
    "mode": "NULLABLE",
    "description": "Unix timestamp of the price tick"
  }
]
EOF

  time_partitioning {
    type = "DAY"
  }

  clustering = ["symbol"]

  depends_on = [google_bigquery_dataset.crypto_analysis]
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
    "description": "The end time of the processing window"
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