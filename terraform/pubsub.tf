# 1. Create ALL Topics dynamically
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