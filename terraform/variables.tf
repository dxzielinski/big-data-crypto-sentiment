variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "europe-central2"
}

variable "zone" {
  description = "GCP zone"
  type        = string
  default     = "europe-central2-a"
}

variable "vm_name" {
  description = "Name of the VM"
  type        = string
  default     = "big-data-crypto-vm"
}

variable "machine_type" {
  description = "GCE machine type"
  type        = string
  default     = "e2-small"
}

variable "disk_size_gb" {
  description = "Boot disk size in GB"
  type        = number
  default     = 20
}

variable "docker_repo_id" {
  description = "Artifact Registry repository ID for Docker images"
  type        = string
  default     = "big-data-crypto-sentiment-repo"
}

variable "pubsub_topics" {
  description = "Map of all data pipelines to create"
  type = map(object({
    topic_name   = string
    retention    = optional(string, "604800s") # Default 7 days
    ack_deadline = optional(number, 20)
  }))

  default = {
    # Pipeline 1: Twitter
    twitter = {
      topic_name = "crypto-tweets-stream"
    }
    # Pipeline 2: Market Prices
    prices = {
      topic_name = "crypto-prices-stream"
    }
    # Pipeline 3: Technical Analysis
    ta = {
      topic_name = "crypto-ta-indicators"
      retention  = "86400s"
    }
  }
}