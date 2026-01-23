resource "google_compute_firewall" "allow_mongodb_internal" {
  name    = "${var.project_id}-allow-mongodb-internal"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["27017"]
  }

  source_ranges = ["10.128.0.0/9"]
  target_tags   = ["big-data-crypto-vm"]
}
