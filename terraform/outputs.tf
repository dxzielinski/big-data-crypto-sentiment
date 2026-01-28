output "vm_name" {
  description = "Name of the created VM"
  value       = google_compute_instance.vm.name
}

output "vm_zone" {
  description = "Zone of the created VM"
  value       = google_compute_instance.vm.zone
}

output "vm_external_ip" {
  description = "External IP address of the VM"
  value       = google_compute_instance.vm.network_interface[0].access_config[0].nat_ip
}

output "vm_internal_ip" {
  description = "Internal IP address of the VM"
  value       = google_compute_instance.vm.network_interface[0].network_ip
}
