# big-data-crypto-sentiment

Big Data Project

Terraform workflow:

```bash
terraform init -upgrade
terraform fmt
terraform validate
terraform apply -var="project_id=<PROJECT_ID>"
```

To access vm:

```bash
gcloud compute ssh big-data-crypto-vm --zone=europe-central2-a
# logs from startup script:
sudo journalctl -u google-startup-scripts.service --no-pager | grep startup-script
exit
```

To stop vm:

```bash
gcloud compute instances stop big-data-crypto-vm --zone=europe-central2-a
```
