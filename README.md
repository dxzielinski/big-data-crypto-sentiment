# big-data-crypto-sentiment

Big Data Project

Terraform workflow:

```terraform
terraform init -upgrade
terraform fmt
terraform validate
terraform apply -var="project_id=<PROJECT_ID>"

To stop vm:

```bash
gcloud compute instances stop big-data-crypto-vm --zone=europe-central2-a
```
