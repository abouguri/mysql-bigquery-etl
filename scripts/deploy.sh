#!/usr/bin/env bash
# Plan infrastructure; apply the reviewed saved plan as a separate operation.
set -euo pipefail
cd "$(dirname "$0")/../infra"
: "${TF_VAR_project_id:?Set TF_VAR_project_id to the chosen sandbox project}"
: "${TF_VAR_network:?Set TF_VAR_network to the existing source VPC}"
: "${TF_VAR_subnetwork:?Set TF_VAR_subnetwork to the source subnet}"
terraform init
terraform validate
terraform plan -out=deployment.tfplan
printf '%s\n' 'Review: terraform show deployment.tfplan' 'Apply: terraform apply deployment.tfplan'
