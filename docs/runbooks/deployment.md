# Sandbox deployment and operations

Status: infrastructure and runtime code are prepared for validation. No cloud resources, alerts, notification delivery, billing or deployment have been verified. A selected sandbox project, credentials and agreed spend limit are required before applying infrastructure or running cloud tests.

## Ownership and prerequisites

Terraform owns deployment configuration; Cloud Build only tests and publishes images. Jobs execute the CLI, not the Flask development server. Scheduler uses OAuth to call the Cloud Run Jobs API. The reconcile job has a separate schedule and arguments, avoiding invocation-time override permissions.

Provide an existing VPC/subnet with private routes and a source firewall rule admitting the worker, a MySQL read-only account, a CA certificate matching the source hostname, pinned secret versions and verified Monitoring notification channels. The source must use positive integer keys and UTC timestamps. Shared-VPC cross-project IAM is not provisioned by this module.

| Identity | Scope | Access |
|---|---|---|
| Worker | One BigQuery dataset | Data editor for staging, targets and metadata |
| Worker | Project | BigQuery job user |
| Worker | Five named secrets | Read pinned MySQL configuration and CA versions |
| Scheduler | Two Cloud Run jobs | Run invoker |
| Build identity | Artifact Registry repository | Writer; grant separately to your chosen build identity |
| Deployment operator | Provisioning resources | Terraform privileges; never assigned to the runtime worker |

## Sequence

1. Agree a project and test spend limit. Configure billing alerts; alerts are not a hard spending cap. Authenticate outside the repository. Copy `infra/example.tfvars` to a private `infra/local.tfvars` and fill the source network and project. Use a protected remote Terraform state backend for shared use; local state is ignored by Git.
2. Bootstrap with `enable_jobs=false`: `terraform -chdir=infra init`, `terraform -chdir=infra plan -var-file=local.tfvars -out=deployment.tfplan`, review and apply that saved plan. It creates secret containers, not values. Populate secret versions through a secure operator workflow, never in Terraform variables or committed files.
3. Grant the chosen Cloud Build identity repository writer, and submit `gcloud builds submit --config cloudbuild.yaml --project PROJECT`. Inspect the tested image's Artifact Registry digest. Supply that immutable digest in `image`, set `enable_jobs=true`, keep `enable_schedules=false`, and plan/apply again.
4. Run the opt-in BigQuery tests in the approved sandbox. Execute `gcloud run jobs execute mysql-bigquery-ingest --region REGION --project PROJECT --wait`. Check counts/revenue, job IDs, table events and complete logs. Then run reconciliation. Verify that an unauthorized principal cannot execute either job.
5. Deliberately exercise source failure in a dedicated test configuration; verify both failure and missing-success alerts reach the selected channel. Absence metrics may not detect a never-started pipeline before the first time series exists: test initial startup and inspect Scheduler/Cloud Run executions directly. An empty channel list creates policies but delivers no notifications.
6. Enable schedules only after those gates pass. Normal ingestion is every six hours; reconciliation runs daily at 02:30 UTC. Cloud Run task timeout is 25 minutes and the writer lease is 30 minutes. Automatic execution retries are disabled; the client performs bounded retries of known operation job IDs. Follow the recovery runbook before rerunning an uncertain execution.

## Rollback and retention

Roll back by setting `image` to the previously tested digest in Terraform and applying a reviewed plan. Image rollback does not roll back a schema or state migration; inspect compatibility first. Never deploy the old append-only worker against the new state/destination protocol.

Staging tables expire after one day. Target/state/audit tables currently have no automatic retention deletion; decide retention before collecting real data. Use synthetic records for the demo. Pause schedules before teardown, resolve active job outcomes, retain evidence, and explicitly export or delete sandbox data as appropriate. Dataset deletion intentionally fails when contents remain. `terraform destroy` is an operator action, not performed by test cleanup.

## Operational objectives to verify

Proposed: 99% of scheduled batches finish within 30 minutes of schedule over a 30-day observation period. The repository has no such observation history yet. Monitor successful pipeline events, per-table row counts/source watermarks, BigQuery job IDs, execution failures and missing scheduled success. Quality failures currently reject a batch; no quarantine or rejection-rate dashboard is claimed.

References: [scheduled jobs](https://docs.cloud.google.com/run/docs/execute/jobs-on-schedule), [Cloud Run Terraform resource](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/cloud_run_v2_job), [Monitoring policies](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/monitoring_alert_policy).
