#!/bin/bash

# Script to deploy the ETL pipeline to Google Cloud Run

set -e

# Configuration
PROJECT_ID=${1:-$GCP_PROJECT_ID}
REGION="us-central1"
REPO_NAME="mysql-bigquery-etl"
IMAGE_NAME="gcr.io/$PROJECT_ID/$REPO_NAME:latest"
SERVICE_NAME="mysql-bigquery-etl"

if [ -z "$PROJECT_ID" ]; then
    echo "Error: PROJECT_ID must be provided as argument or set as GCP_PROJECT_ID environment variable"
    exit 1
fi

echo "Deploying to project: $PROJECT_ID"

# Build the Docker image
echo "Building Docker image..."
docker build -t $IMAGE_NAME .

# Push the image to Google Container Registry
echo "Pushing image to GCR..."
docker push $IMAGE_NAME

# Deploy to Cloud Run
echo "Deploying to Cloud Run..."
gcloud run deploy $SERVICE_NAME \
    --image $IMAGE_NAME \
    --platform managed \
    --region $REGION \
    --cpu 2 \
    --memory 2Gi \
    --timeout 1800 \
    --concurrency 10 \
    --max-instances 5 \
    --set-secrets MYSQL_HOST=mysql-host:latest \
    --set-secrets MYSQL_USER=mysql-user:latest \
    --set-secrets MYSQL_PASSWORD=mysql-password:latest \
    --set-secrets MYSQL_DATABASE=mysql-database:latest \
    --set-env-vars GCP_PROJECT_ID=$PROJECT_ID \
    --set-env-vars BIGQUERY_DATASET=mysql_etl \
    --set-env-vars BIGQUERY_LOCATION=US \
    --no-allow-unauthenticated

# Get the Cloud Run URL
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --region $REGION --format 'value(status.url)')
echo "Service deployed successfully: $SERVICE_URL"

# Create Cloud Scheduler job
echo "Creating Cloud Scheduler job..."
gcloud scheduler jobs create http ${SERVICE_NAME}-scheduler \
    --schedule="0 */6 * * *" \  # Every 6 hours
    --uri="$SERVICE_URL" \
    --http-method=GET \
    --oidc-service-account-email="${PROJECT_ID}@appspot.gserviceaccount.com" \
    --oidc-token-audience="$SERVICE_URL" \
    --location=$REGION \
    --time-zone="UTC"

echo "Deployment completed successfully!"