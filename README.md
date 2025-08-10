# GCP Data Analysis Platform

This project hosts a web app to upload SQL, CSV, and JSON files to Google Cloud Storage, process them, and store results.

## Setup

1. Edit `deployment/create_resources.sh` with your GCP project details.
2. Run it to create bucket, BigQuery dataset, and Pub/Sub topic.
3. Deploy each component to GCP Cloud Functions / Cloud Run.
4. Push this repo to GitHub for CI/CD if desired.
