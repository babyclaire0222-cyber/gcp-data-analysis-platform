name: Deploy to Cloud Run

on:
  push:
    branches:
      - main

env:
  GCP_PROJECT: data-analysis-webapp
  REGION: asia-southeast1
  SERVICE_NAME: data-analysis-webapp
  REPO_NAME: cloud-run-artifacts # Artifact Registry repo name

jobs:
  deploy:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout source
        uses: actions/checkout@v4

      - name: Set up gcloud CLI
        uses: google-github-actions/setup-gcloud@v2
        with:
          project_id: ${{ env.GCP_PROJECT }}
          service_account_key: ${{ secrets.GCP_SA_KEY }}
          export_default_credentials: true

      - name: Configure Docker for Artifact Registry
        run: |
          gcloud auth configure-docker asia-southeast1-docker.pkg.dev --quiet

      - name: Build and push Docker image
        run: |
          IMAGE="asia-southeast1-docker.pkg.dev/${{ env.GCP_PROJECT }}/${{ env.REPO_NAME }}/${{ env.SERVICE_NAME }}"
          docker build -t $IMAGE ./webapp
          docker push $IMAGE

      - name: Deploy to Cloud Run
        run: |
          IMAGE="asia-southeast1-docker.pkg.dev/${{ env.GCP_PROJECT }}/${{ env.REPO_NAME }}/${{ env.SERVICE_NAME }}"
          gcloud run deploy ${{ env.SERVICE_NAME }} \
            --image $IMAGE \
            --region ${{ env.REGION }} \
            --platform managed \
            --allow-unauthenticated

