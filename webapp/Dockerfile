# Use official Python slim image
FROM python:3.9-slim

# Environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    GCP_PROJECT=data-analysis-webapp \
    BUCKET_NAME=data-analysis-upload-1000 \
    PUBSUB_TOPIC_FOR_SQL_IMPORT=sql-import-topic \
    BIGQUERY_DATASET=analysis_dataset

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy dependencies and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir gunicorn

# Copy project files
COPY . .

# Run with Gunicorn
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app

