# Use official Python slim image
FROM python:3.9-slim

# Set environment variables to avoid writing pyc files and enable stdout flushing
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Default environment variables (can be overridden in Cloud Run)
ENV GCP_PROJECT=data-analysis-webapp \
    BUCKET_NAME=data-analysis-upload-1000 \
    PUBSUB_TOPIC_FOR_SQL_IMPORT=sql-import-topic \
    BIGQUERY_DATASET=analysis_dataset

# Set working directory
WORKDIR /app

# Install system dependencies for Google Cloud Python libs
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy and install dependencies first (better caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir gunicorn

# Copy application files
COPY . .

# Use gunicorn for serving Flask on Cloud Run
# Cloud Run will set $PORT automatically
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app

