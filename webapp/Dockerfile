# Use official Python slim image
FROM python:3.9-slim

# Environment variables for performance/logging
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Install system dependencies for Google Cloud Python libs
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir gunicorn

# Copy application source
COPY . .

# Ensure Cloud Run can access the correct port
EXPOSE 8080

# Start Gunicorn (Cloud Run sets $PORT automatically)
CMD ["gunicorn", "--bind", ":${PORT}", "--workers", "1", "--threads", "8", "--timeout", "0", "main:app"]

