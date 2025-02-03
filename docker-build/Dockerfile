# Use Python 3.11 as base image (same as Cloud Composer 3)
FROM python:3.11.8-slim

# Set appropriate arg
ARG VERSION=2.1.25
ARG OS=linux
ARG ARCH=amd64

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential curl \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://github.com/GoogleCloudPlatform/docker-credential-gcr/releases/download/v${VERSION}/docker-credential-gcr_${OS}_${ARCH}-${VERSION}.tar.gz" \
    | tar xz docker-credential-gcr \
    && chmod +x docker-credential-gcr && mv docker-credential-gcr /usr/bin/


# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy DAGs and plugins
COPY ./dags/ /app/dags/
COPY ./plugins/ /app/plugins/

# Set environment variables
ENV PYTHONPATH=/app

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD python -c "import dags.github_commits_etl" || exit 1
