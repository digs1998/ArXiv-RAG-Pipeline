FROM python:3.12-slim AS base

WORKDIR /app

# Install OS deps (needed for psycopg2, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        git \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency list and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src /app/src

EXPOSE 8000

# Run app
CMD ["uvicorn", "src.routers.hybrid_search:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
