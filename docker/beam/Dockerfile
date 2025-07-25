FROM apache/beam_python3.10_sdk:2.52.0

WORKDIR /app

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY docker/beam/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create necessary directories
RUN mkdir -p data/raw data/processed data/analytics logs

# Copy application code
COPY scripts/beam_pipeline.py scripts/
COPY scripts/utils/ scripts/utils/
COPY .env .

# Add health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD python -c "import psycopg2, os; conn = psycopg2.connect( \
        host=os.getenv('DB_HOST', 'postgres'), \
        port=os.getenv('DB_PORT', 5432), \
        dbname=os.getenv('DB_NAME', 'spotify_data'), \
        user=os.getenv('DB_USER', 'spotify_user'), \
        password=os.getenv('DB_PASSWORD', 'spotify_password')); \
        conn.close()" || exit 1

# Default command to run
ENTRYPOINT ["python", "scripts/beam_pipeline.py"]
CMD ["--start_date", "yesterday", "--end_date", "today", "--runner", "DirectRunner"] 