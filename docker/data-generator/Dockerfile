FROM python:3.10-slim

WORKDIR /app

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Copy only requirements first to leverage Docker cache
COPY requirements.txt .

# Install required Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Create necessary directories
RUN mkdir -p data/raw data/processed data/analytics logs

# Copy application code
COPY scripts/data_generator.py scripts/
COPY scripts/utils/ scripts/utils/
COPY .env .

# Add a health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD python -c "import psycopg2, os; conn = psycopg2.connect( \
        host=os.getenv('DB_HOST', 'postgres'), \
        port=os.getenv('DB_PORT', 5432), \
        dbname=os.getenv('DB_NAME', 'spotify_data'), \
        user=os.getenv('DB_USER', 'spotify_user'), \
        password=os.getenv('DB_PASSWORD', 'spotify_password')); \
        conn.close()" || exit 1

# Default command - can be overridden in compose file
ENTRYPOINT ["python", "scripts/data_generator.py"]
CMD ["--scale", "small"] 