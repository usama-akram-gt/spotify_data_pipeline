FROM sbtscala/scala-sbt:eclipse-temurin-17.0.5_8_1.9.4_2.13.12

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    procps \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Create necessary directories
RUN mkdir -p data/raw data/processed data/analytics logs

# Install Python dependencies for any helper scripts
RUN pip3 install --no-cache-dir \
    psycopg2-binary \
    python-dotenv \
    requests \
    confluent-kafka

# Copy SBT configuration files
COPY build.sbt .
COPY project/ project/

# Copy source code
COPY src/ src/

# Copy resources
COPY .env .

# Build the Scio application
RUN sbt clean compile assembly

# Add health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Default command to run
ENTRYPOINT ["java", "-jar", "target/scala-2.13/spotify-pipeline.jar"]
CMD ["--streaming"] 