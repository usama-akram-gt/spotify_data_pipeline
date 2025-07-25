FROM adoptopenjdk/openjdk11:alpine

# Install required packages
RUN apk add --no-cache bash curl procps git

# Set up SBT
ENV SBT_VERSION=1.9.6
RUN curl -L -o sbt-$SBT_VERSION.tgz https://github.com/sbt/sbt/releases/download/v$SBT_VERSION/sbt-$SBT_VERSION.tgz && \
    mkdir -p /opt && \
    tar -xf sbt-$SBT_VERSION.tgz -C /opt && \
    rm sbt-$SBT_VERSION.tgz

# Set up environment variables
ENV PATH=$PATH:/opt/sbt/bin

# Set the working directory
WORKDIR /app

# Copy SBT configuration files first for better caching
COPY build.sbt ./
COPY project ./project/

# Pre-download dependencies
RUN sbt update

# Copy the source code
COPY src ./src/

# Build the project
RUN sbt clean assembly

# Default command
CMD ["bash"] 