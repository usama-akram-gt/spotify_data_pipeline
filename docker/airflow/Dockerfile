FROM apache/airflow:2.6.3-python3.9

USER root

# Install system dependencies
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
         default-libmysqlclient-dev \
         libpq-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy DAG initialization script
COPY scripts/init_dags.sh /opt/airflow/scripts/init_dags.sh
RUN chmod +x /opt/airflow/scripts/init_dags.sh

# Copy custom plugins if any
COPY plugins/ /opt/airflow/plugins/

# Copy entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"] 