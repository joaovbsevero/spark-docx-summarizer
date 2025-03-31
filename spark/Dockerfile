FROM bitnami/spark:latest

# Switch to root to modify permissions
USER root
RUN mkdir -p /tmp/shared && chmod -R 777 /tmp/shared


# Install requirements
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy application code
COPY worker.py /opt/spark-app/worker.py