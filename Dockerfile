FROM apache/airflow:2.8.1-python3.10

# Install additional Python dependencies if needed
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy DAGs into the Airflow DAGs folder
COPY dags/ /opt/airflow/dags/

# Set environment variables (edit as needed)
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__EXECUTOR=SequentialExecutor

# Expose Airflow webserver port
EXPOSE 8080

# Install Jenkins and Java (required for Jenkins)
USER root
RUN apt-get update && \
    apt-get install -y openjdk-11-jre wget gnupg && \
    wget -q -O - https://pkg.jenkins.io/debian-stable/jenkins.io.key | apt-key add - && \
    echo "deb https://pkg.jenkins.io/debian-stable binary/" > /etc/apt/sources.list.d/jenkins.list && \
    apt-get update && \
    apt-get install -y jenkins && \
    apt-get clean;

# Expose Jenkins default port
EXPOSE 8081

# Default Airflow entrypoint
CMD ["airflow", "standalone"]
