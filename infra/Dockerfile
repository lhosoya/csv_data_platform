# Use the official Airflow image as the base
FROM apache/airflow:2.9.3

ENV AIRFLOW_HOME=/opt/airflow

USER root

# Install OpenJDK 17 and clean up apt lists to keep image size small
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set the JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64


COPY requirements.txt ./requirements.txt

USER airflow

# Install Python dependencies from requirements.txt as airflow user
RUN pip install --no-cache-dir -r requirements.txt

