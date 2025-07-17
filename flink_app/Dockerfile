# flink_app/Dockerfile

# Start from the official Flink image as the base
FROM apache/flink:1.18.1-scala_2.12-java11

# Switch to root user to perform installations
USER root

# Install system dependencies
RUN apt-get update -y && \
    apt-get install -y python3-pip wget && \
    rm -rf /var/lib/apt/lists/*

# Create a symbolic link so that the 'python' command executes 'python3'
RUN ln -s /usr/bin/python3 /usr/bin/python

# Download Kafka connector JARs
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.1.0-1.18/flink-connector-kafka-3.1.0-1.18.jar && \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.2.3/kafka-clients-3.2.3.jar

# Create the /opt/flink/usrlib directory where we'll place our code
WORKDIR /opt/flink/usrlib

# Copy the Python script, model, and requirements into the container.
COPY fraud_detector.py .
COPY model ./model
COPY requirements.txt .

# Install Python dependencies from the copied requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# --- ADD THIS NEW RUN COMMAND ---
# Upgrade specific transitive dependencies to fix vulnerabilities found by Trivy.
RUN pip install --no-cache-dir --upgrade pyarrow>=14.0.1 protobuf>=4.25.0
# --------------------------------

# Switch back to the default flink user
USER flink