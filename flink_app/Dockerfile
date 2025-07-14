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

# Copy requirements file
COPY requirements.txt /tmp/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Switch back to the default flink user
USER flink