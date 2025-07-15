# payment_api/main.py

# --- Standard Library Imports ---
import os
import json
import logging
import time

# --- Third-Party Imports ---
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field  # For data validation and serialization.
from kafka import KafkaProducer         # The client for sending messages to Kafka/Redpanda.
from kafka.errors import NoBrokersAvailable

# --- Configuration ---
# Set up basic logging to see events in the container logs.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Get Kafka configuration from environment variables, with defaults.
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "redpanda:29092")
TRANSACTIONS_TOPIC = os.environ.get("TRANSACTIONS_TOPIC", "transactions")

# Initialize the FastAPI application instance.
app = FastAPI(title="Payment API", description="An API to simulate and produce transaction events to Redpanda/Kafka.")

# --- Pydantic Model for Data Validation ---
# This model defines the expected structure of a transaction POST request.
# FastAPI uses this to automatically validate incoming data.
class Transaction(BaseModel):
    transaction_id: str = Field(..., description="Unique identifier for the transaction")
    user_id: str = Field(..., description="Identifier for the user")
    card_number: str = Field(..., description="Masked card number")
    amount: float = Field(..., gt=0, description="Transaction amount, must be positive")
    timestamp: str = Field(..., description="Timestamp of the transaction in ISO 8601 format")
    merchant_id: str = Field(..., description="Identifier for the merchant")
    location: str = Field(..., description="Location of the transaction")

# --- Kafka Producer Management ---
# The KafkaProducer is a global variable to be shared across the application.
producer = None

@app.on_event("startup")
def startup_event():
    """
    This function is called by FastAPI when the application starts.
    It handles the initial connection to Kafka with a retry mechanism.
    """
    global producer
    retry_count = 0
    max_retries = 5
    retry_delay = 5  # seconds

    # This loop makes the service resilient to startup order issues.
    # It will keep trying to connect to Kafka until it succeeds or runs out of retries.
    while retry_count < max_retries:
        try:
            # Initialize the KafkaProducer.
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                # Serialize the value of each message from a dict to a UTF-8 encoded JSON string.
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                client_id="payment-api-producer"
            )
            logging.info("Successfully connected to Redpanda/Kafka.")
            
            # Send a test message on startup. This is a good practice as it will
            # trigger Redpanda's auto-topic-creation feature if the topic doesn't exist yet.
            producer.send(TRANSACTIONS_TOPIC, {'status': 'API producer is up and running'})
            producer.flush() # Ensure the message is sent immediately.
            logging.info(f"Test message sent to '{TRANSACTIONS_TOPIC}' topic.")
            return # Exit the loop on successful connection.
        except NoBrokersAvailable:
            # If Kafka isn't ready yet, log a warning and wait before retrying.
            retry_count += 1
            logging.warning(f"Could not connect to Redpanda/Kafka. Retrying in {retry_delay}s... ({retry_count}/{max_retries})")
            time.sleep(retry_delay)
    
    # If all retries fail, log a critical error.
    logging.error("Failed to connect to Redpanda/Kafka after multiple retries. The application will start but cannot produce messages.")

@app.on_event("shutdown")
def shutdown_event():
    """This function is called by FastAPI when the application is shutting down."""
    if producer:
        # Gracefully close the Kafka producer connection.
        producer.close()
        logging.info("Redpanda/Kafka producer connection closed.")

# --- API Endpoints ---
@app.post("/transaction", status_code=202)
def create_transaction(transaction: Transaction):
    """
    Receives a transaction, validates it against the Transaction model,
    and sends it to the 'transactions' Kafka topic.
    """
    # If the producer failed to initialize, return a service unavailable error.
    if not producer:
        raise HTTPException(status_code=503, detail="Service Unavailable: Kafka producer is not connected.")

    try:
        # Convert the Pydantic model to a dictionary.
        transaction_dict = transaction.dict()
        # Send the transaction to the Kafka topic. This is an asynchronous operation.
        future = producer.send(TRANSACTIONS_TOPIC, value=transaction_dict)
        # Block until the message is successfully sent (or times out).
        record_metadata = future.get(timeout=10)
        logging.info(f"Successfully produced transaction {transaction.transaction_id} to topic '{record_metadata.topic}' partition {record_metadata.partition}")
        # Return an "Accepted" status code to the client.
        return {"status": "accepted", "transaction_id": transaction.transaction_id}
    except Exception as e:
        logging.error(f"Failed to send transaction to Kafka: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error: Could not produce message to Kafka.")

@app.get("/health")
def health_check():
    """A simple health check endpoint to verify service and Kafka connectivity."""
    kafka_status = "connected" if producer and producer.bootstrap_connected() else "disconnected"
    return {"api_status": "ok", "kafka_status": kafka_status}