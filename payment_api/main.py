# payment_api/main.py
import os
import json
import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time

# --- Configuration ---
# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Environment variables for Kafka
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "redpanda:29092")
TRANSACTIONS_TOPIC = os.environ.get("TRANSACTIONS_TOPIC", "transactions")

# Initialize FastAPI app
app = FastAPI(title="Payment API", description="An API to simulate and produce transaction events to Redpanda/Kafka.")

# --- Pydantic Model for Data Validation ---
class Transaction(BaseModel):
    transaction_id: str = Field(..., description="Unique identifier for the transaction")
    user_id: str = Field(..., description="Identifier for the user")
    card_number: str = Field(..., description="Masked card number")
    amount: float = Field(..., gt=0, description="Transaction amount, must be positive")
    timestamp: str = Field(..., description="Timestamp of the transaction in ISO 8601 format")
    merchant_id: str = Field(..., description="Identifier for the merchant")
    location: str = Field(..., description="Location of the transaction")

# --- Kafka Producer ---
# This producer will be initialized on startup
producer = None

@app.on_event("startup")
def startup_event():
    global producer
    retry_count = 0
    max_retries = 5
    retry_delay = 5  # seconds

    while retry_count < max_retries:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                client_id="payment-api-producer"
            )
            logging.info("Successfully connected to Redpanda/Kafka.")
            # Send a test message to ensure the topic is created
            producer.send(TRANSACTIONS_TOPIC, {'status': 'API producer is up and running'})
            producer.flush()
            logging.info(f"Test message sent to '{TRANSACTIONS_TOPIC}' topic.")
            return
        except NoBrokersAvailable:
            retry_count += 1
            logging.warning(f"Could not connect to Redpanda/Kafka. Retrying in {retry_delay}s... ({retry_count}/{max_retries})")
            time.sleep(retry_delay)
    
    logging.error("Failed to connect to Redpanda/Kafka after multiple retries. The application will start but cannot produce messages.")
    # The application can still run, but the producer will be None.
    # Endpoints will fail with an internal server error.


@app.on_event("shutdown")
def shutdown_event():
    if producer:
        producer.close()
        logging.info("Redpanda/Kafka producer connection closed.")

# --- API Endpoint ---
@app.post("/transaction", status_code=202)
def create_transaction(transaction: Transaction):
    """
    Receives a transaction, validates it, and sends it to the 'transactions' Kafka topic.
    """
    if not producer:
        raise HTTPException(status_code=503, detail="Service Unavailable: Kafka producer is not connected.")

    try:
        transaction_dict = transaction.dict()
        future = producer.send(TRANSACTIONS_TOPIC, value=transaction_dict)
        # Block for 'successful' sends
        record_metadata = future.get(timeout=10)
        logging.info(f"Successfully produced transaction {transaction.transaction_id} to topic '{record_metadata.topic}' partition {record_metadata.partition}")
        return {"status": "accepted", "transaction_id": transaction.transaction_id}
    except Exception as e:
        logging.error(f"Failed to send transaction to Kafka: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error: Could not produce message to Kafka.")

@app.get("/health")
def health_check():
    """
    Health check endpoint to verify service and Kafka connectivity.
    """
    kafka_status = "connected" if producer and producer.bootstrap_connected() else "disconnected"
    return {"api_status": "ok", "kafka_status": kafka_status}