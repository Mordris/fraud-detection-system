# payment_api/main.py

# --- Standard Library Imports ---
import os
import json
import logging
import time

# --- Third-Party Imports ---
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError, KafkaError

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "redpanda:29092")
TRANSACTIONS_TOPIC = os.environ.get("TRANSACTIONS_TOPIC", "transactions")

app = FastAPI(title="Payment API", description="An API to simulate and produce transaction events to Redpanda/Kafka.")

class Transaction(BaseModel):
    transaction_id: str = Field(..., description="Unique identifier for the transaction")
    user_id: str = Field(..., description="Identifier for the user")
    card_number: str = Field(..., description="Masked card number")
    amount: float = Field(..., gt=0, description="Transaction amount, must be positive")
    timestamp: str = Field(..., description="Timestamp of the transaction in ISO 8601 format")
    merchant_id: str = Field(..., description="Identifier for the merchant")
    location: str = Field(..., description="Location of the transaction")

# --- Globals for Kafka clients ---
producer = None
admin_client = None

def create_topic_if_not_exists():
    """
    Connects to Kafka as an admin and creates the 'transactions' topic.
    This function is resilient and will retry if Kafka is not ready yet.
    """
    global admin_client
    retries = 10
    delay = 6  # seconds

    for i in range(retries):
        try:
            logging.info(f"Attempting to connect to Kafka AdminClient... (Attempt {i+1}/{retries})")
            admin_client = KafkaAdminClient(
                bootstrap_servers=KAFKA_BROKER,
                client_id='payment-api-admin'
            )
            logging.info("Kafka AdminClient connected successfully.")
            
            topic_list = [NewTopic(name=TRANSACTIONS_TOPIC, num_partitions=1, replication_factor=1)]
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            logging.info(f"Topic '{TRANSACTIONS_TOPIC}' created or already exists.")
            return True # Success
        except TopicAlreadyExistsError:
            logging.info(f"Topic '{TRANSACTIONS_TOPIC}' already exists. No action needed.")
            return True # Success
        except (NoBrokersAvailable, KafkaError) as e:
            logging.warning(f"Could not create topic yet. Kafka might not be ready. Error: {e}. Retrying in {delay}s...")
            time.sleep(delay)
        finally:
            if admin_client:
                admin_client.close()

    logging.error("Failed to create Kafka topic after multiple retries. The application cannot start.")
    return False

@app.on_event("startup")
def startup_event():
    """
    Handles Kafka connection and topic creation on application startup.
    """
    global producer

    # First, ensure the topic exists. This will block until it succeeds or fails.
    if not create_topic_if_not_exists():
        # A more graceful way to handle failure would be to exit the process
        # so Kubernetes restarts it, but for now, we prevent the producer from starting.
        logging.critical("FATAL: Topic creation failed. Producer will not be initialized.")
        return

    # Now, connect the producer, which should succeed as Kafka is verified to be up.
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            client_id="payment-api-producer"
        )
        logging.info("Successfully connected KafkaProducer.")
        # We no longer need to send a test message as the admin client verified connectivity.
    except NoBrokersAvailable:
        logging.error("Failed to connect KafkaProducer even after admin client succeeded. This should not happen.")

@app.on_event("shutdown")
def shutdown_event():
    """Gracefully closes connections on shutdown."""
    if producer:
        producer.close()
        logging.info("Kafka producer connection closed.")
    if admin_client:
        admin_client.close()
        logging.info("Kafka admin client connection closed.")

@app.post("/transaction", status_code=202)
def create_transaction(transaction: Transaction):
    if not producer:
        raise HTTPException(status_code=503, detail="Service Unavailable: Kafka producer is not connected.")
    try:
        transaction_dict = transaction.dict()
        future = producer.send(TRANSACTIONS_TOPIC, value=transaction_dict)
        record_metadata = future.get(timeout=10)
        logging.info(f"Successfully produced transaction {transaction.transaction_id} to topic '{record_metadata.topic}'")
        return {"status": "accepted", "transaction_id": transaction.transaction_id}
    except Exception as e:
        logging.error(f"Failed to send transaction to Kafka: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error: Could not produce message to Kafka.")

@app.get("/health")
def health_check():
    kafka_status = "connected" if producer and producer.bootstrap_connected() else "disconnected"
    return {"api_status": "ok", "kafka_status": kafka_status}