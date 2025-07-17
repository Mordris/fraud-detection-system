# payment_api/main.py
# --- Standard Library Imports ---
import os
import json
import logging
import time
from contextlib import asynccontextmanager

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

# --- Globals for Kafka clients ---
# We store them in a dictionary to manage them within the lifespan context
kafka_clients = {}

def create_topic_if_not_exists():
    """
    Connects to Kafka as an admin and creates the 'transactions' topic.
    This function is resilient and will retry if Kafka is not ready yet.
    """
    admin_client = None
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

# --- Lifespan Context Manager (Modern Replacement for on_event) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages the application's startup and shutdown logic.
    """
    # --- Code to run on startup ---
    logging.info("Application startup: Initializing Kafka connection...")
    
    if create_topic_if_not_exists():
        try:
            kafka_clients["producer"] = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                client_id="payment-api-producer"
            )
            logging.info("Successfully connected KafkaProducer.")
        except NoBrokersAvailable:
            logging.error("Failed to connect KafkaProducer during startup.")
            kafka_clients["producer"] = None
    else:
        # If topic creation failed, ensure producer is None
        kafka_clients["producer"] = None
    
    yield # The application runs here

    # --- Code to run on shutdown ---
    logging.info("Application shutdown: Closing Kafka connection...")
    producer = kafka_clients.get("producer")
    if producer:
        producer.close()
        logging.info("Kafka producer connection closed.")

# --- FastAPI App Initialization ---
# The lifespan manager is passed directly to the FastAPI constructor.
app = FastAPI(
    title="Payment API", 
    description="An API to simulate and produce transaction events to Redpanda/Kafka.",
    lifespan=lifespan
)

class Transaction(BaseModel):
    transaction_id: str = Field(..., description="Unique identifier for the transaction")
    user_id: str = Field(..., description="Identifier for the user")
    card_number: str = Field(..., description="Masked card number")
    amount: float = Field(..., gt=0, description="Transaction amount, must be positive")
    timestamp: str = Field(..., description="Timestamp of the transaction in ISO 8601 format")
    merchant_id: str = Field(..., description="Identifier for the merchant")
    location: str = Field(..., description="Location of the transaction")

@app.post("/transaction", status_code=202)
def create_transaction(transaction: Transaction):
    producer = kafka_clients.get("producer")
    if not producer:
        raise HTTPException(status_code=503, detail="Service Unavailable: Kafka producer is not connected.")
    try:
        # Use .model_dump() instead of the deprecated .dict()
        transaction_dict = transaction.model_dump()
        future = producer.send(TRANSACTIONS_TOPIC, value=transaction_dict)
        # Block for a result to ensure the message is sent before returning a success response
        record_metadata = future.get(timeout=10)
        logging.info(f"Successfully produced transaction {transaction.transaction_id} to topic '{record_metadata.topic}'")
        return {"status": "accepted", "transaction_id": transaction.transaction_id}
    except Exception as e:
        logging.error(f"Failed to send transaction to Kafka: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error: Could not produce message to Kafka.")

@app.get("/health")
def health_check():
    producer = kafka_clients.get("producer")
    kafka_status = "connected" if producer and producer.bootstrap_connected() else "disconnected"
    return {"api_status": "ok", "kafka_status": kafka_status}