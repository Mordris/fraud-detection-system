# flink_app/fraud_detector.py

# --- Standard Library Imports ---
import os
import json
import logging
from datetime import datetime

# --- PyFlink Core Imports ---
# Import essential classes from PyFlink for building the data stream.
from pyflink.common import Types, WatermarkStrategy, Row
from pyflink.common.typeinfo import RowTypeInfo
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.functions import RuntimeContext, KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor

# --- Third-Party Imports ---
import joblib  # For loading the pre-trained machine learning models.
import redis   # For connecting to Redis to sink fraud alerts.

# --- Configuration ---
# Load configuration from environment variables, providing defaults for local execution.
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "redpanda:29092")
TRANSACTIONS_TOPIC = os.environ.get("TRANSACTIONS_TOPIC", "transactions")
CONSUMER_GROUP_ID = "fraud-detector-group"
MODEL_PATH = '/opt/flink/usrlib/model/isolation_forest.joblib'
SCALER_PATH = '/opt/flink/usrlib/model/scaler.joblib'
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_DB = int(os.environ.get("REDIS_DB", "0"))
REDIS_KEY = "fraud_alerts"  # The Redis list key for alerts.

# --- Stateful Fraud Detection Logic ---
class FraudDetector(KeyedProcessFunction):
    """
    A stateful Flink ProcessFunction that detects fraudulent transactions.

    For each transaction, it:
    1. Maintains historical state for the user (last transaction time, recent amounts).
    2. Engineers features based on this history.
    3. Uses a pre-trained Isolation Forest model to predict if the transaction is an anomaly.
    4. Pushes detected fraud alerts to a Redis list.
    """

    def open(self, runtime_context: RuntimeContext):
        """
        Initialization method for the function. It is called once per parallel
        instance before any records are processed.
        """
        logging.info("Initializing FraudDetector...")
        # Load the serialized machine learning model and scaler from files.
        # These files are mounted into the container via the docker-compose.yml.
        self.model = joblib.load(MODEL_PATH)
        self.scaler = joblib.load(SCALER_PATH)
        
        # Initialize the Redis client. `decode_responses=True` ensures that
        # data read from Redis is automatically decoded from bytes to strings.
        self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        
        # Define the structure for Flink's managed state. This state will be
        # scoped to each unique key (user_id). We store a tuple containing
        # the timestamp of the last transaction and a list of recent amounts.
        state_descriptor = ValueStateDescriptor("user_history", Types.TUPLE([Types.STRING(), Types.LIST(Types.DOUBLE())]))
        self.user_history_state = runtime_context.get_state(state_descriptor)
        logging.info("FraudDetector initialized successfully.")

    def process_element(self, value: Row, ctx: KeyedProcessFunction.Context):
        """
        This method is called for each individual record in the stream.
        `value` is a Row object from the KafkaSource.
        `ctx` provides access to context, such as the current key.
        """
        try:
            # Convert the PyFlink Row object to a Python dictionary for easier field access.
            tx_data = value.as_dict()
            amount = float(tx_data['amount'])
            current_time = datetime.fromisoformat(tx_data['timestamp'].replace('Z', '+00:00'))

            # --- 1. State Retrieval and Feature Engineering ---
            # Retrieve the historical data for the current user from Flink's state.
            user_history = self.user_history_state.value()

            if user_history is None:
                # This is the first transaction we've seen for this user.
                # We create default features and initialize the state.
                time_since_last_tx_seconds = 0.0
                avg_amount_last_5_tx = amount
                new_history = (current_time.isoformat(), [amount])
            else:
                # The user has a history. Calculate features based on it.
                last_tx_time_str, last_amounts = user_history
                last_tx_time = datetime.fromisoformat(last_tx_time_str)
                time_since_last_tx_seconds = (current_time - last_tx_time).total_seconds()
                avg_amount_last_5_tx = sum(last_amounts) / len(last_amounts) if last_amounts else amount
                
                # Update the user's history with the current transaction.
                last_amounts.append(amount)
                # Keep the list of amounts capped at the last 5.
                if len(last_amounts) > 5:
                    last_amounts.pop(0)
                new_history = (current_time.isoformat(), last_amounts)
            
            # Persist the updated history back into Flink's state for the next event.
            self.user_history_state.update(new_history)
            
            # --- 2. Machine Learning Prediction ---
            # Create the feature vector in the same order as used for training.
            feature_vector = [[amount, time_since_last_tx_seconds, avg_amount_last_5_tx]]
            # Apply the same scaling that was applied during training.
            scaled_features = self.scaler.transform(feature_vector)
            # Predict using the Isolation Forest model. Returns -1 for anomalies (fraud).
            prediction = self.model.predict(scaled_features)[0]
            
            # --- 3. Alerting ---
            # If the model flagged the transaction as an anomaly:
            if prediction == -1:
                # Enrich the transaction data with our findings.
                tx_data['is_fraud'] = str(True) 
                tx_data['processed_at'] = datetime.now().isoformat()
                
                # Serialize the enriched data to a JSON string.
                alert_json = json.dumps(tx_data)
                
                # Push the alert into the Redis list.
                try:
                    logging.info(f"Attempting to push alert to Redis: {alert_json}")
                    self.redis_client.rpush(REDIS_KEY, alert_json)
                    logging.info("Successfully pushed alert to Redis.")
                except Exception as redis_e:
                    logging.error(f"!!! FAILED TO PUSH TO REDIS: {redis_e}")
                
                # Yield the fraudulent transaction to the next operator in the Flink job (the print sink).
                # All values are converted to strings to match the declared output type.
                yield {k: str(v) for k, v in tx_data.items()}
        except Exception as e:
            # Catch and log any unexpected errors during processing.
            logging.error(f"Error processing element: {value}. Error: {e}")

# --- Flink Job Definition ---
def main():
    """Defines and executes the Flink streaming job."""
    # Set up the Flink StreamExecutionEnvironment.
    env = StreamExecutionEnvironment.get_execution_environment()
    # Enable checkpointing every 10 seconds (10000 ms) for fault tolerance.
    env.enable_checkpointing(10000)

    # Define the precise schema of the JSON data from Kafka.
    # This is crucial for the JsonRowDeserializationSchema to work correctly.
    source_type_info = Types.ROW_NAMED(
        ['transaction_id', 'user_id', 'card_number', 'amount', 'timestamp', 'merchant_id', 'location'],
        [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()]
    )

    # Configure the KafkaSource to connect to Redpanda and read the transaction topic.
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_topics(TRANSACTIONS_TOPIC) \
        .set_group_id(CONSUMER_GROUP_ID) \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(JsonRowDeserializationSchema.builder().type_info(source_type_info).build()) \
        .build()

    # Create the initial data stream from the Kafka source.
    stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Redpanda Source")
    
    # Filter out the initial "status" message sent by the payment-api on startup.
    transaction_stream = stream.filter(lambda row: hasattr(row, 'transaction_id') and row.transaction_id is not None)

    # The core logic:
    # 1. Key the stream by `user_id` so all transactions from a single user go to the same task.
    # 2. Apply our stateful FraudDetector function.
    fraud_alerts = transaction_stream \
        .key_by(lambda row: row.user_id) \
        .process(FraudDetector(), output_type=Types.MAP(Types.STRING(), Types.STRING()))

    # Add a sink to print the detected fraud alerts to the TaskManager logs for debugging.
    fraud_alerts.print()
    
    # Give the job a name and execute it.
    env.execute("Stateful Fraud Detection with Redis Alerts")

# --- Script Entry Point ---
if __name__ == '__main__':
    # Set up basic logging for the Flink job client.
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    # Run the main job definition function.
    main()