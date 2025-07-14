import os
import json
import logging
from datetime import datetime

from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.functions import RuntimeContext, KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor

import joblib

# --- Configuration ---
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "redpanda:29092")
TRANSACTIONS_TOPIC = os.environ.get("TRANSACTIONS_TOPIC", "transactions")
CONSUMER_GROUP_ID = "fraud-detector-group"
MODEL_PATH = '/opt/flink/usrlib/model/isolation_forest.joblib'
SCALER_PATH = '/opt/flink/usrlib/model/scaler.joblib'

class FraudDetector(KeyedProcessFunction):
    """
    A stateful process function to enrich transactions with fraud predictions.
    """
    def open(self, runtime_context: RuntimeContext):
        """Initializes the function by loading the model and setting up state."""
        logging.info("Initializing FraudDetector...")
        self.model = joblib.load(MODEL_PATH)
        self.scaler = joblib.load(SCALER_PATH)
        
        # Define the structure for our managed state for a single user's history
        state_descriptor = ValueStateDescriptor(
            "user_history", 
            Types.TUPLE([Types.STRING(), Types.LIST(Types.DOUBLE())])  # (last_ts_iso, [amounts])
        )
        self.user_history_state = runtime_context.get_state(state_descriptor)
        logging.info("State and models initialized successfully.")

    def process_element(self, value, ctx):
        """Processes one transaction at a time."""
        tx_data = value[1]  # The Map from Kafka
        user_id = tx_data['user_id']
        amount = float(tx_data['amount'])
        current_time_str = tx_data['timestamp']
        current_time = datetime.fromisoformat(current_time_str.replace('Z', '+00:00'))

        # 1. Retrieve and Update State
        user_history = self.user_history_state.value()
        if user_history is None:
            time_since_last_tx_seconds = 0.0
            avg_amount_last_5_tx = amount
            new_history = (current_time.isoformat(), [amount])
        else:
            last_tx_time_str, last_amounts = user_history
            last_tx_time = datetime.fromisoformat(last_tx_time_str)
            time_diff = current_time - last_tx_time
            time_since_last_tx_seconds = time_diff.total_seconds()
            avg_amount_last_5_tx = sum(last_amounts) / len(last_amounts)
            
            last_amounts.append(amount)
            if len(last_amounts) > 5:
                last_amounts.pop(0)
            new_history = (current_time.isoformat(), last_amounts)

        self.user_history_state.update(new_history)
        
        # 2. Predict
        feature_vector = [[amount, time_since_last_tx_seconds, avg_amount_last_5_tx]]
        scaled_features = self.scaler.transform(feature_vector)
        prediction = self.model.predict(scaled_features)[0]
        
        # 3. Enrich and return the transaction
        tx_data['is_fraud'] = bool(prediction == -1)
        yield tx_data

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10000)

    source_type_info = Types.ROW([Types.STRING(), Types.MAP(Types.STRING(), Types.STRING())])
    
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_topics(TRANSACTIONS_TOPIC) \
        .set_group_id(CONSUMER_GROUP_ID) \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(JsonRowDeserializationSchema.builder().type_info(source_type_info).build()) \
        .build()

    stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Redpanda Source")

    # Key by user_id and apply the stateful process function
    processed_stream = stream \
        .key_by(lambda x: x[1]['user_id']) \
        .process(FraudDetector(), output_type=Types.MAP(Types.STRING(), Types.BOOLEAN()))
    
    processed_stream.print()

    env.execute("Stateful Fraud Detection")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()