# flink_app/fraud_detector.py
import os
import json
import logging
from datetime import datetime

from pyflink.common import Types, WatermarkStrategy, Row
from pyflink.common.typeinfo import RowTypeInfo
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.functions import RuntimeContext, KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor

import joblib
import redis

# --- Configuration ---
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "redpanda:29092")
TRANSACTIONS_TOPIC = os.environ.get("TRANSACTIONS_TOPIC", "transactions")
CONSUMER_GROUP_ID = "fraud-detector-group"
MODEL_PATH = '/opt/flink/usrlib/model/isolation_forest.joblib'
SCALER_PATH = '/opt/flink/usrlib/model/scaler.joblib'
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_DB = int(os.environ.get("REDIS_DB", "0"))
REDIS_KEY = "fraud_alerts"

class FraudDetector(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        logging.info("Initializing FraudDetector...")
        self.model = joblib.load(MODEL_PATH)
        self.scaler = joblib.load(SCALER_PATH)
        self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        state_descriptor = ValueStateDescriptor("user_history", Types.TUPLE([Types.STRING(), Types.LIST(Types.DOUBLE())]))
        self.user_history_state = runtime_context.get_state(state_descriptor)
        logging.info("FraudDetector initialized successfully.")

    def process_element(self, value: Row, ctx: KeyedProcessFunction.Context):
        try:
            tx_data = value.as_dict()
            amount = float(tx_data['amount'])
            current_time = datetime.fromisoformat(tx_data['timestamp'].replace('Z', '+00:00'))

            user_history = self.user_history_state.value()
            if user_history is None:
                time_since_last_tx_seconds = 0.0
                avg_amount_last_5_tx = amount
                new_history = (current_time.isoformat(), [amount])
            else:
                last_tx_time_str, last_amounts = user_history
                last_tx_time = datetime.fromisoformat(last_tx_time_str)
                time_since_last_tx_seconds = (current_time - last_tx_time).total_seconds()
                avg_amount_last_5_tx = sum(last_amounts) / len(last_amounts) if last_amounts else amount
                last_amounts.append(amount)
                if len(last_amounts) > 5:
                    last_amounts.pop(0)
                new_history = (current_time.isoformat(), last_amounts)
            
            self.user_history_state.update(new_history)
            feature_vector = [[amount, time_since_last_tx_seconds, avg_amount_last_5_tx]]
            scaled_features = self.scaler.transform(feature_vector)
            prediction = self.model.predict(scaled_features)[0]
            
            if prediction == -1:
                tx_data['is_fraud'] = str(True) 
                tx_data['processed_at'] = datetime.now().isoformat()
                
                alert_json = json.dumps(tx_data)
                
                # --- ADDED LOGGING FOR DEBUGGING ---
                try:
                    logging.info(f"Attempting to push alert to Redis: {alert_json}")
                    self.redis_client.rpush(REDIS_KEY, alert_json)
                    logging.info("Successfully pushed alert to Redis.")
                except Exception as redis_e:
                    logging.error(f"!!! FAILED TO PUSH TO REDIS: {redis_e}")
                # ------------------------------------
                
                yield {k: str(v) for k, v in tx_data.items()}
        except Exception as e:
            logging.error(f"Error processing element: {value}. Error: {e}")

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10000)

    source_type_info = Types.ROW_NAMED(
        ['transaction_id', 'user_id', 'card_number', 'amount', 'timestamp', 'merchant_id', 'location'],
        [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()]
    )

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_topics(TRANSACTIONS_TOPIC) \
        .set_group_id(CONSUMER_GROUP_ID) \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(JsonRowDeserializationSchema.builder().type_info(source_type_info).build()) \
        .build()

    stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Redpanda Source")
    transaction_stream = stream.filter(lambda row: hasattr(row, 'transaction_id') and row.transaction_id is not None)

    fraud_alerts = transaction_stream \
        .key_by(lambda row: row.user_id) \
        .process(FraudDetector(), output_type=Types.MAP(Types.STRING(), Types.STRING()))

    fraud_alerts.print()
    env.execute("Stateful Fraud Detection with Redis Alerts")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()