import os
import json
import time
import logging
from uuid import uuid4
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Producer
from faker import Faker
from pydantic import BaseModel, Field

# 1. Enterprise Standard: Configure Structured Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# 2. Enterprise Standard: Strict Data Validation using Pydantic
class Transaction(BaseModel):
    transaction_id: str
    user_id: str
    amount: float = Field(..., gt=0) # Must be strictly greater than 0
    currency: str
    merchant_id: str
    timestamp: str
    ip_address: str

class TransactionSimulator:
    def __init__(self):
        self.faker = Faker()
        self.currencies = ["USD", "EUR", "GBP", "NPR", "JPY"]
        
    def generate_transaction(self) -> Transaction:
        """Generates a single, schema-validated transaction event."""
        return Transaction(
            transaction_id=str(uuid4()),
            user_id=self.faker.uuid4(),
            amount=round(self.faker.pyfloat(min_value=5.00, max_value=2500.00), 2),
            currency=self.faker.random_element(self.currencies),
            merchant_id=self.faker.uuid4(),
            timestamp=datetime.now(timezone.utc).isoformat(),
            ip_address=self.faker.ipv4()
        )

# 3. Enterprise Standard: Object-Oriented Client
class KafkaTransactionProducer:
    def __init__(self, broker: str, topic: str):
        self.topic = topic
        self.producer = Producer({'bootstrap.servers': broker})
        logger.info(f"Initialized Kafka Producer connected to {broker}")

    def delivery_report(self, err, msg):
        """Callback triggered upon message delivery success or failure."""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            # We use debug here so we don't spam our own console during high throughput
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def produce_stream(self, simulator: TransactionSimulator, events_per_second: int):
        """Continuously produces events to the Kafka topic."""
        logger.info(f"Starting transaction stream to topic: {self.topic}")
        sleep_time = 1.0 / events_per_second

        try:
            while True:
                # Generate valid data
                transaction = simulator.generate_transaction()
                
                # Produce to Kafka (Redpanda)
                self.producer.produce(
                    topic=self.topic,
                    key=transaction.user_id.encode('utf-8'), # Keying by user_id ensures order per user
                    value=transaction.model_dump_json().encode('utf-8'),
                    on_delivery=self.delivery_report
                )
                
                # Poll handles delivery reports (callbacks)
                self.producer.poll(0)
                time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            logger.info("Graceful shutdown initiated by user...")
        finally:
            # 4. Enterprise Standard: Graceful Shutdown
            logger.info("Flushing pending messages to broker...")
            self.producer.flush()
            logger.info("Shutdown complete.")

if __name__ == "__main__":
    # Fetch configurations securely
    BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
    TOPIC = os.getenv("KAFKA_TOPIC", "live_transactions")
    RATE = int(os.getenv("EVENTS_PER_SECOND", 2))

    simulator = TransactionSimulator()
    producer = KafkaTransactionProducer(broker=BROKER, topic=TOPIC)
    
    producer.produce_stream(simulator=simulator, events_per_second=RATE)