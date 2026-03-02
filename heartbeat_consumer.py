"""
This script consumes heart rate events from a Kafka topic,
performs basic anomaly detection, and inserts the processed
records into a PostgreSQL database.

"""

import json
import logging
import signal
import psycopg2
from kafka import KafkaConsumer

# Kafka Configuration
# ============================================================

# Kafka broker address (Docker exposed port)
KAFKA_BROKER = "127.0.0.1:9092"

# Topic where producer sends heartbeat data
KAFKA_TOPIC = "customer-heartbeat"

# Consumer group enables scalability & fault tolerance
CONSUMER_GROUP_ID = "heartbeat-consumer-group"


# PostgreSQL Configuration
# ============================================================

# Database runs inside Docker but is exposed to localhost
DB_HOST = "127.0.0.1"
DB_NAME = "heartbeat_db"
DB_USER = "heartbeat_user"
DB_PASSWORD = "heartbeat_pass"
DB_PORT = "5432"


# Logging Configuration
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | Consumer | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# Database Connection
# ============================================================

def get_connection():
    """
    Establish connection to PostgreSQL database.

    Returns:
        psycopg2 connection object
    """
    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
    )


# Anomaly Detection Logic
# ============================================================

def is_anomaly(heart_rate):
    """
    Detect abnormal heart rate values.

        - Heart rate < 30 BPM  -> Too low (critical)
        - Heart rate > 180 BPM -> Too high (critical)

    Returns:
        Boolean indicating anomaly status
    """
    return heart_rate < 30 or heart_rate > 180


# Main Consumer Logic
# ============================================================

def main():
    """
    Main execution function:
    1. Connect to Kafka
    2. Consume streaming messages
    3. Validate & detect anomalies
    4. Insert into PostgreSQL
    """

    logger.info("Starting Heartbeat Consumer...")

    # Create Kafka consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",  # Read from beginning if no offset
        group_id=CONSUMER_GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    # Connect to PostgreSQL
    conn = get_connection()
    cursor = conn.cursor()

    running = True

    # Graceful shutdown handler (CTRL + C)
    def shutdown(sig, frame):
        nonlocal running
        logger.info("Shutdown signal received. Stopping consumer...")
        running = False

    signal.signal(signal.SIGINT, shutdown)

    # Continuous streaming loop
    for message in consumer:

        # Deserialize Kafka message
        data = message.value

        # Detect anomaly
        anomaly_flag = is_anomaly(data["heart_rate"])

        # Idempotent insert using UNIQUE constraint
        query = """
        INSERT INTO customer_heartbeats
        (customer_id, event_timestamp, heart_rate, is_anomaly)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (customer_id, event_timestamp)
        DO NOTHING;
        """

        cursor.execute(
            query,
            (
                data["customer_id"],
                data["timestamp"],
                data["heart_rate"],
                anomaly_flag,
            ),
        )

        # Commit transaction
        conn.commit()

        # Logging for observability
        if anomaly_flag:
            logger.warning(f"Anomaly detected: {data}")
        else:
            logger.info(f"Inserted record: {data}")

        if not running:
            break

    # Cleanup resources
    cursor.close()
    conn.close()
    consumer.close()
    logger.info("Consumer stopped gracefully.")


# Entry Point
# ============================================================

if __name__ == "__main__":
    main()