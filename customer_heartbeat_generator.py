"""
This script simulates real-time heart rate data
for multiple customers and publishes it to Kafka.

- This simulates IoT sensor data.
- Kafka acts as the streaming pipeline.
- Each message is a JSON event.
"""

import json
import logging
import random
import signal
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


# Kafka Configuration
# ============================================================

KAFKA_BROKER = "127.0.0.1:9092"
KAFKA_TOPIC = "customer-heartbeat"
NUM_CUSTOMERS = 10
MIN_DELAY = 0.5
MAX_DELAY = 2.0

# Logging Configuration
# ============================================================

# Configure logging format for better observability
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | Producer | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

# Heart Rate Simulation Logic
# ============================================================

def simulate_heart_rate():
    """
    Simulates realistic heart rate values.

    Normal heart rate:
        Around 60–100 bpm

    We simulate:
    - Normal distribution centered around 75 bpm
    - Small probability of anomaly (very low or very high)

    This mimics real sensor behavior.
    """
    base = random.gauss(75, 10)    # Generate normal heart rate using Gaussian distribution
    anomaly = random.random()      # Generate anomaly probability

    if anomaly < 0.02:                          # 2% chance of extremely low heart rate (critical condition)
        return random.uniform(20, 30)

    if anomaly > 0.98:                          # 2% chance of extremely high heart rate (emergency)   
        return random.uniform(180, 220)

    return max(50, min(140, base))              # Normal safe range constraint


# Kafka Producer Creation
# ============================================================

def create_producer():
    """
    Creates Kafka producer instance.

    Important configuration:
    - bootstrap_servers: Kafka broker address
    - value_serializer: convert Python dict → JSON → bytes
    - acks="all": ensures message durability (wait for all replicas)

    """

    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",  # Strong durability guarantee
    )

# Main Streaming Loop
# ============================================================

def main():
    """
    Main producer loop.

    This continuously:
    1. Generates heartbeat event
    2. Sends event to Kafka topic
    3. Logs the event
    4. Waits small random delay
    """

    logger.info("Starting Heartbeat Producer")

    producer = create_producer()

    running = True

    # Graceful shutdown handler (Ctrl + C)
    def shutdown(sig, frame):
        """
        Stops infinite loop safely.
        Prevents abrupt termination.
        """
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, shutdown)

    # Continuous streaming loop
    while running:

        # Create event payload
        event = {
            "customer_id": f"CUST_{random.randint(1, NUM_CUSTOMERS)}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "heart_rate": round(simulate_heart_rate(), 2),
        }

        # Send event to Kafka topic
        producer.send(KAFKA_TOPIC, value=event)

        # Force immediate delivery
        producer.flush()

        logger.info(f"Sent: {event}")

        # Simulate real-world streaming delay
        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

    # Clean shutdown
    producer.close()
    logger.info("Producer stopped.")


if __name__ == "__main__":
    main()