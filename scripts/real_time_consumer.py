import json
import logging
import signal
import sys
from typing import Dict, Any
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from config import (
    KAFKA_BOOTSTRAP_SERVERS, REALTIME_TOPIC,
    REALTIME_CONSUMER_GROUP, FREQUENCY_MIN, FREQUENCY_MAX
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

running = True

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    global running
    logger.info("Shutdown signal received")
    running = False

def create_consumer() -> KafkaConsumer:
    """Create Kafka consumer with retry logic"""
    logger.info("🔌 Connecting to real-time topic...")
    max_attempts = 10
    
    for attempt in range(1, max_attempts + 1):
        try:
            consumer = KafkaConsumer(
                REALTIME_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset='latest',
                group_id=REALTIME_CONSUMER_GROUP,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                session_timeout_ms=30000,
                max_poll_interval_ms=300000
            )
            logger.info(f"✅ Monitoring topic '{REALTIME_TOPIC}'...")
            return consumer
        except KafkaError as e:
            logger.warning(f"Attempt {attempt}/{max_attempts} failed: {e}")
            if attempt < max_attempts:
                import time
                time.sleep(2 ** attempt)
            else:
                logger.error("❌ Failed to connect consumer")
                raise

def check_frequency(data: Dict[str, Any]) -> None:
    """Check frequency and log alerts"""
    freq = data.get('grid_frequency', 50.0)
    device_id = data.get('device_id', 'UNKNOWN')
    
    status = "OK" if FREQUENCY_MIN < freq < FREQUENCY_MAX else "🚨 ALERT! OUT OF BOUNDS! 🚨"
    logger.info(f"REAL-TIME: {device_id}, Freq: {freq} Hz → {status}")

def main():
    global running
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        consumer = create_consumer()
    except Exception as e:
        logger.error(f"Cannot start consumer: {e}")
        sys.exit(1)
    
    message_count = 0
    alert_count = 0
    
    try:
        while running:
            msg_batch = consumer.poll(timeout_ms=1000)
            for topic_partition, messages in msg_batch.items():
                for message in messages:
                    try:
                        data = message.value
                        check_frequency(data)
                        message_count += 1
                        
                        freq = data.get('grid_frequency', 50.0)
                        if not (FREQUENCY_MIN < freq < FREQUENCY_MAX):
                            alert_count += 1
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info(f"🛑 Stopped. Processed: {message_count}, Alerts: {alert_count}")
        consumer.close()
        logger.info("🔌 Consumer closed")

if __name__ == "__main__":
    main()