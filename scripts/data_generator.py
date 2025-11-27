import json
import time
import random
import logging
import signal
import sys
from datetime import datetime
from typing import Tuple, Dict, Any
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_RETRIES, KAFKA_ACKS,
    REALTIME_TOPIC, ANALYTICS_TOPIC, NUM_DEVICES,
    REALTIME_PROBABILITY, MESSAGE_INTERVAL_SECONDS
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global producer for graceful shutdown
producer = None
running = True

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global running
    logger.info("Shutdown signal received. Cleaning up...")
    running = False

def create_producer() -> KafkaProducer:
    """Create Kafka producer with retry logic"""
    logger.info("🔌 Connecting to Kafka as Producer...")
    max_attempts = 10
    
    for attempt in range(1, max_attempts + 1):
        try:
            prod = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks=KAFKA_ACKS,
                retries=KAFKA_RETRIES,
                max_in_flight_requests_per_connection=5,
                compression_type='gzip'
            )
            logger.info("✅ Producer ready!")
            return prod
        except KafkaError as e:
            logger.warning(f"Attempt {attempt}/{max_attempts} failed: {e}")
            if attempt < max_attempts:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                logger.error("❌ Failed to connect Producer after all attempts")
                raise

def generate_bess_data(device_id: int) -> Tuple[Dict[str, Any], bool]:
    """Generate data for a Battery Energy Storage System (BESS)"""
    is_realtime_mode = random.random() < REALTIME_PROBABILITY
    
    if is_realtime_mode:
        mode = "frequency_regulation"
        grid_frequency = round(random.uniform(49.95, 50.05), 3)
        power_output = round(random.uniform(-15000.0, 15000.0), 2)
    else:
        mode = random.choice(["peak_shaving", "arbitrage", "backup"])
        grid_frequency = 50.0
        power_output = random.choice([-50000.0, 50000.0])

    data = {
        "device_id": f"BESS_{device_id:03d}",
        "timestamp": datetime.now().isoformat(),
        "power_output": power_output,
        "efficiency": round(random.uniform(88.0, 94.5), 2),
        "temperature": round(random.uniform(20.0, 35.0), 2),
        "voltage": round(random.uniform(850.0, 1450.0), 2),
        "current": round(abs(power_output / 1000), 2),
        "status": random.choice(["charging", "discharging", "idle"]),
        "location": {
            "lat": round(random.uniform(46.0, 52.0), 4),
            "lon": round(random.uniform(22.0, 40.0), 4)
        },
        "maintenance_hours": random.randint(500, 15000),
        "soc": round(random.uniform(15.0, 85.0), 2),
        "grid_frequency": grid_frequency,
        "mode": mode
    }
    return data, is_realtime_mode

def send_message(producer: KafkaProducer, topic: str, data: Dict[str, Any]) -> bool:
    """Send message with error handling"""
    try:
        future = producer.send(topic, value=data)
        future.get(timeout=10)  # Wait for acknowledgment
        return True
    except KafkaError as e:
        logger.error(f"Failed to send message to {topic}: {e}")
        return False

def main():
    global producer, running
    
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        producer = create_producer()
    except Exception as e:
        logger.error(f"Cannot start producer: {e}")
        sys.exit(1)

    logger.info("🚀 Starting hybrid BESS data stream...")
    msg_count = 0
    failed_count = 0
    
    try:
        while running:
            device_num = random.randint(1, NUM_DEVICES)
            bess_data, is_realtime = generate_bess_data(device_num)
            
            topic = REALTIME_TOPIC if is_realtime else ANALYTICS_TOPIC
            log_msg = (f"→ {bess_data['device_id']}: "
                      f"Freq={bess_data['grid_frequency']} Hz" if is_realtime
                      else f"→ {bess_data['device_id']}: SoC={bess_data['soc']}%")
            
            if send_message(producer, topic, bess_data):
                logger.info(f"{'REAL-TIME' if is_realtime else 'ANALYTICS'} {log_msg}")
                msg_count += 1
            else:
                failed_count += 1
            
            time.sleep(MESSAGE_INTERVAL_SECONDS)
            
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info(f"🛑 Stopped. Sent: {msg_count}, Failed: {failed_count}")
        if producer:
            producer.flush()
            producer.close()
            logger.info("🔌 Kafka connection closed")

if __name__ == "__main__":
    main()