import json
import random
import time
import threading
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import logging
import signal
import sys

KAFKA_BROKERS = ['kafka:29092']
KAFKA_TOPIC = 'bess.raw.data'
NUM_BESS = 25
SEND_INTERVAL_SEC = 1

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("simulator")

shutdown_event = threading.Event()

def signal_handler(sig, frame):
    logger.info("Shutdown signal received. Stopping simulator...")
    shutdown_event.set()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def create_kafka_topics(retries=5):
    for attempt in range(1, retries + 1):
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=KAFKA_BROKERS,
                client_id='topic_creator'
            )
            logger.info("Kafka AdminClient connected successfully.")
            break
        except Exception as e:
            logger.warning("Failed to connect Kafka AdminClient (attempt %d/%d): %s", attempt, retries, e)
            if attempt >= retries:
                logger.error("Exceeded AdminClient connection retries.")
                raise
            time.sleep(5)

    topic_list = [
        NewTopic(name="bess.raw.data", num_partitions=2, replication_factor=1),
        NewTopic(name="bess.cycles.calculated", num_partitions=2, replication_factor=1),
        NewTopic(name="bess.transaction.log", num_partitions=1, replication_factor=1)
    ]

    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        logger.info("Topics created successfully.")
    except TopicAlreadyExistsError:
        logger.info("Topics already exist, skipping creation.")
    except Exception as e:
        logger.exception("Error creating topics: %s", e)
    finally:
        admin_client.close()

def create_producer(retries=5):
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            logger.info("Kafka Producer connected successfully.")
            return producer
        except Exception as e:
            logger.warning("Failed to connect Kafka Producer (attempt %d/%d): %s", attempt, retries, e)
            if attempt >= retries:
                logger.error("Exceeded Producer connection retries.")
                raise
            time.sleep(5)

def generate_bess_reading(bess_id, state):
    power = random.uniform(45.0, 50.0) if state == "charge" else random.uniform(-50.0, -45.0)
    ts = datetime.now(timezone.utc).isoformat()
    return {
        'bess_id': bess_id,
        'timestamp': ts,
        'soc': random.uniform(20.0, 80.0),
        'power_charge_discharge': power,
        'voltage': random.uniform(400.0, 405.0),
        'current': power / 400.0,
        'temperature': random.uniform(25.0, 30.0),
        'available_capacity': random.uniform(100.0, 101.0),
        'cycle_count': random.randint(500, 550),
        'grid_frequency': random.uniform(49.9, 50.1)
    }

def simulate_cycle(bess_id, producer):
    logger.info("[%s] Starting CHARGE cycle...", bess_id)
    for _ in range(random.randint(10, 20)):
        if shutdown_event.is_set():
            return
        reading = generate_bess_reading(bess_id, "charge")
        try:
            producer.send(KAFKA_TOPIC, reading, key=bess_id.encode('utf-8'))
        except Exception as e:
            logger.exception("[%s] Error sending message: %s", bess_id, e)
        time.sleep(SEND_INTERVAL_SEC)

    gap_time = random.uniform(31, 35)
    logger.info("[%s] Entering GAP for %.1fs to trigger SessionWindow...", bess_id, gap_time)
    shutdown_event.wait(gap_time)
    if shutdown_event.is_set():
        return
    
    logger.info("[%s] Starting DISCHARGE cycle...", bess_id)
    for _ in range(random.randint(10, 20)):
        if shutdown_event.is_set():
            return
        reading = generate_bess_reading(bess_id, "discharge")
        try:
            producer.send(KAFKA_TOPIC, reading, key=bess_id.encode('utf-8'))
        except Exception as e:
            logger.exception("[%s] Error sending message: %s", bess_id, e)
        time.sleep(SEND_INTERVAL_SEC)
    
    gap_time = random.uniform(31, 35)
    logger.info("[%s] Entering GAP for %.1fs...", bess_id, gap_time)
    shutdown_event.wait(gap_time)

def run_simulator_for_bess(bess_id, producer):
    while not shutdown_event.is_set():
        try:
            simulate_cycle(bess_id, producer)
        except Exception as e:
            logger.exception("Error in simulator thread %s: %s", bess_id, e)
            shutdown_event.wait(5)

if __name__ == "__main__":
    logger.info("Simulator starting... waiting 10s for Kafka cluster.")
    time.sleep(10)
    
    try:
        create_kafka_topics()
        producer = create_producer()
        
        threads = []
        for i in range(1, NUM_BESS + 1):
            bess_id = f'BESS_{i:03d}'
            thread = threading.Thread(target=run_simulator_for_bess, args=(bess_id, producer), daemon=True)
            threads.append(thread)
            thread.start()
            time.sleep(random.uniform(0.5, 2.0))

        logger.info("Launched %d BESS simulator threads.", NUM_BESS)
        
        # Wait for shutdown signal
        while not shutdown_event.is_set():
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received.")
        shutdown_event.set()
    except Exception as e:
        logger.exception("Fatal error in simulator: %s", e)
    finally:
        try:
            if 'producer' in locals():
                producer.close()
            logger.info("Simulator shutdown complete.")
        except Exception:
            logger.exception("Error during shutdown.")