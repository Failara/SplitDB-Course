import json
import random
import time
import threading
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

KAFKA_BROKERS = ['kafka:29092']
KAFKA_TOPIC = 'bess.raw.data'
NUM_BESS = 25
SEND_INTERVAL_SEC = 1

def create_kafka_topics():
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKERS,
            client_id='topic_creator'
        )
        print("Kafka AdminClient connected successfully.")
    except Exception as e:
        print(f"Failed to connect Kafka AdminClient: {e}. Retrying...")
        time.sleep(5)
        create_kafka_topics()
        return

    topic_list = [
        NewTopic(name="bess.raw.data", num_partitions=2, replication_factor=1),
        NewTopic(name="bess.cycles.calculated", num_partitions=2, replication_factor=1),
        NewTopic(name="bess.transaction.log", num_partitions=1, replication_factor=1)
    ]

    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print("Topics created successfully (or were skipped).")
    except TopicAlreadyExistsError:
        print("Topics already exist, skipping creation.")
    except Exception as e:
        print(f"An error occurred while creating topics: {e}")
    finally:
        admin_client.close()

def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Kafka Producer connected successfully.")
        return producer
    except Exception as e:
        print(f"Failed to connect Kafka Producer: {e}")
        time.sleep(5)
        return create_producer()

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

def simulate_cycle(bess_id):
    print(f"[{bess_id}] Starting CHARGE cycle...")
    for _ in range(random.randint(10, 20)):
        reading = generate_bess_reading(bess_id, "charge")
        producer.send(KAFKA_TOPIC, reading, key=bess_id.encode('utf-8'))
        time.sleep(SEND_INTERVAL_SEC)

    gap_time = random.uniform(31, 35)
    print(f"[{bess_id}] Entering GAP for {gap_time:.1f}s to trigger SessionWindow...")
    time.sleep(gap_time)
    
    print(f"[{bess_id}] Starting DISCHARGE cycle...")
    for _ in range(random.randint(10, 20)):
        reading = generate_bess_reading(bess_id, "discharge")
        producer.send(KAFKA_TOPIC, reading, key=bess_id.encode('utf-8'))
        time.sleep(SEND_INTERVAL_SEC)
    
    gap_time = random.uniform(31, 35)
    print(f"[{bess_id}] Entering GAP for {gap_time:.1f}s...")
    time.sleep(gap_time)

def run_simulator_for_bess(bess_id):
    while True:
        try:
            simulate_cycle(bess_id)
        except Exception as e:
            print(f"Error in simulator thread {bess_id}: {e}")
            time.sleep(5)

if __name__ == "__main__":
    print("Simulator starting... waiting 10s for Kafka cluster.")
    time.sleep(10)
    
    create_kafka_topics()

    producer = create_producer()
    
    threads = []
    for i in range(1, NUM_BESS + 1):
        bess_id = f'BESS_{i:03d}'
        thread = threading.Thread(target=run_simulator_for_bess, args=(bess_id,), daemon=True)
        threads.append(thread)
        thread.start()
        time.sleep(random.uniform(0.5, 2.0))

    print(f"Launched {NUM_BESS} BESS simulator threads.")
    for t in threads:
        t.join()