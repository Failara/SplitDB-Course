import json
import threading
import time
import uuid
from datetime import datetime
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement

KAFKA_BROKERS = ['kafka:29092']
CASSANDRA_HOSTS = ['cassandra']
KEYSPACE = 'bess_market'

CYCLES_TOPIC = 'bess.cycles.calculated'
TX_LOG_TOPIC = 'bess.transaction.log'

def connect_to_cassandra():
    try:
        cluster = Cluster(CASSANDRA_HOSTS)
        session = cluster.connect()
        print("Cassandra connected successfully.")
        return cluster, session
    except Exception as e:
        print(f"Failed to connect to Cassandra: {e}. Retrying...")
        time.sleep(5)
        return connect_to_cassandra()

def connect_to_kafka(topic, group_id):
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BROKERS,
            group_id=group_id,
            auto_offset_reset='earliest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        print(f"Kafka consumer for topic '{topic}' connected.")
        return consumer
    except Exception as e:
        print(f"Failed to connect Kafka consumer for '{topic}': {e}. Retrying...")
        time.sleep(5)
        return connect_to_kafka(topic, group_id)

def create_schema(session):
    try:
        print("Creating keyspace 'bess_market' (if not exists)...")
        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
        """)
        
        session.set_keyspace(KEYSPACE)

        print("Creating table 'cycle_sessions' (if not exists)...")
        session.execute("""
            CREATE TABLE IF NOT EXISTS cycle_sessions (
                bess_id text,
                cycle_end_time timestamp,
                cycle_start_time timestamp,
                total_charged double,
                total_discharged double,
                roundtrip_efficiency float,
                tx_id uuid,
                PRIMARY KEY (bess_id, cycle_end_time)
            ) WITH CLUSTERING ORDER BY (cycle_end_time DESC);
        """)

        print("Creating table 'market_settlement' (if not exists)...")
        session.execute("""
            CREATE TABLE IF NOT EXISTS market_settlement (
                tx_id uuid PRIMARY KEY,
                bess_id text,
                cycle_end_time timestamp,
                service_provided text,
                committed_energy double,
                delivered_energy double,
                payout_amount double
            );
        """)

        print("Creating table 'transaction_log' (if not exists)...")
        session.execute("""
            CREATE TABLE IF NOT EXISTS transaction_log (
                tx_id uuid PRIMARY KEY,
                coordinator_node text,
                status text,
                details text,
                event_time timestamp
            );
        """)
        print("Schema created successfully.")
    except Exception as e:
        print(f"CRITICAL ERROR: Failed to create schema: {e}")
        raise

def writer_tx_log(consumer, session):
    print(f"Starting Transaction Log Writer...")
    try:
        query = session.prepare("""
            INSERT INTO transaction_log (tx_id, coordinator_node, status, details, event_time)
            VALUES (?, ?, ?, ?, ?)
        """)
        
        for message in consumer:
            log = message.value
            try:
                session.execute(query, (
                    uuid.UUID(log['tx_id']),
                    log['coordinator_node'],
                    log['status'],
                    log['details'],
                    datetime.fromisoformat(log['event_time'])
                ))
                print(f"[LogWriter]: Saved TX {log['tx_id']} with status {log['status']}")
            except Exception as e:
                print(f"[LogWriter] Error writing log: {e}")
    except Exception as e:
        print(f"[LogWriter] Thread failed to prepare query: {e}")


def writer_settlements(consumer, session):
    print(f"Starting Settlements Writer...")
    try:
        query_p1 = session.prepare("""
            INSERT INTO cycle_sessions (bess_id, cycle_end_time, cycle_start_time, total_charged, total_discharged, roundtrip_efficiency, tx_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """)
        
        query_p2 = session.prepare("""
            INSERT INTO market_settlement (tx_id, bess_id, cycle_end_time, service_provided, committed_energy, delivered_energy, payout_amount)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """)
    
        for message in consumer:
            settlement = message.value
            tx_id = uuid.UUID(settlement['tx_id'])
            print(f"[SettlementWriter]: Processing COMMIT for TX {tx_id}...")
            
            try:
                batch = BatchStatement()
                
                batch.add(query_p1, (
                    settlement['bess_id'],
                    datetime.fromisoformat(settlement['end_time']),
                    datetime.fromisoformat(settlement['start_time']),
                    settlement['total_charged_wh'],
                    settlement['total_discharged_wh'],
                    settlement['roundtrip_efficiency'],
                    tx_id
                ))
                
                batch.add(query_p2, (
                    tx_id,
                    settlement['bess_id'],
                    datetime.fromisoformat(settlement['end_time']),
                    "simulated_service",
                    settlement['committed_energy'],
                    settlement['delivered_energy'],
                    settlement['payout_amount']
                ))
                
                session.execute(batch)
                print(f"[SettlementWriter]: SUCCESSFULLY committed TX {tx_id} to Cassandra.")
                
            except KeyError as e:
                print(f"[SettlementWriter] CRITICAL KEY ERROR: Failed to write BATCH for TX {tx_id}. Missing key: {e}")
            except Exception as e:
                print(f"[SettlementWriter] CRITICAL ERROR: Failed to write BATCH for TX {tx_id}: {e}")
    except Exception as e:
        print(f"[SettlementWriter] Thread failed to prepare query: {e}")


if __name__ == "__main__":
    print("Starting Cassandra Writers... waiting 20s for services.")
    time.sleep(20)
    
    cluster, session = connect_to_cassandra()
    
    try:
        create_schema(session)
    except Exception as e:
        print("Could not create schema. Exiting.")
        exit(1)
    
    log_consumer = connect_to_kafka(TX_LOG_TOPIC, "cassandra-log-writer-group")
    log_thread = threading.Thread(target=writer_tx_log, args=(log_consumer, session), daemon=True)
    
    settlement_consumer = connect_to_kafka(CYCLES_TOPIC, "cassandra-settlement-writer-group")
    settlement_thread = threading.Thread(target=writer_settlements, args=(settlement_consumer, session), daemon=True)

    log_thread.start()
    settlement_thread.start()
    
    print("Writers are running.")
    log_thread.join()
    settlement_thread.join()
    
    cluster.shutdown()