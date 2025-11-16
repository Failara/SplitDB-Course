import json
import threading
import time
import uuid
from datetime import datetime
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement
import logging

KAFKA_BROKERS = ['kafka:29092']
CASSANDRA_HOSTS = ['cassandra']
KEYSPACE = 'bess_market'

CYCLES_TOPIC = 'bess.cycles.calculated'
TX_LOG_TOPIC = 'bess.transaction.log'

# configure structured logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("cassandra_writer")

def connect_to_cassandra(retries=5, backoff_secs=5):
    attempt = 0
    while True:
        try:
            cluster = Cluster(CASSANDRA_HOSTS)
            session = cluster.connect()
            logger.info("Cassandra connected successfully.")
            return cluster, session
        except Exception as e:
            attempt += 1
            logger.warning("Failed to connect to Cassandra: %s. Attempt %d", e, attempt)
            if retries and attempt >= retries:
                logger.error("Exceeded Cassandra connection retries.")
                raise
            time.sleep(backoff_secs)

def connect_to_kafka(topic, group_id, retries=5, backoff_secs=5):
    attempt = 0
    while True:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BROKERS,
                group_id=group_id,
                auto_offset_reset='earliest',
                value_deserializer=lambda v: json.loads(v.decode('utf-8')) if v is not None else None,
                enable_auto_commit=True
            )
            logger.info("Kafka consumer for topic '%s' connected.", topic)
            return consumer
        except Exception as e:
            attempt += 1
            logger.warning("Failed to connect Kafka consumer for '%s': %s. Attempt %d", topic, e, attempt)
            if retries and attempt >= retries:
                logger.error("Exceeded Kafka consumer connection retries for topic %s.", topic)
                raise
            time.sleep(backoff_secs)

def create_schema(session):
    try:
        logger.info("Creating keyspace 'bess_market' (if not exists)...")
        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
        """)
        
        session.set_keyspace(KEYSPACE)

        logger.info("Creating table 'cycle_sessions' (if not exists)...")
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

        logger.info("Creating table 'market_settlement' (if not exists)...")
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

        logger.info("Creating table 'transaction_log' (if not exists)...")
        session.execute("""
            CREATE TABLE IF NOT EXISTS transaction_log (
                tx_id uuid PRIMARY KEY,
                coordinator_node text,
                status text,
                details text,
                event_time timestamp
            );
        """)
        logger.info("Schema created successfully.")
    except Exception as e:
        logger.exception("CRITICAL ERROR: Failed to create schema: %s", e)
        raise

def writer_tx_log(consumer, session):
    logger.info("Starting Transaction Log Writer...")
    try:
        query = session.prepare("""
            INSERT INTO transaction_log (tx_id, coordinator_node, status, details, event_time)
            VALUES (?, ?, ?, ?, ?)
        """)
        
        for message in consumer:
            log = message.value
            if not log:
                continue
            try:
                tx_uuid = uuid.UUID(log['tx_id']) if isinstance(log.get('tx_id'), str) else log.get('tx_id')
                event_time = datetime.fromisoformat(log['event_time']) if isinstance(log.get('event_time'), str) else log.get('event_time')
                session.execute(query, (
                    tx_uuid,
                    log.get('coordinator_node'),
                    log.get('status'),
                    log.get('details'),
                    event_time
                ))
                logger.info("[LogWriter]: Saved TX %s with status %s", log.get('tx_id'), log.get('status'))
            except Exception as e:
                logger.exception("[LogWriter] Error writing log: %s", e)
    except Exception as e:
        logger.exception("[LogWriter] Thread failed to prepare query: %s", e)


def writer_settlements(consumer, session):
    logger.info("Starting Settlements Writer...")
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
            if not settlement:
                continue
            try:
                tx_id = uuid.UUID(settlement['tx_id']) if isinstance(settlement.get('tx_id'), str) else settlement.get('tx_id')
                logger.info("[SettlementWriter]: Processing COMMIT for TX %s...", tx_id)
                
                batch = BatchStatement()
                
                batch.add(query_p1, (
                    settlement.get('bess_id'),
                    datetime.fromisoformat(settlement['end_time']),
                    datetime.fromisoformat(settlement['start_time']),
                    settlement.get('total_charged_wh'),
                    settlement.get('total_discharged_wh'),
                    settlement.get('roundtrip_efficiency'),
                    tx_id
                ))
                
                batch.add(query_p2, (
                    tx_id,
                    settlement.get('bess_id'),
                    datetime.fromisoformat(settlement['end_time']),
                    "simulated_service",
                    settlement.get('committed_energy'),
                    settlement.get('delivered_energy'),
                    settlement.get('payout_amount')
                ))
                
                session.execute(batch)
                logger.info("[SettlementWriter]: SUCCESSFULLY committed TX %s to Cassandra.", tx_id)
                
            except KeyError as e:
                logger.exception("[SettlementWriter] CRITICAL KEY ERROR: Missing key: %s", e)
            except Exception as e:
                logger.exception("[SettlementWriter] CRITICAL ERROR: Failed to write BATCH for TX: %s", e)
    except Exception as e:
        logger.exception("[SettlementWriter] Thread failed to prepare query: %s", e)


if __name__ == "__main__":
    logger.info("Starting Cassandra Writers... waiting 20s for services.")
    time.sleep(20)
    
    cluster = session = None
    log_consumer = settlement_consumer = None
    try:
        cluster, session = connect_to_cassandra()
        create_schema(session)
    
        log_consumer = connect_to_kafka(TX_LOG_TOPIC, "cassandra-log-writer-group")
        log_thread = threading.Thread(target=writer_tx_log, args=(log_consumer, session), daemon=True)
        
        settlement_consumer = connect_to_kafka(CYCLES_TOPIC, "cassandra-settlement-writer-group")
        settlement_thread = threading.Thread(target=writer_settlements, args=(settlement_consumer, session), daemon=True)

        log_thread.start()
        settlement_thread.start()
        
        logger.info("Writers are running.")
        # Wait until interrupted
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutdown requested (KeyboardInterrupt).")
    except Exception as e:
        logger.exception("Fatal error in main: %s", e)
    finally:
        try:
            if log_consumer:
                log_consumer.close()
            if settlement_consumer:
                settlement_consumer.close()
            if cluster:
                cluster.shutdown()
            logger.info("Clean shutdown complete.")
        except Exception:
            logger.exception("Error during shutdown.")