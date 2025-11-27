import json
import logging
import signal
import sys
import time
from typing import List, Dict, Any
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from config import (
    KAFKA_BOOTSTRAP_SERVERS, ANALYTICS_TOPIC,
    ANALYTICS_CONSUMER_GROUP, ANALYTICS_BATCH_SIZE
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
    logger.info("🔌 Підключення до аналітичного топіка...")
    max_attempts = 10
    
    for attempt in range(1, max_attempts + 1):
        try:
            consumer = KafkaConsumer(
                ANALYTICS_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset='latest',
                group_id=ANALYTICS_CONSUMER_GROUP,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                session_timeout_ms=30000,
                max_poll_interval_ms=300000
            )
            logger.info(f"✅ Очікування аналітичних даних з '{ANALYTICS_TOPIC}'...")
            return consumer
        except KafkaError as e:
            logger.warning(f"Спроба {attempt}/{max_attempts} не вдалася: {e}")
            if attempt < max_attempts:
                time.sleep(2 ** attempt)
            else:
                logger.error("❌ Не вдалося підключити споживача")
                raise

def process_batch(soc_values: List[float]) -> None:
    """Process and analyze batch of SoC values"""
    if not soc_values:
        return
    
    avg_soc = sum(soc_values) / len(soc_values)
    min_soc = min(soc_values)
    max_soc = max(soc_values)
    
    logger.info(
        f"\n📊 ANALYTICS BATCH:\n"
        f"   Зразків: {len(soc_values)}\n"
        f"   Середній SoC: {avg_soc:.2f}%\n"
        f"   Мін SoC: {min_soc:.2f}%\n"
        f"   Макс SoC: {max_soc:.2f}%\n"
    )

def main():
    global running
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        consumer = create_consumer()
    except Exception as e:
        logger.error(f"Неможливо запустити споживача: {e}")
        sys.exit(1)

    soc_values: List[float] = []
    message_count = 0
    batch_count = 0
    
    try:
        while running:
            msg_batch = consumer.poll(timeout_ms=1000)
            for topic_partition, messages in msg_batch.items():
                for message in messages:
                    try:
                        data = message.value
                        soc = data.get('soc', 0)
                        soc_values.append(soc)
                        message_count += 1
                        
                        logger.info(
                            f"RECEIVED FOR ANALYTICS: {data.get('device_id', 'N/A')}, "
                            f"SoC: {soc}%, Mode: {data.get('mode', 'N/A')}"
                        )

                        if len(soc_values) >= ANALYTICS_BATCH_SIZE:
                            process_batch(soc_values)
                            batch_count += 1
                            soc_values = []
                            
                    except Exception as e:
                        logger.error(f"Помилка обробки повідомлення: {e}")
    
    except Exception as e:
        logger.error(f"Неочікувана помилка: {e}")
    finally:
        # Process remaining values
        if soc_values:
            logger.info("Обробка залишкових значень...")
            process_batch(soc_values)
            batch_count += 1
        
        logger.info(
            f"🛑 Зупинено. Повідомлень: {message_count}, "
            f"Пакетів: {batch_count}"
        )
        consumer.close()
        logger.info("🔌 Споживач закрито")

if __name__ == "__main__":
    main()