"""Configuration module for Kafka BESS application"""

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = ['kafka:9092']
KAFKA_RETRIES = 5
KAFKA_ACKS = 'all'

# Topics
REALTIME_TOPIC = "bess-realtime-critical"
ANALYTICS_TOPIC = "bess-analytics-soc"

# Consumer Groups
REALTIME_CONSUMER_GROUP = "realtime-frequency-monitor-group"
ANALYTICS_CONSUMER_GROUP = "analytics-soc-group"

# Data Generation
NUM_DEVICES = 8
REALTIME_PROBABILITY = 0.3  # 30% real-time, 70% analytics
MESSAGE_INTERVAL_SECONDS = 5
ANALYTICS_BATCH_SIZE = 5

# Thresholds
FREQUENCY_MIN = 49.9
FREQUENCY_MAX = 50.1
