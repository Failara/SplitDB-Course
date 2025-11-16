from bytewax.inputs import StatefulSourcePartition, FixedPartitionedSource
from bytewax.outputs import StatefulSinkPartition, FixedPartitionedSink
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import json
import logging

logger = logging.getLogger("kafka_connectors")

class KafkaSourcePartition(StatefulSourcePartition):
    def __init__(self, consumer, topic, partition):
        self.consumer = consumer
        self.topic = topic
        self.partition = partition
        tp = TopicPartition(topic, partition)
        self.consumer.assign([tp])

    def next_batch(self):
        try:
            messages = self.consumer.poll(timeout_ms=1000, max_records=100)
            batch = []
            for tp, msgs in messages.items():
                for msg in msgs:
                    batch.append((msg.key, msg.value))
            return batch
        except Exception as e:
            logger.exception("Error polling Kafka: %s", e)
            return []

    def snapshot(self):
        return None

    def close(self):
        try:
            self.consumer.close()
        except Exception:
            logger.exception("Error closing consumer")

class KafkaSource(FixedPartitionedSource):
    def __init__(self, brokers, topic, group_id='bytewax-group', offset='latest'):
        self.brokers = brokers
        self.topic = topic
        self.group_id = group_id
        self.offset = offset
        self._partitions = None

    def list_parts(self):
        if self._partitions is None:
            temp_consumer = KafkaConsumer(
                bootstrap_servers=self.brokers,
                group_id=f"{self.group_id}-temp-list",
                enable_auto_commit=False
            )
            partitions = temp_consumer.partitions_for_topic(self.topic)
            temp_consumer.close()
            if partitions:
                self._partitions = [f"partition-{p}" for p in sorted(partitions)]
            else:
                self._partitions = ["partition-0"]
        return self._partitions

    def build_part(self, step_id, for_part, resume_state):
        partition_num = int(for_part.split("-")[1])
        consumer = KafkaConsumer(
            bootstrap_servers=self.brokers,
            group_id=self.group_id,
            auto_offset_reset=self.offset,
            enable_auto_commit=False,
            consumer_timeout_ms=1000
        )
        return KafkaSourcePartition(consumer, self.topic, partition_num)

class KafkaSinkPartition(StatefulSinkPartition):
    def __init__(self, producer, topic):
        self.producer = producer
        self.topic = topic

    def write_batch(self, batch):
        for item in batch:
            try:
                if isinstance(item, tuple) and len(item) == 2:
                    key, value = item
                else:
                    key = None
                    value = item
                
                # normalize key to bytes
                if key is not None and isinstance(key, str):
                    key = key.encode('utf-8')
                elif key is None:
                    key = None

                # normalize value to bytes
                if isinstance(value, (dict, list)):
                    value = json.dumps(value, default=str).encode('utf-8')
                elif isinstance(value, str):
                    value = value.encode('utf-8')
                # if it's already bytes, leave as-is

                self.producer.send(self.topic, key=key, value=value)
            except Exception as e:
                logger.exception("Error writing to Kafka: %s", e)
        try:
            self.producer.flush()
        except Exception:
            logger.exception("Error flushing Kafka producer")

    def snapshot(self):
        return None

    def close(self):
        try:
            self.producer.close()
        except Exception:
            logger.exception("Error closing producer")

class KafkaSink(FixedPartitionedSink):
    def __init__(self, brokers, topic):
        self.brokers = brokers
        self.topic = topic

    def list_parts(self):
        return ["kafka-sink"]

    def build_part(self, step_id, for_part, resume_state=None):
        producer = KafkaProducer(
            bootstrap_servers=self.brokers,
            acks='all',
            retries=3
        )
        return KafkaSinkPartition(producer, self.topic)