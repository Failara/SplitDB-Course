from bytewax.inputs import StatefulSourcePartition, FixedPartitionedSource
from bytewax.outputs import StatefulSinkPartition, FixedPartitionedSink
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import json

class KafkaSourcePartition(StatefulSourcePartition):
    def __init__(self, consumer, topic, partition):
        self.consumer = consumer
        self.topic = topic
        self.partition = partition
        tp = TopicPartition(topic, partition)
        self.consumer.assign([tp])

    def next_batch(self):
        messages = self.consumer.poll(timeout_ms=1000, max_records=100)
        batch = []
        for tp, msgs in messages.items():
            for msg in msgs:
                batch.append((msg.key, msg.value))
        return batch

    def snapshot(self):
        return None

    def close(self):
        self.consumer.close()

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
                group_id=f"{self.group_id}-temp-list"
            )
            partitions = temp_consumer.partitions_for_topic(self.topic)
            temp_consumer.close()
            if partitions:
                self._partitions = [f"partition-{p}" for p in partitions]
            else:
                self._partitions = ["partition-0"]
        return self._partitions

    def build_part(self, step_id, for_part, resume_state):
        partition_num = int(for_part.split("-")[1])
        consumer = KafkaConsumer(
            bootstrap_servers=self.brokers,
            group_id=self.group_id,
            auto_offset_reset=self.offset,
            enable_auto_commit=True
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
                
                if key and isinstance(key, str):
                    key = key.encode('utf-8')
                
                if isinstance(value, (dict, list)):
                    value = json.dumps(value, default=str).encode('utf-8')
                elif isinstance(value, str):
                    value = value.encode('utf-8')
                
                self.producer.send(self.topic, key=key, value=value)
            except Exception as e:
                print(f"Error writing to Kafka: {e}")
        self.producer.flush()

    def snapshot(self):
        return None

    def close(self):
        self.producer.close()

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