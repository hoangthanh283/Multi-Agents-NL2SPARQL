import json
import logging
import os
from typing import Any, Callable, Dict

from confluent_kafka import Consumer, KafkaError, Producer

logger = logging.getLogger(__name__)

class KafkaHandler:
    def __init__(self):
        self.producer_config = {
            'bootstrap.servers': os.getenv('KAFKA_BROKERS', 'localhost:9092'),
            'client.id': 'nl2sparql-producer',
            'acks': 'all',
            'retries': 5,
            'linger.ms': 5
        }
        
        self.consumer_config = {
            'bootstrap.servers': os.getenv('KAFKA_BROKERS', 'localhost:9092'),
            'group.id': 'nl2sparql-consumer',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'max.poll.interval.ms': 300000
        }
        
        self.producer = Producer(self.producer_config)
        self.running = False

    def produce_message(self, topic: str, message: Dict[str, Any], key: str = None) -> None:
        """Produce a message to a Kafka topic"""
        try:
            self.producer.produce(
                topic,
                key=key.encode('utf-8') if key else None,
                value=json.dumps(message).encode('utf-8'),
                callback=self._delivery_report
            )
            self.producer.poll(0)
        except Exception as e:
            logger.error(f"Error producing message to Kafka: {e}")
            raise

    def consume_messages(self, topics: list, message_handler: Callable) -> None:
        """Consume messages from Kafka topics"""
        consumer = Consumer(self.consumer_config)
        consumer.subscribe(topics)
        self.running = True

        try:
            while self.running:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        break

                try:
                    value = json.loads(msg.value().decode('utf-8'))
                    message_handler(value)
                    consumer.commit()
                except Exception as e:
                    logger.error(f"Error processing message: {e}")

        finally:
            consumer.close()

    def stop_consuming(self) -> None:
        """Stop consuming messages"""
        self.running = False

    def _delivery_report(self, err, msg) -> None:
        """Delivery report callback for producer"""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Kafka topics
QUERY_TOPIC = "nl2sparql-queries"
RESULT_TOPIC = "nl2sparql-results"
ERROR_TOPIC = "nl2sparql-errors"
METRIC_TOPIC = "nl2sparql-metrics"

# Initialize Kafka handler
kafka_handler = KafkaHandler()