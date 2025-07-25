#!/usr/bin/env python3
"""
Kafka utilities for the Spotify data pipeline.

This module provides common Kafka operations used across the pipeline:
- Producer and consumer creation
- Message encoding/decoding
- Error handling
"""

import json
import logging
import os
import time
from typing import Dict, List, Any, Optional, Callable, Union

from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from dotenv import load_dotenv

# Configure logging
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Kafka configuration
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:29092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "spotify-consumers")

# Default Kafka topics
DEFAULT_TOPICS = {
    "play_events": "spotify.events.plays",
    "user_events": "spotify.events.users",
    "system_events": "spotify.events.system"
}


def get_kafka_producer(client_id: str = "spotify-producer",
                      acks: str = "all") -> Producer:
    """
    Create and return a Kafka producer.
    
    Args:
        client_id: Client ID for the producer
        acks: Acknowledgment level (all, 1, 0)
        
    Returns:
        Producer: Kafka producer
        
    Raises:
        SystemExit: If connection fails
    """
    try:
        producer_conf = {
            'bootstrap.servers': KAFKA_BROKERS,
            'client.id': client_id,
            'acks': acks
        }
        producer = Producer(producer_conf)
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        raise


def get_kafka_consumer(group_id: Optional[str] = None,
                      auto_offset_reset: str = "earliest",
                      auto_commit: bool = False) -> Consumer:
    """
    Create and return a Kafka consumer.
    
    Args:
        group_id: Consumer group ID (defaults to env var)
        auto_offset_reset: Where to start consuming from
        auto_commit: Whether to auto-commit offsets
        
    Returns:
        Consumer: Kafka consumer
        
    Raises:
        SystemExit: If connection fails
    """
    try:
        consumer_conf = {
            'bootstrap.servers': KAFKA_BROKERS,
            'group.id': group_id or KAFKA_GROUP_ID,
            'auto.offset.reset': auto_offset_reset,
            'enable.auto.commit': auto_commit
        }
        consumer = Consumer(consumer_conf)
        return consumer
    except Exception as e:
        logger.error(f"Failed to create Kafka consumer: {e}")
        raise


def delivery_callback(err, msg) -> None:
    """
    Callback for Kafka message delivery.
    
    Args:
        err: Error (if any)
        msg: Message that was delivered
    """
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def produce_message(producer: Producer, 
                   topic: str, 
                   value: Union[str, Dict, List],
                   key: Optional[str] = None,
                   headers: Optional[Dict] = None,
                   callback: Callable = delivery_callback) -> None:
    """
    Produce a message to Kafka.
    
    Args:
        producer: Kafka producer
        topic: Topic to produce to
        value: Message value (string or dict/list to be JSON-encoded)
        key: Message key
        headers: Message headers
        callback: Delivery callback function
    """
    try:
        # Convert dict/list to JSON string
        if isinstance(value, (dict, list)):
            value = json.dumps(value)
        
        # Convert headers to list of tuples if present
        kafka_headers = None
        if headers:
            kafka_headers = [(k, str(v).encode()) for k, v in headers.items()]
        
        # Produce the message
        producer.produce(
            topic=topic,
            key=key.encode() if key else None,
            value=value.encode() if isinstance(value, str) else value,
            headers=kafka_headers,
            callback=callback
        )
        
        # Poll to handle delivery reports
        producer.poll(0)
        
    except Exception as e:
        logger.error(f"Error producing message to {topic}: {e}")


def flush_producer(producer: Producer, timeout: float = 5.0) -> None:
    """
    Flush any remaining messages in the producer.
    
    Args:
        producer: Kafka producer
        timeout: Timeout in seconds
    """
    producer.flush(timeout=timeout)


def consume_messages(consumer: Consumer, 
                    topics: List[str],
                    num_messages: int = -1,
                    timeout: float = 1.0,
                    parse_json: bool = True) -> List[Dict]:
    """
    Consume messages from Kafka topics.
    
    Args:
        consumer: Kafka consumer
        topics: Topics to consume from
        num_messages: Number of messages to consume (-1 for infinite)
        timeout: Timeout for each poll in seconds
        parse_json: Whether to parse message values as JSON
        
    Returns:
        List of message dictionaries
    """
    messages = []
    
    try:
        consumer.subscribe(topics)
        remaining = num_messages
        
        while num_messages == -1 or remaining > 0:
            msg = consumer.poll(timeout=timeout)
            
            if msg is None:
                if num_messages == -1:
                    continue
                else:
                    break
                    
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(f"Reached end of partition {msg.partition()}")
                else:
                    logger.error(f"Error consuming message: {msg.error()}")
                continue
                
            try:
                # Parse the message value
                value = msg.value().decode('utf-8')
                if parse_json:
                    try:
                        value = json.loads(value)
                    except json.JSONDecodeError:
                        logger.warning(f"Failed to parse message as JSON: {value}")
                
                # Extract headers if present
                headers = {}
                if msg.headers():
                    headers = {k: v.decode('utf-8') for k, v in msg.headers()}
                
                # Create message dictionary
                message = {
                    'topic': msg.topic(),
                    'partition': msg.partition(),
                    'offset': msg.offset(),
                    'key': msg.key().decode('utf-8') if msg.key() else None,
                    'value': value,
                    'headers': headers,
                    'timestamp': msg.timestamp()[1]
                }
                
                messages.append(message)
                
                if num_messages != -1:
                    remaining -= 1
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")
        
        return messages
        
    except KafkaException as e:
        logger.error(f"Kafka error: {e}")
        return messages
    except Exception as e:
        logger.error(f"Error consuming messages: {e}")
        return messages


def create_topics_if_not_exist(broker: str, topics: List[str], 
                              num_partitions: int = 3, 
                              replication_factor: int = 1) -> bool:
    """
    Create Kafka topics if they don't exist.
    
    Args:
        broker: Kafka broker address
        topics: List of topic names to create
        num_partitions: Number of partitions for each topic
        replication_factor: Replication factor for each topic
        
    Returns:
        bool: Success status
    """
    try:
        from confluent_kafka.admin import AdminClient, NewTopic
        
        # Create an admin client
        admin_client = AdminClient({'bootstrap.servers': broker})
        
        # Get existing topics
        existing_topics = admin_client.list_topics(timeout=10).topics
        
        # Create list of topics to create
        new_topics = []
        for topic in topics:
            if topic not in existing_topics:
                new_topics.append(NewTopic(
                    topic,
                    num_partitions=num_partitions,
                    replication_factor=replication_factor
                ))
        
        # Create the topics
        if new_topics:
            futures = admin_client.create_topics(new_topics)
            
            # Wait for each creation to finish
            for topic, future in futures.items():
                try:
                    future.result()
                    logger.info(f"Topic {topic} created")
                except Exception as e:
                    logger.error(f"Failed to create topic {topic}: {e}")
        
        return True
    except Exception as e:
        logger.error(f"Error creating topics: {e}")
        return False 