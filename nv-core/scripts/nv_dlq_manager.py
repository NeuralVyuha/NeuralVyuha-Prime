import os
import sys
import argparse
import json
import logging
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

# Common config
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dlq-manager")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
# Use secrets/SSL if configured in env (omitted for CLI tool simplicity/flexibility, assumes user sets env)

def get_kafka_client():
    # In real usage, this should use the common secrets loader for SSL
    # For T4 proof, basic connection
    return KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP), KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)

def list_dlqs(topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
        group_id=None # ephemeral
    )
    count = 0
    for msg in consumer:
        count += 1
        try:
            val = json.loads(msg.value)
            print(f"Offset {msg.offset}: {val.get('type', 'Unknown')} - Reason: {val.get('payload', {}).get('reason', 'N/A')}")
        except:
            print(f"Offset {msg.offset}: [Raw Data]")
    print(f"Total: {count}")
    consumer.close()

def replay_dlq(dlq_topic, target_topic, max_messages=100, delay=0.1):
    consumer = KafkaConsumer(
        dlq_topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset='earliest',
        enable_auto_commit=True, # Commit as we replay? Or manual? Manual safer.
        group_id=f"dlq-replay-{int(time.time())}"
    )
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)

    count = 0
    for msg in consumer:
        if count >= max_messages:
            break
        try:
            val = json.loads(msg.value)
            original_payload = val.get('payload', {}).get('original_message')

            if original_payload:
                # Add replay header
                headers = [('replayed', b'true')]
                if isinstance(original_payload, str):
                     # If it was stringified
                     producer.send(target_topic, value=original_payload.encode('utf-8'), headers=headers)
                else:
                     producer.send(target_topic, value=json.dumps(original_payload).encode('utf-8'), headers=headers)

                print(f"Replayed offset {msg.offset}")
                count += 1
                time.sleep(delay)
            else:
                print(f"Skipping offset {msg.offset}: No original message found")
        except Exception as e:
             print(f"Failed to replay offset {msg.offset}: {e}")

    producer.flush()
    consumer.close()
    producer.close()
    print(f"Replayed {count} messages.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="v5 DLQ Manager")
    subparsers = parser.add_subparsers(dest="command")

    list_parser = subparsers.add_parser("list")
    list_parser.add_argument("topic", help="DLQ Topic Name")

    replay_parser = subparsers.add_parser("replay")
    replay_parser.add_argument("dlq_topic", help="Source DLQ Topic")
    replay_parser.add_argument("target_topic", help="Target Topic")
    replay_parser.add_argument("--max", type=int, default=100)

    args = parser.parse_args()

    if args.command == "list":
        list_dlqs(args.topic)
    elif args.command == "replay":
        replay_dlq(args.dlq_topic, args.target_topic, args.max)
    else:
        parser.print_help()
