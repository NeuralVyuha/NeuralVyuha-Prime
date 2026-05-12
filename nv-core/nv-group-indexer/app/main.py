import os
import json
import logging
import signal
import sys
import time
from kafka import KafkaConsumer, KafkaProducer
from opensearchpy import OpenSearch, helpers

# Robust common library discovery (handles local dev and Docker context)
_base_dir = os.path.dirname(__file__)
_paths_to_check = [
    os.path.abspath(os.path.join(_base_dir, '../../')), # Local dev (parent of common)
    os.path.abspath(os.path.join(_base_dir, '../')),     # Docker (parent of common)
]
for _p in _paths_to_check:
    if os.path.exists(os.path.join(_p, 'common')):
        sys.path.append(_p)
        break

from common.reliability.dlq import build_dlq_event, send_dlq
from common.reliability.commit import commit_if_safe
from common.reliability.retry import execute_with_retry
from common.reliability.backpressure import check_backpressure

# Metrics
from metrics_server import start_metrics_server, MESSAGES_PROCESSED, DLQ_PUBLISHED, RETRIES_TOTAL, BACKPRESSURE_EVENTS, CONSUMER_LAG

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:29092")
OPENSEARCH_HOST = os.getenv("OPENSEARCH_HOST", "opensearch")
OPENSEARCH_PORT = int(os.getenv("OPENSEARCH_PORT", 9200))
INDEX_NAME = "groups-v1"
DLQ_TOPIC = "groups.indexer.dlq.v1"
MAX_POLL_RECORDS = 100
METRICS_PORT = int(os.getenv("METRICS_PORT", 9004))

# Topics
TOPICS = ["correlation.group.created.v1", "correlation.group.updated.v1"]

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger("group-indexer")

running = True

def signal_handler(sig, frame):
    global running
    logger.info("Shutdown signal received")
    running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def main():
    logger.info("Starting Group Indexer Worker (Phase 4.3)")
    start_metrics_server(METRICS_PORT)

    os_client = OpenSearch(
        hosts=[{'host': OPENSEARCH_HOST, 'port': OPENSEARCH_PORT}],
        http_compress=True,
        use_ssl=False
    )

    try:
        consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="group-indexer-v1",
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            max_poll_records=MAX_POLL_RECORDS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        logger.error(f"Kafka connection failed: {e}")
        sys.exit(1)

    while running:
        start_time = time.time()
        try:
            msg_pack = consumer.poll(timeout_ms=1000)
            actions = []
            
            for tp, messages in msg_pack.items():
                CONSUMER_LAG.labels(service="group-indexer", topic=tp.topic, partition=tp.partition).set(0)
                for message in messages:
                    try:
                        event = message.value
                        payload = event.get('payload', {})
                        tenant_id = event.get('tenant_id')
                        group_id = payload.get('group_id')

                        if not tenant_id or not group_id:
                            raise ValueError("Missing tenant_id or group_id")

                        doc = payload.copy()
                        doc['tenant_id'] = tenant_id
                        doc['visibility_state'] = payload.get('visibility_state', 'HIDDEN')
                        doc_id = f"{tenant_id}:{group_id}"

                        actions.append({
                            "_op_type": "update",
                            "_index": INDEX_NAME,
                            "_id": doc_id,
                            "doc": doc,
                            "doc_as_upsert": True
                        })
                    except Exception as e:
                        logger.error(f"Mapping error: {e}")
                        MESSAGES_PROCESSED.labels(service="group-indexer", topic=message.topic, status="error").inc()
                        dlq_event = build_dlq_event(str(e), message.value, message.topic, message.partition, message.offset)
                        send_dlq(producer, DLQ_TOPIC, dlq_event)
                        DLQ_PUBLISHED.labels(service="group-indexer", topic=DLQ_TOPIC).inc()

            if actions:
                def bulk_op():
                    return helpers.bulk(os_client, actions, stats_only=False, raise_on_error=False)

                try:
                    success, errors = execute_with_retry(bulk_op, max_retries=3)
                    RETRIES_TOTAL.labels(service="group-indexer", operation="bulk_upsert").inc()
                    MESSAGES_PROCESSED.labels(service="group-indexer", topic="combined", status="success").inc(len(actions))
                    commit_if_safe(consumer, True, False)
                except Exception as e:
                    logger.error(f"Bulk Group Indexing Failed: {e}")
                    MESSAGES_PROCESSED.labels(service="group-indexer", topic="combined", status="fatal_error").inc(len(actions))
                    time.sleep(2)
            else:
                commit_if_safe(consumer, True, False)

            if actions:
                check_backpressure(start_time, len(actions))
                BACKPRESSURE_EVENTS.labels(service="group-indexer").inc()

        except Exception as e:
            logger.error(f"Error in indexing loop: {e}")
            time.sleep(1)

    producer.close()
    consumer.close()

if __name__ == "__main__":
    main()
