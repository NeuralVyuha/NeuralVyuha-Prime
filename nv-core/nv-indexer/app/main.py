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
INPUT_TOPIC = "alerts.accepted.v1"
DLQ_TOPIC = "alerts.indexer.dlq.v1"
INDEX_PREFIX = "alerts-v1"
MAX_POLL_RECORDS = 100
METRICS_PORT = int(os.getenv("METRICS_PORT", 9002))

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger("indexer-worker")

running = True

def signal_handler(sig, frame):
    global running
    logger.info("Shutdown signal received")
    running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def main():
    logger.info("Starting OpenSearch Indexer Worker (Phase 4.3)")
    start_metrics_server(METRICS_PORT)

    os_client = OpenSearch(
        hosts=[{'host': OPENSEARCH_HOST, 'port': OPENSEARCH_PORT}],
        http_compress=True,
        use_ssl=False
    )

    try:
        consumer = KafkaConsumer(
            INPUT_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="indexer-group-v1",
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
                CONSUMER_LAG.labels(service="indexer", topic=tp.topic, partition=tp.partition).set(0) # Mock lag
                for message in messages:
                    try:
                        event = message.value
                        payload = event.get('payload', {})
                        doc = {
                            "event_id": event.get('event_id'),
                            "trace_id": event.get('trace_id'),
                            "timestamp": event.get('timestamp'),
                            "tenant_id": payload.get('tenant_id', 'default'), 
                            "fingerprint": payload.get('fingerprint'),
                            "status": payload.get('decision'),
                        }
                        doc.update(payload.get('original_payload', {}))
                        index_name = f"{INDEX_PREFIX}-{time.strftime('%Y.%m')}"
                        actions.append({
                            "_index": index_name,
                            "_id": payload.get('original_event_id'), 
                            "_source": doc
                        })
                    except Exception as e:
                        logger.error(f"Transform error: {e}")
                        MESSAGES_PROCESSED.labels(service="indexer", topic=INPUT_TOPIC, status="error").inc()
                        dlq_event = build_dlq_event(str(e), message.value, message.topic, message.partition, message.offset)
                        send_dlq(producer, DLQ_TOPIC, dlq_event)
                        DLQ_PUBLISHED.labels(service="indexer", topic=DLQ_TOPIC).inc()

            if actions:
                def bulk_op():
                    return helpers.bulk(os_client, actions, stats_only=False, raise_on_error=False)

                try:
                    success, errors = execute_with_retry(bulk_op, max_retries=3)
                    RETRIES_TOTAL.labels(service="indexer", operation="bulk_index").inc()
                    if errors:
                        logger.warning(f"Bulk had {len(errors)} errors")
                    
                    commit_if_safe(consumer, True, False)
                    MESSAGES_PROCESSED.labels(service="indexer", topic=INPUT_TOPIC, status="success").inc(len(actions))

                except Exception as e:
                    logger.error(f"Bulk Indexing Failed: {e}")
                    MESSAGES_PROCESSED.labels(service="indexer", topic=INPUT_TOPIC, status="fatal_error").inc(len(actions))
                    time.sleep(2)
            else:
                commit_if_safe(consumer, True, False)

            if actions:
                check_backpressure(start_time, len(actions))
                BACKPRESSURE_EVENTS.labels(service="indexer").inc()

        except Exception as e:
            logger.error(f"Error in indexing loop: {e}")
            time.sleep(1)

    producer.close()
    consumer.close()

if __name__ == "__main__":
    main()
