import logging
from prometheus_client import start_http_server, Counter, Gauge

logger = logging.getLogger("metrics-server")

# Metrics
CONSUMER_LAG = Gauge('consumer_lag', 'Approximate consumer lag', ['service', 'topic', 'partition'])
MESSAGES_PROCESSED = Counter('messages_processed_total', 'Total Kafka messages processed', ['service', 'topic', 'status'])
DLQ_PUBLISHED = Counter('dlq_published_total', 'Total messages sent to DLQ', ['service', 'topic'])
RETRIES_TOTAL = Counter('retries_total', 'Total retry attempts', ['service', 'operation'])
BACKPRESSURE_EVENTS = Counter('backpressure_events_total', 'Total backpressure trigger events', ['service'])

def start_metrics_server(port=9001):
    try:
        start_http_server(port)
        logger.info(f"Metrics server started on port {port}")
    except Exception as e:
        logger.error(f"Failed to start metrics server: {e}")
