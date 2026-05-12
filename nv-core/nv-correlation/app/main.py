import os
import json
import uuid
import time
import logging
import signal
import sys
import threading
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import psycopg2
from db import Database
from rules import RuleEngine

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
INPUT_TOPIC = "alerts.accepted.v1"
OUTPUT_TOPIC_GROUP_CREATED = "correlation.group.created.v1"
OUTPUT_TOPIC_GROUP_UPDATED = "correlation.group.updated.v1"
OUTPUT_TOPIC_ALERT_LINKED = "correlation.alert.linked.v1"
OUTPUT_TOPIC_AUDIT = "correlation.audit.v1"
DLQ_TOPIC = "correlation.dlq.v1"
MAX_POLL_RECORDS = 100
METRICS_PORT = int(os.getenv("METRICS_PORT", 9003))

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger("correlation-worker")

# Suppress noisy library logs
logging.getLogger("kafka").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

running = True

def signal_handler(sig, frame):
    global running
    logger.info("Shutdown signal received")
    running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def rule_reloader(engine):
    while running:
        try:
            engine.reload_rules()
            time.sleep(60)
        except Exception as e:
            logger.error(f"Rule reload failed: {e}")
            time.sleep(10)


# ── Cascading Health Check Server ─────────────────────────────────────────────
# Lightweight HTTP server for Kubernetes/Docker health probes
HEALTH_PORT = int(os.getenv("HEALTH_PORT", 9004))

def _check_pg_health():
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            database=os.getenv("POSTGRES_DB", "nv_vault"),
            user=os.getenv("POSTGRES_USER", "nv_user"),
            password=os.getenv("POSTGRES_PASSWORD", "nv_pass"),
            connect_timeout=3
        )
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        conn.close()
        return True
    except Exception:
        return False

def _check_kafka_health():
    try:
        from kafka import KafkaConsumer as KC
        c = KC(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, request_timeout_ms=3000)
        topics = c.topics()
        c.close()
        return len(topics) > 0
    except Exception:
        return False

def _start_health_server():
    """Tiny HTTP health server on HEALTH_PORT."""
    from http.server import HTTPServer, BaseHTTPRequestHandler

    class HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path in ("/health", "/readyz"):
                checks = {}
                overall = "ok"
                for name, fn in [("postgres", _check_pg_health), ("kafka", _check_kafka_health)]:
                    start = time.time()
                    try:
                        ok = fn()
                        ms = round((time.time() - start) * 1000, 1)
                        checks[name] = {"status": "ok" if ok else "fail", "response_ms": ms}
                        if not ok:
                            overall = "degraded"
                    except Exception as e:
                        ms = round((time.time() - start) * 1000, 1)
                        checks[name] = {"status": "error", "error": str(e), "response_ms": ms}
                        overall = "degraded"

                body = json.dumps({"status": overall, "service": "nv-correlation", "checks": checks})
                code = 200 if overall == "ok" else 503
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(body.encode())
            elif self.path == "/healthz":
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"status":"ok"}')
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, format, *args):
            pass  # Suppress access logs

    try:
        server = HTTPServer(("0.0.0.0", HEALTH_PORT), HealthHandler)
        logger.info(f"Health server started on port {HEALTH_PORT}")
        server.serve_forever()
    except Exception as e:
        logger.error(f"Health server failed: {e}")

def main():
    logger.info("Starting Correlation Worker (Phase 4.3)")
    start_metrics_server(METRICS_PORT)
    
    # Start cascading health check server in background
    health_thread = threading.Thread(target=_start_health_server, daemon=True)
    health_thread.start()

    try:
        db = Database()
        rules_engine = RuleEngine(db_instance=db)
        consumer = KafkaConsumer(
            INPUT_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="correlation-v11", # Fresh group for final testing
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
        logger.error(f"Initialization failed: {e}")
        sys.exit(1)

    reloader = threading.Thread(target=rule_reloader, args=(rules_engine,), daemon=True)
    reloader.start()

    logger.info("Starting consumer loop...")
    while running:
        start_time = time.time()
        try:
            msg_pack = consumer.poll(timeout_ms=5000) # Increased timeout
            total_processed = 0

            if not msg_pack:
                # Basic heartbeat to prove we are alive
                if int(time.time()) % 30 == 0:
                    logger.info("Awaiting new messages from Kafka...")
                continue

            for tp, messages in msg_pack.items():
                logger.debug(f"Polled {len(messages)} messages from {tp}")
                CONSUMER_LAG.labels(service="correlation", topic=tp.topic, partition=tp.partition).set(0)
                for message in messages:
                    processing_success = False
                    dlq_success = False
                    
                    try:
                        def process_logic():
                            event = message.value
                            payload = event.get('payload', {})
                            tenant_id = event.get('tenant_id')
                            if not tenant_id:
                                logger.warning(f"Message {message.offset} missing tenant_id: {event}")
                                return
                            
                            timestamp = event.get('timestamp', int(time.time() * 1000))
                            matches = rules_engine.evaluate(tenant_id, payload, timestamp)
                            
                            if matches:
                                logger.info(f"Event {event.get('event_id')} matched {len(matches)} rules")
                            
                            for match in matches:
                                # ... (inner logic same as before)
                                link = {
                                    "tenant_id": tenant_id,
                                    "group_id": match["group_id"],
                                    "original_event_id": event.get("event_id"),
                                    "linked_at": timestamp,
                                    "link_reason": f"Matched rule: {match['rule_name']}"
                                }
                                
                                is_new_group, is_new_link, new_count, new_severity = db.process_correlation(match, link)
                                
                                if is_new_group or (is_new_link and new_count % 5 == 0):
                                    group_event = {
                                        # ... (fields same as before)
                                        "group_id": match["group_id"],
                                        "tenant_id": tenant_id,
                                        "rule_id": match["rule_id"],
                                        "rule_name": match["rule_name"],
                                        "correlation_key": match["correlation_key"],
                                        "alert_count": new_count,
                                        "max_severity": new_severity,
                                        "status": "OPEN",
                                        "updated_at": timestamp
                                    }
                                    producer.send("correlation.group.created.v1", value=group_event)
                                    
                                    # ── AUTO-TRIGGER WORKFLOW ENGINE ──
                                    # When a new incident is created or escalates,
                                    # fire the workflow engine so it can run
                                    # automated playbooks (notify, enrich, create case, etc.)
                                    workflow_trigger = {
                                        "event_id": str(uuid.uuid4()),
                                        "type": "CorrelationMatch",
                                        "tenant_id": tenant_id,
                                        "timestamp": timestamp,
                                        "payload": {
                                            "source": "nv-correlation",
                                            "trigger_reason": "new_group" if is_new_group else "escalation",
                                            "group_id": match["group_id"],
                                            "rule_id": match["rule_id"],
                                            "rule_name": match["rule_name"],
                                            "correlation_key": match["correlation_key"],
                                            "alert_count": new_count,
                                            "max_severity": new_severity,
                                            "category": payload.get("category", "unknown"),
                                            "source_ip": payload.get("source_ip", payload.get("src_ip", "")),
                                            "host": payload.get("host", payload.get("hostname", "")),
                                            "event": payload,
                                        }
                                    }
                                    producer.send("workflow.trigger.v1", value=workflow_trigger)
                            
                            producer.flush()

                        execute_with_retry(process_logic, max_retries=3, retryable_exceptions=(psycopg2.OperationalError, KafkaError))
                        processing_success = True
                        MESSAGES_PROCESSED.labels(service="correlation", topic=INPUT_TOPIC, status="success").inc()

                    except Exception as e:
                        logger.error(f"Error processing message {message.offset}: {e}", exc_info=True)
                        # ... (rest of error block)
                        MESSAGES_PROCESSED.labels(service="correlation", topic=INPUT_TOPIC, status="error").inc()
                        dlq_event = build_dlq_event(str(e), message.value, message.topic, message.partition, message.offset)
                        dlq_success = send_dlq(producer, DLQ_TOPIC, dlq_event)
                        if dlq_success:
                            DLQ_PUBLISHED.labels(service="correlation", topic=DLQ_TOPIC).inc()
                    
                    if not commit_if_safe(consumer, processing_success, dlq_success):
                        break 
                    
                    total_processed += 1
            
            if total_processed > 0:
                logger.info(f"Batch complete: {total_processed} events processed")
                check_backpressure(start_time, total_processed)
                BACKPRESSURE_EVENTS.labels(service="correlation").inc()

        except Exception as e:
            logger.error(f"Error in consumer loop: {e}")
            time.sleep(1)

    producer.close()
    consumer.close()
    db.close()

if __name__ == "__main__":
    main()
