import json
import time
import uuid
import logging
import socket
from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger("dlq-helper")

def build_dlq_event(
    reason: str,
    original_message: dict,
    source_topic: str,
    source_partition: int,
    source_offset: int,
    tenant_id: str = "unknown",
    trace_id: str = None
) -> dict:
    """
    Constructs a standardized DLQ event envelope.
    """
    return {
        "event_id": str(uuid.uuid4()),
        "type": "NeuralDLQEvent",
        "tenant_id": tenant_id,
        "trace_id": trace_id or str(uuid.uuid4()),
        "timestamp": int(time.time() * 1000),
        "schema_version": "1.0",
        "payload": {
            "reason": reason,
            "original_message": original_message,
            "source_topic": source_topic,
            "source_partition": source_partition,
            "source_offset": source_offset,
            "failed_by": socket.gethostname()
        }
    }

def send_dlq(
    producer: KafkaProducer,
    dlq_topic: str,
    dlq_event: dict
) -> bool:
    """
    Sends an event to the DLQ topic.
    Returns True if send AND flush succeeded (or at least send was accepted).
    Returns False if send failed.
    """
    if not producer:
        logger.error("DLQ Producer is None. Cannot send DLQ event.")
        return False

    try:
        # We assume value_serializer handles JSON if configured,
        # but to be safe/explicit with this helper, we might encode if raw.
        # However, callers usually configure the producer. 
        # Let's assume the producer passed in is configured to handle dicts if that's the convention,
        # OR we rely on standardizing here.
        # Safest: use send() and let the configured serializer handle it, but catch errors.
        future = producer.send(dlq_topic, value=dlq_event)
        
        # We MUST wait for ack to be sure DLQ is safe before proceeding to commit.
        # This makes it synchronous, which is correct for error handling path.
        future.get(timeout=10) 
        return True
    except Exception as e:
        logger.error(f"Failed to publish to DLQ {dlq_topic}: {e}")
        return False
