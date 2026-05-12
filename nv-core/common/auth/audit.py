import logging
import time
import uuid

# Simple audit helper (placeholder for Kafka producer)
# In production, this would produce to 'audit.event.v1'

logger = logging.getLogger("audit-logger")

def emit_audit_event(event_type: str, ctx: dict, details: dict):
    event = {
        "event_id": str(uuid.uuid4()),
        "type": event_type,
        "timestamp": int(time.time() * 1000),
        "tenant_id": ctx.get("tenant_id"),
        "user_id": ctx.get("user_id"),
        "details": details
    }
    logger.info(f"AUDIT: {event}")
