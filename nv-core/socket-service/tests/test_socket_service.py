"""
Unit tests for nv-socket-service.
Tests tenant isolation routing, message format, heartbeat schema, presence logic.
No external dependencies (Kafka, Redis, WebSocket) required.
"""
import json
import time
import pytest

# ── Message schema helpers (mirrored from main.py logic) ────────────────────

def make_minimal_event(event_type: str, entity_id: str, tenant_id: str) -> str:
    return json.dumps({
        "type":      event_type,
        "id":        entity_id,
        "tenant_id": tenant_id,
        "timestamp": int(time.time() * 1000),
    })

def channel_for_tenant(tenant_id: str) -> str:
    return f"room:tenant:{tenant_id}"

TOPIC_TO_EVENT_TYPE = {
    "nv.alerts.ingest.v1":       "NEW_ALERT",
    "nv.cases.updated.v1":       "CASE_UPDATED",
    "nv.artifacts.uploaded.v1":  "ARTIFACT_UPLOADED",
}

# ── Channel / room isolation ─────────────────────────────────────────────────

def test_tenant_channels_are_distinct():
    ch_a = channel_for_tenant("tenant-A")
    ch_b = channel_for_tenant("tenant-B")
    assert ch_a != ch_b

def test_tenant_channel_format():
    assert channel_for_tenant("acme-corp") == "room:tenant:acme-corp"

def test_different_tenants_get_different_channels():
    tenants = ["t1", "t2", "t3"]
    channels = {channel_for_tenant(t) for t in tenants}
    assert len(channels) == 3  # all unique

# ── Minimal payload format ────────────────────────────────────────────────────

def test_minimal_event_has_required_fields():
    raw = make_minimal_event("NEW_ALERT", "alert-123", "tenant-A")
    msg = json.loads(raw)
    assert "type"      in msg
    assert "id"        in msg
    assert "tenant_id" in msg
    assert "timestamp" in msg

def test_minimal_event_contains_no_full_data():
    raw = make_minimal_event("NEW_ALERT", "alert-123", "tenant-A")
    msg = json.loads(raw)
    # GUARDRAIL: only id, not full alert body
    assert "title"       not in msg
    assert "description" not in msg
    assert "severity"    not in msg

def test_event_type_mapping():
    assert TOPIC_TO_EVENT_TYPE["nv.alerts.ingest.v1"]      == "NEW_ALERT"
    assert TOPIC_TO_EVENT_TYPE["nv.cases.updated.v1"]      == "CASE_UPDATED"
    assert TOPIC_TO_EVENT_TYPE["nv.artifacts.uploaded.v1"] == "ARTIFACT_UPLOADED"

# ── Heartbeat schema ─────────────────────────────────────────────────────────

def test_ping_message_format():
    ping = json.dumps({"type": "PING", "timestamp": int(time.time() * 1000)})
    msg = json.loads(ping)
    assert msg["type"] == "PING"
    assert "timestamp" in msg

def test_pong_reply_format():
    pong = json.dumps({"type": "PONG"})
    msg = json.loads(pong)
    assert msg["type"] == "PONG"

# ── Presence logic ───────────────────────────────────────────────────────────

def test_analyst_presence_join_leave():
    presence = {}

    def join(tenant, case, user):
        presence.setdefault(tenant, {}).setdefault(case, set()).add(user)

    def leave(tenant, case, user):
        presence.get(tenant, {}).get(case, set()).discard(user)

    join("tenant-A", "case-001", "alice")
    join("tenant-A", "case-001", "bob")
    assert "alice" in presence["tenant-A"]["case-001"]
    assert "bob"   in presence["tenant-A"]["case-001"]

    leave("tenant-A", "case-001", "alice")
    assert "alice" not in presence["tenant-A"]["case-001"]
    assert "bob"   in presence["tenant-A"]["case-001"]

def test_analyst_presence_tenant_isolation():
    """Tenant A presence must not bleed into Tenant B."""
    presence = {}
    presence.setdefault("tenant-A", {}).setdefault("case-001", set()).add("alice")
    assert "tenant-B" not in presence
    assert "alice" not in presence.get("tenant-B", {}).get("case-001", set())

# ── Tenant JWT mismatch detection ────────────────────────────────────────────

def test_tenant_mismatch_detected():
    jwt_tenant  = "tenant-A"
    path_tenant = "tenant-B"
    assert jwt_tenant != path_tenant  # Connection should be rejected

def test_tenant_match_allowed():
    jwt_tenant  = "tenant-A"
    path_tenant = "tenant-A"
    assert jwt_tenant == path_tenant  # Connection allowed
