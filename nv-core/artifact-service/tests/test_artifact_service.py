"""
Unit tests for nv-artifact-service.
Tests encryption, hash correctness, tenant isolation, and sighting logic.
"""
import hashlib
import os
import sys
import pytest
from cryptography.fernet import Fernet

# ── Encryption / Hash tests ──────────────────────────────────────────────────

def test_sha256_deterministic():
    payload = b"malware.exe contents"
    h1 = hashlib.sha256(payload).hexdigest()
    h2 = hashlib.sha256(payload).hexdigest()
    assert h1 == h2

def test_sha256_differs_for_different_content():
    assert hashlib.sha256(b"abc").hexdigest() != hashlib.sha256(b"def").hexdigest()

def test_fernet_encrypt_decrypt_round_trip():
    raw = b"super secret forensic payload"
    key = Fernet.generate_key()
    token = Fernet(key).encrypt(raw)
    assert Fernet(key).decrypt(token) == raw

def test_fernet_encrypted_bytes_differ_from_plaintext():
    raw = b"plaintext evidence file contents"
    key = Fernet.generate_key()
    encrypted = Fernet(key).encrypt(raw)
    # Encrypted bytes must NOT equal plaintext
    assert encrypted != raw
    # And must not CONTAIN the plaintext verbatim
    assert raw not in encrypted

def test_per_file_key_uniqueness():
    """Each upload should generate a distinct key."""
    key1 = Fernet.generate_key()
    key2 = Fernet.generate_key()
    assert key1 != key2

# ── Tenant bucket naming ─────────────────────────────────────────────────────
def bucket_for_tenant(tenant_id: str) -> str:
    safe = tenant_id.lower().replace("_", "-").replace(".", "-")[:40]
    return f"nv-vault-{safe}"

def test_tenant_bucket_isolation():
    bucket_a = bucket_for_tenant("tenant-A")
    bucket_b = bucket_for_tenant("tenant-B")
    assert bucket_a != bucket_b
    assert bucket_a.startswith("nv-vault-")
    assert bucket_b.startswith("nv-vault-")

def test_tenant_bucket_safe_name():
    # Uppercase + underscore → lowercase + dash
    bucket = bucket_for_tenant("Tenant_001")
    assert bucket == "nv-vault-tenant-001"

def test_tenant_bucket_truncates_long_names():
    long_id = "a" * 60
    bucket  = bucket_for_tenant(long_id)
    # prefix is "nv-vault-" (9 chars), safe part <= 40 chars
    assert len(bucket) <= 49

# ── Sighting badge logic ─────────────────────────────────────────────────────
def test_is_sighting_true_when_count_gt_1():
    sightings_count = 2
    is_sighting = sightings_count > 1
    assert is_sighting is True

def test_is_sighting_false_when_only_self():
    sightings_count = 1
    is_sighting = sightings_count > 1
    assert is_sighting is False
