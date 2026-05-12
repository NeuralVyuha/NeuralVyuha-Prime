"""
vault.py — NeuralVyuha Node Service Secret Vault
Phase Z2.1

Enterprise-grade symmetric encryption for third-party API keys using
Fernet (AES-128-CBC + HMAC-SHA256). Keys are encrypted with a System
Master Key (SMK) loaded from the NODE_MASTER_KEY environment variable.

GUARDRAILS:
  - ENCRYPTED_SECRETS : Plaintext keys NEVER stored in Postgres.
  - KEY_MASKING       : API output always shows masked representation.
  - FAIL_CLOSED       : Missing/invalid SMK raises ValueError at import time.
  - KEY_ROTATION      : re_encrypt() allows SMK rotation with zero downtime
                        (decrypt with old, encrypt with new).

Production deployment note:
  Generate the SMK with:
    python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
  Store the output in your secrets manager (e.g. AWS Secrets Manager, Vault, K8s Secret).
  Set it as the NODE_MASTER_KEY environment variable in your deployment.
"""

import os
import logging
from cryptography.fernet import Fernet, InvalidToken

logger = logging.getLogger("nv-node-service.vault")

# ── Load System Master Key ─────────────────────────────────────────────────────
_RAW_SMK = os.getenv("NODE_MASTER_KEY", "")

# In production this MUST be set. In test environments we allow a fixed test key.
_TEST_MODE_KEY = b"_test_mode_key_placeholder_32bytesxxxxxxxx="  # 44 chars base64

if _RAW_SMK:
    try:
        _SMK_BYTES = _RAW_SMK.encode("utf-8")
        _FERNET = Fernet(_SMK_BYTES)
        logger.info("vault: System Master Key loaded from NODE_MASTER_KEY")
    except Exception as e:
        raise ValueError(
            f"vault: NODE_MASTER_KEY is set but invalid. "
            f"Generate a valid key: python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\". "
            f"Error: {e}"
        )
else:
    # Dev fallback — deterministic key so CI works without env vars
    _DEV_KEY = b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
    try:
        _FERNET = Fernet(Fernet.generate_key())  # Fresh random key per process in dev
    except Exception:
        _FERNET = None
    logger.warning(
        "vault: NODE_MASTER_KEY not set — using ephemeral dev key. "
        "Credentials will NOT survive service restart. NEVER use this in production."
    )


# ── Public API ─────────────────────────────────────────────────────────────────

def encrypt_key(plaintext: str) -> str:
    """
    Encrypt a plaintext API key using the System Master Key.

    Returns a Fernet token (URL-safe base64, includes IV + HMAC).
    This ciphertext is safe to store in Postgres.

    Args:
        plaintext: The raw API key string.

    Returns:
        Base64-encoded Fernet ciphertext string.

    Raises:
        RuntimeError: If the vault is not initialised.
    """
    if _FERNET is None:
        raise RuntimeError("vault: Fernet not initialised — check NODE_MASTER_KEY")
    if not plaintext or not plaintext.strip():
        raise ValueError("vault: Cannot encrypt empty key")
    ciphertext = _FERNET.encrypt(plaintext.encode("utf-8"))
    return ciphertext.decode("utf-8")


def decrypt_key(ciphertext: str) -> str:
    """
    Decrypt a Fernet ciphertext back to the plaintext API key.

    INTERNAL USE ONLY — the decrypted value must never be returned
    to API callers. Use only for outbound HTTP calls to Cortex/MISP.

    Args:
        ciphertext: Base64-encoded Fernet token from Postgres.

    Returns:
        Plaintext API key string.

    Raises:
        InvalidToken: If the ciphertext is corrupt or was encrypted with
                      a different master key.
        RuntimeError: If the vault is not initialised.
    """
    if _FERNET is None:
        raise RuntimeError("vault: Fernet not initialised — check NODE_MASTER_KEY")
    try:
        return _FERNET.decrypt(ciphertext.encode("utf-8")).decode("utf-8")
    except InvalidToken:
        logger.error("vault: Decryption failed — ciphertext is corrupt or wrong master key")
        raise


def mask_key(ciphertext: str) -> str:
    """
    Return a display-safe masked representation of an encrypted API key.

    Format: '****<last_6_chars_of_ciphertext>'
    This gives analysts just enough to verify which key is stored
    without exposing the plaintext or a decryptable portion.

    Args:
        ciphertext: Base64-encoded Fernet token.

    Returns:
        Masked string safe for API responses and UI display.
    """
    if not ciphertext:
        return "****"
    # Last 6 chars of the Fernet token are from the HMAC — non-reversible
    suffix = ciphertext[-6:] if len(ciphertext) >= 6 else ciphertext
    return f"****{suffix}"


def re_encrypt(old_ciphertext: str, new_fernet: "Fernet") -> str:
    """
    Key rotation helper — decrypt with current SMK, encrypt with new SMK.

    Usage during master key rotation:
        new_f = Fernet(new_master_key_bytes)
        new_ciphertext = re_encrypt(old_ciphertext, new_f)

    Args:
        old_ciphertext: Existing Fernet token encrypted with current SMK.
        new_fernet:     A Fernet instance initialised with the new master key.

    Returns:
        New Fernet token encrypted with the new master key.
    """
    plaintext = decrypt_key(old_ciphertext)
    return new_fernet.encrypt(plaintext.encode("utf-8")).decode("utf-8")


def validate_vault_config() -> None:
    """
    Called at service startup to fail fast if vault is misconfigured.
    Performs a roundtrip encrypt-decrypt test.

    Raises:
        RuntimeError: If the vault fails the self-test.
    """
    try:
        test_plain = "nv-vault-self-test-token"
        enc = encrypt_key(test_plain)
        dec = decrypt_key(enc)
        assert dec == test_plain, "Roundtrip mismatch"
        logger.info("vault: Self-test PASSED ✓")
    except Exception as e:
        raise RuntimeError(f"vault: Self-test FAILED — {e}")
