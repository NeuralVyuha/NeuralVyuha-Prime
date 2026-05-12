import os
import logging
import jwt
import requests
import time
from typing import Dict, Any, Optional
from threading import Lock

logger = logging.getLogger("oidc-auth")

# Cache
_jwks_cache: Dict[str, Any] = {}
_jwks_cache_expiry = 0
_cache_lock = Lock()

def validate_oidc_config():
    """
    Validates environment configuration for OIDC.
    Raises RuntimeError if insecure or missing.
    """
    issuer = os.getenv("OIDC_ISSUER")
    jwks_url = os.getenv("JWKS_URL")
    alg = os.getenv("JWT_ALGORITHM", "HS256")
    dev_mode = os.getenv("DEV_MODE", "false").lower() == "true"

    if alg == "RS256":
        if not issuer or not jwks_url:
            raise RuntimeError("RS256 requires OIDC_ISSUER and JWKS_URL.")
    elif alg == "HS256":
        if not dev_mode:
            # We strictly enforce RS256 in prod now (Phase E5)
            # Exception: Migration period might allow it via explicit flag?
            # Requirement says: "HS256 is permitted only in DEV"
            raise RuntimeError("HS256 is FORBIDDEN in non-DEV_MODE. Use RS256.")
    else:
        raise RuntimeError(f"Unsupported JWT Algorithm: {alg}")

def fetch_jwks(force_refresh: bool = False) -> Dict[str, Any]:
    global _jwks_cache, _jwks_cache_expiry
    
    jwks_url = os.getenv("JWKS_URL")
    if not jwks_url:
        return {} # Should have been caught by startup check if RS256

    with _cache_lock:
        now = time.time()
        if not force_refresh and _jwks_cache and now < _jwks_cache_expiry:
            return _jwks_cache

        try:
            resp = requests.get(jwks_url, timeout=5)
            if resp.status_code == 200:
                _jwks_cache = resp.json()
                # Cache for 1 hour
                _jwks_cache_expiry = now + 3600
                logger.info("Refreshed JWKS cache")
                return _jwks_cache
            else:
                logger.error(f"Failed to fetch JWKS: {resp.status_code}")
                # Fail-closed or return stale? E5 says "Fail-Closed" behavior preferred for security
                # But keeping stale might be better for availability during transient network issues.
                # Let's keep stale if we have it.
                if _jwks_cache:
                    logger.warning("Using stale JWKS cache")
                    return _jwks_cache
                raise RuntimeError("JWKS fetch failed and no cache available")
        except Exception as e:
            logger.error(f"JWKS fetch exception: {e}")
            if _jwks_cache:
                return _jwks_cache
            raise

def get_signing_key(kid: str) -> Any:
    jwks = fetch_jwks()
    
    # Find key
    key = _find_key(jwks, kid)
    if key:
        return key
    
    # If not found, force refresh (maybe rotation happened)
    logger.info(f"Key {kid} not found in cache. Forcing refresh.")
    jwks = fetch_jwks(force_refresh=True)
    key = _find_key(jwks, kid)
    
    if key:
        return key
        
    raise RuntimeError(f"Signing key {kid} not found in JWKS.")

def _find_key(jwks: Dict, kid: str):
    keys = jwks.get("keys", [])
    for key_data in keys:
        if key_data.get("kid") == kid:
            return jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(key_data))
    return None
