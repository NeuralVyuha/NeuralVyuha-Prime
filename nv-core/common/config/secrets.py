import os
import logging

logger = logging.getLogger("secrets-loader")

def get_secret(name: str, default: str = None, required: bool = True) -> str:
    """
    Retrieves a secret from /run/secrets/{name} (Docker/K8s) or ENV.
    """
    # 1. Try file (Docker Swarm / K8s style)
    file_path = f"/run/secrets/{name}"
    try:
        if os.path.exists(file_path):
            with open(file_path, "r") as f:
                value = f.read().strip()
                if value:
                    return value
    except Exception as e:
        logger.warning(f"Failed to read secret file {file_path}: {e}")

    # 2. Try Environment Variable
    value = os.getenv(name)
    if value:
        return value

    # 3. Default or Fail
    if default is not None:
        return default
    
    if required:
        # Check if we are in DEV mode to allow some leniency or fail fast
        # But per requirements: fail fast if required.
        msg = f"CRITICAL: Missing required secret: {name}"
        logger.critical(msg)
        raise RuntimeError(msg)
    
    return None

def redact(value: str) -> str:
    """
    Redacts a string for logging (first 2 chars visible, rest ***).
    """
    if not value:
        return "<None>"
    if len(value) < 4:
        return "***"
    return f"{value[:2]}***{value[-1]}"
