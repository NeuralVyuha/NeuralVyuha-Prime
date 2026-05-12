import time
import logging

logger = logging.getLogger("backpressure-helper")

def check_backpressure(
    start_time: float,
    batch_size: int,
    threshold_ms: int = 5000,
    sleep_sec: float = 1.0
):
    """
    Checks if processing time exceeded threshold. If so, sleeps to throttle.
    """
    duration = (time.time() - start_time) * 1000
    if duration > threshold_ms:
        logger.info(f"Backpressure Active: Batch of {batch_size} took {duration:.2f}ms. Sleeping {sleep_sec}s.")
        time.sleep(sleep_sec)
