import time
import logging
from typing import Callable, Any

logger = logging.getLogger("retry-helper")

def execute_with_retry(
    func: Callable[[], Any],
    max_retries: int = 3,
    base_delay: float = 1.0,
    retryable_exceptions: tuple = (Exception,)
) -> Any:
    """
    Executes a function with exponential backoff retry.
    Raises the last exception if retries are exhausted.
    """
    attempt = 0
    while attempt <= max_retries:
        try:
            return func()
        except retryable_exceptions as e:
            attempt += 1
            if attempt > max_retries:
                logger.error(f"Operation failed after {attempt} attempts: {e}")
                raise e
            
            delay = base_delay * (2 ** (attempt - 1))
            logger.warning(f"Attempt {attempt}/{max_retries} failed: {e}. Retrying in {delay}s...")
            time.sleep(delay)
