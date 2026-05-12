import logging
from kafka import KafkaConsumer

logger = logging.getLogger("commit-helper")

def commit_if_safe(
    consumer: KafkaConsumer,
    processing_success: bool,
    dlq_success: bool
) -> bool:
    """
    Decides whether to commit offsets based on processing and DLQ status.
    
    Rule:
    - If processing_success is True -> Commit.
    - If processing_success is False BUT dlq_success is True -> Commit (message handled via DLQ).
    - If processing_success is False AND dlq_success is False -> DO NOT COMMIT (risk of data loss).
    
    Returns True if commit was attempted, False otherwise.
    """
    if processing_success or dlq_success:
        try:
            consumer.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to commit offsets: {e}")
            return False
    else:
        logger.critical("CRITICAL: Processing failed AND DLQ failed. Offsets NOT committed. Replay will occur.")
        return False
