import unittest
from unittest.mock import MagicMock
from ..dlq import build_dlq_event, send_dlq
from ..commit import commit_if_safe

class TestReliability(unittest.TestCase):

    def test_build_dlq_event(self):
        event = build_dlq_event(
            reason="Validation Error",
            original_message={"foo": "bar"},
            source_topic="in-topic",
            source_partition=0,
            source_offset=100
        )
        self.assertEqual(event["type"], "NeuralDLQEvent")
        self.assertEqual(event["payload"]["reason"], "Validation Error")
        self.assertEqual(event["payload"]["source_offset"], 100)

    def test_send_dlq_success(self):
        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_producer.send.return_value = mock_future
        
        success = send_dlq(mock_producer, "dlq-topic", {})
        
        self.assertTrue(success)
        mock_producer.send.assert_called()
        mock_future.get.assert_called()

    def test_send_dlq_failure(self):
        mock_producer = MagicMock()
        mock_producer.send.side_effect = Exception("Kafka Down")
        
        success = send_dlq(mock_producer, "dlq-topic", {})
        
        self.assertFalse(success)

    def test_commit_if_safe_processing_success(self):
        mock_consumer = MagicMock()
        committed = commit_if_safe(mock_consumer, processing_success=True, dlq_success=False)
        self.assertTrue(committed)
        mock_consumer.commit.assert_called()

    def test_commit_if_safe_dlq_success(self):
        mock_consumer = MagicMock()
        committed = commit_if_safe(mock_consumer, processing_success=False, dlq_success=True)
        self.assertTrue(committed)
        mock_consumer.commit.assert_called()

    def test_commit_if_safe_failure(self):
        mock_consumer = MagicMock()
        committed = commit_if_safe(mock_consumer, processing_success=False, dlq_success=False)
        self.assertFalse(committed)
        mock_consumer.commit.assert_not_called()

if __name__ == "__main__":
    unittest.main()
