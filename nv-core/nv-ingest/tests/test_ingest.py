import unittest
from unittest.mock import MagicMock, patch
import sys
import os

# Add app directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../app')))

# Setup clean mocks for kafka
kafka_mock = MagicMock()
# We need KafkaError to be an exception class
class MockKafkaError(Exception): pass
kafka_mock.errors.KafkaError = MockKafkaError
kafka_mock.KafkaProducer = MagicMock()

sys.modules['kafka'] = kafka_mock
sys.modules['kafka.errors'] = kafka_mock.errors

# Now import main
from main import app
from fastapi.testclient import TestClient

client = TestClient(app)

class TestIngestionService(unittest.TestCase):

    def setUp(self):
        # We need to ensure main.producer is our mock
        # Since we mocked KafkaProducer class, main.producer might be a Mock instance already (if initialization succeeded)
        # Or None if it failed.
        # But since we mocked KafkaProducer, the constructor shouldn't fail unless we told it to.
        # It returns a MagicMock by default.

        # We want to assert on calls to this producer.
        # We can get the producer from main.
        import main
        self.mock_producer = main.producer
        # Reset mocks
        if self.mock_producer:
            self.mock_producer.reset_mock()
        else:
            # If main.producer is None (exception in try/except), we inject a mock
            self.mock_producer = MagicMock()
            main.producer = self.mock_producer

    def test_ingest_with_tenant_id(self):
        payload = {
            "source": "test",
            "type": "alert",
            "sourceRef": "ref1",
            "title": "Test Alert",
            "tenant_id": "tenant-custom"
        }

        headers = {"Idempotency-Key": "key1"}

        response = client.post("/ingest", json=payload, headers=headers)

        self.assertEqual(response.status_code, 202)

        # Verify Kafka message
        self.mock_producer.send.assert_called_once()
        topic = self.mock_producer.send.call_args[0][0]
        event = self.mock_producer.send.call_args[1]['value']

        self.assertEqual(event['payload']['tenant_id'], "tenant-custom")

    def test_ingest_default_tenant_id(self):
        payload = {
            "source": "test",
            "type": "alert",
            "sourceRef": "ref1",
            "title": "Test Alert"
            # Missing tenant_id
        }

        headers = {"Idempotency-Key": "key2"}

        response = client.post("/ingest", json=payload, headers=headers)

        self.assertEqual(response.status_code, 202)

        # Verify Kafka message
        self.mock_producer.send.assert_called_once()
        event = self.mock_producer.send.call_args[1]['value']

        self.assertEqual(event['payload']['tenant_id'], "default")

if __name__ == '__main__':
    unittest.main()
