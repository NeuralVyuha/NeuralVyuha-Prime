import unittest
from unittest.mock import MagicMock, patch
import json
import sys
import os

# Add app directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../app')))

# Set environment variables BEFORE import
os.environ['KAFKA_BOOTSTRAP_SERVERS'] = 'localhost:9092'
os.environ['REDIS_HOST'] = 'localhost'
os.environ['REDIS_PORT'] = '6379'
os.environ['DEDUP_WINDOW_SECONDS'] = '600'
os.environ['AUTO_OFFSET_RESET'] = 'latest'

# Import main
try:
    import main
except ImportError as e:
    print(f"Import failed: {e}")
    sys.exit(1)

class TestDedupService(unittest.TestCase):

    def setUp(self):
        self.mock_redis = MagicMock()
        self.mock_producer = MagicMock()

    @patch('main.generate_fingerprint')
    def test_process_event_accepted(self, mock_fingerprint):
        # Setup
        mock_fingerprint.return_value = "fp1"
        event = {
            "event_id": "e1",
            "trace_id": "t1",
            "payload": {
                "source": "s1", "type": "t1", "sourceRef": "ref1",
                "tenant_id": "tenantA"
            }
        }

        # Redis set returns True (new key)
        self.mock_redis.set.return_value = True

        # Execute
        main.process_event(event, self.mock_redis, self.mock_producer)

        # Verify
        # Check Redis SETNX called (first call)
        # There might be multiple calls (dedup check, then count set)
        calls = self.mock_redis.set.call_args_list
        self.assertTrue(len(calls) >= 1)

        dedup_call = calls[0]
        args, kwargs = dedup_call
        self.assertTrue(kwargs.get('nx'))
        self.assertIn("dedup:tenantA:fp1", args[0])

        # Check Producer sends Accepted
        self.mock_producer.send.assert_called_once()
        topic = self.mock_producer.send.call_args[0][0]
        value = self.mock_producer.send.call_args[1]['value']

        self.assertEqual(topic, main.OUTPUT_TOPIC_ACCEPTED)
        self.assertEqual(value['type'], "AlertAccepted")
        self.assertEqual(value['payload']['decision'], "accepted")

    @patch('main.generate_fingerprint')
    def test_process_event_duplicate(self, mock_fingerprint):
        # Setup
        mock_fingerprint.return_value = "fp1"
        event = {
            "event_id": "e2",
            "trace_id": "t2",
            "payload": {
                "source": "s1", "type": "t1", "sourceRef": "ref1",
                "tenant_id": "tenantA"
            }
        }

        # Redis set returns None (key exists)
        self.mock_redis.set.return_value = None
        self.mock_redis.incr.return_value = 2

        # Execute
        main.process_event(event, self.mock_redis, self.mock_producer)

        # Verify
        # Check Producer sends Duplicate
        self.mock_producer.send.assert_called_once()
        topic = self.mock_producer.send.call_args[0][0]
        value = self.mock_producer.send.call_args[1]['value']

        self.assertEqual(topic, main.OUTPUT_TOPIC_DUPLICATE)
        self.assertEqual(value['type'], "AlertDuplicateRejected")
        self.assertEqual(value['payload']['decision'], "rejected")
        self.assertEqual(value['payload']['count'], 2)

    def test_process_event_missing_tenant(self):
        # Setup
        event = {
            "event_id": "e3",
            "trace_id": "t3",
            "payload": {
                "source": "s1", "type": "t1", "sourceRef": "ref1"
                # Missing tenant_id
            }
        }

        # Execute
        main.process_event(event, self.mock_redis, self.mock_producer)

        # Verify
        # Check DLQ
        self.mock_producer.send.assert_called_once()
        topic = self.mock_producer.send.call_args[0][0]
        value = self.mock_producer.send.call_args[1]['value']

        self.assertEqual(topic, main.DLQ_TOPIC)
        self.assertEqual(value['error'], "Missing tenant_id")

if __name__ == '__main__':
    unittest.main()
