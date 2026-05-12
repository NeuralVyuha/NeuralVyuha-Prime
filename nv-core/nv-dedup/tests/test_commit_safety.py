import unittest
from unittest.mock import MagicMock, patch
import sys
import os

# Add common to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../common')))

from reliability.commit import commit_if_safe

class TestDedupCommitSafety(unittest.TestCase):
    def test_dedup_uses_safe_commit(self):
        # This is a conceptual test ensuring the helper works as expected in this context
        mock_consumer = MagicMock()
        
        # Scenario: Processing success
        result = commit_if_safe(mock_consumer, True, False)
        self.assertTrue(result)
        mock_consumer.commit.assert_called()
        
        # Scenario: Failure
        mock_consumer.reset_mock()
        result = commit_if_safe(mock_consumer, False, False)
        self.assertFalse(result)
        mock_consumer.commit.assert_not_called()

if __name__ == '__main__':
    unittest.main()
