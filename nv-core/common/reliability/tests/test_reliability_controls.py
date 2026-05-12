import unittest
from unittest.mock import MagicMock, patch
import time
from ..retry import execute_with_retry
from ..backpressure import check_backpressure

class TestReliabilityControls(unittest.TestCase):

    def test_execute_with_retry_success(self):
        mock_func = MagicMock(return_value="ok")
        res = execute_with_retry(mock_func)
        self.assertEqual(res, "ok")
        mock_func.assert_called_once()

    def test_execute_with_retry_failure_then_success(self):
        mock_func = MagicMock(side_effect=[ValueError("fail"), "success"])
        res = execute_with_retry(mock_func, retryable_exceptions=(ValueError,))
        self.assertEqual(res, "success")
        self.assertEqual(mock_func.call_count, 2)

    def test_execute_with_retry_exhausted(self):
        mock_func = MagicMock(side_effect=ValueError("fail"))
        with self.assertRaises(ValueError):
            execute_with_retry(mock_func, max_retries=2, base_delay=0.01)
        self.assertEqual(mock_func.call_count, 3) # Initial + 2 retries

    @patch('time.sleep')
    def test_backpressure_active(self, mock_sleep):
        check_backpressure(start_time=time.time() - 10, batch_size=10, threshold_ms=1000)
        mock_sleep.assert_called_with(1.0)

    @patch('time.sleep')
    def test_backpressure_inactive(self, mock_sleep):
        check_backpressure(start_time=time.time(), batch_size=10, threshold_ms=5000)
        mock_sleep.assert_not_called()

if __name__ == "__main__":
    unittest.main()
