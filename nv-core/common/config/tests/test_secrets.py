import unittest
from unittest.mock import patch, mock_open
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from common.config.secrets import get_secret, redact

class TestSecrets(unittest.TestCase):

    @patch("os.path.exists", return_value=True)
    @patch("builtins.open", new_callable=mock_open, read_data="file_secret_value")
    def test_get_secret_from_file(self, mock_file, mock_exists):
        val = get_secret("MY_SECRET")
        self.assertEqual(val, "file_secret_value")

    @patch("os.path.exists", return_value=False)
    @patch.dict(os.environ, {"MY_SECRET": "env_secret_value"})
    def test_get_secret_from_env(self, mock_exists):
        val = get_secret("MY_SECRET")
        self.assertEqual(val, "env_secret_value")

    @patch("os.path.exists", return_value=False)
    @patch.dict(os.environ, {}, clear=True)
    def test_get_secret_missing_required(self, mock_exists):
        with self.assertRaises(RuntimeError):
            get_secret("MISSING_SECRET", required=True)

    def test_redact(self):
        self.assertEqual(redact("123456"), "12***6")
        self.assertEqual(redact("123"), "***")
        self.assertEqual(redact(None), "<None>")

if __name__ == "__main__":
    unittest.main()
