import unittest
from unittest.mock import MagicMock, patch
import sys
import os

# Add app directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../app')))

# Mock deps
sys.modules['opensearchpy'] = MagicMock()
sys.modules['psycopg2'] = MagicMock()

from main import app, get_group_alerts

class TestIsolation(unittest.TestCase):

    @patch('main.os_client')
    @patch('main.get_db_conn')
    def test_get_group_alerts_isolation(self, mock_get_db, mock_os):
        # Setup DB mock to return one alert link
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_get_db.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        # DB returns 2 alerts
        mock_cur.fetchall.return_value = [
            ("alert1", 100, "rule1"),
            ("alert2", 101, "rule1")
        ]

        # Setup OpenSearch Mock
        # Return 2 docs: one VALID (tenant A), one MALICIOUS (tenant B)
        mock_os.search.return_value = {
            "hits": {
                "hits": [
                    {"_id": "alert1", "_source": {"title": "Valid", "tenant_id": "tenant-A"}},
                    {"_id": "alert2", "_source": {"title": "Leaked", "tenant_id": "tenant-B"}}
                ]
            }
        }

        # Execute
        result = get_group_alerts("group1", tenant_id="tenant-A")

        # Verify
        # Should only contain alert1
        self.assertEqual(len(result['hits']), 1)
        self.assertEqual(result['hits'][0]['title'], "Valid")

        # Verify call args
        # Should search for both IDs
        call_args = mock_os.search.call_args
        self.assertIn("alert1", str(call_args))
        self.assertIn("alert2", str(call_args))

if __name__ == '__main__':
    unittest.main()
