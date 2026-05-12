import pytest
from fastapi.testclient import TestClient
import sys
import os
from unittest.mock import MagicMock, patch

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../app')))
# Mock middleware imports before importing main
sys.modules['auth.middleware'] = MagicMock()
sys.modules['auth.rbac'] = MagicMock()

from app.main import app

client = TestClient(app)

def test_health():
    # Mock get_db_conn
    with patch('app.main.get_db_conn') as mock_db:
        mock_db.return_value.close = MagicMock()
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()['status'] == "ok"
