import unittest
from unittest.mock import patch, MagicMock
import os
import json
import sys
import os

# Fix path to allow importing from parent
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from common.auth.oidc import validate_oidc_config, fetch_jwks, get_signing_key

class TestOIDC(unittest.TestCase):

    @patch.dict(os.environ, {"JWT_ALGORITHM": "HS256", "DEV_MODE": "false"})
    def test_validate_hs256_prod_fails(self):
        with self.assertRaises(RuntimeError):
            validate_oidc_config()

    @patch.dict(os.environ, {"JWT_ALGORITHM": "RS256", "OIDC_ISSUER": "iss", "JWKS_URL": "url"})
    def test_validate_rs256_success(self):
        validate_oidc_config() # Should not raise

    @patch("requests.get")
    @patch.dict(os.environ, {"JWKS_URL": "http://mock"})
    def test_fetch_jwks(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"keys": [{"kid": "1", "kty": "RSA"}]}
        mock_get.return_value = mock_resp
        
        jwks = fetch_jwks(force_refresh=True)
        self.assertEqual(jwks["keys"][0]["kid"], "1")

if __name__ == "__main__":
    unittest.main()
