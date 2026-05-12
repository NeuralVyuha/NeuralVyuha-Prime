import unittest
from unittest.mock import MagicMock, patch
import os
import sys
import pytest

# Add common to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from common.auth.middleware import validate_auth_config

class TestStartupGuardrail(unittest.TestCase):

    @patch.dict('os.environ', {'JWT_ALGORITHM': 'RS256', 'JWKS_URL': '', 'JWT_SECRET': '', 'DEV_MODE': 'false'})
    def test_startup_fails_rs256_no_key(self):
        # Simulate insecure config

        with self.assertRaises(RuntimeError) as cm:
            validate_auth_config()
        self.assertIn("RS256 requires OIDC_ISSUER and JWKS_URL.", str(cm.exception))

    @patch.dict('os.environ', {'JWT_ALGORITHM': 'HS256', 'JWKS_URL': '', 'JWT_SECRET': 'some-secret', 'DEV_MODE': 'true'})
    def test_startup_passes_hs256(self):
        # Simulate valid HS256 config
        # Should not raise
        validate_auth_config()

if __name__ == "__main__":
    unittest.main()
