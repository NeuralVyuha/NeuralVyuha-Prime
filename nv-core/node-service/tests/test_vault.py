"""
test_vault.py — Unit tests for nv-node-service Secret Vault
Phase Z2.1

CI Assertions:
  ✓ Encrypt → Decrypt roundtrip returns original plaintext.
  ✓ Masked key NEVER contains the original plaintext key.
  ✓ Decryption with wrong key raises InvalidToken.
  ✓ Empty key raises ValueError (not stored).
  ✓ mask_key returns predictable suffix format.
"""

import os
import sys
import unittest

# ── Set test master key before importing vault ──────────────────────────────
# Must be a valid Fernet key (32 bytes, base64-url-safe with padding)
from cryptography.fernet import Fernet
_TEST_KEY = Fernet.generate_key().decode("utf-8")
os.environ["NODE_MASTER_KEY"] = _TEST_KEY

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../app')))

# Re-import after env is set so vault picks up the test key
import importlib
import vault as vault_module

# Reinitialise after env var set
os.environ["NODE_MASTER_KEY"] = _TEST_KEY
importlib.reload(vault_module)

encrypt_key          = vault_module.encrypt_key
decrypt_key          = vault_module.decrypt_key
mask_key             = vault_module.mask_key
validate_vault_config = vault_module.validate_vault_config


class TestVaultRoundtrip(unittest.TestCase):

    def test_encrypt_decrypt_roundtrip(self):
        """Core invariant: encrypt then decrypt must return original key."""
        for plaintext in [
            "mySuperSecretCortexApiKey123!",
            "misp-api-key-abc-xyz-9999",
            "a" * 200,  # Long key
            "key with spaces and symbols !@#$%",
        ]:
            with self.subTest(plaintext=plaintext[:20]):
                enc = encrypt_key(plaintext)
                dec = decrypt_key(enc)
                self.assertEqual(dec, plaintext,
                    f"Roundtrip failed for key starting with: {plaintext[:20]}")

    def test_ciphertext_is_different_every_time(self):
        """Fernet uses a random IV — same plaintext must produce different ciphertexts."""
        key = "same-api-key-value"
        enc1 = encrypt_key(key)
        enc2 = encrypt_key(key)
        self.assertNotEqual(enc1, enc2,
            "Ciphertexts must differ (Fernet uses random IV)")

    def test_ciphertext_not_plaintext(self):
        """Ciphertext must not contain the original key."""
        api_key = "top-secret-key-do-not-expose"
        enc = encrypt_key(api_key)
        self.assertNotIn(api_key, enc,
            "Ciphertext must not contain plaintext key")


class TestVaultMasking(unittest.TestCase):

    def test_mask_never_shows_plaintext(self):
        """The masked key must never reveal the original API key."""
        api_key   = "my-sensitive-cortex-key"
        enc       = encrypt_key(api_key)
        masked    = mask_key(enc)
        self.assertNotIn(api_key, masked,
            "Masked output must not contain plaintext key")

    def test_mask_starts_with_stars(self):
        """Masked key format: '****<suffix>'."""
        enc    = encrypt_key("any-key")
        masked = mask_key(enc)
        self.assertTrue(masked.startswith("****"),
            f"Masked key should start with '****', got: {masked}")

    def test_mask_format_stable(self):
        """Masking the same ciphertext twice gives the same result."""
        enc = encrypt_key("stable-key")
        self.assertEqual(mask_key(enc), mask_key(enc))

    def test_mask_empty_string(self):
        """mask_key on empty string returns safe fallback."""
        self.assertEqual(mask_key(""), "****")

    def test_mask_short_ciphertext(self):
        """mask_key handles ciphertexts shorter than suffix length."""
        self.assertEqual(mask_key("AB"), "****AB")


class TestVaultEdgeCases(unittest.TestCase):

    def test_empty_key_raises_error(self):
        """Encrypting an empty key must raise ValueError (prevents blank-key storage)."""
        with self.assertRaises(ValueError):
            encrypt_key("")
        with self.assertRaises(ValueError):
            encrypt_key("   ")  # whitespace only

    def test_wrong_master_key_raises_invalid_token(self):
        """Decrypting with a different Fernet key must raise InvalidToken."""
        from cryptography.fernet import Fernet, InvalidToken
        other_fernet = Fernet(Fernet.generate_key())

        # Encrypt with current key
        enc = encrypt_key("my-real-key")

        # Try to decrypt with a DIFFERENT key → must fail
        with self.assertRaises(InvalidToken):
            other_fernet.decrypt(enc.encode("utf-8"))

    def test_tampered_ciphertext_raises(self):
        """A tampered ciphertext must raise InvalidToken."""
        from cryptography.fernet import InvalidToken
        enc = encrypt_key("real-key")
        tampered = enc[:-10] + "AAAAAAAAAA"  # Corrupt the HMAC
        with self.assertRaises(Exception):  # InvalidToken or binascii.Error
            decrypt_key(tampered)


class TestVaultSelfTest(unittest.TestCase):

    def test_validate_vault_config_passes(self):
        """The startup self-test must pass when NODE_MASTER_KEY is valid."""
        # Should not raise
        try:
            validate_vault_config()
        except RuntimeError as e:
            self.fail(f"validate_vault_config raised unexpectedly: {e}")


if __name__ == "__main__":
    unittest.main()
