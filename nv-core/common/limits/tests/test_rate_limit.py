import unittest
import time
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from common.limits.rate_limit import RateLimiter

class TestRateLimiter(unittest.TestCase):
    def test_allow_within_limit(self):
        limiter = RateLimiter(rate=5, per=1)
        for _ in range(5):
            self.assertTrue(limiter.check("tenant-A"))
            
    def test_block_exceed_limit(self):
        limiter = RateLimiter(rate=1, per=60)
        self.assertTrue(limiter.check("tenant-B"))
        self.assertFalse(limiter.check("tenant-B"))
        
    def test_refill(self):
        limiter = RateLimiter(rate=1, per=1)
        self.assertTrue(limiter.check("tenant-C"))
        self.assertFalse(limiter.check("tenant-C"))
        time.sleep(1.1)
        self.assertTrue(limiter.check("tenant-C"))

if __name__ == "__main__":
    unittest.main()
