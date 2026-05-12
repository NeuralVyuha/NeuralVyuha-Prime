import time
import logging
from fastapi import Request, HTTPException, status
from starlette.middleware.base import BaseHTTPMiddleware
from collections import defaultdict
from threading import Lock

logger = logging.getLogger("rate-limiter")

# Simple In-Memory Token Bucket (Proof of Concept for E5)
# In production, use Redis.
class RateLimiter:
    def __init__(self, rate: int = 100, per: int = 60):
        self.rate = rate
        self.per = per
        self.buckets = defaultdict(lambda: {"tokens": rate, "last_update": time.time()})
        self.lock = Lock()

    def check(self, key: str) -> bool:
        with self.lock:
            now = time.time()
            bucket = self.buckets[key]
            elapsed = max(0, now - bucket["last_update"])
            
            # Refill
            tokens_to_add = elapsed * (self.rate / self.per)
            bucket["tokens"] = min(self.rate, bucket["tokens"] + tokens_to_add)
            bucket["last_update"] = now
            
            if bucket["tokens"] >= 1:
                bucket["tokens"] -= 1
                return True
            else:
                return False

# We implement it as a Dependency that takes AuthContext
from ..auth.middleware import AuthContext

def rate_limit(limit: int = 100, window: int = 60):
    limiter = RateLimiter(rate=limit, per=window)
    
    def dependency(auth: AuthContext):
        if not limiter.check(auth.tenant_id):
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Rate limit exceeded",
                headers={"Retry-After": str(window)}
            )
        return True
    return dependency
