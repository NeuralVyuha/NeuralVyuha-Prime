import logging
from typing import Dict, Callable, Any
import time

logger = logging.getLogger("health")

class HealthRegistry:
    def __init__(self):
        self._checks: Dict[str, Callable[[], bool]] = {}

    def add_check(self, name: str, check_func: Callable[[], bool]):
        self._checks[name] = check_func

    def check_health(self) -> Dict[str, Any]:
        results = {}
        overall_status = "ok"
        
        for name, check in self._checks.items():
            start = time.time()
            try:
                status = check()
                duration = (time.time() - start) * 1000
                results[name] = {"status": "ok" if status else "fail", "duration_ms": duration}
                if not status:
                    overall_status = "fail"
            except Exception as e:
                logger.error(f"Health check {name} failed: {e}")
                results[name] = {"status": "error", "error": str(e)}
                overall_status = "fail"
        
        return {"status": overall_status, "checks": results}

global_health_registry = HealthRegistry()
