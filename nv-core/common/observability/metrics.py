import logging
import time
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger("metrics")

# Standard Labels
COMMON_LABELS = ['service', 'env']

# Metrics Registry (Global)
REQUESTS_TOTAL = Counter(
    'requests_total', 'Total HTTP requests',
    COMMON_LABELS + ['method', 'route', 'status_code']
)
REQUEST_LATENCY = Histogram(
    'request_latency_seconds', 'HTTP request latency',
    COMMON_LABELS + ['method', 'route']
)
MESSAGES_PROCESSED = Counter(
    'messages_processed_total', 'Total Kafka messages processed',
    COMMON_LABELS + ['topic', 'status']
)
DLQ_PUBLISHED = Counter(
    'dlq_published_total', 'Total messages sent to DLQ',
    COMMON_LABELS + ['topic']
)
CONSUMER_LAG = Gauge(
    'consumer_lag', 'Approximate consumer lag',
    COMMON_LABELS + ['topic', 'partition']
)
SERVICE_UPTIME = Gauge(
    'service_uptime_seconds', 'Service uptime',
    COMMON_LABELS
)

class MetricsMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, service_name, env="prod"):
        super().__init__(app)
        self.service_name = service_name
        self.env = env
        SERVICE_UPTIME.labels(service=service_name, env=env).set_function(lambda: time.time() - self.start_time)
        self.start_time = time.time()

    async def dispatch(self, request: Request, call_next):
        if request.url.path in ["/metrics", "/healthz", "/readyz"]:
            return await call_next(request)

        start_time = time.time()
        response = await call_next(request)
        duration = time.time() - start_time

        route = request.url.path
        # Simplify route if parameterized (basic heuristic)
        # In FastAPI, we might access request.scope['route'].path if strictly needed,
        # but this is a lightweight middleware.

        REQUESTS_TOTAL.labels(
            service=self.service_name,
            env=self.env,
            method=request.method,
            route=route,
            status_code=response.status_code
        ).inc()

        REQUEST_LATENCY.labels(
            service=self.service_name,
            env=self.env,
            method=request.method,
            route=route
        ).observe(duration)

        return response

def get_metrics_response():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
