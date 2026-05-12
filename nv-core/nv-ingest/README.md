# Alert Ingestion Service

## Overview
Stateless microservice for validating and publishing alerts to the v5 event spine.

## API
- `POST /ingest`: Accepts alerts. Requires `Idempotency-Key` header.
- `GET /health`: Liveness probe.

## Configuration
- `KAFKA_BOOTSTRAP_SERVERS`: Address of Redpanda/Kafka (default: `redpanda:29092`).
- `TOPIC_NAME`: Target topic (default: `alerts.ingest.v1`).
