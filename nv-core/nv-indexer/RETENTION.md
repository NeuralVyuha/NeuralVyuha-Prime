# Index Retention Strategy

## Index Pattern
Indices are time-partitioned monthly:
`alerts-v1-YYYY.MM` (e.g., `alerts-v1-2024.01`)

## Default Policy (Planned)
- **Hot**: Current month (SSD, fast access).
- **Warm**: Past 3 months (Standard disk, slower).
- **Cold**: Past 12 months (Archival storage).
- **Delete**: > 12 months.

## Implementation (Phase 3C)
Currently managed via OpenSearch Index State Management (ISM) policies (not yet automated in CI).
Manual cleanup is required for dev environments.

## Operations
To manually delete old indices:
```bash
curl -X DELETE "http://localhost:9200/alerts-v1-2023.12"
```
