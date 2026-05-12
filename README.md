# NeuralVyuha (formerly NeuralVyuha v5-Internal)

## Overview
**NeuralVyuha** is the next-generation Security Incident Response Platform (SIRP) engine, evolved from NeuralVyuha v5-Internal project. It is designed to be the "Neural System" of a modern Security Operations Center (SOC), providing high-performance ingestion, correlation, and case management capabilities.

This repository contains the complete source code for the NeuralVyuha engine (`nv-core`) alongside the legacy v4-LTS components it is designed to strangle and eventually replace.

## Key Features
*   **High-Volume Ingestion**: Stateless `nv-ingest` service capable of handling massive alert streams.
*   **Real-Time Correlation**: `nv-correlation` engine for grouping related alerts into meaningful incidents.
*   **Deduplication**: Redis-backed `nv-dedup` to reduce noise and alert fatigue.
*   **Unified Case Management**: `nv-case-engine` serves as the master of record for investigations.
*   **Legacy Compatibility**: Seamlessly integrates with existing v4 frontends via `reverse-bridge` synchronization.

## Architecture
The system is composed of several microservices orchestrated via the `nv-mesh` Docker network:

*   **nv-ingest**: Alert ingestion and validation.
*   **nv-dedup**: Alert deduplication and cross-correlation.
*   **nv-correlation**: Incident grouping logic.
*   **nv-case-engine**: Core domain logic for cases, tasks, and observables.
*   **nv-query**: Unified read API for dashboards and reports.
*   **nv-indexer**: OpenSearch indexing for fast search.

## Getting Started

### Prerequisites
*   Docker & Docker Compose
*   Python 3.10+ (for local development)
*   Java 8 (for legacy component builds)

### Running the Stack
To launch the full NeuralVyuha stack locally for development:

```bash
docker-compose -f docker-compose.slim.yml -f docker-compose.dev.yml up --build
```

This will start Redpanda (Kafka), Redis, Postgres, OpenSearch, and hot-reload all core microservices. The UI will be available at `http://localhost:80`.

### Documentation & Contribution
Detailed architectural documentation and the system blueprint can be found in our comprehensive agents guide:
*   [Agents Guide & Architecture](agents.md)

If you'd like to contribute, please read our contribution guidelines:
*   [Contributing to NeuralVyuha](CONTRIBUTING.md)

## License
This project is licensed under the Apache 2.0 License. See [LICENSE](LICENSE) for details.

## Contact
If you encounter any issues or have feature requests, please use the [GitHub Issues](https://github.com/NeuralVyuha/NeuralVyuha-Prime/issues) page.
