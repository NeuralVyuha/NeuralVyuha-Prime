# NeuralVyuha (Project Agents Guide)

This document serves as the master technical blueprint for **NeuralVyuha**, a next-generation Security Incident Response Platform (SIRP). It details the architecture, component locations, and how they interact.

---

## 1. Project Overview
NeuralVyuha is a microservices-based evolution of the legacy NeuralVyuha project. It utilizes the **Strangler Fig Pattern** to replace monolithic functions with stateless, high-performance microservices.

- **Stack**: Python (FastAPI), Redis, Redpanda (Kafka), OpenSearch, PostgreSQL, AngularJS (Legacy UI).
- **Core Vision**: Real-time alert ingestion, automated correlation (The Detective), and sovereign AI-assisted investigation.

---

## 2. Global Structure & Locations

### Root Directory (`/`)
- `docker-compose.slim.yml`: The master orchestration file for all 15+ services.
- `init.sql`: The consolidated database initialization script.
- `.env.example`: Template for required environment variables (Gemini API, etc.).
- `README.md`: High-level project introduction.
- `docs/`: Extensive documentation on ADRs, architecture, and system state.

---

## 3. Core Microservices (`nv-core/`)

Each service is located in `nv-core/<service-name>/` and follows a similar structure: `app/main.py` for logic, `Dockerfile` for containerization, and `requirements.txt`.

### [nv-ingest](file:///c:/Users/NITESH%20KUMAR/Downloads/neuralvyuha-working/nv-core/nv-ingest) (The Gateway)
- **Role**: Entry point for all raw alerts (Wazuh, Suricata, etc.).
- **Logic**: Handles rate limiting, authentication, and routing (DROP/LOG/ALERT).
- **Key File**: `app/main.py` - Contains the FastAPI routes and policy engine.

### [nv-dedup](file:///c:/Users/NITESH%20KUMAR/Downloads/neuralvyuha-working/nv-core/nv-dedup) (The Detective)
- **Role**: Deduplication and real-time correlation engine.
- **Logic**: Uses Redis to track alert hashes and groups related events into "Correlation Groups".
- **Key File**: `app/main.py` - Implements the sliding-window deduplication logic.

### [nv-correlation](file:///c:/Users/NITESH%20KUMAR/Downloads/neuralvyuha-working/nv-core/nv-correlation) (Logic Engine)
- **Role**: Higher-level correlation rules (e.g., Brute Force -> Successful Login).
- **Logic**: Evaluates complex rules across multiple sources.

### [nv-case-engine](file:///c:/Users/NITESH%20KUMAR/Downloads/neuralvyuha-working/nv-core/nv-case-engine) (Domain Master)
- **Role**: Source of truth for Cases, Tasks, Observables, and TTPs.
- **Logic**: Business logic for incident lifecycle.
- **Key File**: `app/main.py` - Handles CRUD for the Case management domain.

### [nv-query](file:///c:/Users/NITESH%20KUMAR/Downloads/neuralvyuha-working/nv-core/nv-query) (Unified Read API)
- **Role**: Aggregates data for the UI and provides a search-optimized read-path.
- **Logic**: Dual-reads from Postgres and OpenSearch.
- **Key File**: `app/main.py` - The most complex service, acting as the primary UI backend.

### [nv-mcp-server](file:///c:/Users/NITESH%20KUMAR/Downloads/neuralvyuha-working/nv-core/nv-mcp-server) (AI Threat Copilot)
- **Role**: AI integration layer using the Model Context Protocol.
- **Logic**: Connects to LLMs (Gemini/Ollama) to assist in threat hunting and investigation.
- **Key File**: `app/main.py` - Implements the Copilot chat with investigation caching.

---

## 4. Frontend (`nv-ui/`)
- **Role**: The Administrative and Investigation interface.
- **Locations**:
    - `app/views/nv/`: HTML templates for rewritten v5 components (Admin, Health, etc.).
    - `app/scripts/controllers/nv/`: JavaScript logic for the new UI components.
    - `Caddyfile`: Routing configuration for the web server.

---

## 5. Shared Logic (`nv-core/common/`)
- **auth/**: Shared JWT verification and RBAC middleware.
- **observability/**: Health check registry and Prometheus metrics.
- **config/**: Secret management and configuration loading.

---

## 6. Infrastructure & Deployment
- **PostgreSQL**: Stores relational data (Cases, Tasks, Rules).
- **Redis**: Low-latency cache for Deduplication and Rate Limiting.
- **Redpanda**: High-throughput event backbone (Kafka compatible).
- **OpenSearch**: Full-text search and long-term alert storage.

---

## 7. Operational Runbooks
- **Consolidated SQL**: `init.sql` handles all table creation on first boot.
- **Health Checks**: Core services use HTTP probes (`/status` or `/healthz`) for real-time monitoring.
- **Development**: Use `nv-stress-test.py` to verify ingestion performance.

---

## Technical Summary Table

| Component | Location | Port | Primary Responsibility |
| :--- | :--- | :--- | :--- |
| **Ingest** | `nv-core/nv-ingest` | 8090 | Alert Gateway |
| **Query** | `nv-core/nv-query` | 8000 | UI Backend / Read API |
| **Case Engine** | `nv-core/nv-case-engine` | 8080 | Domain Logic |
| **Copilot** | `nv-core/nv-mcp-server` | 8088 | AI Integration |
| **UI** | `nv-ui` | 9000 | Frontend (Admin/Dev) |
