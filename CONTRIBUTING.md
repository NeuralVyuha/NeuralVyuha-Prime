# Contributing to NeuralVyuha Prime

First off, thank you for considering contributing to NeuralVyuha! It's people like you that make open-source software such a great community.

## Development Environment Setup

NeuralVyuha runs as a microservice architecture. The easiest way to get started is by spinning up the entire stack locally using Docker Compose.

1. **Clone the repository:**
   ```bash
   git clone https://github.com/NiteshKS22/NeuralVyuha-Prime.git
   cd NeuralVyuha-Prime
   ```

2. **Set up your environment variables:**
   ```bash
   cp .env.example .env
   # Edit .env with your specific keys if needed (e.g., GEMINI_API_KEY)
   ```

3. **Install bower components:**
   ```bash
   cd nv-ui
   npx bower install
   cd ..
   ```

4. **Boot the stack:**
   ```bash
   docker-compose -f docker-compose.slim.yml up --build
   ```

5. **Verify it's running:**
   - The UI should be available at `http://localhost:9000`
   - The Query API should be available at `http://localhost:8000`

## Architecture Overview

If you want to understand how the services communicate, please read our master architecture document: [`agents.md`](agents.md).

- `/nv-core`: Python FastAPI Microservices.
- `/nv-ui`: The AngularJS frontend.

## Pull Request Process

1. Create a new branch for your feature (`git checkout -b feature/amazing-feature`)
2. Make your changes and commit them (`git commit -m 'Add some amazing feature'`)
3. Ensure the project still builds and tests pass locally.
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request targeting the `develop` branch.

All PRs must pass the automated GitHub Actions CI pipeline before they can be merged.

## Reporting Bugs
Please use the GitHub Issue Tracker and select the "Bug Report" template. Include as much detail as possible, including logs if the UI crashed or API returned a 500.
