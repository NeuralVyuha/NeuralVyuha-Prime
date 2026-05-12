FROM python:3.10-slim

WORKDIR /app

COPY nv-ingest/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY nv-ingest/app/ ./app
COPY common /app/common

# Run as non-root
RUN useradd -m ingestion && chown -R ingestion:ingestion /app
USER ingestion

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
