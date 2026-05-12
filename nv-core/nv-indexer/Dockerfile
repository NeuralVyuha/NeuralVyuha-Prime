FROM python:3.10-slim

WORKDIR /app

COPY nv-indexer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY nv-indexer/app/ ./app
COPY common /app/common

# Run as non-root
RUN useradd -m worker && chown -R worker:worker /app
USER worker

CMD ["python", "app/main.py"]
