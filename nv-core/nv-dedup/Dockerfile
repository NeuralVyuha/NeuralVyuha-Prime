FROM python:3.10-slim

WORKDIR /app

COPY nv-dedup/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY nv-dedup/app/ ./app
COPY common /app/common

# Run as non-root
RUN useradd -m worker && chown -R worker:worker /app
USER worker

CMD ["python", "app/main.py"]
