FROM python:3.10-slim

WORKDIR /app

COPY nv-query/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY nv-query/app/ ./app
COPY common /app/common

# Run as non-root
RUN useradd -m api && chown -R api:api /app
USER api

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
