FROM python:3.12-slim

WORKDIR /app

COPY node-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY node-service/app app/
COPY common /app/common

# Non-root user for production security
RUN adduser --disabled-password --gecos "" nvuser
USER nvuser

EXPOSE 8085

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8085", "--workers", "2"]
