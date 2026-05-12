FROM python:3.12-slim

WORKDIR /app

COPY nv-mcp-server/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY nv-mcp-server/app app/
COPY common /app/common

# Non-root user for production security
RUN adduser --disabled-password --gecos "" nvuser
USER nvuser

EXPOSE 8087

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8087", "--workers", "1"]
