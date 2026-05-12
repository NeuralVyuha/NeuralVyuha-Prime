import time
import uuid
import hashlib
import logging
import sys
import os
import io
import psycopg2
from fastapi import FastAPI, Header, HTTPException, Response, Depends, UploadFile, File, Form
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, List
import boto3
from botocore.exceptions import ClientError
from cryptography.fernet import Fernet

# Robust common library discovery (handles local dev and Docker context)
_base_dir = os.path.dirname(__file__)
_paths_to_check = [
    os.path.abspath(os.path.join(_base_dir, '../../')), # Local dev (parent of common)
    os.path.abspath(os.path.join(_base_dir, '../')),     # Docker (parent of common)
]
for _p in _paths_to_check:
    if os.path.exists(os.path.join(_p, 'common')):
        sys.path.append(_p)
        break

from common.auth.middleware import get_auth_context, AuthContext, validate_auth_config, require_permission
from common.auth.rbac import PERM_CASE_READ, PERM_CASE_WRITE
from common.observability.metrics import MetricsMiddleware, get_metrics_response
from common.observability.health import global_health_registry
from common.config.secrets import get_secret

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger("nv-artifact-service")

app = FastAPI(title="NeuralVyuha Artifact Service", version="1.0.0")
app.add_middleware(MetricsMiddleware, service_name="nv-artifact-service")

# ── Configuration ────────────────────────────────────────────────────────────
POSTGRES_HOST     = get_secret("POSTGRES_HOST", "postgres")
POSTGRES_USER     = get_secret("POSTGRES_USER", "nv_user")
POSTGRES_PASSWORD = get_secret("POSTGRES_PASSWORD", "nv_pass")
POSTGRES_DB       = get_secret("POSTGRES_DB", "nv_vault")

MINIO_ENDPOINT    = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY  = get_secret("MINIO_ACCESS_KEY", "nvadmin")
MINIO_SECRET_KEY  = get_secret("MINIO_SECRET_KEY", "nvadmin123")

PRESIGN_TTL_SECONDS = int(os.getenv("PRESIGN_TTL_SECONDS", "60"))

# ── DB ───────────────────────────────────────────────────────────────────────
def get_db_conn():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

# ── MinIO (S3) Client ────────────────────────────────────────────────────────
def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

def ensure_bucket(s3, bucket_name: str):
    """Create bucket if it does not exist."""
    try:
        s3.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("404", "NoSuchBucket"):
            s3.create_bucket(Bucket=bucket_name)
            logger.info(f"Created bucket: {bucket_name}")
        else:
            raise

def bucket_for_tenant(tenant_id: str) -> str:
    """Derive the isolated bucket name for a tenant."""
    safe = tenant_id.lower().replace("_", "-").replace(".", "-")[:40]
    return f"nv-vault-{safe}"

# ── Startup ──────────────────────────────────────────────────────────────────
@app.on_event("startup")
def startup_event():
    validate_auth_config()

    def check_db():
        try:
            conn = get_db_conn()
            conn.close()
            return True
        except Exception:
            return False

    def check_minio():
        try:
            s3 = get_s3()
            s3.list_buckets()
            return True
        except Exception:
            return False

    global_health_registry.add_check("db", check_db)
    global_health_registry.add_check("minio", check_minio)

# ── Health ───────────────────────────────────────────────────────────────────
@app.get("/healthz")
def healthz():
    return {"status": "ok"}

@app.get("/readyz")
def readyz():
    import json
    result = global_health_registry.check_health()
    if result["status"] != "ok":
        return Response(content=json.dumps(result), status_code=503, media_type="application/json")
    return result

@app.get("/metrics")
def metrics():
    return get_metrics_response()

# ── Upload ───────────────────────────────────────────────────────────────────
@app.post("/artifacts/upload", status_code=201)
@app.post("/upload", status_code=201)
async def upload_artifact(
    case_id: str = Form(...),
    file: UploadFile = File(...),
    auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE)),
):
    """
    Encrypt (AES-256/Fernet) → Hash (SHA-256) → Store in MinIO per-tenant bucket.
    GUARDRAIL: Encryption is mandatory. Plain bytes never reach MinIO.
    """
    artifact_id  = str(uuid.uuid4())
    created_at   = int(time.time() * 1000)
    tenant_id    = auth.tenant_id

    # 1. Read raw bytes
    raw_bytes = await file.read()
    file_size = len(raw_bytes)

    # 2. SHA-256 of RAW (pre-encryption) bytes — for sighting logic
    sha256_digest = hashlib.sha256(raw_bytes).hexdigest()

    # 3. Generate a fresh per-file Fernet key and encrypt
    fernet_key    = Fernet.generate_key()
    fernet        = Fernet(fernet_key)
    encrypted_bytes = fernet.encrypt(raw_bytes)

    # Store the key as a string; in production this should be a KMS reference
    encryption_key_id = fernet_key.decode("utf-8")

    # 4. Target: tenant-isolated bucket
    bucket_name = bucket_for_tenant(tenant_id)
    minio_key   = f"{case_id}/{artifact_id}/{file.filename}"

    try:
        s3 = get_s3()
        ensure_bucket(s3, bucket_name)
        s3.put_object(
            Bucket=bucket_name,
            Key=minio_key,
            Body=io.BytesIO(encrypted_bytes),
            ContentLength=len(encrypted_bytes),
            Metadata={
                "x-nv-encrypted": "true",
                "x-nv-sha256":    sha256_digest,
                "x-nv-tenant":    tenant_id,
            }
        )
    except Exception as e:
        logger.error(f"MinIO upload failed: {e}")
        raise HTTPException(status_code=500, detail="Storage error")

    # 5. Write record to Postgres
    try:
        conn = get_db_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO artifacts
                    (id, case_id, tenant_id, filename, sha256, size, minio_key, encryption_key_id, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (artifact_id, case_id, tenant_id, file.filename,
                 sha256_digest, file_size, minio_key, encryption_key_id, created_at)
            )
        conn.commit()
    except Exception as e:
        logger.error(f"DB insert failed: {e}")
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        conn.close()

    # 6. Sightings count (same hash, same tenant, other cases)
    sightings_count = _get_sightings_count(sha256_digest, tenant_id)

    logger.info(f"Artifact uploaded: {artifact_id} sha256={sha256_digest} sightings={sightings_count}")

    return {
        "artifact_id":     artifact_id,
        "sha256":          sha256_digest,
        "size":            file_size,
        "sightings_count": sightings_count,
        "is_sighting":     sightings_count > 1,
    }

# ── Sightings ────────────────────────────────────────────────────────────────
def _get_sightings_count(sha256: str, tenant_id: str) -> int:
    try:
        conn = get_db_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM artifacts WHERE sha256 = %s AND tenant_id = %s",
                (sha256, tenant_id)
            )
            row = cur.fetchone()
            return row[0] if row else 0
    except Exception:
        return 0
    finally:
        conn.close()

@app.get("/artifacts/sightings/{sha256}")
@app.get("/sightings/{sha256}")
def get_sightings(
    sha256: str,
    auth: AuthContext = Depends(require_permission(PERM_CASE_READ)),
):
    """
    Return all cases in this tenant that contain a file with the given SHA-256 hash.
    GUARDRAIL: Tenant boundary enforced — Tenant A cannot see Tenant B sightings.
    """
    try:
        conn = get_db_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, case_id, filename, created_at
                FROM artifacts
                WHERE sha256 = %s AND tenant_id = %s
                ORDER BY created_at ASC
                """,
                (sha256, auth.tenant_id)
            )
            rows = cur.fetchall()
    except Exception as e:
        logger.error(f"Sightings query failed: {e}")
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        conn.close()

    sightings = [
        {"artifact_id": str(r[0]), "case_id": str(r[1]), "filename": r[2], "created_at": r[3]}
        for r in rows
    ]

    return {
        "sha256":          sha256,
        "sightings_count": len(sightings),
        "is_sighting":     len(sightings) > 1,
        "sightings":       sightings,
    }

# ── Download (Presigned URL) ─────────────────────────────────────────────────
@app.get("/artifacts/{artifact_id}/download")
@app.get("/{artifact_id}/download")
def get_download_url(
    artifact_id: str,
    auth: AuthContext = Depends(require_permission(PERM_CASE_READ)),
):
    """
    Generate a 60-second presigned GET URL.
    GUARDRAIL: The UI never receives MinIO credentials — only a time-limited URL.
    """
    try:
        conn = get_db_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT minio_key, tenant_id, filename FROM artifacts WHERE id = %s",
                (artifact_id,)
            )
            row = cur.fetchone()
    except Exception as e:
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Artifact not found")

    minio_key, artifact_tenant, filename = row

    # Strict tenant check
    if artifact_tenant != auth.tenant_id:
        raise HTTPException(status_code=403, detail="Access denied")

    bucket_name = bucket_for_tenant(auth.tenant_id)

    try:
        s3          = get_s3()
        presigned   = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": minio_key},
            ExpiresIn=PRESIGN_TTL_SECONDS,
        )
    except Exception as e:
        logger.error(f"Presigned URL generation failed: {e}")
        raise HTTPException(status_code=500, detail="Storage error")

    return {
        "url":        presigned,
        "expires_in": PRESIGN_TTL_SECONDS,
        "filename":   filename,
    }

# ── List artifacts for a case ────────────────────────────────────────────────
@app.get("/artifacts/case/{case_id}")
@app.get("/case/{case_id}")
def list_case_artifacts(
    case_id: str,
    auth: AuthContext = Depends(require_permission(PERM_CASE_READ)),
):
    try:
        conn = get_db_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, filename, sha256, size, created_at
                FROM artifacts
                WHERE case_id = %s AND tenant_id = %s
                ORDER BY created_at DESC
                """,
                (case_id, auth.tenant_id)
            )
            rows = cur.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        conn.close()

    # Annotate each artifact with its sighting count
    artifacts = []
    for r in rows:
        sc = _get_sightings_count(r[2], auth.tenant_id)
        artifacts.append({
            "artifact_id":     str(r[0]),
            "filename":        r[1],
            "sha256":          r[2],
            "size":            r[3],
            "created_at":      r[4],
            "sightings_count": sc,
            "is_sighting":     sc > 1,
        })

    return {"case_id": case_id, "artifacts": artifacts}
