import os
import jwt
import logging
import sys
from fastapi import Request, HTTPException, status, Depends
from typing import Optional, List, Union, Set
from pydantic import BaseModel
from .rbac import resolve_permissions, ROLE_ADMIN
from .oidc import get_signing_key, validate_oidc_config
from ..config.secrets import get_secret

logger = logging.getLogger("auth-middleware")

def validate_auth_config():
    validate_oidc_config()

def get_config():
    return {
        "DEV_MODE": os.getenv("DEV_MODE", "false").lower() == "true",
        "ALLOW_DEV_OVERRIDES": os.getenv("ALLOW_DEV_OVERRIDES", "false").lower() == "true",
        "JWT_SECRET": get_secret("JWT_SECRET", default="dev-secret-do-not-use-in-prod", required=False),
        "JWT_ALGORITHM": os.getenv("JWT_ALGORITHM", "HS256"),
        "OIDC_ISSUER": os.getenv("OIDC_ISSUER", "https://auth.example.com"),
        "OIDC_AUDIENCE": os.getenv("OIDC_AUDIENCE", "neural-vyuha-engine"),
    }

class AuthContext(BaseModel):
    user_id: str
    tenant_id: str
    roles: List[str] = []
    permissions: Set[str] = set()
    token: Optional[str] = None

async def get_auth_context(request: Request) -> AuthContext:
    config = get_config()

    if config["DEV_MODE"]:
        user_id = "dev-user"
        tenant_id = "dev-tenant"
        roles = [ROLE_ADMIN]

        if config["ALLOW_DEV_OVERRIDES"]:
            if "X-Dev-Tenant" in request.headers:
                tenant_id = request.headers["X-Dev-Tenant"]
                logger.info(f"DEV_MODE: Tenant override -> {tenant_id}")
            if "X-Dev-User" in request.headers:
                user_id = request.headers["X-Dev-User"]
            if "X-Dev-Roles" in request.headers:
                roles_str = request.headers["X-Dev-Roles"]
                roles = roles_str.split(",") if "," in roles_str else [roles_str]
        elif "X-Dev-Tenant" in request.headers:
            logger.warning("DEV_MODE: Header overrides present but ALLOW_DEV_OVERRIDES=False. Ignoring.")
        
        permissions = resolve_permissions(roles)
        return AuthContext(user_id=user_id, tenant_id=tenant_id, roles=roles, permissions=permissions)

    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing or invalid Authorization header")

    token = auth_header.split(" ")[1]

    try:
        if config["JWT_ALGORITHM"] == "RS256":
            # 1. Decode header to get KID
            header = jwt.get_unverified_header(token)
            kid = header.get("kid")
            if not kid:
                raise jwt.InvalidTokenError("Missing kid in token header")
            
            # 2. Fetch public key
            public_key = get_signing_key(kid)
            
            # 3. Verify
            payload = jwt.decode(
                token,
                public_key,
                algorithms=["RS256"],
                audience=config["OIDC_AUDIENCE"],
                issuer=config["OIDC_ISSUER"]
            )
        else:
            # HS256 Legacy/Dev
            payload = jwt.decode(
                token,
                config["JWT_SECRET"],
                algorithms=[config["JWT_ALGORITHM"]],
                audience=config["OIDC_AUDIENCE"],
                issuer=config["OIDC_ISSUER"]
            )

        tenant_id = payload.get("tenant_id")
        user_id = payload.get("sub")
        roles_raw = payload.get("roles", [])

        if not tenant_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Token missing tenant_id")
        if not user_id:
             raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Token missing sub (user_id)")

        if isinstance(roles_raw, str):
            roles = [roles_raw]
        elif isinstance(roles_raw, list):
            roles = roles_raw
        else:
            roles = []

        permissions = resolve_permissions(roles)
        return AuthContext(user_id=user_id, tenant_id=tenant_id, roles=roles, permissions=permissions, token=token)

    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired")
    except jwt.InvalidTokenError as e:
        logger.warning(f"Invalid token: {e}")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    except Exception as e:
        logger.error(f"Auth error: {e}")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication failed")

def require_role(required_roles: List[str]):
    def dependency(ctx: AuthContext = Depends(get_auth_context)):
        if not any(role in ctx.roles for role in required_roles):
             raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Insufficient role. Required: {required_roles}")
        return ctx
    return dependency

def require_permission(required_permission: str):
    def dependency(ctx: AuthContext = Depends(get_auth_context)):
        if required_permission not in ctx.permissions:
             raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Insufficient permission. Required: {required_permission}")
        return ctx
    return dependency
