import jwt
import os
import sys
import time

def generate_token(user, tenant, roles):
    secret = os.getenv("JWT_SECRET", "dev-secret-do-not-use-in-prod")
    payload = {
        "sub": user,
        "tenant_id": tenant,
        "roles": roles.split(","),
        "exp": int(time.time()) + 3600,
        "iss": os.getenv("OIDC_ISSUER", "https://auth.example.com")
    }
    token = jwt.encode(payload, secret, algorithm="HS256")
    print(token)

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: generate_test_token.py <user> <tenant> <roles>")
        sys.exit(1)
    generate_token(sys.argv[1], sys.argv[2], sys.argv[3])
