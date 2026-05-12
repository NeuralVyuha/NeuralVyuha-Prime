import psycopg2
import os
import httpx
import jwt
from typing import List, Dict

def get_service_token():
    """Generates a synthetic Admin JWT to authenticate the MCP server against other internal APIs."""
    secret = os.environ.get("JWT_SECRET_KEY", "nv_super_secret_jwt_key_2024_secure_123")
    payload = {
        "sub": "mcp-service-ai",
        "username": "AI Copilot",
        "role": "admin",
        "permissions": ["ADMIN_INTEGRATION", "NODE_STATUS_READ"]
    }
    return jwt.encode(payload, secret, algorithm="HS256")

def get_db_conn():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        database=os.environ.get("POSTGRES_DB", "nv_vault"),
        user=os.environ.get("POSTGRES_USER", "nv_user"),
        password=os.environ.get("POSTGRES_PASSWORD", "nv_pass")
    )

def search_wazuh_logs(query: str, limit: int = 10) -> List[Dict]:
    """Search OpenSearch Wazuh logs for an IP, Hash, or Username."""
    opensearch_url = os.environ.get("OPENSEARCH_URL", "http://opensearch:9200")
    try:
        res = httpx.post(f"{opensearch_url}/wazuh-alerts-*/_search", json={
            "query": {"query_string": {"query": query}},
            "size": limit
        }, timeout=5.0)
        if res.status_code == 200:
            hits = res.json().get("hits", {}).get("hits", [])
            return [h["_source"] for h in hits]
    except Exception as e:
        return [{"error": str(e)}]
    return []

def get_recent_cases(limit: int = 5) -> List[Dict]:
    """Fetch the most recent incident cases from PostgreSQL."""
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, title, severity, status, created_at FROM cases ORDER BY created_at DESC LIMIT %s", (limit,))
            cols = [desc[0] for desc in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()

def create_vyuha_case(title: str, description: str, severity: str = "MEDIUM") -> Dict:
    """Creates a new incident case in the NeuralVyuha database."""
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO cases (title, description, severity, status, created_at) VALUES (%s, %s, %s, 'OPEN', NOW()) RETURNING id",
                (title, description, severity)
            )
            case_id = cur.fetchone()[0]
            conn.commit()
            return {"status": "success", "case_id": case_id, "message": f"Case '{title}' created successfully."}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        conn.close()

def query_integration_node(node_name: str, observable_type: str, observable_value: str) -> Dict:
    """Interrogates a Threat Intel integration node (VirusTotal, AbuseIPDB) for a given observable."""
    # First, lookup the node ID by name
    conn = get_db_conn()
    node_id = None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM integration_nodes WHERE name ILIKE %s OR node_type ILIKE %s LIMIT 1", (f"%{node_name}%", f"%{node_name}%"))
            row = cur.fetchone()
            if row: node_id = row[0]
    finally:
        conn.close()

    if not node_id:
        return {"error": f"Active integration node matching '{node_name}' could not be found in the Integration Hub."}

    # Query the Node Service endpoint
    node_service_url = os.environ.get("NODE_SERVICE_URL", "http://node-service:8085")
    token = get_service_token()
    
    try:
        res = httpx.post(
            f"{node_service_url}/nodes/{node_id}/scan",
            json={"observable_type": observable_type, "observable_value": observable_value},
            headers={"Authorization": f"Bearer {token}"},
            timeout=15.0
        )
        return res.json()
    except Exception as e:
        return {"error": f"Failed to contact Node Service: {str(e)}"}

AVAILABLE_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "search_wazuh_logs",
            "description": "Searches the central OpenSearch SIEM database for raw Wazuh security logs. Highly useful for finding related IP addresses, usernames, or file hashes.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "The Lucene query string (e.g. 'data.srcip:192.168.1.1' or a raw hash)"},
                    "limit": {"type": "integer", "description": "Max logs to return"}
                },
                "required": ["query"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_recent_cases",
            "description": "Retrieves the latest open incident cases from the NeuralVyuha database.",
            "parameters": {
                "type": "object",
                "properties": {
                    "limit": {"type": "integer"}
                }
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "create_vyuha_case",
            "description": "Opens a new incident case in the NeuralVyuha platform. Use this when you have confirmed a threat and need to track investigation progress.",
            "parameters": {
                "type": "object",
                "properties": {
                    "title": {"type": "string", "description": "Short summary of the incident"},
                    "description": {"type": "string", "description": "Detailed notes on what was found"},
                    "severity": {"enum": ["LOW", "MEDIUM", "HIGH", "CRITICAL"]}
                },
                "required": ["title", "description"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "query_integration_node",
            "description": "Queries a threat intelligence integration node (e.g., 'VirusTotal', 'AbuseIPDB', 'AlienVault') configured in the system's Integration Hub to get a reputation score for an observable.",
            "parameters": {
                "type": "object",
                "properties": {
                    "node_name": {"type": "string", "description": "The name or type of the integration node (e.g. 'VirusTotal' or 'AbuseIPDB')"},
                    "observable_type": {"enum": ["ip", "hash", "domain", "url"], "description": "The type of the observable"},
                    "observable_value": {"type": "string", "description": "The actual IP address, file hash, or domain name"}
                },
                "required": ["node_name", "observable_type", "observable_value"]
            }
        }
    }
]

def execute_tool(name: str, args: dict):
    if name == "search_wazuh_logs":
        return search_wazuh_logs(args.get("query"), args.get("limit", 10))
    elif name == "get_recent_cases":
        return get_recent_cases(args.get("limit", 5))
    elif name == "create_vyuha_case":
        return create_vyuha_case(args.get("title"), args.get("description"), args.get("severity", "MEDIUM"))
    elif name == "query_integration_node":
        return query_integration_node(args.get("node_name"), args.get("observable_type"), args.get("observable_value"))
    else:
        return {"error": f"Unknown tool: {name}"}
