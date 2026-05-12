import requests
import time
import sys
import uuid

# Configuration
QUERY_URL = "http://localhost:8001"
INGEST_URL = "http://localhost:8000/ingest"
TOKEN_SCRIPT = ".github/scripts/generate_test_token.py"

def get_token(user, tenant, role):
    # Simulate token generation (in real CI, this calls the script, but here we mock or assume CI env)
    # We will use the script if available via subprocess, or just mocked JWT if we had the key.
    # For now, let's assume we can generate it via subprocess as in CI.
    import subprocess
    result = subprocess.run(["python3", TOKEN_SCRIPT, user, tenant, role], capture_output=True, text=True)
    return result.stdout.strip()

def main():
    print("Starting Replay Safety Test...")
    
    tenant_id = "replay-tenant"
    token = get_token("replay-user", tenant_id, "SOC_ANALYST")
    
    # 1. Ingest Alert A (Twice)
    alert_payload = {
        "source": "test-replay",
        "type": "event",
        "sourceRef": "ref-replay-1",
        "title": "Replay Test Alert",
        "severity": 1,
        "tlp": 2,
        "pap": 2
    }
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Idempotency-Key": "replay-test-1" # Same key implies idempotency at ingest level too
    }
    
    # First Send
    print("Sending Alert A (1st time)...")
    r1 = requests.post(INGEST_URL, json=alert_payload, headers=headers)
    if r1.status_code != 202:
        print(f"Failed to ingest 1st time: {r1.text}")
        sys.exit(1)
        
    time.sleep(2)
    
    # Second Send (Replay/Retry)
    print("Sending Alert A (2nd time - Duplicate)...")
    r2 = requests.post(INGEST_URL, json=alert_payload, headers=headers)
    if r2.status_code != 202:
        print(f"Failed to ingest 2nd time: {r2.text}")
        sys.exit(1)
        
    print("Waiting for processing...")
    time.sleep(15)
    
    # 2. Verify Count in OpenSearch
    # Should be exactly 1 doc if dedup works on fingerprint
    # Fingerprint depends on source+type+sourceRef.
    
    print("Querying Alerts...")
    q_headers = {"Authorization": f"Bearer {token}"}
    r_query = requests.get(f"{QUERY_URL}/alerts", params={"q": "Replay Test Alert"}, headers=q_headers)
    
    if r_query.status_code != 200:
        print(f"Query failed: {r_query.text}")
        sys.exit(1)
        
    data = r_query.json()
    hits = data.get("hits", [])
    count = data.get("total", 0)
    
    print(f"Found {count} alerts.")
    
    if count == 1:
        print("SUCCESS: Exact deduplication. Only 1 document found.")
    elif count > 1:
        print(f"FAILURE: Duplicates found! Count={count}")
        # Print IDs to see if they differ
        for h in hits:
            print(f" - ID: {h.get('event_id')} / Original: {h.get('original_event_id')}")
        sys.exit(1)
    else:
        print("FAILURE: No alerts found.")
        sys.exit(1)

if __name__ == "__main__":
    main()
