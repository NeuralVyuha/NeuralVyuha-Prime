import requests
import time
import sys
import uuid
import concurrent.futures

# Configuration
INGEST_URL = "http://localhost:8000/ingest"
QUERY_URL = "http://localhost:8001"
TOKEN_SCRIPT = ".github/scripts/generate_test_token.py"

def get_token(user, tenant, role):
    import subprocess
    result = subprocess.run(["python3", TOKEN_SCRIPT, user, tenant, role], capture_output=True, text=True)
    return result.stdout.strip()

def send_alert(token, i):
    alert_payload = {
        "source": "burst-test",
        "type": "event",
        "sourceRef": f"ref-burst-1", # SAME sourceRef
        "title": "Burst Test Alert",
        "severity": 1,
        "tlp": 2,
        "pap": 2
    }
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Idempotency-Key": f"burst-test-{i}" # Different idempotency keys to pass ingest, but same sourceRef/fingerprint for dedup
    }
    
    try:
        r = requests.post(INGEST_URL, json=alert_payload, headers=headers)
        return r.status_code
    except Exception as e:
        return 500

def main():
    print("Starting Replay Burst Test (Storm Simulation)...")
    
    tenant_id = "burst-tenant"
    token = get_token("burst-user", tenant_id, "SOC_ANALYST")
    
    # Send 50 alerts rapidly
    count = 50
    print(f"Sending {count} identical alerts (same fingerprint, different ingest IDs)...")
    
    start_time = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(send_alert, token, i) for i in range(count)]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]
        
    duration = time.time() - start_time
    print(f"Sent {count} alerts in {duration:.2f}s")
    
    # Check Ingest Success
    success_count = results.count(202)
    print(f"Ingest Accepted: {success_count}/{count}")
    
    if success_count < count:
        print("WARNING: Some ingest requests failed (maybe backpressure at ingress?).")
    
    print("Waiting for processing (Dedup + Indexing)...")
    time.sleep(20) # Give it time to crunch
    
    # Verify OpenSearch has exactly 1 document
    print("Querying Alerts...")
    q_headers = {"Authorization": f"Bearer {token}"}
    r_query = requests.get(f"{QUERY_URL}/alerts", params={"q": "Burst Test Alert"}, headers=q_headers)
    
    if r_query.status_code != 200:
        print(f"Query failed: {r_query.text}")
        sys.exit(1)
        
    data = r_query.json()
    total_hits = data.get("total", 0)
    
    print(f"Total hits in OpenSearch: {total_hits}")
    
    if total_hits == 1:
        print("SUCCESS: Burst Dedup worked. Only 1 document persisted.")
    elif total_hits > 1:
        print(f"FAILURE: Duplicates found! Count={total_hits}. Deterministic ID failure?")
        sys.exit(1)
    else:
        print("FAILURE: No documents found. Pipeline too slow or broken?")
        sys.exit(1)

if __name__ == "__main__":
    main()
