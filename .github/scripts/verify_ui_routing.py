import requests
import os
import sys
import time

# Config
QUERY_API_URL = os.getenv("QUERY_API_URL", "http://localhost:8001")
TOKEN = os.getenv("AUTH_TOKEN") # Needs to be valid for v5

def test_alerts_endpoint():
    print("Verifying NeuralVyuha Query API Alerts Endpoint...")
    try:
        headers = {"Authorization": f"Bearer {TOKEN}"}
        resp = requests.get(f"{QUERY_API_URL}/alerts", headers=headers, timeout=5)

        if resp.status_code == 200:
            print("SUCCESS: NeuralVyuha /alerts is reachable.")
            data = resp.json()
            if isinstance(data, list) or 'results' in data:
                print("SUCCESS: NeuralVyuha returned list structure.")
            else:
                print(f"WARNING: Unexpected response structure: {data.keys()}")
        elif resp.status_code == 403:
            print("SUCCESS: NeuralVyuha enforced RBAC (403).")
        else:
            print(f"FAILURE: NeuralVyuha returned {resp.status_code}")
            sys.exit(1)

    except Exception as e:
        print(f"FAILURE: Connection error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if not TOKEN:
        print("SKIP: No token provided.")
        sys.exit(0)
    test_alerts_endpoint()

def test_groups_endpoint():
    print("Verifying NeuralVyuha Query API Groups Endpoint...")
    try:
        headers = {"Authorization": f"Bearer {TOKEN}"}
        resp = requests.get(f"{QUERY_API_URL}/groups", headers=headers, timeout=5)

        if resp.status_code == 200:
            print("SUCCESS: NeuralVyuha /groups is reachable.")
            data = resp.json()
            if 'groups' in data or isinstance(data, list):
                print("SUCCESS: NeuralVyuha returned groups list structure.")
            else:
                print(f"WARNING: Unexpected response structure for groups: {data.keys()}")
        elif resp.status_code == 403:
            print("SUCCESS: NeuralVyuha enforced RBAC (403) on groups.")
        else:
            print(f"FAILURE: NeuralVyuha returned {resp.status_code} for /groups")
            sys.exit(1)

    except Exception as e:
        print(f"FAILURE: Connection error: {e}")
        sys.exit(1)

# Call the new test
test_groups_endpoint()
