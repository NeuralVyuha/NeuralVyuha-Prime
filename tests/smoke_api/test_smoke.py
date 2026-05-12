import pytest
import requests
import time
import json
from tenacity import retry, stop_after_delay, wait_fixed

BASE_URL = "http://localhost:80"

@pytest.fixture(scope="session")
def api_session():
    session = requests.Session()
    # Wait for service
    start = time.time()
    while time.time() - start < 120:
        try:
            resp = session.get(f"{BASE_URL}/api/status")
            if resp.status_code == 200:
                print("NeuralVyuha is up!")
                break
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(2)
    else:
        pytest.fail("NeuralVyuha failed to start within 120 seconds")
    return session

@pytest.fixture(scope="session")
def auth_session(api_session):
    # Try to create initial user (NeuralVyuha 4 style)
    # The UI calls POST /api/v1/user usually, or /api/user.
    # We try to create an admin user.
    user_data = {
        "login": "admin@neuralvyuha.local",
        "name": "Administrator",
        "password": "secretPassword123!",
        "profile": "admin", # v1 profile
        "organisation": "admin",
        "roles": ["admin"] # v0 legacy
    }

    # Try creating user (it might fail if already exists or if auth required)
    # If the DB is empty, NeuralVyuha might allow creation of the first user.
    # But usually the UI hits a specific endpoint.
    # Let's try /api/v1/user

    print("Attempting to create initial user...")
    try:
        resp = api_session.post(f"{BASE_URL}/api/v1/user", json=user_data)
        print(f"Create user response: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"Create user failed: {e}")

    # Try login
    print("Attempting login...")
    login_data = {
        "user": "admin@neuralvyuha.local",
        "password": "secretPassword123!"
    }
    resp = api_session.post(f"{BASE_URL}/api/v1/login", json=login_data)
    if resp.status_code != 200:
        # Fallback to default credentials if pre-seeded
        login_data = {"user": "admin", "password": "secret"}
        resp = api_session.post(f"{BASE_URL}/api/v1/login", json=login_data)

    assert resp.status_code == 200, f"Login failed: {resp.text}"
    print("Login successful")
    return api_session

def test_create_and_search_case(auth_session):
    # Create Case
    case_data = {
        "title": "Smoke Test Case",
        "description": "Created by automated smoke test",
        "severity": 2,
        "flag": False,
        "tags": ["smoke", "test"]
    }
    print("Creating case...")
    resp = auth_session.post(f"{BASE_URL}/api/v1/case", json=case_data)
    assert resp.status_code == 201, f"Create case failed: {resp.text}"
    case_id = resp.json()['id']
    print(f"Case created: {case_id}")

    # Search Case
    # NeuralVyuha 4 search API: POST /api/v1/case/_search or /api/case/_search
    query = {
        "query": {
            "_and": [
                {"_field": "title", "_value": "Smoke Test Case"},
                {"_field": "status", "_value": "Open"}
            ]
        }
    }
    print("Searching case...")
    resp = auth_session.post(f"{BASE_URL}/api/v1/case/_search", json=query)
    assert resp.status_code == 200, f"Search failed: {resp.text}"
    found = any(c['id'] == case_id for c in resp.json())
    assert found, "Created case not found in search results"

def test_add_observable(auth_session):
    # Create a case first to add observable to
    case_data = {
        "title": "Observable Test Case",
        "description": "Case for observable test",
        "severity": 2,
        "tags": ["smoke", "observable"]
    }
    resp = auth_session.post(f"{BASE_URL}/api/v1/case", json=case_data)
    case_id = resp.json()['id']

    obs_data = {
        "dataType": "ip",
        "data": "1.2.3.4",
        "message": "Smoke test observable",
        "tlp": 2,
        "ioc": True
    }
    print("Adding observable...")
    resp = auth_session.post(f"{BASE_URL}/api/v1/case/{case_id}/observable", json=obs_data)
    assert resp.status_code == 201, f"Add observable failed: {resp.text}"

def test_create_alert(auth_session):
    alert_data = {
        "title": "Smoke Test Alert",
        "description": "Alert from smoke test",
        "type": "external",
        "source": "smoke-test",
        "sourceRef": f"smoke-{int(time.time())}",
        "severity": 2,
        "tlp": 2
    }
    print("Creating alert...")
    resp = auth_session.post(f"{BASE_URL}/api/v1/alert", json=alert_data)
    assert resp.status_code == 201, f"Create alert failed: {resp.text}"
