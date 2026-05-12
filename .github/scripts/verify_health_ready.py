import requests
import sys

def check_endpoint(url):
    try:
        r = requests.get(url, timeout=2)
        if r.status_code != 200:
            print(f"Check failed {url}: {r.status_code} - {r.text}")
            return False
        return True
    except Exception as e:
        print(f"Check exception {url}: {e}")
        return False

def main():
    endpoints = [
        "http://localhost:8000/healthz", "http://localhost:8000/readyz",
        "http://localhost:8001/healthz", "http://localhost:8001/readyz",
        "http://localhost:8002/healthz", "http://localhost:8002/readyz"
    ]
    
    success = True
    for ep in endpoints:
        if not check_endpoint(ep):
            success = False
            
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
