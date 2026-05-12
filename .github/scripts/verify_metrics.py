import requests
import sys
import time

def check_metrics(url, expected_metrics):
    try:
        r = requests.get(url)
        if r.status_code != 200:
            print(f"Metrics endpoint {url} failed: {r.status_code}")
            return False
        
        content = r.text
        missing = []
        for m in expected_metrics:
            if m not in content:
                missing.append(m)
        
        if missing:
            print(f"Missing metrics in {url}: {missing}")
            return False
            
        print(f"Metrics check passed for {url}")
        return True
    except Exception as e:
        print(f"Metrics check exception for {url}: {e}")
        return False

def main():
    services = [
        ("http://localhost:8000/metrics", ["requests_total", "service_uptime_seconds"]),
        ("http://localhost:8001/metrics", ["requests_total", "service_uptime_seconds"]),
        ("http://localhost:8002/metrics", ["requests_total", "service_uptime_seconds"]),
        ("http://localhost:9001/metrics", ["messages_processed_total", "dlq_published_total"]),
        ("http://localhost:9002/metrics", ["messages_processed_total", "dlq_published_total"]),
        ("http://localhost:9003/metrics", ["messages_processed_total", "dlq_published_total"]),
        ("http://localhost:9004/metrics", ["messages_processed_total", "dlq_published_total"]),
    ]
    
    success = True
    for url, metrics in services:
        if not check_metrics(url, metrics):
            success = False
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
