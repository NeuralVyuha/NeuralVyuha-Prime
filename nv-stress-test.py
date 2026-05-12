import asyncio
import json
import random
import time
import uuid
import argparse
import aiohttp
from datetime import datetime

# --- Configuration & Templates ---

WAZUH_TEMPLATE = {
    "source": "wazuh",
    "rule": {
        "id": "100001",
        "level": 0,
        "description": "User login failed"
    },
    "data": {
        "srcip": "1.2.3.4",
        "dstuser": "admin"
    }
}

SURICATA_TEMPLATE = {
    "source": "suricata",
    "event_type": "alert",
    "alert": {
        "signature": "ET SCAN Potential SSH Scan",
        "severity": 2,
        "category": "Attempted Information Leak"
    },
    "src_ip": "1.2.3.4",
    "dest_ip": "10.0.0.50"
}

async def generate_event(source="wazuh", ip="1.2.3.4"):
    event_id = str(uuid.uuid4())
    ts = datetime.utcnow().isoformat() + "Z"
    
    if source == "wazuh":
        event = json.loads(json.dumps(WAZUH_TEMPLATE))
        sev = random.randint(3, 12)
        event["rule"]["level"] = sev
        event["data"]["srcip"] = ip
        event["src_ip"] = ip
        event["tenant_id"] = "tenant-777"
        event["severity"] = 4 if sev >= 12 else (3 if sev >= 8 else (2 if sev >= 5 else 1))
    else:
        event = json.loads(json.dumps(SURICATA_TEMPLATE))
        event["src_ip"] = ip
        event["tenant_id"] = "tenant-777"
        event["severity"] = 2
    
    return event

async def worker(worker_id, target_url, events_to_send, rate_limit, stats):
    async with aiohttp.ClientSession() as session:
        delay = 1.0 / rate_limit if rate_limit > 0 else 0
        
        ips = [f"192.168.1.{i}" for i in range(1, 11)] # 10 distinct IPs for correlation groups
        
        for i in range(events_to_send):
            source = "wazuh" if random.random() > 0.3 else "suricata"
            ip = random.choice(ips)
            event = await generate_event(source, ip)
            
            # Generate a new idempotency key for each request
            idempotency_key = str(uuid.uuid4())
            headers = {"Idempotency-Key": idempotency_key}
            
            start_time = time.time()
            try:
                # The main ingest endpoint is /ingest
                async with session.post(f"{target_url}/ingest", json=event, headers=headers) as resp:
                    if resp.status == 200 or resp.status == 202:
                        stats["success"] += 1
                    else:
                        stats["error"] += 1
                        # print(f"Worker {worker_id} error: {resp.status}")
            except Exception as e:
                stats["error"] += 1
                # print(f"Worker {worker_id} exception: {e}")
            
            stats["latency"].append(time.time() - start_time)
            
            if delay > 0:
                await asyncio.sleep(delay)

async def main():
    parser = argparse.ArgumentParser(description="NeuralVyuha Performance Stress Test")
    parser.add_argument("--url", default="http://localhost:8090", help="Target nv-ingest URL")
    parser.add_argument("--eps", type=int, default=100, help="Events Per Second (total)")
    parser.add_argument("--duration", type=int, default=30, help="Duration in seconds")
    parser.add_argument("--workers", type=int, default=10, help="Number of concurrent workers")
    args = parser.parse_args()

    print(f"--- NeuralVyuha Stress Test ---")
    print(f"Target: {args.url}")
    print(f"Target EPS: {args.eps}")
    print(f"Duration: {args.duration}s")
    print(f"Workers: {args.workers}")
    print(f"-------------------------------")

    stats = {"success": 0, "error": 0, "latency": []}
    events_per_worker = (args.eps * args.duration) // args.workers
    eps_per_worker = args.eps // args.workers

    start_time = time.time()
    tasks = []
    for i in range(args.workers):
        tasks.append(worker(i, args.url, events_per_worker, eps_per_worker, stats))
    
    # Progress monitor
    async def monitor():
        while True:
            await asyncio.sleep(2)
            elapsed = time.time() - start_time
            if elapsed >= args.duration: break
            current_eps = stats["success"] / elapsed
            print(f"Progress: {stats['success']} events sent | Current EPS: {current_eps:.2f} | Errors: {stats['error']}")

    await asyncio.gather(*tasks, monitor())
    
    total_time = time.time() - start_time
    final_eps = stats["success"] / total_time
    avg_latency = (sum(stats["latency"]) / len(stats["latency"])) * 1000 if stats["latency"] else 0

    print(f"\n--- Results ---")
    print(f"Total Events Sent: {stats['success'] + stats['error']}")
    print(f"Success Count:     {stats['success']}")
    print(f"Error Count:       {stats['error']}")
    print(f"Final EPS:         {final_eps:.2f}")
    print(f"Avg Latency:       {avg_latency:.2f} ms")
    print(f"Total Time:        {total_time:.2f} s")
    print(f"----------------")

if __name__ == "__main__":
    asyncio.run(main())
