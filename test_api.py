import requests
import json
import time
import random

job_id = f"test_{random.randint(100000, 999999)}"
print(f"[TEST] Job ID: {job_id}")

# Test /launch
payload = {
    "jobId": job_id,
    "criteria": {"tailles_entreprise": ["PME"]},
    "wait_timeout_seconds": 10
}

print(f"[TEST] Sending /launch request...")
try:
    resp = requests.post(
        "http://localhost:8000/launch",
        json=payload,
        timeout=15
    )
    print(f"[TEST] /launch response status: {resp.status_code}")
    resp_data = resp.json()
    print(f"[TEST] /launch success: {resp_data.get('success')}")
    print(f"[TEST] /launch status: {resp_data.get('job', {}).get('status', 'unknown')}")
except Exception as e:
    print(f"[TEST] /launch error: {e}")

# Wait a bit
time.sleep(2)

# Test /cancel
print(f"\n[TEST] Sending /cancel request for job {job_id}...")
try:
    resp = requests.post(
        f"http://localhost:8000/cancel/{job_id}",
        timeout=5
    )
    print(f"[TEST] /cancel response status: {resp.status_code}")
    print(f"[TEST] /cancel response: {resp.json()}")
except Exception as e:
    print(f"[TEST] /cancel error: {e}")

# Check status
print(f"\n[TEST] Checking /status for job {job_id}...")
try:
    resp = requests.get(
        f"http://localhost:8000/status/{job_id}",
        timeout=5
    )
    status_data = resp.json()
    print(f"[TEST] /status status: {status_data.get('status')}")
    print(f"[TEST] /status totalCollected: {status_data.get('totalCollected')}")
except Exception as e:
    print(f"[TEST] /status error: {e}")

print("\n[TEST] Complete!")
