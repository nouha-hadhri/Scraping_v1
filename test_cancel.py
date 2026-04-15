import requests
import json
import time
import random
import threading

job_id = f"cancel_test_{random.randint(100000, 999999)}"
print(f"[TEST] Job ID: {job_id}\n")

# Critères identiques au précédent test
payload = {
    "jobId": job_id,
    "criteria": {
        "secteurs_activite": ["Finance"],
        "tailles_entreprise": ["PME"],
        "types_entreprise": ["SAS"],
        "pays": ["France"],
        "regions": ["Île-de-France"],
        "villes": ["Antony"],
        "keywords": ["saas"],
        "max_resultats": 100
    },
    "wait_timeout_seconds": 120  # Long timeout so we can cancel
}

def launch_job():
    """Launch the job in the background"""
    print("[LAUNCH] Sending /launch request (120s timeout)...")
    try:
        resp = requests.post(
            "http://localhost:8000/launch",
            json=payload,
            timeout=125
        )
        print(f"[LAUNCH] Response status: {resp.status_code}")
        resp_data = resp.json()
        print(f"[LAUNCH] Final status: {resp_data.get('job', {}).get('status')}")
        print(f"[LAUNCH] Error: {resp_data.get('error')}")
    except Exception as e:
        print(f"[LAUNCH] Error: {e}")

# Start launch in background thread
launch_thread = threading.Thread(target=launch_job, daemon=False)
launch_thread.start()

# Wait 3 seconds for job to start
time.sleep(3)

# Send cancel
print("\n[CANCEL] Sending /cancel request...")
try:
    resp = requests.post(
        f"http://localhost:8000/cancel/{job_id}",
        timeout=5
    )
    print(f"[CANCEL] Response status: {resp.status_code}")
    print(f"[CANCEL] Response: {resp.json()}")
except Exception as e:
    print(f"[CANCEL] Error: {e}")

# Wait for launch to finish
print("\n[WAIT] Waiting for launch thread to finish...")
launch_thread.join(timeout=130)

# Check final status
print("\n[STATUS] Final status check...")
try:
    resp = requests.get(
        f"http://localhost:8000/status/{job_id}",
        timeout=5
    )
    status_data = resp.json()
    print(f"[STATUS] Job status: {status_data.get('status')}")
    print(f"[STATUS] Total collected: {status_data.get('totalCollected')}")
    print(f"[STATUS] Errors: {status_data.get('errors')}")
except Exception as e:
    print(f"[STATUS] Error: {e}")

print("\n[TEST] Complete!")
