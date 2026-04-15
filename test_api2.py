import requests
import json
import time
import random

job_id = f"test_{random.randint(100000, 999999)}"
print(f"[TEST] Job ID: {job_id}\n")

# Test /launch avec critères COMPLETS (copié du job id=35 réussi)
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
    "wait_timeout_seconds": 30
}

print("[TEST] Sending /launch request with complete criteria...")
try:
    resp = requests.post(
        "http://localhost:8000/launch",
        json=payload,
        timeout=35
    )
    print(f"[TEST] /launch response status: {resp.status_code}")
    resp_data = resp.json()
    print(f"[TEST] /launch success: {resp_data.get('success')}")
    
    job_info = resp_data.get('job', {})
    print(f"[TEST] Job status: {job_info.get('status')}")
    print(f"[TEST] Total collected: {job_info.get('total_collected')}")
    print(f"[TEST] Job errors: {job_info.get('errors')}")
    if resp_data.get('error'):
        print(f"[TEST] Response error: {resp_data.get('error')}")
    
except Exception as e:
    print(f"[TEST] /launch error: {e}")

print("\n[TEST] Test complete!")
