"""
FastAPI bridge for SCRAPING_V1.

Routes exposées (utilisées par Spring) :
  POST /launch
  GET  /status/{jobId}
  POST /cancel/{jobId}   ← Amélioré pour tuer correctement l'orchestrateur
  GET  /results/{jobId}
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from storage.repository import ProspectRepository
from config.criteria_normalizer import normalize_criteria

APP_ROOT = Path(__file__).resolve().parent
ORCHESTRATOR_PATH = APP_ROOT / "orchestrator.py"

# ─────────────────────────────────────────────
# Configuration du logger
# ─────────────────────────────────────────────

logger = logging.getLogger("fastapi_app")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# ─────────────────────────────────────────────
# Modèles de requête
# ─────────────────────────────────────────────

class CrmLaunchRequest(BaseModel):
    jobId: Optional[str] = None
    criteria: Optional[Dict[str, Any]] = None
    source_filter: Optional[str] = None
    dry_run: bool = False
    max_enrich: int = 0
    target_key: Optional[str] = None
    cancel_on_parent_exit: bool = True
    wait_timeout_seconds: Optional[int] = None


# ─────────────────────────────────────────────
# État interne des jobs
# ─────────────────────────────────────────────

@dataclass
class JobState:
    id: str
    status: str = "RUNNING"
    payload: Dict[str, Any] = field(default_factory=dict)
    started_at: float = field(default_factory=time.time)
    finished_at: Optional[float] = None
    return_code: Optional[int] = None
    response: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    stderr_tail: str = ""
    process: Optional[subprocess.Popen] = None
    payload_file: Optional[str] = None


app = FastAPI(title="SCRAPING_V1 API", version="1.0.0")

# ─────────────────────────────────────────────
# Middleware pour logger les requêtes HTTP
# ─────────────────────────────────────────────

@app.middleware("http")
async def log_requests(request, call_next):
    """Log chaque requête HTTP reçue"""
    method = request.method
    url = request.url.path
    logger.info(f"[HTTP REQUEST] {method} {url}")
    response = await call_next(request)
    return response

_JOBS: Dict[str, JobState] = {}
_JOBS_LOCK = threading.Lock()
_REPO = ProspectRepository()

_STATUS_CONTROL_C_EXIT = 3221225786
_STATUS_CONTROL_C_EXIT_SIGNED = -1073741510


# ─────────────────────────────────────────────
# Helpers internes
# ─────────────────────────────────────────────

def _nested_get(data: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    cur: Any = data
    for key in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
    return cur if cur is not None else default


def _to_page(items: list[Dict[str, Any]], page: int, size: int) -> Dict[str, Any]:
    total = len(items)
    start = max(0, page) * max(1, size)
    end = start + max(1, size)
    content = items[start:end]
    total_pages = (total + max(1, size) - 1) // max(1, size)
    return {
        "content": content,
        "number": page,
        "size": size,
        "totalElements": total,
        "totalPages": total_pages,
        "numberOfElements": len(content),
        "first": page <= 0,
        "last": (page + 1) >= max(1, total_pages),
        "empty": len(content) == 0,
    }


def _request_to_payload(req: CrmLaunchRequest, job_id: str) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "job_id": job_id,
        "parent_pid": os.getpid(),
        "cancel_on_parent_exit": req.cancel_on_parent_exit,
        "dry_run": req.dry_run,
        "max_enrich": req.max_enrich,
    }

    if req.criteria is not None:
        normalized_criteria = normalize_criteria(req.criteria)
        payload["criteria"] = normalized_criteria

    if req.source_filter:
        payload["source_filter"] = req.source_filter
    if req.target_key:
        payload["target_key"] = req.target_key

    return payload


def _write_payload_file(payload: Dict[str, Any]) -> str:
    handle = tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", prefix="scraping_bridge_", delete=False, encoding="utf-8"
    )
    try:
        json.dump(payload, handle, ensure_ascii=False)
        handle.flush()
        return handle.name
    finally:
        handle.close()


def _build_command(payload_file: str) -> list[str]:
    return [sys.executable, str(ORCHESTRATOR_PATH), "--bridge-json-file", payload_file, "--json-output"]

def _parse_bridge_stdout(stdout: str) -> Dict[str, Any]:
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    if not lines:
        return {}

    try:
        parsed = json.loads(lines[-1])
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        pass

    for line in reversed(lines):
        if not (line.startswith("{") and line.endswith("}")):
            continue
        try:
            parsed = json.loads(line)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            continue

    return {"raw_stdout": stdout}


def _load_job_from_repo(job_id: str) -> Dict[str, Any]:
    try:
        jobs = _REPO.load_jobs()
    except Exception:
        return {}

    target = str(job_id)
    for item in reversed(jobs):
        if str(item.get("id", "")) == target:
            return dict(item)
    return {}


def _watch_job(job_id: str) -> None:
    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
    if job is None or job.process is None:
        return

    proc = job.process
    try:
        stdout, stderr = proc.communicate()
    except Exception as exc:
        with _JOBS_LOCK:
            current = _JOBS.get(job_id)
            if current is not None:
                current.status = "FAILED"
                current.finished_at = time.time()
                current.error = f"watcher communicate failed: {exc}"
        return

    with _JOBS_LOCK:
        current = _JOBS.get(job_id)
        if current is None:
            return

        current.return_code = proc.returncode
        current.finished_at = time.time()
        current.stderr_tail = (stderr or "")[-4000:]
        bridge_response = _parse_bridge_stdout(stdout or "")
        current.response = bridge_response if bridge_response else None

        if current.status == "CANCELED":
            return

        if isinstance(bridge_response, dict) and bridge_response.get("canceled"):
            current.status = "CANCELED"
        elif proc.returncode in (_STATUS_CONTROL_C_EXIT, _STATUS_CONTROL_C_EXIT_SIGNED, 3):
            current.status = "CANCELED"
            if not current.error:
                current.error = "orchestrator canceled"
        elif proc.returncode == 0:
            current.status = "DONE"
        else:
            current.status = "FAILED"
            if isinstance(bridge_response, dict):
                current.error = bridge_response.get("error") or current.error
            if not current.error:
                current.error = f"orchestrator exited with code {proc.returncode}"
            if current.stderr_tail and not current.error.endswith(current.stderr_tail):
                current.error = f"{current.error} | stderr: {current.stderr_tail[-400:]}"

    if job.payload_file:
        try:
            os.remove(job.payload_file)
        except OSError:
            pass


def _launch_job(payload: Dict[str, Any], job_id: str) -> JobState:
    if not ORCHESTRATOR_PATH.exists():
        raise RuntimeError(f"orchestrator.py introuvable: {ORCHESTRATOR_PATH}")

    creationflags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0

    payload_file = _write_payload_file(payload)
    cmd = _build_command(payload_file)

    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(APP_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            creationflags=creationflags,
        )
    except Exception as e:
        logger.error(f"[LAUNCH_JOB] Failed to create process: {e}", exc_info=True)
        try:
            os.remove(payload_file)
        except OSError:
            pass
        raise

    state = JobState(id=job_id, payload=payload, process=proc, payload_file=payload_file)
    with _JOBS_LOCK:
        _JOBS[job_id] = state

    watcher = threading.Thread(target=_watch_job, args=(job_id,), daemon=True)
    watcher.start()

    return state


def _get_job_or_404(job_id: str) -> JobState:
    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job


def _wait_for_job(job_id: str, timeout: Optional[int] = 600) -> JobState:
    elapsed = 0
    while True:
        with _JOBS_LOCK:
            job = _JOBS.get(job_id)

        if job and job.status in ("DONE", "FAILED", "CANCELED"):
            return job

        if job and job.process is not None and job.status == "RUNNING":
            rc = job.process.poll()
            if rc is not None:
                with _JOBS_LOCK:
                    current = _JOBS.get(job_id)
                    if current is not None and current.status == "RUNNING":
                        current.return_code = rc
                        current.finished_at = time.time()
                        if rc in (_STATUS_CONTROL_C_EXIT, _STATUS_CONTROL_C_EXIT_SIGNED, 3):
                            current.status = "CANCELED"
                        elif rc == 0:
                            current.status = "DONE"
                        else:
                            current.status = "FAILED"
                return _get_job_or_404(job_id)

        if timeout is not None and elapsed >= timeout:
            with _JOBS_LOCK:
                current = _JOBS.get(job_id)
                if current is not None and current.status == "RUNNING":
                    current.status = "FAILED"
                    current.finished_at = time.time()
                    current.error = f"timeout waiting for job completion ({timeout}s)"
            return _get_job_or_404(job_id)

        time.sleep(1)
        elapsed += 1


def _build_spring_response(job: JobState) -> Dict[str, Any]:
    if job.status in ("FAILED", "CANCELED"):
        return {
            "success": False,
            "canceled": job.status == "CANCELED",
            "error": job.error or ("Job annulé" if job.status == "CANCELED" else "Job échoué"),
            "job": {},
            "qualified_prospects": [],
        }

    if job.status != "DONE":
        return {
            "success": False,
            "error": f"Job non terminé (status={job.status})",
            "job": {},
            "qualified_prospects": [],
        }

    response = job.response or {}
    bridge_job = response.get("job", {}) if isinstance(response, dict) else {}
    bridge_job = bridge_job if isinstance(bridge_job, dict) else {}
    if not bridge_job:
        bridge_job = _load_job_from_repo(job.id)

    job_stats = dict(bridge_job)
    job_stats["total_collected"] = int(bridge_job.get("total_collected", 0) or 0)
    job_stats["total_cleaned"] = int(bridge_job.get("total_cleaned", 0) or 0)
    job_stats["total_deduped"] = int(bridge_job.get("total_deduped", 0) or 0)
    job_stats["total_scored"] = int(bridge_job.get("total_scored", 0) or 0)
    job_stats["total_saved"] = int(bridge_job.get("total_saved", 0) or 0)
    job_stats["total_qualified"] = int(bridge_job.get("total_qualified", 0) or 0)
    job_stats["total_duplicates"] = int(bridge_job.get("total_duplicates", 0) or 0)
    job_stats["sources_used"] = bridge_job.get("sources_used", [])
    job_stats["errors"] = bridge_job.get("errors", [])

    job_stats["totalCollected"] = job_stats["total_collected"]
    job_stats["totalCleaned"] = job_stats["total_cleaned"]
    job_stats["totalDeduped"] = job_stats["total_deduped"]
    job_stats["totalScored"] = job_stats["total_scored"]
    job_stats["totalSaved"] = job_stats["total_saved"]
    job_stats["totalQualified"] = job_stats["total_qualified"]
    job_stats["totalDuplicates"] = job_stats["total_duplicates"]
    job_stats["sourcesUsed"] = job_stats["sources_used"]

    qualified_prospects = []
    if isinstance(response, dict):
        qualified_prospects = (
            response.get("qualified_prospects")
            or response.get("qualifiedProspects")
            or []
        )

    return {
        "success": True,
        "job": job_stats,
        "qualified_prospects": qualified_prospects,
    }


# ─────────────────────────────────────────────
# CANCEL AMÉLIORÉ
# ─────────────────────────────────────────────

@app.post("/cancel/{job_id}")
def cancel_job(job_id: str) -> bool:
    logger.info(f"[CANCEL REQUEST] Received for job_id={job_id}")

    job = _get_job_or_404(job_id)

    if job.status in ("DONE", "FAILED", "CANCELED"):
        logger.info(f"[CANCEL] Job already in terminal state: {job.status}")
        return True

    proc = job.process
    if proc is None:
        logger.warning(f"[CANCEL] Job {job_id} has no process attached")
        with _JOBS_LOCK:
            job.status = "CANCELED"
            job.finished_at = time.time()
            job.error = "no process to cancel"
        return True

    logger.info(f"[CANCEL] Killing process tree - PID={proc.pid}")

    killed_successfully = False

    try:
        # Méthode principale Windows : tuer tout l'arbre de processus
        taskkill_result = subprocess.run(
            ['taskkill', '/F', '/T', '/PID', str(proc.pid)],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=12
        )
        logger.info(f"[CANCEL] taskkill /F /T returned code={taskkill_result.returncode}")
        if taskkill_result.stdout:
            logger.debug(f"[CANCEL] taskkill stdout: {taskkill_result.stdout.strip()}")
        if taskkill_result.stderr:
            logger.warning(f"[CANCEL] taskkill stderr: {taskkill_result.stderr.strip()}")

        killed_successfully = taskkill_result.returncode in (0, 128)

    except subprocess.TimeoutExpired:
        logger.warning("[CANCEL] taskkill timed out")
    except Exception as e:
        logger.error(f"[CANCEL] Error during taskkill: {e}", exc_info=True)

    # Fallback robuste si taskkill n'a pas tout tué
    if not killed_successfully:
        try:
            logger.info("[CANCEL] Fallback: proc.terminate()")
            proc.terminate()
            try:
                proc.wait(timeout=8)
                killed_successfully = True
            except subprocess.TimeoutExpired:
                logger.warning("[CANCEL] terminate() timeout → force kill()")
                proc.kill()
                proc.wait(timeout=5)
                killed_successfully = True
        except Exception as e:
            logger.error(f"[CANCEL] Fallback kill failed: {e}", exc_info=True)

    # Mise à jour de l'état du job
    with _JOBS_LOCK:
        current = _JOBS.get(job_id)
        if current is not None:
            current.status = "CANCELED"
            current.finished_at = time.time()
            current.error = "job canceled by API request"
            if current.return_code is None:
                current.return_code = -9   # Killed

    logger.info(f"[CANCEL] Job {job_id} successfully marked as CANCELED (killed={killed_successfully})")
    return True


# Les autres endpoints restent inchangés
@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/launch")
def launch(req: CrmLaunchRequest) -> Dict[str, Any]:
    job_id = req.jobId or str(uuid.uuid4())
    payload = _request_to_payload(req, job_id)


    try:
        _launch_job(payload, job_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    # Remove timeout: jobs run without artificial time limit
    final_job = _wait_for_job(job_id, timeout=None)
    return _build_spring_response(final_job)


@app.get("/status/{job_id}")
def get_status(job_id: str) -> Dict[str, Any]:
    job = _get_job_or_404(job_id)
    response = job.response or {}
    bridge_job = response.get("job", {}) if isinstance(response, dict) else {}
    errors = bridge_job.get("errors", []) if isinstance(bridge_job, dict) else []

    return {
        "jobId": job.id,
        "status": job.status,
        "totalCollected": int(_nested_get(bridge_job, "total_collected", default=0) or 0),
        "totalCleaned": int(_nested_get(bridge_job, "total_cleaned", default=0) or 0),
        "totalDeduped": int(_nested_get(bridge_job, "total_deduped", default=0) or 0),
        "totalScored": int(_nested_get(bridge_job, "total_scored", default=0) or 0),
        "totalSaved": int(_nested_get(bridge_job, "total_saved", default=0) or 0),
        "totalQualified": int(_nested_get(bridge_job, "total_qualified", default=0) or 0),
        "totalDuplicates": int(_nested_get(bridge_job, "total_duplicates", default=0) or 0),
        "errors": errors if isinstance(errors, list) else [str(errors)],
    }



@app.get("/results/{job_id}")
def get_results(job_id: str):
    # Minimal implementation to avoid syntax/runtime errors
    job = _get_job_or_404(job_id)
    # You can customize the returned data as needed
    return job.response if hasattr(job, 'response') else {}