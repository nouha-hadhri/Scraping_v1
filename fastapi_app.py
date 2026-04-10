"""
FastAPI bridge for SCRAPING_V1.

Expose des endpoints HTTP internes appelés par Spring Boot (AutoProspectionOrchestratorService).
Routes exposées (utilisées par Spring) :
  POST /launch              ← Spring appelle ça via AutoProspectionOrchestratorService
  GET  /status/{jobId}      ← polling Spring (si besoin futur)
  POST /cancel/{jobId}      ← annulation depuis Spring
  GET  /results/{jobId}     ← pagination résultats

Les routes /api/crm/auto-prospection/* sont gérées exclusivement par Spring Controller.

Run:
  uvicorn fastapi_app:app --host 0.0.0.0 --port 8000
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import uuid
import copy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from storage.repository import ProspectRepository

APP_ROOT = Path(__file__).resolve().parent
ORCHESTRATOR_PATH = APP_ROOT / "orchestrator.py"

# ─────────────────────────────────────────────
# NORMALISATION CRITÈRES (Frontend Spring Boot → Backend Python)
# ─────────────────────────────────────────────

_CATEGORY_MAP = {
    "1-10":     "TPE",
    "11-50":    "PME",
    "51-250":   "ETI",
    "251-5000": "ETI",
    "251+":     "GE",
    "5000+":    "GE",
    "5000":     "GE",
    # Variantes possibles venant du frontend
    "tpe":      "TPE",
    "pme":      "PME",
    "eti":      "ETI",
    "ge":       "GE",
    "grande":   "GE",
}


def normalize_criteria(criteria: dict) -> dict:
    """
    Normalise les critères envoyés par le frontend Spring Boot
    avant de les passer au pipeline Python (orchestrator).

    Transformations principales :
      - Mapping des catégories de taille d'entreprise (1-10 → TPE, etc.)
      - Aplatissement des structures pour compatibilité avec SearchTarget et ProspectScorer
      - Renommage de clés camelCase → snake_case quand nécessaire
    """
    if not isinstance(criteria, dict):
        return criteria

    # Copie profonde pour éviter de modifier l'original
    crit = copy.deepcopy(criteria)

    # 1. Normalisation de la taille d'entreprise
    taille = crit.get("taille_entreprise") or crit.get("tailleEntreprise")
    if isinstance(taille, dict):
        raw_categories = taille.get("categories") or taille.get("category") or []
        if isinstance(raw_categories, list):
            normalized_cats = [
                _CATEGORY_MAP.get(str(cat).strip(), str(cat).strip())
                for cat in raw_categories if cat
            ]
            taille["categories"] = normalized_cats

        # Mise à plat pour SearchTarget + Scorer
        crit["tailles_entreprise"] = taille.get("categories", [])
        crit["employes_min"] = taille.get("nb_employes_min") or taille.get("nbEmployesMin", 1)
        crit["employes_max"] = taille.get("nb_employes_max") or taille.get("nbEmployesMax", None)

        # On garde aussi la structure originale si besoin
        crit["taille_entreprise"] = taille

    # 2. Normalisation des autres champs (camelCase → snake_case)
    if "secteurActivite" in crit:
        crit["secteurs_activite"] = crit.pop("secteurActivite")

    if "typesEntreprise" in crit:
        crit["types_entreprise"] = crit.pop("typesEntreprise")

    if "motsCles" in crit:
        crit["keywords"] = crit.pop("motsCles")

    # Support payload snake_case (Spring DTO sérialisé)
    if "mots_cles" in crit and "keywords" not in crit:
        crit["keywords"] = crit.pop("mots_cles")

    if "zoneGeographique" in crit:
        zone = crit.pop("zoneGeographique")
        if isinstance(zone, dict):
            crit["pays"]    = zone.get("pays") or zone.get("zone_geographique", ["France"])
            crit["regions"] = zone.get("regions", [])
            crit["villes"]  = zone.get("villes", [])

    # Support payload snake_case (Spring DTO sérialisé)
    if "zone_geographique" in crit:
        zone = crit.pop("zone_geographique")
        if isinstance(zone, dict):
            crit["pays"] = zone.get("pays") or zone.get("zone_geographique", ["France"])
            crit["regions"] = zone.get("regions", [])
            crit["villes"] = zone.get("villes", [])

    # 3. Max résultats
    if "max_prospects_total" in crit:
        crit["max_resultats"] = crit.pop("max_prospects_total")

    if "maxResultats" in crit:
        crit["max_resultats"] = crit.pop("maxResultats")

    return crit


# ─────────────────────────────────────────────
# Modèles de requête
# ─────────────────────────────────────────────

class CrmLaunchRequest(BaseModel):
    """
    Body envoyé par AutoProspectionOrchestratorService.invokeScrapingApi() :
      { "jobId": "...", "criteria": { ... } }
    """
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

_JOBS: Dict[str, JobState] = {}
_JOBS_LOCK = threading.Lock()
_REPO = ProspectRepository()

# Windows NTSTATUS returned when a process is terminated by CTRL+C / CTRL+BREAK.
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
        logger.info(f"[Normalize] Critères normalisés → tailles={normalized_criteria.get('tailles_entreprise')}")

    if req.source_filter:
        payload["source_filter"] = req.source_filter
    if req.target_key:
        payload["target_key"] = req.target_key

    return payload


def _write_payload_file(payload: Dict[str, Any]) -> str:
    handle = tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".json",
        prefix="scraping_bridge_",
        delete=False,
        encoding="utf-8",
    )
    try:
        json.dump(payload, handle, ensure_ascii=False)
        handle.flush()
        return handle.name
    finally:
        handle.close()


def _build_command(payload_file: str) -> list[str]:
    return [
        sys.executable,
        str(ORCHESTRATOR_PATH),
        "--bridge-json-file",
        payload_file,
        "--json-output",
    ]


def _parse_bridge_stdout(stdout: str) -> Dict[str, Any]:
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    if not lines:
        return {}
    try:
        return json.loads(lines[-1])
    except json.JSONDecodeError:
        return {"raw_stdout": stdout}


def _watch_job(job_id: str) -> None:
    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
    if job is None or job.process is None:
        return

    proc = job.process
    stdout, stderr = proc.communicate()

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
        elif proc.returncode in (_STATUS_CONTROL_C_EXIT, _STATUS_CONTROL_C_EXIT_SIGNED):
            current.status = "CANCELED"
            current.error = "orchestrator terminated by console control signal"
        elif proc.returncode == 0:
            current.status = "DONE"
        else:
            current.status = "FAILED"
            if isinstance(bridge_response, dict):
                current.error = bridge_response.get("error") or current.error
            if not current.error and proc.returncode == 2:
                current.error = "orchestrator exited with code 2 (invalid payload/arguments)"
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

    creationflags = 0
    if os.name == "nt" and hasattr(subprocess, "CREATE_NEW_PROCESS_GROUP"):
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP

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
    except Exception:
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


def _wait_for_job(job_id: str, timeout: int = 600) -> JobState:
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
                        if rc in (_STATUS_CONTROL_C_EXIT, _STATUS_CONTROL_C_EXIT_SIGNED):
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

        time.sleep(2)
        elapsed += 2


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
    bridge_job = response.get("job", {})

    job_stats = {
        "totalCollected":  int(bridge_job.get("total_collected", 0) or 0),
        "totalCleaned":    int(bridge_job.get("total_cleaned", 0) or 0),
        "totalDeduped":    int(bridge_job.get("total_deduped", 0) or 0),
        "totalScored":     int(bridge_job.get("total_scored", 0) or 0),
        "totalQualified":  int(bridge_job.get("total_qualified", 0) or 0),
        "totalDuplicates": int(bridge_job.get("total_duplicates", 0) or 0),
        "sourcesUsed":     bridge_job.get("sources_used", []),
        "errors":          bridge_job.get("errors", []),
        # snake_case fallback
        "total_collected":  int(bridge_job.get("total_collected", 0) or 0),
        "total_cleaned":    int(bridge_job.get("total_cleaned", 0) or 0),
        "total_deduped":    int(bridge_job.get("total_deduped", 0) or 0),
        "total_scored":     int(bridge_job.get("total_scored", 0) or 0),
        "total_qualified":  int(bridge_job.get("total_qualified", 0) or 0),
        "total_duplicates": int(bridge_job.get("total_duplicates", 0) or 0),
        "sources_used":     bridge_job.get("sources_used", []),
        "started_at":       bridge_job.get("started_at"),
        "finished_at":      bridge_job.get("finished_at"),
    }

    qualified_prospects = (
        response.get("qualified_prospects")
        or response.get("qualifiedProspects")
        or []
    )

    return {
        "success": True,
        "job": job_stats,
        "qualified_prospects": qualified_prospects,
        "output": {
            "qualified_prospects": qualified_prospects,
        },
    }


# ─────────────────────────────────────────────
# Logging (ajouté ici car utilisé dans normalize)
# ─────────────────────────────────────────────

import logging
logger = logging.getLogger("fastapi_app")


# ─────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────

@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/launch")
def launch(req: CrmLaunchRequest) -> Dict[str, Any]:
    """
    Appelé par AutoProspectionOrchestratorService.invokeScrapingApi().
    """
    job_id = req.jobId or str(uuid.uuid4())
    payload = _request_to_payload(req, job_id)

    try:
        _launch_job(payload, job_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    final_job = _wait_for_job(job_id, timeout=req.wait_timeout_seconds)
    return _build_spring_response(final_job)


@app.get("/status/{job_id}")
def get_status(job_id: str) -> Dict[str, Any]:
    job = _get_job_or_404(job_id)
    response = job.response or {}
    bridge_job = response.get("job", {})
    errors = bridge_job.get("errors", [])

    return {
        "jobId": job.id,
        "status": job.status,
        "totalCollected": int(bridge_job.get("total_collected", 0) or 0),
        "totalQualified": int(bridge_job.get("total_qualified", 0) or 0),
        "totalDuplicates": int(bridge_job.get("total_duplicates", 0) or 0),
        "startedAt": bridge_job.get("started_at"),
        "finishedAt": bridge_job.get("finished_at"),
        "errors": errors if isinstance(errors, list) else [str(errors)],
    }


@app.post("/cancel/{job_id}")
def cancel_job(job_id: str) -> bool:
    job = _get_job_or_404(job_id)

    if job.status in ("DONE", "FAILED", "CANCELED"):
        return True

    proc = job.process
    if proc is None:
        raise HTTPException(status_code=409, detail="job process unavailable")

    try:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"cancel failed: {exc}") from exc

    with _JOBS_LOCK:
        current = _JOBS.get(job_id)
        if current is not None:
            current.status = "CANCELED"
            current.finished_at = time.time()
            current.return_code = proc.returncode
            current.error = "job canceled by API request"

    return True


@app.get("/results/{job_id}")
def get_results(job_id: str, page: int = 0, size: int = 20) -> Dict[str, Any]:
    if page < 0:
        raise HTTPException(status_code=400, detail="page must be >= 0")
    if size <= 0:
        raise HTTPException(status_code=400, detail="size must be > 0")

    scored = _REPO.load_scored()
    filtered = [p for p in scored if str(p.get("job_id", "")) == str(job_id)]
    return _to_page(filtered, page=page, size=size)