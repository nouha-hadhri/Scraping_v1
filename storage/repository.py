"""
SCRAPING_V1 - Repository
Couche de persistance : lecture/écriture CSV et JSON.
"""
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import csv
import json
import logging
from datetime import datetime
from pathlib  import Path
from typing   import List, Dict, Any, Optional

from config.settings import (
    OUTPUT_JSON, OUTPUT_CSV, SCORED_JSON, SCORED_CSV,
    QUALIFIED_JSON, QUALIFIED_CSV, JOBS_JSON,
)
from storage.models import Prospect, CollectionJob

logger = logging.getLogger(__name__)


class ProspectRepository:
    """
    Gère la persistance des prospects dans les fichiers CSV et JSON.
    Peut être instancié avec des chemins custom (utile pour les tests).
    """

    def __init__(
        self,
        json_path:   Optional[Path] = None,
        csv_path:    Optional[Path] = None,
        scored_json: Optional[Path] = None,
        scored_csv:  Optional[Path] = None,
    ):
        self.json_path   = Path(json_path)   if json_path   else OUTPUT_JSON
        self.csv_path    = Path(csv_path)    if csv_path    else OUTPUT_CSV
        self.scored_json = Path(scored_json) if scored_json else SCORED_JSON
        self.scored_csv  = Path(scored_csv)  if scored_csv  else SCORED_CSV

    # ──────────────────────────────────────────
    # Save
    # ──────────────────────────────────────────

    def save_all(self, prospects: List[Prospect]) -> Dict[str, Any]:
        """
        Sauvegarde une liste de Prospect en JSON + CSV.
        Retourne un dict d'info (chemins, nb sauvegardés).
        """
        data = [p.to_dict() if isinstance(p, Prospect) else p for p in prospects]
        self._write_json(self.json_path, data)
        self._write_csv(self.csv_path,  [p.to_csv_row() if isinstance(p, Prospect) else p for p in prospects])
        logger.info(f"[Repo] Saved {len(data)} prospects -> {self.json_path.name}")
        return {
            "json_path": str(self.json_path),
            "csv_path":  str(self.csv_path),
            "n_saved":   len(data),
        }

    def save_scored(self, prospects: List[Any]) -> None:
        """Sauvegarde les prospects scorés."""
        data     = [p.to_dict() if isinstance(p, Prospect) else p for p in prospects]
        csv_data = [p.to_csv_row() if isinstance(p, Prospect) else p for p in prospects]
        self._write_json(self.scored_json, data)
        self._write_csv(self.scored_csv,  csv_data)
        logger.info(f"[Repo] Saved {len(data)} scored prospects")

    def save_raw(self, prospects: List[Any]) -> None:
        """Alias pour save_all (raw).
        
        FIX : le CSV utilise maintenant to_csv_row() comme save_all, ce qui exclut
        score_detail, raw_text et hash_dedup — des champs JSON imbriqués qui
        produisaient des cellules illisibles dans les fichiers CSV bruts.
        """
        data     = [p.to_dict()     if isinstance(p, Prospect) else p for p in prospects]
        csv_data = [p.to_csv_row()  if isinstance(p, Prospect) else p for p in prospects]
        self._write_json(self.json_path, data)
        self._write_csv(self.csv_path,   csv_data)

    def save_job(self, job: CollectionJob) -> None:
        jobs = self.load_jobs()
        found = False
        for i, j in enumerate(jobs):
            if j.get("id") == job.id:
                jobs[i] = job.to_dict()
                found = True
                break
        if not found:
            jobs.append(job.to_dict())
        self._write_json(JOBS_JSON, jobs)

    def export_qualified_only(self) -> Path:
        """Exporte uniquement les prospects qualifiés."""
        scored = self._read_json(self.scored_json)
        qualified = [p for p in scored if p.get("statut") == "QUALIFIE"]
        self._write_json(QUALIFIED_JSON, qualified)
        self._write_csv(QUALIFIED_CSV,   qualified)
        logger.info(f"[Repo] Exported {len(qualified)} qualified prospects")
        return QUALIFIED_CSV

    # ──────────────────────────────────────────
    # Load
    # ──────────────────────────────────────────

    def load_from_json(self) -> List[Prospect]:
        """Charge les prospects depuis le JSON principal."""
        data = self._read_json(self.json_path)
        return [Prospect.from_dict(d) for d in data]

    def load_from_csv(self) -> List[Dict]:
        """Charge les prospects depuis le CSV principal."""
        return self._read_csv(self.csv_path)

    def load_raw(self) -> List[Dict]:
        return self._read_json(self.json_path)

    def load_scored(self) -> List[Dict]:
        return self._read_json(self.scored_json)

    def load_jobs(self) -> List[Dict]:
        return self._read_json(JOBS_JSON)

    # ──────────────────────────────────────────
    # Internal I/O
    # ──────────────────────────────────────────

    @staticmethod
    def _write_json(path: Path, data: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        # FIX – Default Values Risk / Silent Failure: using default=str hides
        # serialisation bugs by silently converting unknown objects to strings.
        # Use an explicit fallback that logs a warning so issues are visible.
        def _safe_default(obj):
            logger.warning(
                f"[Repo] Non-serialisable type {type(obj).__name__!r} "
                f"in JSON output — converting to string."
            )
            return str(obj)

        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=_safe_default)
        except Exception as e:
            logger.error(f"[Repo] JSON write error {path}: {e}")

    @staticmethod
    def _read_json(path: Path) -> List[Dict]:
        if not path.exists():
            return []
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, list) else data.get("prospects", [])
        except json.JSONDecodeError as e:
            # FIX – Silent Failure: surface JSON corruption clearly instead of
            # returning an empty list with no indication of what went wrong.
            logger.error(f"[Repo] Corrupt JSON file {path}: {e}")
            return []
        except Exception as e:
            logger.error(f"[Repo] JSON read error {path}: {e}")
            return []

    @staticmethod
    def _write_csv(path: Path, data: List[Dict[str, Any]]) -> None:
        if not data:
            return
        path.parent.mkdir(parents=True, exist_ok=True)

        # Build field order
        priority = [
            "nom_commercial", "raison_sociale", "email", "telephone", "website",
            "adresse", "ville", "region", "pays", "code_postal",
            "secteur_activite", "taille_entreprise", "type_entreprise",
            "nombre_employes", "chiffre_affaires", "description",
            "siren", "siret", "code_naf",
            "qualification_score", "score_pct", "statut", "segment",
            "enrich_score", "email_mx_verified",
            "criteria_met", "criteria_total",
            "source", "email_valid", "website_active", "linkedin_url",
            "created_at",
        ]
        all_keys = set()
        for row in data:
            all_keys.update(row.keys())

        fieldnames = [f for f in priority if f in all_keys]
        fieldnames += sorted(all_keys - set(priority))

        try:
            with open(path, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=fieldnames,
                    extrasaction="ignore",
                    quoting=csv.QUOTE_ALL,
                )
                writer.writeheader()
                for row in data:
                    flat = {
                        k: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v
                        for k, v in row.items()
                    }
                    writer.writerow(flat)
        except Exception as e:
            logger.error(f"[Repo] CSV write error {path}: {e}")

    @staticmethod
    def _read_csv(path: Path) -> List[Dict]:
        if not path.exists():
            return []
        try:
            with open(path, "r", encoding="utf-8-sig") as f:
                return list(csv.DictReader(f))
        except Exception as e:
            logger.error(f"[Repo] CSV read error {path}: {e}")
            return []