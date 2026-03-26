"""
SCRAPING_V1 - ProspectEmbedder
Analyse NLP pour détecter secteur et taille depuis le texte.
Mode SBERT (si installé) ou keyword fallback.
"""
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import re
from typing import List, Dict, Tuple, Any

from config.settings import SBERT_MODEL, USE_EMBEDDINGS
from config.targets  import SECTOR_KEYWORDS, SIZE_RANGES
from storage.models  import Prospect

logger = logging.getLogger(__name__)

_sbert_model = None


def _load_sbert():
    global _sbert_model
    if _sbert_model is None:
        try:
            from sentence_transformers import SentenceTransformer
            logger.info(f"[Embedder] Loading SBERT: {SBERT_MODEL}…")
            _sbert_model = SentenceTransformer(SBERT_MODEL)
            logger.info("[Embedder] SBERT ready.")
        except ImportError:
            logger.warning("[Embedder] sentence-transformers non installé -> mode keyword.")
            _sbert_model = "UNAVAILABLE"
    return _sbert_model


class ProspectEmbedder:
    """
    Enrichit les Prospect avec :
    - secteur_activite (si absent)
    - taille_entreprise (si absente)
    - sector_confidence (0-1)
    """

    def __init__(self):
        self._use_sbert        = USE_EMBEDDINGS
        self._labels           = list(SECTOR_KEYWORDS.keys())
        self._sector_texts     = [" ".join(kws) for kws in SECTOR_KEYWORDS.values()]
        self._sector_embeddings = None

    def enrich_all(self, prospects: List[Prospect]) -> List[Prospect]:
        return [self.enrich_one(p) for p in prospects]

    # Alias used by orchestrator
    def analyze_batch(self, prospects) -> List[Prospect]:
        items = []
        for p in prospects:
            if isinstance(p, dict):
                items.append(Prospect.from_dict(p))
            else:
                items.append(p)
        return self.enrich_all(items)

    def analyze_one(self, p: Prospect) -> Prospect:
        return self.enrich_one(p)

    def enrich_one(self, p: Prospect) -> Prospect:
        # Build text corpus for analysis
        text = " ".join(filter(None, [
            p.nom_commercial, p.raison_sociale,
            p.description, p.raw_text, p.secteur_activite,
        ]))

        # ── Secteur ───────────────────────────────
        sector_missing = (
            not p.secteur_activite
            or p.secteur_activite in ("Non spécifié", "")
            or len(p.secteur_activite) < 4
        )
        if sector_missing:
            sector, conf = self._detect_sector(text)
            if sector:
                p.secteur_activite  = sector
                p.sector_confidence = round(conf, 3)
            else:
                p.sector_confidence = 0.0
        else:
            normalized = self._normalize_sector(p.secteur_activite)
            if normalized:
                p.secteur_activite = normalized
            p.sector_confidence = 1.0

        # ── Taille ────────────────────────────────
        if not p.taille_entreprise or p.taille_entreprise not in SIZE_RANGES:
            taille = self._detect_taille(text, p.nombre_employes)
            if taille:
                p.taille_entreprise = taille

        return p

    # ──────────────────────────────────────────
    # Sector detection
    # ──────────────────────────────────────────

    def _detect_sector(self, text: str) -> Tuple[str, float]:
        if not text.strip():
            return "", 0.0
        if self._use_sbert:
            sector, conf = self._detect_sbert(text)
            # Seuil 0.30 : en dessous, la similarité cosinus est trop faible
            # pour être fiable (valeurs proches de 0 ou négatives indiquent
            # l'absence de signal pertinent).  On bascule sur keyword fallback
            # plutôt que de retourner un secteur peu pertinent.
            # La similarité cosinus peut être négative — on la clamp à 0 pour
            # éviter de retourner une confiance absurde à l'appelant.
            conf = max(conf, 0.0)
            if conf > 0.30:
                return sector, conf
        return self._detect_keywords(text)

    def _detect_sbert(self, text: str) -> Tuple[str, float]:
        try:
            import numpy as np
            model = _load_sbert()
            if model == "UNAVAILABLE":
                return "", 0.0

            if self._sector_embeddings is None:
                self._sector_embeddings = model.encode(
                    self._sector_texts, convert_to_numpy=True
                )

            emb   = model.encode([text[:1000]], convert_to_numpy=True)[0]
            norms = (
                (self._sector_embeddings @ emb)
                / (
                    np.linalg.norm(self._sector_embeddings, axis=1)
                    * np.linalg.norm(emb)
                    + 1e-8
                )
            )
            best = int(np.argmax(norms))
            return self._labels[best], float(norms[best])
        except Exception as e:
            logger.debug(f"[Embedder] SBERT error: {e}")
            return "", 0.0

    def _detect_keywords(self, text: str) -> Tuple[str, float]:
        tl     = text.lower()
        scores = {
            s: sum(tl.count(kw.lower()) for kw in kws)
            for s, kws in SECTOR_KEYWORDS.items()
        }
        scores = {s: c for s, c in scores.items() if c > 0}
        if not scores:
            return "", 0.0
        best      = max(scores, key=scores.__getitem__)
        total     = sum(scores.values())
        conf      = min(scores[best] / max(total, 1), 1.0)
        return best, conf

    def _normalize_sector(self, raw: str) -> str:
        rl = raw.lower()
        for sector, kws in SECTOR_KEYWORDS.items():
            if any(kw.lower() in rl for kw in kws):
                return sector
        return raw

    # ──────────────────────────────────────────
    # Size detection
    # ──────────────────────────────────────────

    @staticmethod
    def _detect_taille(text: str, employes: Any = None) -> str:
        if employes:
            try:
                n = int(employes)
                for size, (lo, hi) in SIZE_RANGES.items():
                    if lo <= n <= hi:
                        return size
            except (ValueError, TypeError):
                pass

        tl      = text.lower()
        patterns = [
            (r"\b[1-9]\s+(?:employé|salarié|collaborateur)", "TPE"),
            (r"\btpe\b|très petite entreprise|micro.entreprise",           "TPE"),
            (r"\b(?:1[0-9]|[2-9]\d|1\d{2}|2[0-3]\d)\s+(?:employé|salarié)", "PME"),
            (r"\bpme\b|petite.moyenne entreprise",                         "PME"),
            (r"\b(?:2[4-9]\d|[3-9]\d{2}|[1-4]\d{3})\s+(?:employé|salarié)", "ETI"),
            (r"\beti\b|taille intermédiaire",                              "ETI"),
            (r"\b(?:[5-9]\d{3}|\d{5,})\s+(?:employé|salarié)",           "GE"),
            (r"\bgrand groupe\b|grande entreprise|multinationale",         "GE"),
        ]
        for pat, size in patterns:
            if re.search(pat, tl):
                return size
        return ""