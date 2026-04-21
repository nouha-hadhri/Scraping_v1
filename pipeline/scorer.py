"""
SCRAPING_V1 - ProspectScorer
Scoring & qualification des prospects.
Score 0-100, seuil configurable.
Statuts : QUALIFIE / NON_QUALIFIE
"""
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from typing import List, Dict, Any, Tuple, Optional

from config.settings import SCORING_RULES, QUALIFICATION_THRESHOLD, MAX_SCORE
from config.targets  import TARGETING_CRITERIA, SECTOR_KEYWORDS
from storage.models  import Prospect

logger = logging.getLogger(__name__)


class ProspectScorer:
    """
    Calcule le score de qualification d'un Prospect.

    Critères (total 100 pts) :
      sector_match   20 pts — secteur correspond au ciblage
      size_match     15 pts — taille entreprise correspond
      type_match     15 pts — type d'entreprise correspond
      geo_match      10 pts — zone géographique correspond
      email_valid    10 pts — email valide présent
      website_active 10 pts — site web présent
      phone_present  10 pts — téléphone présent
      name_complete   5 pts — nom commercial complet (≥ 3 chars)
      raison_sociale  5 pts — raison sociale présente

    Statuts :
      QUALIFIE     ≥ QUALIFICATION_THRESHOLD (50)
      NON_QUALIFIE  < QUALIFICATION_THRESHOLD
    """

    def __init__(self, criteria: Optional[Dict[str, Any]] = None):
        self.criteria = criteria or TARGETING_CRITERIA

    # ──────────────────────────────────────────
    # Public methods
    # ──────────────────────────────────────────

    def score_all(self, prospects: List[Prospect]) -> List[Prospect]:
        scored    = [self.score_one(p) for p in prospects]
        qualified = sum(1 for p in scored if p.qualification == "QUALIFIE")
        logger.info(
            f"[Scorer] {len(scored)} scorés — "
            f"{qualified} qualifiés ({100*qualified//max(len(scored),1)}%)"
        )
        return sorted(scored, key=lambda p: p.qualification_score, reverse=True)

    # Alias used by orchestrator
    def score_batch(self, prospects) -> List[Prospect]:
        items = []
        for p in prospects:
            if isinstance(p, dict):
                items.append(Prospect.from_dict(p))
            else:
                items.append(p)
        return self.score_all(items)

    def score_one(self, p: Prospect) -> Prospect:
        score  = 0
        detail = {}

        # 1. Secteur
        s, note = self._check_sector(p)
        score += s
        detail["sector_match"] = {"points": s, "max": SCORING_RULES["sector_match"], "note": note}

        # 2. Taille
        s, note = self._check_size(p)
        score += s
        detail["size_match"] = {"points": s, "max": SCORING_RULES["size_match"], "note": note}

        # 3. Type
        s, note = self._check_type(p)
        score += s
        detail["type_match"] = {"points": s, "max": SCORING_RULES["type_match"], "note": note}

        # 4. Géographie
        s, note = self._check_geo(p)
        score += s
        detail["geo_match"] = {"points": s, "max": SCORING_RULES["geo_match"], "note": note}

        # 5. Email valide
        ep = SCORING_RULES["email_valid"] if (p.email and p.email_valid) else 0
        score += ep
        detail["email_valid"] = {"points": ep, "max": SCORING_RULES["email_valid"], "note": p.email}

        # 6. Website
        wp = SCORING_RULES["website_active"] if p.website else 0
        score += wp
        detail["website_active"] = {"points": wp, "max": SCORING_RULES["website_active"], "note": p.website}

        # 7. Téléphone
        pp = SCORING_RULES["phone_present"] if p.telephone else 0
        score += pp
        detail["phone_present"] = {"points": pp, "max": SCORING_RULES["phone_present"], "note": p.telephone}

        # 8. Nom complet
        np_ = SCORING_RULES["name_complete"] if len(p.nom_commercial) >= 3 else 0
        score += np_
        detail["name_complete"] = {"points": np_, "max": SCORING_RULES["name_complete"], "note": ""}

        # 9. Raison sociale
        rp = SCORING_RULES["raison_sociale"] if p.raison_sociale else 0
        score += rp
        detail["raison_sociale"] = {"points": rp, "max": SCORING_RULES["raison_sociale"], "note": ""}

        score = min(score, MAX_SCORE)

        p.qualification_score = score
        p.score_pct           = round(100 * score / MAX_SCORE, 1)
        p.qualification       = "QUALIFIE" if score >= QUALIFICATION_THRESHOLD else "NON_QUALIFIE"
        p.score_detail        = detail
        p.criteria_met        = sum(1 for v in detail.values() if v["points"] > 0)
        p.criteria_total      = len(detail)

        return p

    # ──────────────────────────────────────────
    # Criteria checks
    # ──────────────────────────────────────────

    def _check_sector(self, p: Prospect) -> Tuple[int, str]:
        targets = self.criteria.get("secteurs_activite", [])
        if not targets:
            return SCORING_RULES["sector_match"], "no_filter"

        ps = (p.secteur_activite or "").lower()
        if not ps:
            return 0, "no_sector_data"

        for t in targets:
            if t.lower() in ps or ps in t.lower():
                return SCORING_RULES["sector_match"], f"match:{t}"

        # Keyword partial match
        for t in targets:
            for kw in SECTOR_KEYWORDS.get(t, []):
                if kw.lower() in ps:
                    return SCORING_RULES["sector_match"] // 2, f"partial:{t}"

        return 0, f"no_match ({ps})"

    def _check_size(self, p: Prospect) -> Tuple[int, str]:
        targets = self.criteria.get("tailles_entreprise", [])
        if not targets:
            return SCORING_RULES["size_match"], "no_filter"

        ps = (p.taille_entreprise or "").upper()
        if ps in targets:
            return SCORING_RULES["size_match"], f"match:{ps}"

        # Check from employes range
        if p.nombre_employes:
            try:
                n    = int(p.nombre_employes)
                lo   = self.criteria.get("employes_min", 0)
                hi   = self.criteria.get("employes_max", float("inf"))
                if lo <= n <= hi:
                    return SCORING_RULES["size_match"], f"employes:{n}"
            except (ValueError, TypeError):
                pass

        return 0, f"no_match:{ps}" if ps else "no_size_data"

    def _check_type(self, p: Prospect) -> Tuple[int, str]:
        targets = self.criteria.get("types_entreprise", [])
        if not targets:
            return SCORING_RULES["type_match"], "no_filter"

        pt = (p.type_entreprise or "").upper()
        if not pt:
            return 0, "no_type_data"

        for t in targets:
            if t.upper() in pt or pt in t.upper():
                return SCORING_RULES["type_match"], f"match:{t}"
        return 0, f"no_match:{pt}"

    def _check_geo(self, p: Prospect) -> Tuple[int, str]:
        loc            = self.criteria.get("localisation", {})
        target_pays    = [x.lower() for x in loc.get("pays", [])]
        target_regions = [x.lower() for x in loc.get("regions", [])]
        target_villes  = [x.lower() for x in loc.get("villes", [])]

        if not any([target_pays, target_regions, target_villes]):
            return SCORING_RULES["geo_match"], "no_filter"

        pp = (p.pays   or "").lower()
        pr = (p.region or "").lower()
        pv = (p.ville  or "").lower()

        pts = SCORING_RULES["geo_match"]
        if target_pays and pp:
            if not any(t in pp for t in target_pays):
                pts = pts // 2   # Malus si pays différent

        if target_regions and pr and any(t in pr for t in target_regions):
            return pts, f"region:{pr}"
        if target_villes and pv and any(t in pv for t in target_villes):
            return pts, f"ville:{pv}"

        return pts if pp else 0, f"pays:{pp}"

    # ──────────────────────────────────────────
    # Stats
    # ──────────────────────────────────────────

    def get_summary(self) -> str:
        return (
            f"\n[Scorer] Seuil qualification: {QUALIFICATION_THRESHOLD}/100\n"
            f"  QUALIFIE ≥ {QUALIFICATION_THRESHOLD}  |  NON_QUALIFIE < {QUALIFICATION_THRESHOLD}\n"
        )

    @staticmethod
    def get_stats(prospects: List[Any]) -> Dict[str, Any]:
        if not prospects:
            return {
                "total": 0, "qualified": 0, "non_qualified": 0,
                "qualification_rate_pct": 0.0, "avg_score": 0.0,
                "max_score": 0, "min_score": 0,
                "threshold": QUALIFICATION_THRESHOLD,
                "enrich": {
                    "avg_score": 0.0, "full_4": 0,
                    "dist": {"0": 0, "1": 0, "2": 0, "3": 0, "4": 0},
                    "with_email_pct": 0.0, "with_phone_pct": 0.0,
                    "with_website_pct": 0.0, "mx_verified": 0,
                },
                "sector_mapping": {
                    "avg_sector_confidence": 0.0, "avg_nlp_confidence": 0.0,
                    "exact_fk": 0, "partial_fk": 0, "fallback_fk": 0,
                },
            }

        def _get(p, field, default=0):
            return getattr(p, field, None) if isinstance(p, Prospect) else p.get(field, default)

        scores    = [_get(p, "qualification_score") or 0 for p in prospects]
        qualified = [p for p in prospects if (_get(p, "qualification") or _get(p, "statut") or "") == "QUALIFIE"]

        # ── Enrich score stats ────────────────────────────────────────
        e_scores  = [_get(p, "enrich_score") or 0 for p in prospects]
        n         = max(len(prospects), 1)
        e_dist    = {str(s): e_scores.count(s) for s in range(5)}

        with_email   = sum(1 for p in prospects if _get(p, "email"))
        with_phone   = sum(1 for p in prospects if _get(p, "telephone"))
        with_website = sum(1 for p in prospects if _get(p, "website"))
        mx_verified  = sum(1 for p in prospects if _get(p, "email_mx_verified", False))

        # Statistiques de confiance secteur (FK mapping) et NLP
        sector_confs  = [_get(p, "sector_confidence") or 0.0 for p in prospects]
        nlp_confs     = [_get(p, "nlp_confidence") or 0.0 for p in prospects]
        n_exact_fk    = sum(1 for c in sector_confs if c >= 0.90)
        n_partial_fk  = sum(1 for c in sector_confs if 0.60 <= c < 0.90)
        n_fallback_fk = sum(1 for c in sector_confs if 0 < c < 0.60)

        return {
            "total":                  len(prospects),
            "qualified":              len(qualified),
            "non_qualified":          len(prospects) - len(qualified),
            "qualification_rate_pct": round(100 * len(qualified) / n, 1),
            "avg_score":              round(sum(scores) / max(len(scores), 1), 1),
            "max_score":              max(scores) if scores else 0,
            "min_score":              min(scores) if scores else 0,
            "threshold":              QUALIFICATION_THRESHOLD,
            "enrich": {
                "avg_score":       round(sum(e_scores) / max(len(e_scores), 1), 2),
                "full_4":          e_dist.get("4", 0),
                "dist":            e_dist,
                "with_email_pct":   round(100 * with_email   / n, 1),
                "with_phone_pct":   round(100 * with_phone   / n, 1),
                "with_website_pct": round(100 * with_website / n, 1),
                "mx_verified":      mx_verified,
            },
            "sector_mapping": {
                "avg_sector_confidence": round(sum(sector_confs) / max(len(sector_confs), 1), 3),
                "avg_nlp_confidence":    round(sum(nlp_confs)    / max(len(nlp_confs),    1), 3),
                "exact_fk":    n_exact_fk,
                "partial_fk":  n_partial_fk,
                "fallback_fk": n_fallback_fk,
            },
        }


# Alias for backward compatibility
Scorer = ProspectScorer