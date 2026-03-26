"""
SCRAPING_V1 - Models
Dataclasses représentant les entités du système.
"""
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dataclasses import dataclass, field, asdict
from typing     import Optional, List, Dict
from datetime   import datetime
from enum       import Enum


class LeadStatut(str, Enum):
    QUALIFIE     = "QUALIFIE"
    NON_QUALIFIE = "NON_QUALIFIE"
    EN_ATTENTE   = "EN_ATTENTE"


class LeadSource(str, Enum):
    API      = "API"
    FORM     = "FORM"
    SCRAPING = "SCRAPING"
    MANUAL   = "MANUAL"


class TailleEntreprise(str, Enum):
    TPE = "TPE"
    PME = "PME"
    ETI = "ETI"
    GE  = "GE"


class JobStatut(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DONE    = "DONE"
    FAILED  = "FAILED"


@dataclass
class Prospect:
    """
    Entité Prospect — représente une entreprise collectée pour prospection B2B.
    """

    # ── Identité ──────────────────────────────────────────────────────
    nom_commercial: str = ""
    raison_sociale: str = ""

    # ── Contacts ──────────────────────────────────────────────────────
    email:        str = ""
    telephone:    str = ""
    website:      str = ""
    linkedin_url: str = ""

    # ── Localisation ──────────────────────────────────────────────────
    adresse:     str = ""
    ville:       str = ""
    region:      str = ""
    pays:        str = ""
    code_postal: str = ""

    # ── Entreprise ────────────────────────────────────────────────────
    secteur_activite:  str            = ""
    type_entreprise:   str            = ""
    taille_entreprise: str            = ""
    nombre_employes:   Optional[int]  = None
    chiffre_affaires:  Optional[float] = None
    description:       str            = ""
    code_naf:          str            = ""

    # ── Identifiants légaux ───────────────────────────────────────────
    siren: str = ""
    siret: str = ""

    # ── Scoring & Qualification ───────────────────────────────────────
    qualification_score: int   = 0
    score_pct:           float = 0.0
    statut:              str   = LeadStatut.NON_QUALIFIE.value
    score_detail:        Dict  = field(default_factory=dict)
    criteria_met:        int   = 0
    criteria_total:      int   = 0

    # ── Métadonnées ───────────────────────────────────────────────────
    source:            str   = LeadSource.SCRAPING.value
    segment:           str   = ""
    sector_confidence: float = 0.0
    email_valid:       bool  = False
    website_active:    bool  = False

    # ── Enrichissement contacts ───────────────────────────────────────
    # Score de qualité des données de contact (0-4) :
    #   +1 email présent et valide
    #   +1 téléphone présent
    #   +1 website présent et actif
    #   +1 linkedin_url présent
    # Distinct du qualification_score (critères CRM) — mesure la
    # complétude des données de contact après l'étape d'enrichissement.
    enrich_score:       int  = 0
    # True si le domaine MX a été vérifié (vérification DNS)
    email_mx_verified:  bool = False

    # ── Déduplication ─────────────────────────────────────────────────
    hash_dedup: str = ""

    # ── NLP ───────────────────────────────────────────────────────────
    raw_text: str = ""

    # ── Timestamps ────────────────────────────────────────────────────
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())

    # ──────────────────────────────────────────
    # Helper methods
    # ──────────────────────────────────────────

    def is_contactable(self) -> bool:
        """True si le prospect a au moins un moyen de contact."""
        return bool(self.email or self.telephone or self.website)

    def completeness_score(self) -> float:
        """
        Calcule un score de complétude (0-100) basé sur les champs remplis.
        """
        scored_fields = {
            "nom_commercial":    10,
            "raison_sociale":     5,
            "email":             15,
            "telephone":         15,
            "website":           10,
            "adresse":            5,
            "ville":              5,
            "pays":               5,
            "secteur_activite":  10,
            "taille_entreprise":  5,
            "type_entreprise":    5,
            "description":        5,
            "siren":              5,
        }
        total_weight = sum(scored_fields.values())
        earned = sum(
            w for f, w in scored_fields.items()
            if getattr(self, f, None)
        )
        return round(100 * earned / total_weight, 1)

    def to_dict(self) -> dict:
        return asdict(self)

    def to_csv_row(self) -> dict:
        """Retourne les champs pour export CSV (sans score_detail et raw_text)."""
        d = asdict(self)
        d.pop("score_detail", None)
        d.pop("raw_text",     None)
        d.pop("hash_dedup",   None)
        return d

    @classmethod
    def from_dict(cls, data: dict) -> "Prospect":
        valid = {k: v for k, v in data.items() if k in cls.__dataclass_fields__}
        return cls(**valid)

    def __repr__(self) -> str:
        return (
            f"<Prospect {self.nom_commercial!r} | {self.ville} | "
            f"score={self.qualification_score} | enrich={self.enrich_score}/4 | {self.statut}>"
        )


@dataclass
class CollectionJob:
    """
    Représente un job de collecte (enregistrement de l'exécution).
    """
    id:                 str            = ""
    name:               str            = ""
    type:               str            = "SCRAPING"
    status:             str            = JobStatut.PENDING.value  # Utiliser JobStatut enum
    parameters_json:    str            = ""
    started_at:         Optional[str]  = None
    finished_at:        Optional[str]  = None
    total_collected:    int            = 0
    total_cleaned:      int            = 0
    total_deduped:      int            = 0
    total_scored:       int            = 0
    total_saved:        int            = 0
    total_qualified:    int            = 0
    total_duplicates:   int            = 0
    sources_used:       List[str]      = field(default_factory=list)
    errors:             List[str]      = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)