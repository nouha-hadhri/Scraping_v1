"""
SCRAPING_V1 - Deduplicator
Détecte et supprime les doublons dans les Prospect collectés.
Stratégies : hash fort (email/domain/phone), fuzzy name matching.
supporte aussi la fusion enrichissante (merge_and_deduplicate) pour combiner les données de doublons au lieu de simplement les rejeter.
"""
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import hashlib
import logging
import re
from typing import List, Dict, Set, Tuple, Optional
from urllib.parse import urlparse

from storage.models  import Prospect

logger = logging.getLogger(__name__)


class Deduplicator:
    """
    Déduplique une liste de Prospect.

    Critères (par priorité décroissante):
      1. hash_dedup identique (généré par BaseScraper._make_hash)
      2. Email identique
      3. Domaine web identique
      4. SIREN identique (si 9 chiffres)
      5. Nom normalisé + ville identiques (fuzzy via similarity_threshold)
    """

    def __init__(self, similarity_threshold: float = 0.85):
        # NOTE : similarity_threshold est réservé pour une future implémentation
        # de fuzzy matching (ex: rapidfuzz.ratio) sur le nom+ville.
        # La déduplication actuelle par nom utilise un hash MD5 exact (_name_key)
        # — aucune comparaison par seuil n'est effectuée.  Le paramètre est
        # conservé dans la signature pour ne pas casser les appelants existants,
        # mais il n'influence pas le comportement tant que le fuzzy n'est pas
        # implémenté.
        # TODO: implémenter la comparaison fuzzy quand rapidfuzz est disponible.
        self.threshold          = similarity_threshold
        self._seen_hashes: Set[str]   = set()
        self._seen_emails: Set[str]   = set()
        self._seen_domains: Set[str]  = set()
        self._seen_sirens: Set[str]   = set()
        self._seen_names: Set[str]    = set()

    def deduplicate(
        self, prospects: List[Prospect]
    ) -> Tuple[List[Prospect], int]:
        """
        Déduplique la liste. Retourne (uniques, nb_doublons).
        """
        unique: List[Prospect] = []
        n_dupes = 0

        for p in prospects:
            reason = self._is_dup(p)
            if reason:
                logger.debug(f"[Dedup] Doublon: {p.nom_commercial!r} — {reason}")
                n_dupes += 1
            else:
                self._register(p)
                unique.append(p)

        logger.info(f"[Dedup] {len(unique)} uniques / {n_dupes} doublons supprimés")
        return unique, n_dupes

    def merge_and_deduplicate(
        self, existing: List[Prospect], new_prospects: List[Prospect]
    ) -> Tuple[List[Prospect], int]:
        """
        Fusionne une liste existante avec de nouveaux prospects.
        Si doublon détecté, enrichit l'existant plutôt que de le rejeter.
        """
        for p in existing:
            self._register(p)

        merged = list(existing)
        added  = 0

        for p in new_prospects:
            if self._is_dup(p):
                idx = self._find_idx(merged, p)
                if idx is not None:
                    merged[idx] = self._enrich(merged[idx], p)
            else:
                self._register(p)
                merged.append(p)
                added += 1

        logger.info(f"[Dedup] +{added} nouveaux prospects")
        return merged, added

    # ──────────────────────────────────────────
    # Core
    # ──────────────────────────────────────────

    def _is_dup(self, p: Prospect) -> str:
        if p.hash_dedup and p.hash_dedup in self._seen_hashes:
            return f"hash={p.hash_dedup}"

        email = p.email.strip().lower()
        if email and email in self._seen_emails:
            return f"email={email}"

        domain = self._domain(p.website)
        if domain and domain in self._seen_domains:
            return f"domain={domain}"

        siren = re.sub(r"\D", "", str(p.siren or ""))
        if len(siren) == 9 and siren in self._seen_sirens:
            return f"siren={siren}"

        name_key = self._name_key(p)
        if name_key and name_key in self._seen_names:
            return f"name_city={name_key}"

        return ""

    def _register(self, p: Prospect) -> None:
        if p.hash_dedup:
            self._seen_hashes.add(p.hash_dedup)

        email = p.email.strip().lower()
        if email:
            self._seen_emails.add(email)

        domain = self._domain(p.website)
        if domain:
            self._seen_domains.add(domain)

        siren = re.sub(r"\D", "", str(p.siren or ""))
        if len(siren) == 9:
            self._seen_sirens.add(siren)

        name_key = self._name_key(p)
        if name_key:
            self._seen_names.add(name_key)

    def _find_idx(self, prospects: List[Prospect], target: Prospect) -> Optional[int]:
        email  = target.email.strip().lower()
        domain = self._domain(target.website)
        siren  = re.sub(r"\D", "", str(target.siren or ""))
        nkey   = self._name_key(target)

        for i, p in enumerate(prospects):
            if email and p.email.strip().lower() == email:
                return i
            if domain and self._domain(p.website) == domain:
                return i
            if len(siren) == 9 and re.sub(r"\D", "", str(p.siren or "")) == siren:
                return i
            if nkey and self._name_key(p) == nkey:
                return i
        return None

    @staticmethod
    def _enrich(existing: Prospect, new: Prospect) -> Prospect:
        """Complète les champs vides de `existing` avec les valeurs de `new`."""
        skip = {
            "qualification_score", "qualification", "segment", "source", "source_origin",
            "created_at", "hash_dedup",
            # Les scores d'enrichissement sont calculés après la dédup —
            # ne pas les écraser si l'existant a déjà été enrichi.
            "enrich_score", "email_mx_verified",
            # secteur_activite_scraped : valeur brute figée au moment du scraping —
            # jamais écrasée par un doublon entrant (la traçabilité FK côté CRM
            # repose sur la valeur originale, pas sur une fusion).
            "secteur_activite_scraped",
        }
        for f in Prospect.__dataclass_fields__:
            if f in skip:
                continue
            old_val = getattr(existing, f)
            new_val = getattr(new, f)
            if not old_val and new_val:
                setattr(existing, f, new_val)
        return existing

    # ──────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────

    @staticmethod
    def _domain(url: str) -> str:
        if not url:
            return ""
        try:
            parsed = urlparse(url if "://" in url else "https://" + url)
            d = parsed.netloc.lower().lstrip("www.")
            return d.split(":")[0]
        except Exception:
            return ""

    @staticmethod
    def _name_key(p: Prospect) -> str:
        """Hash(nom_normalisé + ville) pour détecter doublons approximatifs."""
        name = re.sub(r"\W+", "", str(p.nom_commercial or "").lower())
        city = re.sub(r"\W+", "", str(p.ville or "").lower())
        if not name:
            return ""
        raw = f"{name}|{city}"
        return hashlib.md5(raw.encode()).hexdigest()[:12]

    def reset(self) -> None:
        self._seen_hashes.clear()
        self._seen_emails.clear()
        self._seen_domains.clear()
        self._seen_sirens.clear()
        self._seen_names.clear()