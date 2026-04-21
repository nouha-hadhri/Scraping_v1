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

    # Phrases descriptives par secteur — ancres SBERT.
    # SBERT (paraphrase-multilingual-MiniLM-L12-v2) est entraîné sur des phrases
    # naturelles, pas des listes de mots-clés collés.  Encoder " ".join(kws)
    # produit un vecteur bruité qui confond des secteurs proches (ex: Informatique
    # vs Cloud vs Développement logiciel).  Une phrase courte + 2-3 termes
    # représentatifs donne une similarité cosinus bien plus discriminante.
    # Règle : la phrase doit rester distincte de celles des secteurs voisins.
    # Fallback automatique sur " ".join(kws) si un label ne figure pas ici.
    _SECTOR_PHRASES: Dict[str, str] = {
        "Informatique":              "Entreprise de services informatiques, développement logiciel, systèmes et réseaux.",
        "Cloud computing":           "Fournisseur de services cloud, hébergement, infrastructure as a service, SaaS.",
        "Intelligence artificielle": "Entreprise spécialisée en intelligence artificielle, machine learning et data science.",
        "Cybersécurité":             "Société de cybersécurité, protection des systèmes, audit de sécurité informatique, SOC.",
        "Conseil IT":                "Cabinet de conseil en informatique et transformation digitale, intégration de systèmes.",
        "E-commerce":                "Plateforme de vente en ligne, commerce électronique, marketplace, boutique web.",
        "Télécommunications":        "Opérateur télécom, réseaux mobiles, fibre optique, communications d'entreprise.",
        "Édition de logiciels":      "Éditeur de logiciels, développement d'applications SaaS et solutions métier.",
        "Traitement de données":     "Traitement et analyse de données massives, data engineering, ETL, datawarehouse.",
        "Ingénierie & R&D":          "Bureau d'études, ingénierie industrielle, recherche et développement technique.",
        "Marketing digital":         "Agence de marketing digital, publicité en ligne, SEO, réseaux sociaux, growth hacking.",
        "Fintech":                   "Entreprise fintech, services financiers numériques, paiement en ligne, néobanque.",
        "Medtech / Santé digitale":  "Technologie médicale, dispositif de santé numérique, télémédecine, e-santé.",
        "Éducation / Edtech":        "Plateforme éducative, formation en ligne, e-learning, EdTech, enseignement numérique.",
        "Logistique & Supply chain": "Logistique, transport de marchandises, supply chain, entreposage, gestion de flux.",
        "Industrie manufacturière":  "Entreprise industrielle, fabrication, production manufacturière, usine.",
        "Energie & Environnement":   "Énergies renouvelables, transition écologique, gestion environnementale, cleantech.",
        "Immobilier":                "Agence immobilière, promotion immobilière, gestion locative, transaction foncière.",
        "Finance & Assurance":       "Banque, assurance, gestion d'actifs, courtage financier, services bancaires.",
        "Conseil & Management":      "Cabinet de conseil en management, stratégie d'entreprise, transformation organisationnelle.",
        "Ressources humaines":       "Cabinet RH, recrutement, gestion des talents, formation professionnelle.",
        "Commerce de gros":          "Grossiste, distribution en gros, négoce, import-export, B2B commercial.",
        "Restauration & Hôtellerie": "Restaurant, hôtel, hébergement touristique, restauration collective, café.",
        "Santé":                     "Établissement de santé, clinique, cabinet médical, pharmacie, soins aux patients.",
        "Construction & BTP":        "Construction, bâtiment, travaux publics, génie civil, maçonnerie, rénovation.",
        "Transport":                 "Transports de voyageurs ou marchandises, logistique routière, aviation, maritime.",
        "Agriculture & Agroalimentaire": "Agriculture, production agricole, agroalimentaire, transformation alimentaire.",
        "Médias & Communication":    "Médias, presse, production audiovisuelle, communication institutionnelle, agence de presse.",
        "Architecture & Design":     "Cabinet d'architecture, design graphique, design produit, aménagement intérieur.",
        "Juridique & Comptabilité":  "Cabinet d'avocats, expertise comptable, conseil juridique, audit financier.",
        "Technologie":               "Entreprise technologique, innovation numérique, R&D tech, startup technologique.",
        "Développement logiciel":    "Développement de logiciels sur mesure, applications mobiles, web et API.",
    }

    def __init__(self):
        self._use_sbert         = USE_EMBEDDINGS
        self._labels            = list(SECTOR_KEYWORDS.keys())
        # Fix 1 — Descriptive phrases: encode natural sentences instead of raw
        # keyword dumps.  SBERT cosine similarity is calibrated on sentence pairs —
        # a space-joined keyword list is out-of-distribution and produces noisy
        # vectors that conflate neighbouring sectors (Informatique / Cloud /
        # Développement logiciel all share the same top keywords).
        # Fallback: if a label has no entry in _SECTOR_PHRASES, use joined keywords.
        self._sector_texts      = [
            self._SECTOR_PHRASES.get(label, " ".join(kws))
            for label, kws in SECTOR_KEYWORDS.items()
        ]
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
                p.nlp_confidence    = round(conf, 3)
            else:
                p.nlp_confidence    = 0.0
        else:
            normalized = self._normalize_sector(p.secteur_activite)
            if normalized:
                p.secteur_activite = normalized
            p.nlp_confidence = 1.0

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
            conf = max(conf, 0.0)

            # Fix 2 — Raise threshold 0.30 → 0.50.
            # With paraphrase-multilingual-MiniLM-L12-v2, cosine similarity
            # between unrelated texts routinely falls in [0.20, 0.40], so a
            # threshold of 0.30 produces many false positives.  Empirically a
            # genuine sector match scores ≥ 0.50 with descriptive phrase anchors.
            #
            # Hybrid confirmation band [0.35, 0.50):
            # Rather than silently falling back to keywords for scores in this
            # grey zone, we cross-check with keyword matching.  If both SBERT
            # and keywords agree on the same sector, we accept it at the SBERT
            # confidence level.  If they disagree, keywords win (higher precision
            # on short/generic company names) and the confidence is capped at 0.45
            # to signal the lower certainty to downstream consumers (scorer, FK
            # resolver).
            if conf >= 0.50:
                return sector, conf

            if conf >= 0.35:
                kw_sector, kw_conf = self._detect_keywords(text)
                if kw_sector and kw_sector == sector:
                    # Both agree — accept SBERT label, signal hybrid origin
                    return sector, conf
                if kw_sector:
                    # Disagreement — trust keywords, cap confidence
                    return kw_sector, min(kw_conf, 0.45)
                # Keywords found nothing — SBERT alone is not reliable here
                return "", 0.0

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