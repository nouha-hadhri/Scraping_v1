"""
SCRAPING_V1 - OpenDataScraper
Collecte depuis les sources open data légales et officielles :
  - data.gouv.fr / Sirene (France)
  - OpenCorporates (international)
  - OpenStreetMap / Overpass API
  - BODACC — Bulletin Officiel Des Annonces Civiles Et Commerciales

CORRECTIONS v10 (suite aux audits) :

  FIX A — opencorporates activé silencieusement
    AVANT : _od_cfg.get("opencorporates", True)
            -> si SOURCES_CONFIG mal chargé, la source se déclenchait malgré
               "opencorporates": False dans settings.
    APRÈS : _od_cfg.get("opencorporates", False)
            La valeur par défaut est False pour toutes les sous-sources
            non-critiques. Seule "sirene" garde True (source principale).

  FIX B — _GeoResolver instanciée deux fois
    AVANT : geo = _GeoResolver(self.client)  # ligne ~320
            geo_ref = _GeoResolver(self.client)  # ligne ~355 (doublon)
    APRÈS : une seule instance geo_resolver réutilisée pour Sirene, OSM, BODACC.

  FIX C — Erreurs _parse_bodacc_record ignorées silencieusement
    AVANT : except Exception -> logger.debug  (invisible en prod avec LOG_LEVEL=INFO)
    APRÈS : except Exception -> logger.warning avec compteur d'erreurs

  FIX D — Constantes de pagination lues depuis settings.COLLECTION_LIMITS
    AVANT : SIRENE_MAX_NAF = 8, SIRENE_MAX_PAGES = 4, etc. codées en dur
    APRÈS : lues depuis settings.COLLECTION_LIMITS (source unique)
            Les attributs de classe sont conservés comme propriétés
            pour ne pas casser le code existant.

  FIX E — GEO_COMMUNES_URL / GEO_REGIONS_URL
    AVANT : codées en dur dans ce fichier
    APRÈS : importées depuis config.settings (source unique)
"""
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import logging
import re
import time
from typing import List, Dict, Any, Optional

from urllib.parse import quote as _quote, urlencode
from typing import Set, Tuple

from sources.base_scraper        import BaseScraper
from sources.sirene_query_builder import SireneQueryBuilder
from storage.models               import Prospect
from config.settings              import (
    DATA_GOUV_URL, NOMINATIM_USER_AGENT, OPENCORPORATES_URL,
    OVERPASS_BASE_URL, NOMINATIM_BASE_URL,
    BODACC_API_URL, OPENCORPORATES_API_KEY,
    SOURCES_CONFIG, COLLECTION_LIMITS, get_sources_config,
    # FIX E : importées depuis settings (plus de codage en dur local)
    GEO_COMMUNES_URL, GEO_REGIONS_URL,
)

# ---------------------------------------------------------------------------
# FIX – Memory Leak / Global _qbuilder:
# The previous code instantiated SireneQueryBuilder() at *import time*, which
# means the mapping JSON files were read (and the resulting dicts kept in RAM)
# even when OpenDataScraper was never used in a given run.  Two additional
# problems followed:
#   1. Any ImportError in SireneQueryBuilder would crash the whole module.
#   2. Tests could not inject a mock builder without monkey-patching a live object.
#
# Solution: lazy singleton protected by a threading.Lock so that the builder
# is created exactly once, on first use, and can be replaced in tests via
# _set_qbuilder_for_tests().
# ---------------------------------------------------------------------------
import threading as _threading

_qbuilder:      Optional["SireneQueryBuilder"] = None
_qbuilder_lock: _threading.Lock                = _threading.Lock()


def _get_qbuilder() -> "SireneQueryBuilder":
    """Return the shared SireneQueryBuilder, creating it on first call."""
    global _qbuilder
    if _qbuilder is None:
        with _qbuilder_lock:
            if _qbuilder is None:          # double-checked locking
                _qbuilder = SireneQueryBuilder()
    return _qbuilder


def _set_qbuilder_for_tests(builder: "SireneQueryBuilder") -> None:
    """Allow test fixtures to inject a mock builder without monkey-patching."""
    global _qbuilder
    with _qbuilder_lock:
        _qbuilder = builder


logger = logging.getLogger(__name__)


def _is_france_target(pays: List[str]) -> bool:
    if not pays:
        return True
    return any(str(p).strip().lower() == "france" for p in pays)

# ─────────────────────────────────────────────
# Résolution géographique — geo.api.gouv.fr
# FIX E : URLs importées depuis settings, plus définies ici
# ─────────────────────────────────────────────

# ─────────────────────────────────────────────
# Codes NAF — dans config/mappings/naf_by_sector.json
# Lus via SireneQueryBuilder (_qbuilder.get_naf_for_sector)
# ─────────────────────────────────────────────

# Tranche effectif Sirene → effectif estimé
_TRANCHE_MAP: Dict[str, int] = {
    "00": 0,   "01": 1,   "02": 3,   "03": 6,
    "11": 10,  "12": 20,  "21": 50,  "22": 100,
    "31": 200, "32": 250, "41": 500, "42": 1000,
    "51": 2000, "52": 5000, "53": 10000,
}
_TRANCHES_TPE = {"00", "01", "02", "03"}  # Codes tranche correspondant aux TPE (0-9 salariés)

# Mapping forme juridique → fragments attendus dans nature_juridique_libelle
# (filtre post-traitement types_entreprise — BUG 7 FIX)
_FORME_TO_NJ_FRAGMENT: Dict[str, List[str]] = {
    "SAS":    ["société par actions simplifiée", "SAS"],
    "SASU":   ["société par actions simplifiée unipersonnelle", "SASU"],
    "SARL":   ["société à responsabilité limitée", "SARL"],
    "SA":     ["société anonyme", "SA"],
    "SNC":    ["société en nom collectif", "SNC"],
    "EI":     ["entrepreneur individuel", "EI"],
    "EIRL":   ["entrepreneur individuel à responsabilité", "EIRL"],
    "SCI":    ["société civile immobilière", "SCI"],
    "GIE":    ["groupement d'intérêt économique", "GIE"],
    "SCOP":   ["coopérative", "SCOP"],
    "SELARL": ["société d'exercice libéral à responsabilité", "SELARL"],
    "SELASU": ["société d'exercice libéral par actions simplifiée unipersonnelle", "SELASU"],
}


# ═══════════════════════════════════════════════════════════════════
# _GeoResolver — Résout noms de villes + régions → codes postaux CSV
# BUG 5 FIX : les régions sont maintenant lues ET résolues
# ═══════════════════════════════════════════════════════════════════

class _GeoResolver:
    """
    Résout noms de villes et régions → préfixes codes postaux.

    SOURCE UNIQUE : config/mappings/postal_by_geo.json (via _qbuilder).
    Aucun appel HTTP — tout est dans le JSON local.

    Logique de priorité pour le filtre Sirene :
      ┌─────────────────────────────┬──────────────────────────────────────────┐
      │ Sélection utilisateur       │ Préfixes renvoyés                        │
      ├─────────────────────────────┼──────────────────────────────────────────┤
      │ Région seule                │ Préfixes courts de la région ("75","77") │
      │ Villes seules               │ CP complets des villes ("75001"…"75020") │
      │ Région + Villes             │ CP complets des villes UNIQUEMENT        │
      │                             │ (la région est le contexte, pas le filtre│
      │ Rien                        │ [] → recherche nationale                 │
      └─────────────────────────────┴──────────────────────────────────────────┘
    """

    def __init__(self, client):
        self._client = client  # conservé pour OSM/BODACC qui font des appels HTTP

    def resolve_all(self, villes: List[str], regions: List[str]) -> List[str]:
        """Retourne les préfixes CP pour le filtre post-traitement Sirene."""
        seen: Set[str] = set()
        result: List[str] = []

        if villes:
            for v in villes:
                for cp in _get_qbuilder().get_postal_for_ville(v):
                    if cp not in seen:
                        seen.add(cp)
                        result.append(cp)
            logger.info(
                f"[GeoResolver] {len(villes)} ville(s) -> {len(result)} codes postaux "
                f"(région ignorée pour le filtre Sirene)"
            )
        elif regions:
            for r in regions:
                for pfx in _get_qbuilder().get_postal_for_region(r):
                    if pfx not in seen:
                        seen.add(pfx)
                        result.append(pfx)
            logger.info(
                f"[GeoResolver] {len(regions)} région(s) -> {len(result)} préfixes dept"
            )
        else:
            logger.info("[GeoResolver] Aucune géo -> recherche nationale Sirene")

        return result

    def regions_to_capitals(self, regions: List[str]) -> List[str]:
        """Capitales régionales pour OSM/BODACC (itèrent par ville)."""
        return _get_qbuilder().get_all_region_capitals(regions)

# ─────────────────────────────────────────────
# Constantes communes
# ─────────────────────────────────────────────

_CC_MAP = {
    "France": "fr", "Tunisie": "tn", "Maroc": "ma",
    "Belgique": "be", "Suisse": "ch", "Allemagne": "de",
    "Espagne": "es", "Italie": "it",
}

_OSM_OFFICE_TYPES: Dict[str, str] = {
    "it":                "Informatique",
    "software":          "Informatique",
    "consulting":        "Conseil IT",
    "telecommunication": "Télécommunications",
    "financial":         "Finance",
    "insurance":         "Finance",
    "lawyer":            "Juridique & Droit",
    "accountant":        "Comptabilité",
    "real_estate":       "Immobilier",
    "educational":       "Éducation & Formation",
    "research":          "Informatique",
    "advertising":       "Marketing & Communication",
    "yes": "", "company": "", "coworking": "", "ngo": "",
}


_BODACC_FORMES = [
    "SARL", "SAS", "SASU", "SA", "SNC", "EI",
    "EIRL", "SCI", "GIE", "SELARL", "SELASU", "SCOP",
]


def _make_bodacc_url(base_url, params):
    """
    URL BODACC valide pour curl.
      where/refine : safe="%:" -> % wildcard intact, : facette intact, " -> %22, espace -> %20
      q            : safe=""   -> encodage complet
      autres       : safe=","  -> virgules CSV preservees
    """
    parts = []
    for k, v in params.items():
        ek = _quote(str(k), safe="")
        if k in ("where", "refine"):
            ev = _quote(str(v), safe="%:")
        elif k == "q":
            ev = _quote(str(v), safe="")
        else:
            ev = _quote(str(v), safe=",")
        parts.append(f"{ek}={ev}")
    return f"{base_url}?{'&'.join(parts)}"


class OpenDataScraper(BaseScraper):
    source_name = "OpenData"

    # ── Paramètres de contrôle lus depuis settings.COLLECTION_LIMITS ──
    # FIX D : ces constantes n'étaient plus la source de vérité — elles
    # sont maintenant des propriétés qui délèguent à COLLECTION_LIMITS.
    # Le code interne continue d'utiliser self.SIRENE_MAX_NAF etc. sans
    # changement, mais la valeur vient de settings.py.
    @property
    def SIRENE_MAX_NAF(self) -> int:
        return COLLECTION_LIMITS.get("sirene_max_naf", 8)

    @property
    def SIRENE_MAX_PAGES(self) -> int:
        return COLLECTION_LIMITS.get("sirene_max_pages", 4)

    @property
    def SIRENE_PER_PAGE(self) -> int:
        return COLLECTION_LIMITS.get("sirene_per_page", 25)

    @property
    def SIRENE_SLEEP(self) -> float:
        return COLLECTION_LIMITS.get("sirene_sleep", 0.4)

    @property
    def SIRENE_GEO_CHUNK(self) -> int:
        return COLLECTION_LIMITS.get("sirene_geo_chunk", 20)

    def search(self, criteria: Dict[str, Any]) -> List[Prospect]:
        """
        Entrée : dict issu de normalize_criteria() (targets.py) OU dict brut
        du search_config.json v3.

        Clés lues :
          secteurs_activite, types_entreprise, tailles_entreprise,
          employes_min, employes_max,
          pays, regions, villes,            ← BUG 5 FIX : regions maintenant lu
          mots_cles / keywords,             ← v8 : transmis à q= Sirene
          codes_naf
        """
        # ── Lecture critères ───────────────────────────────────────────
        secteurs = criteria.get("secteurs_activite", [])

        # Géographie : orchestrator produit toujours {"localisation": {pays, regions, villes}}
        # Fallback zone_geographique : format search_config.json brut (appel direct sans orchestrator)
        _loc    = (
            criteria.get("localisation")
            or criteria.get("zone_geographique")
            or {}
        )
        pays    = _loc.get("pays",    []) or criteria.get("pays",    [])
        regions = _loc.get("regions", []) or criteria.get("regions", [])
        villes  = _loc.get("villes",  []) or criteria.get("villes",  [])

        # Mots-clés : orchestrator produit "keywords"
        # Fallback "mots_cles" : format search_config.json brut
        mots_cles = criteria.get("keywords", []) or criteria.get("mots_cles", [])

        # Types d'entreprise
        types_ent = criteria.get("types_entreprise", [])

        # Taille : orchestrator produit "tailles_entreprise"
        # Fallback "tailles_categorie" : ancienne clé (rétro-compatibilité)
        tailles_cat = (
            criteria.get("tailles_entreprise", [])
            or criteria.get("tailles_categorie", [])
        )

        # Effectifs
        employes_min = int(criteria.get("employes_min", 0))
        employes_max = criteria.get("employes_max")

        # Codes NAF : fournis ou auto-générés (codes exacts EN PRIORITÉ)
        codes_naf = criteria.get("codes_naf") or self._secteurs_to_naf(secteurs)
        if codes_naf:
            logger.debug(f"[OpenData] {len(codes_naf)} codes NAF: {codes_naf[:4]}…")

        prospects: List[Prospect] = []

        # Raccourci vers la config des sous-sources open_data
        # Utilise la config isolée du thread si disponible (parallélisation)
        _od_cfg = get_sources_config(criteria).get("open_data", {})

        # FIX B : une seule instance _GeoResolver réutilisée pour toutes les sources
        geo_resolver = _GeoResolver(self.client)

        # ── 1. Sirene / data.gouv.fr ───────────────────────────────────
        if _od_cfg.get("sirene", True) and _is_france_target(pays):
            # BUG 5 FIX : résolution villes + régions → codes postaux CSV
            codes_postaux = geo_resolver.resolve_all(villes, regions)

            sirene_results = self._search_sirene(
                codes_naf     = codes_naf,
                tailles_cat   = tailles_cat,
                types_ent     = types_ent,
                codes_postaux = codes_postaux,
                keywords      = mots_cles,
                employes_min  = employes_min,
                employes_max  = employes_max,
            )
            prospects.extend(sirene_results)
            logger.info(f"[OpenData/Sirene] Total: {len(sirene_results)}")
        else:
            logger.info("[OpenData/Sirene] desactive (SOURCES_CONFIG)")

        # ── 2. OpenCorporates ──────────────────────────────────────────
        # FIX A : valeur par défaut False (était True -> activait la source
        #          silencieusement si SOURCES_CONFIG non chargé)
        if _od_cfg.get("opencorporates", False):
            oc_terms = self._build_oc_terms(secteurs, mots_cles)
            for country in (pays or ["France"])[:3]:
                for kw in oc_terms[:2]:
                    try:
                        results = self._search_opencorporates(kw, country)
                        prospects.extend(results)
                        logger.info(f"[OpenData/OC] '{kw}' / {country} -> {len(results)}")
                        time.sleep(1.5)
                    except Exception as e:
                        logger.warning(f"[OpenData/OC] Erreur '{kw}': {e}")
        else:
            logger.info("[OpenData/OC] desactive (SOURCES_CONFIG)")

        # ── 3. Overpass / OSM ──────────────────────────────────────────
        # La valeur par défaut est True pour correspondre à settings.py
        # ("osm": True).  opencorporates et bodacc restent à False car ils
        # sont des sources non-critiques / à clé API.
        # FIX B : utilise geo_resolver existant au lieu d'en créer un second
        if _od_cfg.get("osm", True):
            osm_villes = (
                villes[:6] if villes
                else geo_resolver.regions_to_capitals(regions)[:4] if regions
                else (["Paris","Lyon","Marseille","Bordeaux","Toulouse"] if _is_france_target(pays) else [])
            )
            osm_office_filter = self._secteurs_to_osm_offices(secteurs)
            for ville in osm_villes:
                try:
                    results = self._search_overpass(ville, office_filter=osm_office_filter)
                    prospects.extend(results)
                    logger.info(f"[OpenData/OSM] {ville} -> {len(results)}")
                    time.sleep(2.0)
                except Exception as e:
                    logger.warning(f"[OpenData/OSM] Erreur {ville}: {e}")
        else:
            logger.info("[OpenData/OSM] desactive (SOURCES_CONFIG)")

        # ── 4. BODACC ──────────────────────────────────────────────────
        # FIX A : valeur par défaut False
        # FIX B : utilise geo_resolver existant
        if _od_cfg.get("bodacc", False) and _is_france_target(pays):
            bodacc_naf    = codes_naf or self._secteurs_to_naf(secteurs)
            bodacc_locs   = (
                villes[:6] if villes
                else geo_resolver.regions_to_capitals(regions)[:4] if regions
                else ["Paris","Lyon","Marseille","Bordeaux","Toulouse"]
            )
            bodacc_kw     = self._build_bodacc_keywords(secteurs, mots_cles) or [None]
            bodacc_formes = types_ent if types_ent else [None]

            # Produit cartesien : loc x forme x kw
            for loc in bodacc_locs:
                for forme in bodacc_formes:
                    for kw in bodacc_kw:
                        try:
                            results = self._search_bodacc_by_keyword(
                                keyword  = kw,
                                forme    = forme,
                                location = loc,
                            )
                            prospects.extend(results)
                            label = f"{loc} + {forme or 'tous'} + kw='{kw or ''}'"
                            logger.info(f"[OpenData/BODACC] {label} -> {len(results)}")
                            time.sleep(1.0)
                        except Exception as e:
                            logger.warning(f"[OpenData/BODACC] Erreur {loc}/{forme}/{kw}: {e}")
        else:
            logger.info("[OpenData/BODACC] desactive (SOURCES_CONFIG)")

        logger.info(f"[OpenData] Total brut: {len(prospects)}")
        return prospects

    # ══════════════════════════════════════════════════════════════════
    # SIRENE / data.gouv.fr
    # ══════════════════════════════════════════════════════════════════

    def _search_sirene(
        self,
        codes_naf:     List[str],
        tailles_cat:   List[str],
        types_ent:     List[str],
        codes_postaux: List[str],
        keywords:      List[str] = None,
        employes_min:  int           = 0,
        employes_max:  Optional[int] = None,
    ) -> List[Prospect]:
        results:     List[Prospect] = []
        seen_sirens: Set[str]       = set()

        target_prefixes: Optional[Set[str]] = set(codes_postaux) if codes_postaux else None
        if target_prefixes:
            logger.info(f"[Sirene] Géo post-traitement : {len(target_prefixes)} préfixe(s) CP")
        else:
            logger.info("[Sirene] Géo : recherche nationale (aucun filtre)")

        if keywords:
            kw_list: List[Optional[str]] = [
                k.strip() for k in keywords if k.strip() and len(k.strip()) <= 40
            ]
            if not kw_list:
                kw_list = [None]
            else:
                logger.info(f"[Sirene] {len(kw_list)} mot(s)-clé(s) -> {len(kw_list)} série(s) de requêtes q=")
        else:
            kw_list = [None]

        api_cats = [c for c in tailles_cat if c in ("PME", "ETI", "GE")]
        need_tpe = "TPE" in tailles_cat
        only_tpe = need_tpe and not api_cats
        if not api_cats:
            api_cats = [None]

        naf_list = codes_naf[:self.SIRENE_MAX_NAF] if codes_naf else [None]
        if codes_naf and len(codes_naf) > self.SIRENE_MAX_NAF:
            logger.warning(
                f"[Sirene] {len(codes_naf)} codes NAF disponibles, mais seuls les "
                f"{self.SIRENE_MAX_NAF} premiers sont utilisés (SIRENE_MAX_NAF). "
                f"Codes ignorés : {codes_naf[self.SIRENE_MAX_NAF:]}"
            )

        for naf in naf_list:
            for cat in api_cats:
                for q_param in kw_list:
                    batch = self._sirene_paginated(naf, cat, q_param=q_param, codes_postaux=list(target_prefixes) if target_prefixes else None)
                    nb_added = 0
                    for item in batch:
                        siren = item.get("siren", "")
                        if siren and siren in seen_sirens:
                            continue
                        if siren:
                            seen_sirens.add(siren)

                        if target_prefixes:
                            siege   = item.get("siege", {}) or {}
                            item_cp = siege.get("code_postal", "") or ""
                            if not any(item_cp.startswith(pfx) for pfx in target_prefixes):
                                continue

                        p = self._parse_sirene_item(item)
                        if not p:
                            continue
                        if not self._pass_taille(p, employes_min, employes_max, need_tpe, only_tpe):
                            continue
                        if not self._pass_type(p, types_ent):
                            continue

                        results.append(p)
                        nb_added += 1

                    logger.info(
                        f"[Sirene] NAF={naf or 'ALL'} cat={cat or 'ALL'} "
                        f"q={q_param or 'NONE'} "
                        f"geo={('CP(' + str(len(target_prefixes)) + ' préfixes)') if target_prefixes else 'FR'} "
                        f"-> +{nb_added}"
                    )

        return results

    def _sirene_paginated(
        self,
        naf:            Optional[str],
        cat:            Optional[str],
        q_param:        Optional[str] = None,
        codes_postaux:  List[str]     = None,
    ) -> List[Dict]:
        all_items: List[Dict] = []
        for page in range(1, self.SIRENE_MAX_PAGES + 1):
            params: Dict[str, Any] = {
                "etat_administratif": "A",
                "per_page":           self.SIRENE_PER_PAGE,
                "page":               page,
            }
            if naf:
                params["activite_principale"] = naf.upper()
            if cat:
                params["categorie_entreprise"] = cat
            if q_param:
                params["q"] = q_param
            if codes_postaux:
                params["code_postal"] = ",".join(codes_postaux[:self.SIRENE_GEO_CHUNK])

            final_url = DATA_GOUV_URL + "?" + urlencode(params, doseq=True, safe=",")
            logger.info(f"[Sirene][DEBUG] URL -> {final_url}")

            data = self.client.get_json(final_url)
            time.sleep(self.SIRENE_SLEEP)

            if not data:
                logger.warning(f"[Sirene][DEBUG] Reponse vide ou None pour URL -> {final_url}")
                break

            total   = data.get("total_results", "?")
            nb_page = len(data.get("results", []))
            logger.info(f"[Sirene][DEBUG] Reponse -> total_results={total} | results sur cette page={nb_page}")

            page_items = data.get("results", [])
            all_items.extend(page_items)
            if len(page_items) < self.SIRENE_PER_PAGE:
                break
        return all_items

    @staticmethod
    def _normalize_type_juridique(libelle: str) -> str:
        """
        Mappe le libellé complet Sirene (nature_juridique_libelle) vers le code
        court attendu par TypeEntreprise (SAS, SARL, SASU, etc.).

        L'ordre de priorité est important : SASU avant SAS, EURL avant SARL,
        AUTO_ENTREPRENEUR avant EI pour éviter les faux positifs sur les
        sous-chaînes.
        """
        if not libelle:
            return ""
        _MAP = [
            # Libellés complets Sirene (correspondances prioritaires)
            ("société par actions simplifiée unipersonnelle", "SASU"),
            ("société par actions simplifiée",                "SAS"),
            ("société à responsabilité limitée unipersonnelle", "EURL"),
            ("société à responsabilité limitée",              "SARL"),
            ("société anonyme",                               "SA"),
            ("auto-entrepreneur",                             "AUTO_ENTREPRENEUR"),
            ("micro-entrepreneur",                            "AUTO_ENTREPRENEUR"),
            ("entrepreneur individuel à responsabilité",      "EI"),
            ("entrepreneur individuel",                       "EI"),
            ("entreprise individuelle",                       "EI"),
        ]
        lb = libelle.lower().strip()
        for fragment, code in _MAP:
            if fragment in lb:
                return code
        # Fallback : cherche l'abréviation directement dans le libellé brut
        for code in ("SASU", "EURL", "SAS", "SARL", "SA", "EI"):
            if re.search(rf"\b{code}\b", libelle, re.IGNORECASE):
                return code
        return ""

    # Mapping tranche effectif Sirene → catégorie TailleEntreprise
    _TRANCHE_TO_TAILLE: Dict[str, str] = {
        "00": "TPE", "01": "TPE", "02": "TPE", "03": "TPE",   # 0-9 salariés
        "11": "PME", "12": "PME", "21": "PME", "22": "PME",   # 10-249 salariés
        "31": "PME", "32": "ETI",                              # 200 → PME / 250 → ETI
        "41": "ETI", "42": "ETI",                              # 500-1999 salariés
        "51": "ETI",                                           # 2000-4999 salariés
        "52": "GE",  "53": "GE",                               # 5000+ salariés
    }

    def _parse_sirene_item(self, item: Dict) -> Optional[Prospect]:
        try:
            siege   = item.get("siege", {}) or {}
            tranche = str(item.get("tranche_effectif_salarie", ""))
            nom     = item.get("nom_complet") or item.get("nom_raison_sociale", "")
            if not nom:
                return None

            # FIX Bug 2 — type_entreprise : normaliser le libellé complet Sirene
            # vers le code court (SAS, SARL…) avant de créer le Prospect, pour
            # que _enum() dans pg_repository trouve une correspondance dans
            # TypeEntreprise et ne retombe pas systématiquement sur UNKNOWN → NULL.
            type_normalise = self._normalize_type_juridique(
                item.get("nature_juridique_libelle", "")
            )

            # FIX Bug 2 — taille_entreprise : dériver la catégorie directement
            # depuis la tranche Sirene (source fiable) avant que le Cleaner
            # n'essaie de la déduire depuis nombre_employes (souvent absent
            # quand la tranche est "NN" ou vide).
            taille_normalisee = self._TRANCHE_TO_TAILLE.get(tranche, "")

            # FIX Bug 3 — secteur_activite_scraped : capturer le libellé brut
            # de l'API Sirene (activite_principale_libelle) avant toute
            # normalisation NLP ou résolution FK.  Sans cette ligne, le champ
            # reste vide jusqu'au fallback orchestrator (ligne ~668) qui écrit
            # à la place la valeur *post-cleaner*, perdant ainsi la traçabilité
            # de la valeur originale telle que retournée par l'API Sirene.
            secteur_brut = item.get("activite_principale_libelle", "")

            raw = {
                "nom_commercial":            nom,
                "raison_sociale":            item.get("nom_raison_sociale", ""),
                "siren":                     item.get("siren", ""),
                "siret":                     siege.get("siret", ""),
                "adresse":                   siege.get("adresse", ""),
                "ville":                     siege.get("libelle_commune", ""),
                "region":                    siege.get("libelle_region", ""),
                "code_postal":               siege.get("code_postal", ""),
                "pays":                      "France",
                "secteur_activite":          secteur_brut,
                "secteur_activite_scraped":  secteur_brut,
                "code_naf":                  item.get("activite_principale", ""),
                "type_entreprise":           type_normalise,
                "taille_entreprise":         taille_normalisee,
                "nombre_employes":           _TRANCHE_MAP.get(tranche),
                "_tranche":                  tranche,
                "website":                   self.clean_url(siege.get("site_internet", "")),
                "source":                    "data.gouv.fr/Sirene",
            }
            return self.normalize_prospect(raw)
        # FIX – Inconsistent Error Handling: previously caught as DEBUG, making
        # Sirene API schema changes invisible in production (LOG_LEVEL=INFO).
        # KeyError / TypeError / AttributeError indicate a changed API response
        # shape and deserve a WARNING with enough context to diagnose.
        except (KeyError, TypeError, AttributeError) as e:
            siren_hint = item.get("siren", "?") if isinstance(item, dict) else "?"
            logger.warning(
                f"[Sirene] Parse error on siren={siren_hint!r}: "
                f"{type(e).__name__}: {e}"
            )
            return None
        except Exception as e:
            logger.error(f"[Sirene] Unexpected parse error: {type(e).__name__}: {e}")
            return None

    def _pass_taille(
        self,
        p:           Prospect,
        employes_min: int,
        employes_max: Optional[int],
        need_tpe:    bool,
        only_tpe:    bool,
    ) -> bool:
        nb      = p.nombre_employes
        tranche = getattr(p, "_tranche", "") or ""

        if employes_min > 0 or employes_max is not None:
            if nb is not None:
                if employes_min > 0 and nb < employes_min:
                    return False
                if employes_max is not None and nb > employes_max:
                    return False

        if only_tpe:
            if tranche and tranche not in _TRANCHES_TPE:
                return False
            if nb is not None and nb > 9:
                return False

        return True

    def _pass_type(self, p: Prospect, types_ent: List[str]) -> bool:
        if not types_ent:
            return True
        libelle = (p.type_entreprise or "").lower()
        if not libelle:
            return True
        for forme in types_ent:
            for fragment in _FORME_TO_NJ_FRAGMENT.get(forme, [forme]):
                if fragment.lower() in libelle:
                    return True
        return False

    # ══════════════════════════════════════════
    # OPENCORPORATES
    # ══════════════════════════════════════════

    def _search_opencorporates(
        self, query: str, country: str, per_page: int = 20
    ) -> List[Prospect]:
        cc = _CC_MAP.get(country, country.lower()[:2])
        params: Dict[str, Any] = {
            "q":                 query,
            "jurisdiction_code": cc,
            "per_page":          per_page,
            "inactive":          "false",
        }
        if OPENCORPORATES_API_KEY:
            params["api_token"] = OPENCORPORATES_API_KEY

        data = self.client.get_json(OPENCORPORATES_URL, params=params)
        if not data:
            return []

        results = []
        for item in data.get("results", {}).get("companies", []):
            co   = item.get("company", {})
            addr = (co.get("registered_address") or {})
            raw = {
                "nom_commercial":  co.get("name", ""),
                "raison_sociale":  co.get("name", ""),
                "adresse":         addr.get("street_address", ""),
                "ville":           addr.get("locality", ""),
                "region":          addr.get("region", ""),
                "pays":            country,
                "type_entreprise": co.get("company_type", ""),
                "source":          "OpenCorporates",
            }
            results.append(self.normalize_prospect(raw))
        return results

    # ══════════════════════════════════════════
    # OVERPASS / OSM
    # ══════════════════════════════════════════

    def _search_overpass(
        self,
        city:          str,
        office_filter: List[str] = None,
    ) -> List[Prospect]:
        bbox = self._nominatim_bbox(city)
        if not bbox:
            logger.warning(f"[OpenData/OSM] Nominatim: bbox introuvable pour '{city}'")
            return []

        south, west, north, east = bbox

        query = (
            f'[out:json][timeout:25];\n'
            f'(\n'
            f'  nwr["office"]["name"]({south},{west},{north},{east});\n'
            f'  nwr["amenity"~"^(coworking_space|office)$"]["name"]({south},{west},{north},{east});\n'
            f'  nwr["office:it"]["name"]({south},{west},{north},{east});\n'
            f');\n'
            f'out center;'
        )

        raw_resp = self.client.post(
            OVERPASS_BASE_URL,
            data={"data": query},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        if not raw_resp:
            return []

        try:
            parsed = json.loads(raw_resp)
        except json.JSONDecodeError as e:
            # FIX – Inconsistent Error Handling: surface Overpass parse failures.
            logger.warning(f"[OpenData/OSM] Overpass response is not valid JSON: {e}")
            return []

        results    = []
        seen_names = set()

        for el in parsed.get("elements", []):
            tags = el.get("tags", {})
            name = tags.get("name") or tags.get("brand")
            if not name or name in seen_names:
                continue

            office_type = (tags.get("office") or "").lower()

            if office_filter and office_type and office_type not in office_filter:
                continue

            seen_names.add(name)

            website = self.clean_url(
                tags.get("website") or tags.get("contact:website")
                or tags.get("url") or tags.get("contact:url") or ""
            )
            email = tags.get("email") or tags.get("contact:email") or ""
            telephone = (
                tags.get("phone") or tags.get("contact:phone")
                or tags.get("telephone") or tags.get("contact:mobile") or ""
            )
            secteur = (
                _OSM_OFFICE_TYPES.get(office_type, "")
                or office_type or ""
            )
            adresse = " ".join(filter(None, [
                tags.get("addr:housenumber", ""),
                tags.get("addr:street", ""),
            ])).strip()

            # Récupère la région via Nominatim reverse geocoding si coordonnées disponibles
            region = ""
            if "center" in el:
                lat = el["center"].get("lat")
                lon = el["center"].get("lon")
                if lat and lon:
                    region = self._get_region_from_nominatim(lat, lon) or ""
            
            # Fallback 1: Si pas de région et on a un code postal, essayer postal_by_geo.json
            if not region and tags.get("addr:postcode"):
                try:
                    cp = tags.get("addr:postcode", "").strip()
                    if cp:
                        region = _get_qbuilder().get_region_for_postal(cp) or ""
                        if region:
                            logger.debug(f"[OSM/Fallback-CP] {cp} → {region}")
                except Exception:
                    pass
            
            # Fallback 2: Si rien n'a marché, utiliser la ville + orchestrator enrichissement
            # (l'orchestrator a déjà _enrich_region qui le fait)

            raw = {
                "nom_commercial":           name,
                "adresse":                  adresse,
                "ville":                    tags.get("addr:city", city),
                "region":                   region,
                "code_postal":              tags.get("addr:postcode", ""),
                "pays":                     tags.get("addr:country", "France"),
                "website":                  website,
                "email":                    email,
                "telephone":                telephone,
                "secteur_activite":         secteur,
                # FIX Bug 3 — secteur_activite_scraped : valeur brute OSM
                # (tag office= tel que retourné par Overpass, avant mapping
                # _OSM_OFFICE_TYPES). Permet de garder la traçabilité de la
                # source originale indépendamment de la normalisation NLP.
                "secteur_activite_scraped": office_type or secteur,
                "description":              tags.get("description", ""),
                "source":                   "OpenStreetMap",
            }
            results.append(self.normalize_prospect(raw))

        return results

    def _nominatim_bbox(self, city: str) -> Optional[tuple]:
        from urllib.parse import urlencode
        params = {
            "q":              city,
            "format":         "json",
            "limit":          1,
            "addressdetails": 0,
        }
        url  = f"{NOMINATIM_BASE_URL}/search?{urlencode(params)}"
        data = self.client.get_json(
            url,
            headers={"User-Agent": NOMINATIM_USER_AGENT},
            timeout=10,
        )
        if not data or not isinstance(data, list) or not data:
            return None
        bb = data[0].get("boundingbox")
        if not bb or len(bb) < 4:
            return None
        try:
            return float(bb[0]), float(bb[2]), float(bb[1]), float(bb[3])
        except (ValueError, TypeError):
            return None

    def _get_region_from_nominatim(self, lat: float, lon: float) -> Optional[str]:
        """Récupère la région via reverse geocoding Nominatim (lat/lon → adresse)."""
        from urllib.parse import urlencode
        try:
            params = {
                "lat": lat,
                "lon": lon,
                "format": "json",
                "zoom": 10,
            }
            url = f"{NOMINATIM_BASE_URL}/reverse?{urlencode(params)}"
            data = self.client.get_json(
                url,
                headers={"User-Agent": NOMINATIM_USER_AGENT},
                timeout=10,
            )
            if data and isinstance(data, dict) and "address" in data:
                addr = data["address"]
                # Cherche la région dans l'adresse (state, région, county...)
                for region_key in ["state", "region", "county", "province"]:
                    if region_key in addr:
                        found_region = addr[region_key]
                        logger.debug(f"[Nominatim] ({lat:.4f},{lon:.4f}) → {found_region}")
                        return found_region
                logger.debug(f"[Nominatim] ({lat:.4f},{lon:.4f}) → pas de région trouvée")
            else:
                logger.debug(f"[Nominatim] ({lat:.4f},{lon:.4f}) → pas d'adresse en réponse")
        except Exception as e:
            logger.debug(f"[Nominatim/ReverseGeo] Erreur ({lat:.4f},{lon:.4f}): {e}")
        return None

    # ══════════════════════════════════════════
    # Helpers — Sirene
    # ══════════════════════════════════════════

    @staticmethod
    def _parse_tranche_sirene(tranche: str) -> Optional[int]:
        # NOTE: le mapping module-level _TRANCHE_MAP est la source de vérité ;
        # cette méthode est conservée pour la rétro-compatibilité avec les
        # appelants externes.  _parse_tranche (doublon exact) a été supprimé.
        return _TRANCHE_MAP.get(str(tranche))

    # ══════════════════════════════════════════
    # Helpers — Termes recherche OC / BODACC
    # ══════════════════════════════════════════

    @staticmethod
    def _build_oc_terms(secteurs: List[str], mots_cles: List[str]) -> List[str]:
        _map = {
            "Informatique": "informatique", "Télécommunications": "télécom",
            "Conseil IT": "conseil", "Cybersécurité": "sécurité",
            "Cloud computing": "cloud", "Intelligence artificielle": "IA",
            "E-commerce": "commerce", "Finance": "finance",
            "Santé": "santé", "BTP": "construction",
        }
        terms, seen = [], set()
        for s in secteurs[:3]:
            t = _map.get(s, s.lower())
            if t not in seen:
                seen.add(t)
                terms.append(t)
        for kw in mots_cles:
            if len(kw) < 20 and kw.lower() not in seen:
                seen.add(kw.lower())
                terms.append(kw)
        return terms[:4] or ["entreprise"]

    @staticmethod
    def _build_bodacc_keywords(secteurs: List[str], mots_cles: List[str]) -> List[str]:
        _map = {
            "Informatique": "informatique", "Télécommunications": "télécommunications",
            "Conseil IT": "conseil informatique", "Cybersécurité": "cybersécurité",
            "Cloud computing": "cloud", "Intelligence artificielle": "intelligence artificielle",
            "E-commerce": "e-commerce", "Finance": "finance",
            "Santé": "santé", "BTP": "construction", "Restauration": "restauration",
        }
        terms, seen = [], set()
        for s in secteurs[:4]:
            t = _map.get(s, s.lower())
            if t not in seen:
                seen.add(t)
                terms.append(t)
        for kw in mots_cles[:3]:
            if kw.lower() not in seen:
                seen.add(kw.lower())
                terms.append(kw)
        return terms[:5]

    # ══════════════════════════════════════════
    # Helpers — OSM
    # ══════════════════════════════════════════

    @staticmethod
    def _secteurs_to_osm_offices(secteurs: List[str]) -> List[str]:
        _map: Dict[str, List[str]] = {
            "Informatique":              ["it", "software", "computer"],
            "Télécommunications":        ["telecommunication"],
            "Conseil IT":                ["consulting", "it"],
            "Finance":                   ["financial", "insurance", "financial_advisor"],
            "Comptabilité":              ["accountant"],
            "Juridique & Droit":         ["lawyer", "notary"],
            "Immobilier":                ["real_estate", "estate_agent"],
            "Éducation & Formation":     ["educational", "training"],
            "Santé":                     ["medical", "doctor"],
            "Marketing & Communication": ["advertising", "marketing"],
            "Ressources humaines":       ["hr", "employment_agency"],
        }
        values = []
        for s in secteurs:
            values.extend(_map.get(s, []))
        seen = set()
        return [v for v in values if not (v in seen or seen.add(v))]

    # ══════════════════════════════════════════════════════════════════
    # BODACC
    # ══════════════════════════════════════════════════════════════════

    _BODACC_SELECT       = (
        "commercant,ville,cp,registre,dateparution,"
        "typeavis,typeavis_lib,familleavis,familleavis_lib,"
        "numerodepartement,listepersonnes,acte,depot,url_complete"
    )
    _BODACC_REFINE_IMMAT = "familleavis:creation"

    @property
    def _BODACC_LIMIT(self) -> int:
        return COLLECTION_LIMITS.get("bodacc_limit", 100)

    @property
    def _BODACC_MAX_PAGES(self) -> int:
        return COLLECTION_LIMITS.get("bodacc_max_pages", 3)

    def _search_bodacc(
        self,
        location:  str,
        forme:     str       = None,
        codes_naf: List[str] = None,
    ) -> List[Prospect]:
        results:     List[Prospect] = []
        seen_sirens: Set[str]       = set()

        if location.isdigit() and len(location) == 2:
            where_geo = f'numerodepartement:"{location}"'
        elif location.isdigit():
            where_geo = f'numerodepartement:"{location[:2]}"'
        else:
            loc_esc   = location.replace('"', "")
            where_geo = f'ville like "{loc_esc}%"'

        if forme:
            fj_label = _FORME_TO_NJ_FRAGMENT.get(forme, [forme])[0]
            where_final = f'{where_geo} AND listepersonnes like "%{fj_label}%"'
        else:
            where_final = where_geo

        logger.info(f"[BODACC] >> loc={location} forme={forme or 'tous'}")

        for page in range(self._BODACC_MAX_PAGES):
            offset = page * self._BODACC_LIMIT
            params: Dict[str, Any] = {
                "where":    where_final,
                "refine":   self._BODACC_REFINE_IMMAT,
                "limit":    self._BODACC_LIMIT,
                "offset":   offset,
                "order_by": "dateparution desc",
                "select":   self._BODACC_SELECT,
            }
            url  = _make_bodacc_url(BODACC_API_URL, params)
            data = self.client.get_json(url)
            time.sleep(0.5)
            if not data:
                break
            records = data.get("results", [])
            if not records:
                break
            for rec in records:
                p = self._parse_bodacc_record(rec)
                if not p:
                    continue
                siren = p.siren or ""
                if siren and siren in seen_sirens:
                    continue
                if siren:
                    seen_sirens.add(siren)
                results.append(p)
            if len(records) < self._BODACC_LIMIT:
                break

        logger.info(f"[BODACC] Retenus : {len(results)}")
        return results

    def _search_bodacc_by_keyword(
        self,
        keyword:  str,
        forme:    str = None,
        location: str = None,
    ) -> List[Prospect]:
        results:     List[Prospect] = []
        seen_sirens: Set[str]       = set()

        conditions = []

        if location:
            if location.isdigit() and len(location) == 2:
                conditions.append(f'numerodepartement:"{location}"')
            elif location.isdigit():
                conditions.append(f'numerodepartement:"{location[:2]}"')
            else:
                loc_esc = location.replace('"', '')
                conditions.append(f'ville like "{loc_esc}%"')

        if keyword:
            kw_escaped = keyword.replace('"', '').replace('%', '')
            conditions.append(f'commercant like "%{kw_escaped}%"')

        if forme:
            fj_label = _FORME_TO_NJ_FRAGMENT.get(forme, [forme])[0]
            conditions.append(f'listepersonnes like "%{fj_label}%"')

        where_final = ' AND '.join(conditions) if conditions else None

        loc_label   = location or "France"
        kw_label    = keyword  or "sans kw"
        forme_label = forme    or "tous types"
        logger.info(f"[BODACC] >> loc={loc_label} + kw='{kw_label}' + forme={forme_label}")

        total_api      = 0
        total_collecte = 0

        for page in range(self._BODACC_MAX_PAGES):
            offset = page * self._BODACC_LIMIT
            params: Dict[str, Any] = {
                "refine":   self._BODACC_REFINE_IMMAT,
                "limit":    self._BODACC_LIMIT,
                "offset":   offset,
                "order_by": "dateparution desc",
                "select":   self._BODACC_SELECT,
            }
            if where_final:
                params["where"] = where_final
            url  = _make_bodacc_url(BODACC_API_URL, params)
            logger.info(f"[BODACC] URL -> {url}")
            data = self.client.get_json(url)
            time.sleep(0.5)
            if not data:
                break

            if page == 0:
                total_api = data.get("total_count", 0)
                logger.info(f"[BODACC] Total API : {total_api} entreprises")

            records = data.get("results", [])
            if not records:
                break

            total_collecte += len(records)

            for rec in records:
                p = self._parse_bodacc_record(rec)
                if not p:
                    continue
                siren = p.siren or ""
                if siren and siren in seen_sirens:
                    continue
                if siren:
                    seen_sirens.add(siren)
                results.append(p)

            if len(records) < self._BODACC_LIMIT:
                break

        logger.info(f"[BODACC] Retenus : {len(results)} / {total_collecte} collectes")
        return results

    def _parse_bodacc_record(self, rec: Dict) -> Optional[Prospect]:
        """
        Parse un enregistrement brut BODACC -> Prospect.

        FIX C : les exceptions sont maintenant loggées en WARNING (était DEBUG).
        En production (LOG_LEVEL=INFO), les erreurs de parsing deviennent visibles,
        ce qui permet de détecter un changement de schéma API BODACC.
        """
        try:
            commercant = rec.get("commercant", "") or ""
            if not commercant:
                return None
            nom   = self._bodacc_clean_name(commercant)
            forme = self._bodacc_extract_forme(commercant)
            registre_raw = rec.get("registre", "") or ""
            if isinstance(registre_raw, list):
                registre_raw = " ".join(str(r) for r in registre_raw)
            siren = self._bodacc_extract_siren(registre_raw)
            ville = (rec.get("ville") or "").strip().title()
            cp    = (rec.get("cp") or "").strip()

            import json as _json
            listep_raw = rec.get("listepersonnes") or ""
            personne   = {}
            if listep_raw and isinstance(listep_raw, str):
                try:
                    lp = _json.loads(listep_raw)
                    personne = lp.get("personne", {}) if isinstance(lp, dict) else {}
                except json.JSONDecodeError as e:
                    # FIX – Inconsistent Error Handling: log at DEBUG (field is
                    # optional enrichment; a bad value is not actionable).
                    logger.debug(f"[BODACC] listepersonnes JSON decode skipped: {e}")
            if not forme and personne:
                fj = personne.get("formeJuridique", "")
                forme = fj if isinstance(fj, str) else ""
            adresse_siege = personne.get("adresseSiegeSocial", {}) or {}
            if isinstance(adresse_siege, dict):
                if not ville:
                    ville = (adresse_siege.get("ville") or "").strip().title()
                if not cp:
                    cp = (adresse_siege.get("codePostal") or "").strip()

            description = ""
            for field in ("acte", "depot"):
                raw_field = rec.get(field) or ""
                if raw_field and isinstance(raw_field, str):
                    try:
                        parsed = _json.loads(raw_field)
                        description = parsed.get("descriptif", "") or parsed.get("typeDepot", "") or ""
                        if description:
                            break
                    except json.JSONDecodeError as e:
                        logger.debug(f"[BODACC] {field!r} JSON decode skipped: {e}")

            raw = {
                "nom_commercial":            nom,
                "raison_sociale":            (personne.get("denomination") or commercant).strip(),
                "siren":                     siren,
                "ville":                     ville,
                "code_postal":               cp,
                "pays":                      "France",
                "secteur_activite":          description[:200] if description else "",
                # FIX Bug 3 — secteur_activite_scraped : valeur brute BODACC
                # (champ acte/depot descriptif, avant normalisation NLP).
                "secteur_activite_scraped":  description[:200] if description else "",
                "type_entreprise":           forme,
                "website":                   rec.get("url_complete", ""),
                "source":                    "BODACC",
            }
            return self.normalize_prospect(raw)
        except Exception as e:
            # FIX C : WARNING au lieu de DEBUG pour visibilité en production
            logger.warning(f"[BODACC] Parse error (enregistrement ignoré): {e}")
            return None

    @staticmethod
    def _bodacc_match_forme(commercant: str, types_forme: List[str]) -> bool:
        if not types_forme:
            return True
        c_upper = commercant.upper()
        for forme in types_forme:
            if re.search(rf"\b{forme}\b", c_upper):
                return True
        return False

    # ══════════════════════════════════════════
    # Helpers — BODACC
    # ══════════════════════════════════════════

    @staticmethod
    def _bodacc_clean_name(name: str) -> str:
        cleaned = name.strip()
        for forme in _BODACC_FORMES:
            cleaned = re.sub(rf"\b{forme}\b", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\(\s*\)", "", cleaned)
        cleaned = re.sub(r"\s{2,}", " ", cleaned).strip(" -–—")
        return cleaned if len(cleaned) >= 2 else name.strip()

    @staticmethod
    def _bodacc_extract_forme(commercant: str) -> str:
        c_upper = commercant.upper()
        for forme in _BODACC_FORMES:
            if re.search(rf"\b{forme}\b", c_upper):
                return forme
        return ""

    @staticmethod
    def _bodacc_extract_siren(registre: str) -> str:
        if not registre:
            return ""
        digits = re.findall(r"\d{3}[\s.]?\d{3}[\s.]?\d{3}", registre)
        if digits:
            return re.sub(r"\D", "", digits[0])[:9]
        all_d = re.findall(r"\d{9,14}", re.sub(r"\s", "", registre))
        return all_d[0][:9] if all_d else ""

    @staticmethod
    def _secteurs_to_naf(secteurs: List[str]) -> List[str]:
        return _get_qbuilder()._map_sectors(secteurs)