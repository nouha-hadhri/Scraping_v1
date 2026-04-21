"""
SCRAPING_V1 - Orchestrator (ProspectCollector)
Point d'entrée principal du pipeline de scraping CRM.

Workflow:
  1. Lecture des critères — priorité :
       (a) SearchTarget explicite via CLI --target
       (b) search_config.json (interface CRM)
       (c) TARGETING_CRITERIA dans targets.py (fallback)
  2. Collecte multi-sources :
       open_data  -> Sirene + OpenCorporates + OSM/Overpass + BODACC
       directory  -> PagesJaunes + data.gouv.fr + Kompass + Europages + Verif.com
       societe    -> Pappers API + societe.com
  3. Nettoyage & normalisation
  4. Déduplication
  5. Analyse NLP / Embeddings (secteur, taille)
 5B. Résolution secteur_activite_id (SecteurResolver — step 4B dans le code)
       -> Charge GET /api/crm/secteur-activite/secteurs_resolver depuis Spring (une seule fois par job)
       -> Mappe le texte scraped → FK secteur_activite_id (conf 0.95/0.80/0.65)
       -> Fallback job si pas de match (sector_confidence=0.50)
  6. Enrichissement contacts (website scraping + DuckDuckGo + PagesJaunes fallback)
  7. Scoring & qualification
  8. Sauvegarde CSV + JSON + PostgreSQL (si PG_ENABLED=True)

Sources activées/désactivées  -> config/settings.py : SOURCES_CONFIG
Limites de collecte            -> config/settings.py : COLLECTION_LIMITS
PostgreSQL                     -> config/settings.py : PG_ENABLED, PG_DSN
Critères utilisateur           -> config/search_config.json

Usage CLI:
  python orchestrator.py                            # utilise search_config.json
  python orchestrator.py --dry-run                  # pipeline sans sauvegarde
  python orchestrator.py --source open_data         # restreindre à une source
  python orchestrator.py --source bodacc            # alias sous-source BODACC
  python orchestrator.py --source europages
  python orchestrator.py --source verif
  python orchestrator.py --target tech_france_pme   # cible prédéfinie CLI
  python orchestrator.py --stats-only               # stats des résultats existants
  python orchestrator.py --list-sources             # liste les sources et leur statut
  python orchestrator.py --max-enrich 30            # limite enrichissement contacts
"""
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import argparse
import copy
import json
import logging
import re
import time
import random
import uuid
import threading
import signal
import unicodedata
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Logging setup ──────────────────────────────────────────────────────────────
from config.settings import LOG_LEVEL, LOG_FILE

import io as _io

# FIX – UnicodeEncodeError on Windows (cp1252 console): wrap stdout in a
# UTF-8 TextIOWrapper so that special characters (—, ->, ..., OK, etc.) in
# log messages don't crash the StreamHandler on non-UTF-8 terminals.
_stdout_utf8 = _io.TextIOWrapper(
    sys.stdout.buffer,
    encoding="utf-8",
    errors="replace",
    line_buffering=True,
)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(_stdout_utf8),
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
    ],
)
logger = logging.getLogger("orchestrator")


def _handle_cancel_signal(signum, frame):
    logger.info(f"[CANCEL] Received signal {signum} -> stopping gracefully")
    raise SystemExit(3)

# ── Imports internes ───────────────────────────────────────────────────────────

from config.targets import (
    TARGETING_CRITERIA,
    SEARCH_KEYWORDS,
    SECTOR_KEYWORDS,
    SIZE_RANGES,
    SearchTarget,
    EXAMPLE_TARGETS,
    load_search_config,
)

from config.criteria_normalizer import normalize_criteria

from config.settings import (
    SOURCES_CONFIG,
    COLLECTION_LIMITS,
    ENRICH_DEFAULT_DELAY,
    ENRICH_MAX_CONCURRENT,
    ENRICH_JITTER_FACTOR,
    ENRICH_N_WORKERS,
    ENRICH_DOMAIN_DELAYS,
    ENRICH_MX_VERIFY,
    ENRICH_MX_TIMEOUT,
    ENRICH_EMAIL_PATTERNS,
    ENRICH_EXTERNAL_API,
    ENRICH_SMTP_VERIFY,
    ENRICH_SMTP_TIMEOUT,
    HUNTER_API_KEY,
    APILAYER_EMAIL_KEY,
    ENRICH_MIN_SCORE_PRIORITY,
    PG_ENABLED,                    # ← PostgreSQL : activation
)

from sources.open_data_scraper import OpenDataScraper
from sources.directory_scraper import DirectoryScraper
from sources.societe_scraper   import SocieteScraper
from sources.website_scraper   import WebsiteScraper
from sources.sirene_query_builder import SireneQueryBuilder

from pipeline.cleaner       import ProspectCleaner
from pipeline.deduplication import Deduplicator
from pipeline.embedder      import ProspectEmbedder
from pipeline.scorer        import ProspectScorer

from storage.models        import CollectionJob, JobStatut, Prospect
from storage.repository    import ProspectRepository
from storage.pg_repository import PgRepository          # ← PostgreSQL


# ── Clés réservées dans SOURCES_CONFIG (non-sous-sources) ─────────────────────
# Toute clé d'un bloc source qui n'est PAS une sous-source à scraper.
# Utilisé par _active_subsources() pour ignorer ces entrées.
_NON_SUBSOURCE_KEYS = frozenset({"enabled", "max_pages_per_site"})


# ══════════════════════════════════════════════════════════════════════════════
# DomainRateLimiter
# ══════════════════════════════════════════════════════════════════════════════
print("[DEBUG] orchestrator.py chargé")
class DomainRateLimiter:
    """
    Rate-limiter intelligent par domaine pour l'enrichissement parallèle.

    Toute la configuration est centralisée dans config/settings.py :
      ENRICH_DEFAULT_DELAY  — délai par défaut entre deux requêtes (domaines non listés)
      ENRICH_MAX_CONCURRENT — requêtes simultanées max par domaine
      ENRICH_JITTER_FACTOR  — variation aléatoire du délai (+/- X %)
      ENRICH_DOMAIN_DELAYS  — délais spécifiques par domaine (override DEFAULT_DELAY)

    Garantit deux propriétés indépendantes :
      1. Concurrence max par domaine : au plus ENRICH_MAX_CONCURRENT requêtes
         simultanées vers le même domaine (1 = sérialisation totale).
      2. Délai minimum entre requêtes : ENRICH_DEFAULT_DELAY (ou délai spécifique
         du domaine dans ENRICH_DOMAIN_DELAYS) entre deux appels successifs.

    Usage dans un thread :
        with rate_limiter.acquire(url):
            response = http_client.get(url)

    Thread-safety :
        - Un Lock par domaine (créé à la demande, stocké dans _locks).
        - _locks lui-même est protégé par _meta_lock (double-checked locking).
        - _last_req est lu/écrit sous le lock domaine : pas de race condition.
    """

    def __init__(self) -> None:
        # Lire la configuration depuis settings — source unique de vérité.
        self._default_delay:  float             = ENRICH_DEFAULT_DELAY
        self._max_concurrent: int               = ENRICH_MAX_CONCURRENT
        self._jitter_factor:  float             = ENRICH_JITTER_FACTOR
        self._domain_delays:  Dict[str, float]  = dict(ENRICH_DOMAIN_DELAYS)

        # Objets de synchronisation, créés à la demande par domaine.
        self._meta_lock: threading.Lock                 = threading.Lock()
        self._locks:     Dict[str, threading.Lock]      = {}
        self._sems:      Dict[str, threading.Semaphore] = {}
        self._last_req:  Dict[str, float]               = {}

    def _get_domain(self, url: str) -> str:
        """Extrait le domaine nu depuis une URL (sans www. ni port)."""
        from urllib.parse import urlparse
        try:
            netloc = urlparse(url if "://" in url else "https://" + url).netloc
            return netloc.lower().split(":")[0]
        except Exception:
            return url

    def _ensure_domain(self, domain: str) -> None:
        """Crée les objets de synchronisation pour un domaine si absents."""
        if domain not in self._locks:               # fast path sans lock
            with self._meta_lock:                   # slow path thread-safe
                if domain not in self._locks:       # double-checked
                    self._locks[domain]    = threading.Lock()
                    self._sems[domain]     = threading.Semaphore(self._max_concurrent)
                    self._last_req[domain] = 0.0

    def acquire(self, url: str):
        """
        Context manager : acquiert le slot du domaine et applique le délai.

        with limiter.acquire("https://example.com/contact"):
            html = client.get(...)
        """
        import contextlib

        domain = self._get_domain(url)
        self._ensure_domain(domain)
        # Délai spécifique au domaine, sinon délai par défaut (tous depuis settings).
        delay = self._domain_delays.get(domain, self._default_delay)

        @contextlib.contextmanager
        def _ctx():
            # 1. Acquérir le slot de concurrence (bloque si max_concurrent atteint)
            self._sems[domain].acquire()
            try:
                # 2. Calculer et appliquer le délai résiduel sous le lock domaine
                with self._locks[domain]:
                    elapsed = time.monotonic() - self._last_req[domain]
                    wait    = max(0.0, delay - elapsed)
                    if wait > 0:
                        jitter = random.uniform(
                            -self._jitter_factor * wait,
                             self._jitter_factor * wait,
                        )
                        time.sleep(wait + jitter)
                    self._last_req[domain] = time.monotonic()

                yield  # <- la requête HTTP s'exécute ici

            finally:
                # 3. Libérer le slot quoi qu'il arrive (exception incluse)
                self._sems[domain].release()

        return _ctx()

    def stats(self) -> str:
        """Retourne un résumé des domaines vus pour les logs."""
        if not self._locks:
            return "(aucun domaine)"
        return ", ".join(
            f"{d}(last={time.monotonic() - t:.1f}s ago)"
            for d, t in sorted(self._last_req.items(), key=lambda x: -x[1])
        )


# ══════════════════════════════════════════════════════════════════════════════
# SecteurResolver
# ══════════════════════════════════════════════════════════════════════════════

def _normalize_label(text: str) -> str:
    """
    Normalise un label de secteur pour la comparaison :
    - minuscules
    - suppression des accents (NFD → ASCII)
    - suppression des caractères non-alphanumériques sauf espace
    """
    if not text:
        return ""
    text = text.strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


class SecteurResolver:
    """
    Résout le secteur_activite_id d'un prospect à partir du texte scraped.

    Stratégie (par ordre de priorité) :
      1. Correspondance exacte sur le label normalisé
      2. Correspondance partielle : le label scraped contient un label connu
         (ou inversement)
      3. Correspondance par mots-clés alternatifs (colonne `keywords` côté Spring)
      4. Fallback : premier id du job (secteur cible de la recherche)

    Le cache `secteur_map` est chargé UNE SEULE FOIS par job depuis l'endpoint
    Spring GET /api/crm/secteur-activite/secteurs_resolver  →  [{ id, label, keywords }, ...]
    et reconstruit en  { label_normalisé: id }.

    `sector_confidence` retournée :
        0.95  exact match
        0.80  partial match (label contient / est contenu dans)
        0.65  keyword match
        0.50  fallback job  ← signal "mapping incertain" visible côté CRM
        0.00  pas d'id disponible (job sans secteur_activite_id)
    """

    # Seuil en-dessous duquel on considère le mapping comme "incertain"
    # (affiché dans les logs, utile pour filtrer côté CRM)
    CONFIDENCE_THRESHOLD = 0.7

    def __init__(
        self,
        secteur_map: Dict[str, int],           # { label_normalisé: id }
        job_secteur_ids: List[int],            # ids du job (critères Spring)
        job_secteur_labels: List[str],         # labels du job (pour les logs)
    ) -> None:
        self._map            = secteur_map
        self._fallback_id    = job_secteur_ids[0] if job_secteur_ids else None
        self._fallback_label = job_secteur_labels[0] if job_secteur_labels else "—"

    # ── API publique ──────────────────────────────────────────────────────────

    def resolve(self, prospect: "Prospect") -> Tuple[Optional[int], float]:
        """
        Résout (secteur_activite_id, sector_confidence) pour un prospect.

        Stratégie de sélection du texte source (par priorité) :
          1. secteur_activite_scraped — valeur brute d'origine (avant NLP).
             Plus fidèle à ce que le scraper a vraiment trouvé.
          2. secteur_activite — valeur post-NLP (fallback si scraped vide).

        Ne modifie PAS le prospect — l'appelant est responsable de l'affectation
        pour garder cette méthode pure et testable.
        """
        # Priorité au texte brut scraped : il n'a pas été altéré par le NLP
        raw_scraped = getattr(prospect, "secteur_activite_scraped", "") or ""
        raw_nlp     = prospect.secteur_activite or ""
        scraped = _normalize_label(raw_scraped if raw_scraped else raw_nlp)

        if scraped:
            # 1. Exact match
            if scraped in self._map:
                logger.debug(
                    f"[SecteurResolver] '{prospect.secteur_activite}' → exact match "
                    f"id={self._map[scraped]} (conf=0.95)"
                )
                return self._map[scraped], 0.95

            # 2. Partial match
            for known_label, known_id in self._map.items():
                if known_label and (known_label in scraped or scraped in known_label):
                    logger.debug(
                        f"[SecteurResolver] '{prospect.secteur_activite}' → partial match "
                        f"'{known_label}' id={known_id} (conf=0.80)"
                    )
                    return known_id, 0.80

            # 3. Word-level overlap (au moins 1 mot en commun de longueur >= 4)
            scraped_words = {w for w in scraped.split() if len(w) >= 4}
            for known_label, known_id in self._map.items():
                known_words = {w for w in known_label.split() if len(w) >= 4}
                if scraped_words & known_words:
                    logger.debug(
                        f"[SecteurResolver] '{prospect.secteur_activite}' → keyword match "
                        f"'{known_label}' id={known_id} (conf=0.65)"
                    )
                    return known_id, 0.65

            # Secteur scraped non reconnu — on logge pour investigation
            logger.debug(
                f"[SecteurResolver] Secteur inconnu '{prospect.secteur_activite}' "
                f"pour '{prospect.nom_commercial}' → fallback job '{self._fallback_label}'"
            )

        # 4. Fallback : secteur du job
        if self._fallback_id is not None:
            confidence = 0.50 if scraped else 0.50  # même valeur, sémantiques différentes
            return self._fallback_id, confidence

        # Aucun id disponible (job lancé sans secteur_activite_id)
        return None, 0.0

    def apply(self, prospect: "Prospect") -> "Prospect":
        """
        Résout et affecte directement sur le prospect.
        Retourne le prospect pour permettre le chaînage.
        """
        sid, conf = self.resolve(prospect)
        prospect.secteur_activite_id = sid
        prospect.sector_confidence   = conf
        return prospect

    # ── Chargement du cache depuis Spring ────────────────────────────────────

    @classmethod
    def load_from_spring(
        cls,
        spring_base_url: str,
        job_secteur_ids: List[int],
        job_secteur_labels: List[str],
        http_client: Any,
        timeout: float = 5.0,
    ) -> "SecteurResolver":
        """
        Charge tous les secteurs connus depuis GET /api/crm/secteur-activite/secteurs_resolver et construit
        le cache { label_normalisé: id }.

        En cas d'échec réseau (Spring injoignable, timeout…), retourne un
        resolver dégradé qui utilisera systématiquement le fallback job —
        le pipeline ne s'arrête PAS.

        Format attendu de l'endpoint Spring :
            [
              { "id": 9,  "label": "Informatique", "keywords": "it,esn,saas" },
              { "id": 12, "label": "Cybersécurité", "keywords": "cyber,sécurité" },
              ...
            ]
        """
        secteur_map: Dict[str, int] = {}

        # ── Construction du map local depuis les labels du job ────────────────
        # Même sans appel réseau, on peut déjà enregistrer les labels du job
        # pour garantir que le secteur cible lui-même sera toujours résolu.
        for sid, label in zip(job_secteur_ids, job_secteur_labels):
            norm = _normalize_label(label)
            if norm:
                secteur_map[norm] = sid

        # ── Appel Spring GET /api/crm/secteur-activite/secteurs_resolver ────────────────────────────────────
        try:
            url = f"{spring_base_url.rstrip('/')}/api/crm/secteur-activite/secteurs_resolver"
            raw_html = http_client.get(url, timeout=timeout, retries=1)
            if raw_html:
                secteurs = json.loads(raw_html)
                if isinstance(secteurs, list):
                    for s in secteurs:
                        sid   = s.get("id")
                        label = s.get("label", "")
                        if not sid or not label:
                            continue

                        # Label principal
                        secteur_map[_normalize_label(label)] = sid
                        raw_keywords = s.get("keywords", [])
                        if isinstance(raw_keywords, list):
                            kw_items = raw_keywords          # nouveau format TEXT[]
                        elif isinstance(raw_keywords, str):
                            kw_items = raw_keywords.split(",")  # ancien format CSV (compatibilité)

                    logger.info(
                        f"[SecteurResolver] Cache chargé depuis Spring : "
                        f"{len(secteurs)} secteurs → {len(secteur_map)} entrées normalisées"
                    )
                else:
                    logger.warning(
                        "[SecteurResolver] Réponse Spring /api/crm/secteur-activite/secteurs_resolver inattendue "
                        f"(type={type(secteurs).__name__}) — fallback job uniquement"
                    )
        except json.JSONDecodeError as e:
            logger.warning(f"[SecteurResolver] JSON invalide depuis Spring : {e} — fallback job")
        except Exception as e:
            logger.warning(
                f"[SecteurResolver] Impossible de charger les secteurs depuis Spring "
                f"({url if 'url' in dir() else '?'}) : {e} — fallback job"
            )

        if not secteur_map:
            logger.warning(
                "[SecteurResolver] Cache vide — tous les prospects utiliseront "
                "le fallback job (sector_confidence=0.50)"
            )

        return cls(secteur_map, job_secteur_ids, job_secteur_labels)

    @classmethod
    def from_static_map(
        cls,
        secteur_map: Dict[str, int],
        job_secteur_ids: List[int],
        job_secteur_labels: List[str],
    ) -> "SecteurResolver":
        """
        Constructeur alternatif depuis un dict déjà construit
        (utile pour les tests unitaires).
        """
        return cls(secteur_map, job_secteur_ids, job_secteur_labels)


# ══════════════════════════════════════════════════════════════════════════════
# ProspectCollector
# ══════════════════════════════════════════════════════════════════════════════

class ProspectCollector:
    """
    Orchestre l'ensemble du pipeline de collecte et qualification de prospects.

    Parallélisme de collecte :
      Chaque sous-source ACTIVE dans SOURCES_CONFIG obtient son propre thread.
      Le nombre de threads est déterminé dynamiquement au moment du run :
        n_threads = len(_active_subsources())
      Exemple : si sirene=True, osm=True, bodacc=True, data_gouv_dir=True,
                societe_com=True  →  5 threads lancés simultanément.
      Chaque thread reçoit une copie isolée de SOURCES_CONFIG (via
      criteria["_isolated_sources_config"]) afin d'éviter tout conflit.

    Usage:
        collector = ProspectCollector()
        job = collector.run()          # lit search_config.json (ou TARGETING_CRITERIA)
        job = collector.run(target)    # SearchTarget explicite via CLI --target
    """

    # Scrapers disponibles — ordre de priorité de collecte
    # BODACC est intégré dans OpenDataScraper
    # Europages et Verif.com sont intégrés dans DirectoryScraper
    _SOURCE_REGISTRY: Dict[str, type] = {
        "open_data": OpenDataScraper,   # Sirene + OpenCorporates + OSM + BODACC
        "directory": DirectoryScraper,  # PagesJaunes + Kompass + Europages + Verif
        "societe":   SocieteScraper,    # Pappers + societe.com
    }

    def __init__(
        self,
        dry_run: bool = False,
        source_filter: Optional[str] = None,
        spring_base_url: Optional[str] = None,
    ):
        self.dry_run         = dry_run
        self.source_filter   = source_filter
        # URL du backend Spring pour les appels inter-services (résolution secteurs).
        # Lit SPRING_BASE_URL depuis l'environnement si non fournie.
        self.spring_base_url = (
            spring_base_url
            or os.environ.get("SPRING_BASE_URL", "http://localhost:8080")
        ).rstrip("/")

        self._thread_local = threading.local()  # Storage isolé par thread
        self._geo_qbuilder: Optional[SireneQueryBuilder] = None

        self.cleaner      = ProspectCleaner()
        self.deduplicator = Deduplicator()
        self.embedder     = ProspectEmbedder()
        self.scorer       = ProspectScorer()
        self.repo         = ProspectRepository()

    # ──────────────────────────────────────────
    # Point d'entrée principal
    # ──────────────────────────────────────────

    def run(
        self,
        target:     Optional[SearchTarget] = None,
        max_enrich: int = 0,
        external_job_id: Optional[str] = None,
    ) -> CollectionJob:
        """
        Lance le pipeline complet.
        Priorité des critères :
          1. SearchTarget explicite (CLI --target)
          2. search_config.json (interface CRM)
          3. TARGETING_CRITERIA (fallback hardcodé dans targets.py)
        """
        if target is None:
            raise ValueError(
                "Backend criteria are required: target must be provided from bridge payload."
            )

        criteria = self._target_to_criteria(target)
        # Log des critères utilisateur (backend)
        logger.info("CRITERES UTILISATEUR (backend) :\n%s", json.dumps(criteria, ensure_ascii=False, indent=2, default=str))
        self.scorer.criteria = criteria

        # ── Résolution secteurs : chargement du cache Spring UNE SEULE FOIS ──
        # On charge tous les secteurs connus depuis Spring avant le pipeline
        # pour pouvoir mapper le texte scraped vers le bon secteur_activite_id.
        _job_secteur_ids:    List[int] = [
            int(x) for x in criteria.get("secteurs_activite_id", []) if x
        ]
        _job_secteur_labels: List[str] = [
            str(x) for x in criteria.get("secteurs_activite", []) if x
        ]
        self._secteur_resolver = SecteurResolver.load_from_spring(
            spring_base_url     = self.spring_base_url,
            job_secteur_ids     = _job_secteur_ids,
            job_secteur_labels  = _job_secteur_labels,
            http_client         = self.client,
        )

        # Calculer les sous-sources actives UNE SEULE FOIS pour tout le run
        active_subsources = self._active_subsources()
        n_threads = len(active_subsources)

        resolved_job_id = str(external_job_id).strip() if external_job_id else str(uuid.uuid4())[:8]

        job = CollectionJob(
            id              = resolved_job_id,
            name            = f"scraping_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            status          = JobStatut.RUNNING.value,
            parameters_json = json.dumps(criteria, ensure_ascii=False, default=str),
            started_at      = datetime.now().isoformat(),
        )

        logger.info("=" * 60)
        logger.info(f" SCRAPING_V1 — Job: {job.name}")
        logger.info(
            f" Sous-sources actives : "
            f"{', '.join(active_subsources.keys()) if active_subsources else '—'}"
        )
        logger.info(f" Threads de collecte  : {n_threads}")
        logger.info(f" Max prospects        : {criteria.get('max_resultats', '—')}")
        logger.info(f" PostgreSQL           : {'activé' if PG_ENABLED else 'désactivé'}")
        logger.info("=" * 60)

        signal.signal(signal.SIGTERM, _handle_cancel_signal)
        signal.signal(signal.SIGINT, _handle_cancel_signal)
        if os.name != "nt":
            signal.signal(signal.SIGQUIT, _handle_cancel_signal)

        start_time   = time.time()
        step_timings = {}


        try:
            # ── Step 1 : Collecte ─────────────────────────────────────
            logger.info("\n [Step 1/7] Collecte des prospects…")
            step_1_start = time.time()
            raw = self._collect(criteria, active_subsources)
            step_1_elapsed = time.time() - step_1_start
            step_timings["1. Collecte"] = step_1_elapsed
            job.total_collected = len(raw)
            logger.info(f"    {len(raw)} prospects bruts collectés ( {step_1_elapsed:.2f}s)")
            if raw:
                logger.info(f"    [DEBUG] Secteurs collectés: {[getattr(p, 'secteur_activite', None) for p in raw[:10]]}")
            else:
                logger.warning("    [DEBUG] Aucun prospect collecté à l'étape 1")

            # ── Step 2 : Nettoyage ────────────────────────────────────
            logger.info("\n [Step 2/7] Nettoyage & normalisation…")
            step_2_start = time.time()
            cleaned = self.cleaner.clean_batch(raw)
            logger.info(f"    [DEBUG] Après nettoyage: {len(cleaned)} prospects. Exemples secteurs: {[getattr(p, 'secteur_activite', None) for p in cleaned[:10]]}")
            enriched_region = self._enrich_region(cleaned)
            logger.info(f"    [DEBUG] Après enrichissement région: {len(enriched_region)} prospects. Exemples régions: {[getattr(p, 'region', None) for p in enriched_region[:10]]}")
            cleaned_geo = self._filter_by_geo(enriched_region, criteria)
            logger.info(f"    [DEBUG] Après filtre géographique: {len(cleaned_geo)} prospects. Exemples villes: {[getattr(p, 'ville', None) for p in cleaned_geo[:10]]}")
            step_2_elapsed = time.time() - step_2_start
            step_timings["2. Nettoyage"] = step_2_elapsed
            job.total_cleaned = len(cleaned_geo)
            logger.info(
                f"    {len(cleaned)} après nettoyage, "
                f"{len(enriched_region)} après enrichissement région, "
                f"{len(cleaned_geo)} après filtre géographique strict ( {step_2_elapsed:.2f}s)"
            )

            # ── Step 3 : Déduplication ────────────────────────────────
            logger.info("\n [Step 3/7] Déduplication…")
            step_3_start = time.time()
            unique, n_dups = self.deduplicator.deduplicate(cleaned_geo)
            logger.info(f"    [DEBUG] Après déduplication: {len(unique)} uniques, {n_dups} doublons supprimés. Exemples secteurs: {[getattr(p, 'secteur_activite', None) for p in unique[:10]]}")
            step_3_elapsed = time.time() - step_3_start
            step_timings["3. Déduplication"] = step_3_elapsed
            job.total_deduped    = len(unique)
            job.total_duplicates = n_dups
            logger.info(f"    {len(unique)} uniques, {n_dups} doublons supprimés ( {step_3_elapsed:.2f}s)")

            # ── Step 4 : NLP ──────────────────────────────────────────
            # Avant le NLP : figer secteur_activite_scraped = valeur issue du cleaner.
            # Le NLP (ProspectEmbedder) peut enrichir/remplacer secteur_activite si
            # le secteur est absent ou trop court — on garde ici la trace de ce que
            # le scraper a réellement trouvé, pour la traçabilité FK côté CRM.
            # La règle : on écrit uniquement si le champ est encore vide (ne pas
            # écraser une valeur déjà posée par un scraper antérieur dans le même job).
            for _p in unique:
                if not _p.secteur_activite_scraped and _p.secteur_activite:
                    _p.secteur_activite_scraped = _p.secteur_activite

            logger.info("\n [Step 4/7] Analyse NLP (secteur, taille)…")
            step_4_start = time.time()
            analyzed = self.embedder.enrich_all(unique)
            logger.info(f"    [DEBUG] Après NLP: {len(analyzed)} prospects. Exemples secteurs: {[getattr(p, 'secteur_activite', None) for p in analyzed[:10]]}")
            step_4_elapsed = time.time() - step_4_start
            step_timings["4. Analyse NLP"] = step_4_elapsed
            logger.info(f"    {len(analyzed)} prospects enrichis NLP ( {step_4_elapsed:.2f}s)")

            # ── Step 4B : Résolution secteur_activite_id ──────────────
            # Après NLP (le secteur_activite textuel est stabilisé),
            # on résout le secteur_activite_id vers la FK Spring.
            # Deux cas :
            #   • Le texte scraped matche un secteur connu → id précis (conf ≥ 0.65)
            #   • Pas de match → fallback sur le secteur du job   (conf = 0.50)
            logger.info("\n [Step 4B/7] Résolution secteur_activite_id…")
            step_4b_start = time.time()
            analyzed = self._step_resolve_secteurs(analyzed)
            step_4b_elapsed = time.time() - step_4b_start
            step_timings["4B. Résolution secteurs"] = step_4b_elapsed
            _n_exact    = sum(1 for p in analyzed if p.sector_confidence >= 0.90)
            _n_partial  = sum(1 for p in analyzed if 0.60 <= p.sector_confidence < 0.90)
            _n_fallback = sum(1 for p in analyzed if p.sector_confidence < 0.60)
            _avg_nlp    = round(
                sum(getattr(p, "nlp_confidence", 0.0) for p in analyzed) / max(len(analyzed), 1), 3
            )
            logger.info(
                f"    exact={_n_exact} | partial/kw={_n_partial} | "
                f"fallback={_n_fallback} | avg_nlp_conf={_avg_nlp} ( {step_4b_elapsed:.2f}s)"
            )

            # ── Step 5 : Enrichissement contacts ─────────────────────
            logger.info("\n [Step 5/7] Enrichissement contacts…")
            step_5_start = time.time()
            _max_enrich = max_enrich if max_enrich > 0 else COLLECTION_LIMITS.get("max_enrich_per_run", 50)
            enriched    = self._enrich_contacts(analyzed, max_enrich=_max_enrich)
            step_5_elapsed = time.time() - step_5_start
            step_timings["5. Enrichissement"] = step_5_elapsed
            logger.info(
                f"    Après enrichissement : "
                f"{sum(1 for p in enriched if p.website)} websites | "
                f"{sum(1 for p in enriched if p.email)} emails | "
                f"{sum(1 for p in enriched if p.telephone)} téléphones ( {step_5_elapsed:.2f}s)"
            )

            # ── Step 5B : Validation d'emails ─────────────────────────
            logger.info("\n [Step 5B/7] Validation d'emails…")
            step_5b_start = time.time()
            validated = self._validate_emails(enriched)
            step_5b_elapsed = time.time() - step_5b_start
            step_timings["5B. Validation emails"] = step_5b_elapsed
            email_valid_count = sum(1 for p in validated if p.email_valid)
            logger.info(
                f"    {email_valid_count}/{sum(1 for p in validated if p.email)} emails valides ( {step_5b_elapsed:.2f}s)"
            )

            # ── Step 6 : Scoring ──────────────────────────────────────
            logger.info("\n [Step 6/7] Scoring & qualification…")
            step_6_start = time.time()
            scored = self.scorer.score_all(validated)
            step_6_elapsed = time.time() - step_6_start
            step_timings["6. Scoring"] = step_6_elapsed
            stats  = ProspectScorer.get_stats(scored)
            job.total_scored    = len(scored)
            job.total_qualified = stats["qualified"]
            if stats["total"] == 0:
                logger.warning("    Aucun prospect — vérifiez connexion réseau et critères")
            else:
                logger.info(
                    f"    {stats['qualified']} qualifiés / {stats['total']} "
                    f"({stats['qualification_rate_pct']}%) — score moyen: {stats['avg_score']} ( {step_6_elapsed:.2f}s)"
                )

            elapsed          = round(time.time() - start_time, 2)
            job.status       = JobStatut.DONE.value
            job.finished_at  = datetime.now().isoformat()
            job.sources_used = list(active_subsources.keys())
            print(f"[DEBUG][orchestrator] Sauvegarde locale : {len(enriched)} prospects enrichis, {len(scored)} prospects qualifiés à sauvegarder pour job_id={job.id}")

            # ── Step 7 : Sauvegarde CSV/JSON + PostgreSQL ─────────────
            logger.info("\n [Step 7/7] Sauvegarde (CSV/JSON + PostgreSQL)…")
            print(f"[DEBUG][orchestrator] Après scoring : {len(scored)} prospects qualifiés pour job_id={job.id}")
            # Job fields (status, finished_at, sources_used) are set above so
            # the row written to PG inside _step_sauvegarde is already complete.
            step_7_start   = time.time()
            step_7_elapsed = self._step_sauvegarde(enriched, scored, job)
            step_timings["7. Sauvegarde"] = step_7_elapsed
            logger.info(f"    ( {step_7_elapsed:.2f}s)")

            # Sauvegarde finale du job en CSV/JSON local (toujours actif)
            if not self.dry_run:
                self.repo.save_job(job)
                # PG job already saved inside _step_sauvegarde — no duplicate needed.

            logger.info(f"\n  TEMPS TOTAL : {elapsed}s")
            self._print_summary(scored, stats, elapsed)
            self._print_timings_table(step_timings)
            return job

        except Exception as exc:
            logger.exception(f" Pipeline error: {exc}")
            job.status      = JobStatut.FAILED.value
            job.finished_at = datetime.now().isoformat()
            job.errors.append(str(exc))
            if not self.dry_run:
                self.repo.save_job(job)
                if PG_ENABLED:
                    try:
                        with PgRepository() as pg:
                            pg.save_job(job)
                    except Exception as pg_exc:
                        logger.error(f"    [PG] Erreur sauvegarde job failed : {pg_exc}")
            raise

    # ──────────────────────────────────────────
    # Step 4B — Résolution secteur_activite_id
    # ──────────────────────────────────────────

    def _step_resolve_secteurs(self, prospects: List[Prospect]) -> List[Prospect]:
        """Resout secteur_activite_id via SecteurResolver (apres NLP, avant enrichissement).

        Le resolver est initialise une seule fois dans run() -- aucun appel reseau ici.
        Logs DEBUG par prospect, resume INFO dans run().
        """
        if not hasattr(self, "_secteur_resolver") or self._secteur_resolver is None:
            logger.warning(
                "[SecteurResolve] _secteur_resolver absent — resolution ignoree. "
                "Verifiez que run() initialise bien le resolver avant le pipeline."
            )
            return prospects

        resolver = self._secteur_resolver

        for p in prospects:
            sid, conf = resolver.resolve(p)
            p.secteur_activite_id = sid
            p.sector_confidence   = conf

            # Log DEBUG uniquement pour eviter le bruit sur grands volumes
            if conf < SecteurResolver.CONFIDENCE_THRESHOLD:
                logger.debug(
                    f"[SecteurResolve] FAIBLE CONFIANCE ({conf:.2f}) "
                    f"'{p.secteur_activite}' -> id={sid} "
                    f"| {p.nom_commercial!r}"
                )

        return prospects

    # ──────────────────────────────────────────
    # Step 7 — Sauvegarde (méthode extraite)
    # ──────────────────────────────────────────

    def _step_sauvegarde(
        self,
        enriched: List[Prospect],
        scored:   List[Prospect],
        job:      CollectionJob,
    ) -> float:
        """
        Sauvegarde CSV + JSON (comportement original) puis PostgreSQL (optionnel).

        Retourne le temps écoulé en secondes.

        Ordre d'exécution :
          7a. CSV + JSON — toujours actif, inchangé par rapport à l'original.
          7b. PostgreSQL — actif uniquement si PG_ENABLED=True dans settings.py.

        Comportement en cas d'erreur PostgreSQL :
          L'erreur est loggée mais NE fait PAS échouer le pipeline.
          Les données CSV/JSON sont déjà écrites à ce stade (7a terminé).

        Dry-run :
          Aucune écriture n'est effectuée (ni CSV/JSON, ni PostgreSQL).
        """
        print(f"[DEBUG][orchestrator] _step_sauvegarde appelée pour job_id={job.id}")
        step_start = time.time()

        if self.dry_run:
            logger.info("    DRY-RUN — pas de sauvegarde")
            job.total_saved = 0
            return time.time() - step_start

        # ── 7a : CSV + JSON (comportement original, inchangé) ─────────────────
        info = self.repo.save_all(enriched)
        self.repo.save_scored(scored)
        self.repo.export_qualified_only()
        job.total_saved = info["n_saved"]
        logger.info(f"    [CSV/JSON] {info['n_saved']} prospects sauvegardés dans output/")

        print(f"[DEBUG][orchestrator] Valeur de PG_ENABLED juste avant test: {PG_ENABLED}")
        # ── 7b : PostgreSQL (nouveau, optionnel, non-bloquant) ────────────────
        if PG_ENABLED:
            print(f"[DEBUG][orchestrator] Appel upsert_prospects avec {len(scored)} prospects qualifiés pour job_id={job.id}")
            try:
                with PgRepository() as pg:
                    # FIX – FK violation: prospects.job_id references collection_jobs.id.
                    # The job row MUST exist before prospects are inserted.
                    # Save the job first, then upsert prospects in the same block
                    # so both operations share one connection and the FK is satisfied.
                    pg.save_job(job)
                    result = pg.upsert_prospects(scored, job_id=job.id)
                n_upserted = result.get("upserted", 0)
                n_skipped  = result.get("skipped", 0)
                logger.info(
                    f"    [PostgreSQL] {n_upserted} prospects upsertés"
                    + (f" | {n_skipped} ignorés (hash_dedup manquant)" if n_skipped else "")
                )
            except Exception as e:
                # Non-bloquant : les données sont déjà dans CSV/JSON.
                logger.error(
                    f"    [PostgreSQL] ERREUR (non bloquant) : {e}\n"
                    f"    Les fichiers CSV/JSON sont intacts."
                )
        else:
            logger.debug("    [PostgreSQL] Désactivé (PG_ENABLED=False dans settings.py)")

        return time.time() - step_start

    # ──────────────────────────────────────────
    # Résolution des critères (priorité 1 -> 2 -> 3)
    # ──────────────────────────────────────────

    def _resolve_criteria(self, target: Optional[SearchTarget]) -> Dict[str, Any]:
        """
        Résout les critères dans l'ordre de priorité :
          1. SearchTarget explicite (CLI --target)        -> _target_to_criteria()
          2. search_config.json présent (interface CRM)  -> _search_config_to_criteria()
          3. TARGETING_CRITERIA (fallback hardcodé)
        """
        if target is not None:
            logger.info("[Criteria] Source : SearchTarget (CLI --target)")
            return self._target_to_criteria(target)

        cfg = load_search_config()
        if cfg:
            logger.info("[Criteria] Source : search_config.json (interface CRM)")
            return self._search_config_to_criteria(cfg)

        logger.info("[Criteria] Source : TARGETING_CRITERIA (fallback — search_config.json absent)")
        criteria = dict(TARGETING_CRITERIA)
        criteria["keywords"]       = SEARCH_KEYWORDS
        criteria["codes_naf"]      = []
        criteria["max_resultats"]  = COLLECTION_LIMITS.get("max_prospects_per_source", 100) * len(self._SOURCE_REGISTRY)
        criteria["max_par_source"] = COLLECTION_LIMITS.get("max_prospects_per_source", 100)
        return criteria

    # ──────────────────────────────────────────
    # Convertisseurs -> criteria dict
    # ──────────────────────────────────────────

    @staticmethod
    def _search_config_to_criteria(cfg: dict) -> Dict[str, Any]:
        """
        Convertit le dict chargé depuis search_config.json en criteria
        compatible avec le pipeline.
        """
        taille     = cfg.get("taille_entreprise", {})
        geo        = cfg.get("zone_geographique", {})
        categories = taille.get("categories", [])

        employes_min: int           = 1
        employes_max: Optional[int] = None
        if categories:
            mins, maxs = [], []
            for cat in categories:
                rng = SIZE_RANGES.get(cat.upper())
                if rng:
                    mins.append(rng[0])
                    maxs.append(rng[1])
            if mins:
                employes_min = min(mins)
            if maxs:
                finite = [m for m in maxs if m != float("inf")]
                employes_max = max(finite) if finite else None

        if taille.get("nb_employes_min") is not None:
            employes_min = int(taille["nb_employes_min"])
        if taille.get("nb_employes_max") is not None:
            employes_max = int(taille["nb_employes_max"])

        # Normalise secteurs_activite_id en liste d'entiers propres
        _raw_ids = cfg.get("secteurs_activite_id", []) or []
        _secteur_ids: List[int] = []
        for _x in _raw_ids:
            try:
                _secteur_ids.append(int(_x))
            except (TypeError, ValueError):
                pass

        return {
            "secteurs_activite":    cfg.get("secteurs_activite", []),
            # IDs des secteurs cibles (FK Spring) — utilisés par SecteurResolver
            "secteurs_activite_id": _secteur_ids,
            "tailles_entreprise":   categories,
            "types_entreprise":     cfg.get("types_entreprise", []),
            "codes_naf":            cfg.get("codes_naf", []),
            "employes_min":         employes_min,
            "employes_max":         employes_max,
            "localisation": {
                "pays":    geo.get("pays") or geo.get("zone_geographique", ["France"]),
                "regions": geo.get("regions", []),
                "villes":  geo.get("villes", []),
            },
            "keywords":        cfg.get("mots_cles", []),
            "max_resultats":   int(cfg.get("max_prospects_total", 700)),
            "max_par_source":  COLLECTION_LIMITS.get("max_prospects_per_source", 100),
        }

    @staticmethod
    def _target_to_criteria(t: SearchTarget) -> Dict[str, Any]:
        """
        Convertit un SearchTarget (CLI --target / EXAMPLE_TARGETS) en criteria dict.
        Auto-génère les keywords depuis SECTOR_KEYWORDS si non fournis.
        """
        auto_keywords: List[str] = []
        if not t.keywords:
            for secteur in t.secteur_activite:
                auto_keywords.extend(SECTOR_KEYWORDS.get(secteur, [])[:4])
        keywords = t.keywords or auto_keywords or SEARCH_KEYWORDS

        employes_min: int           = 1
        employes_max: Optional[int] = None
        if t.taille_entreprise:
            mins, maxs = [], []
            for taille in t.taille_entreprise:
                rng = SIZE_RANGES.get(taille.upper())
                if rng:
                    mins.append(rng[0])
                    maxs.append(rng[1])
            if mins:
                employes_min = min(mins)
            if maxs:
                finite = [m for m in maxs if m != float("inf")]
                employes_max = max(finite) if finite else None

        # Normalise secteurs_activite_id en liste d'entiers propres
        _raw_ids = getattr(t, "secteurs_activite_id", []) or []
        _secteur_ids: List[int] = []
        for _x in _raw_ids:
            try:
                _secteur_ids.append(int(_x))
            except (TypeError, ValueError):
                pass

        return {
            "secteurs_activite":    t.secteur_activite,
            # IDs des secteurs cibles (FK Spring) — utilisés par SecteurResolver
            # comme fallback quand le texte scraped ne matche aucun secteur connu.
            "secteurs_activite_id": _secteur_ids,
            "tailles_entreprise":   t.taille_entreprise,
            "types_entreprise":     t.types_entreprise,
            "codes_naf":            [],
            "employes_min":         employes_min,
            "employes_max":         employes_max,
            "localisation": {
                "pays":    t.pays,
                "regions": t.regions,
                "villes":  t.villes,
            },
            "keywords":           keywords,
            "max_pages_annuaire": t.max_pages_annuaire,
            "max_resultats":      t.max_resultats,
            "max_par_source":     t.max_par_source,
        }

    # ──────────────────────────────────────────
    # Collecte — parallélisme par sous-source
    # ──────────────────────────────────────────

    def _collect(
        self,
        criteria: Dict[str, Any],
        active_subsources: Dict[str, Tuple[str, Any]],
    ) -> List[Prospect]:
        """
        Lance CHAQUE SOUS-SOURCE ACTIVE dans son propre thread et agrège
        les résultats jusqu'à max_resultats.

        Nombre de threads = nombre de sous-sources actives (dynamique).
        Exemple :
          SOURCES_CONFIG actif : sirene, osm, bodacc, data_gouv_dir, societe_com
          → 5 threads lancés simultanément

        Chaque thread reçoit une copie PROFONDE et ISOLÉE de SOURCES_CONFIG
        (via criteria["_isolated_sources_config"]) : aucun partage d'état,
        aucun lock long nécessaire.
        """
        max_total      = int(criteria.get("max_resultats",  COLLECTION_LIMITS.get("max_prospects_per_source", 100) * 3))
        max_per_source = int(criteria.get("max_par_source", COLLECTION_LIMITS.get("max_prospects_per_source", 100)))
        all_raw: List[Prospect] = []

        if not active_subsources:
            logger.warning("[Collect] Aucune sous-source active")
            return []

        n_threads = len(active_subsources)
        logger.info(
            f"[Collect] Lancement PARALLÈLE — "
            f"{n_threads} thread(s) | "
            f"sous-sources : {', '.join(active_subsources.keys())}"
        )

        with ThreadPoolExecutor(max_workers=n_threads) as executor:
            future_to_subsource = {
                executor.submit(
                    self._search_single_subsource,
                    subsource_name,
                    parent_source,
                    scraper,
                    criteria,
                    max_per_source,
                ): subsource_name
                for subsource_name, (parent_source, scraper) in active_subsources.items()
            }

            for future in as_completed(future_to_subsource):
                subsource_name = future_to_subsource[future]

                # Arrêt anticipé si le plafond global est atteint
                if len(all_raw) >= max_total:
                    logger.info(
                        f"[Collect] Plafond global {max_total} atteint "
                        f"— futures restantes annulées"
                    )
                    for pending in future_to_subsource:
                        pending.cancel()
                    break

                try:
                    results = future.result(timeout=120)
                    all_raw.extend(results)
                    logger.info(
                        f"[Collect]  {subsource_name:<22}  "
                        f"{len(results):>3} prospects "
                        f"(total cumulé : {len(all_raw)})"
                    )
                except TimeoutError:
                    logger.error(f"[Collect]  {subsource_name} — TIMEOUT (>120s)")
                except Exception as exc:
                    logger.error(f"[Collect]  {subsource_name} — Erreur : {exc}")

        logger.info(f"[Collect] Collecte terminée : {len(all_raw)} prospects bruts")
        return all_raw[:max_total]

    def _search_single_subsource(
        self,
        subsource_name: str,
        parent_source:  str,
        scraper:        Any,
        criteria:       Dict[str, Any],
        max_per_source: int,
    ) -> List[Prospect]:
        """
        Exécute UNE SEULE sous-source dans un thread isolé.

        Isolation de la configuration :
          1. Copie profonde de SOURCES_CONFIG pour ce thread uniquement.
          2. Dans le bloc parent, toutes les sous-sources sont désactivées.
          3. Seule `subsource_name` est réactivée.
          4. La config isolée est injectée dans une copie profonde de `criteria`
             (criteria["_isolated_sources_config"]) — sans toucher aux objets
             partagés.

        Résultat : zéro état partagé mutable entre threads → pas de race condition,
        pas de lock nécessaire sur la config.
        """
        logger.debug(f"[Thread:{subsource_name}] Démarrage")

        try:
            # ── 1. Config isolée : copie profonde de SOURCES_CONFIG ───
            isolated_config  = copy.deepcopy(SOURCES_CONFIG)
            parent_cfg_block = isolated_config.get(parent_source, {})

            # Désactiver TOUTES les sous-sources du parent dans cette copie
            for key in list(parent_cfg_block.keys()):
                if key not in _NON_SUBSOURCE_KEYS:
                    parent_cfg_block[key] = False

            # Réactiver uniquement la sous-source de ce thread
            parent_cfg_block[subsource_name] = True

            # ── 2. Criteria isolé : copie profonde pour ce thread ─────
            isolated_criteria = copy.deepcopy(criteria)
            isolated_criteria["_isolated_sources_config"] = isolated_config

            logger.debug(
                f"[Thread:{subsource_name}] Config isolée : "
                f"{parent_source}.{subsource_name}=True, autres=False"
            )

            # ── 3. Scraping ───────────────────────────────────────────
            results = scraper.search(isolated_criteria)

            # ── 4. Appliquer la limite fine par sous-source ───────────
            limit = COLLECTION_LIMITS.get(f"{subsource_name}_max_par_run", max_per_source)
            if len(results) > limit:
                logger.debug(
                    f"[Thread:{subsource_name}] Limite {limit} appliquée "
                    f"(brut : {len(results)})"
                )
                results = results[:limit]

            logger.debug(f"[Thread:{subsource_name}] Terminé — {len(results)} prospects")
            return results

        except Exception as exc:
            logger.error(
                f"[Thread:{subsource_name}] Exception {type(exc).__name__} : {exc}",
                exc_info=True,
            )
            return []

    # ──────────────────────────────────────────
    # Sources actives
    # ──────────────────────────────────────────

    def _active_sources(self) -> Dict[str, Any]:
        """
        Instancie les scrapers parents actifs selon SOURCES_CONFIG.
        'website' est exclu ici — réservé à l'enrichissement (step 5).
        Respecte source_filter si fourni (CLI --source).
        """
        sources: Dict[str, Any] = {}

        for name, scraper_cls in self._SOURCE_REGISTRY.items():
            config = SOURCES_CONFIG.get(name, {})

            if not config.get("enabled", True):
                logger.info(f"[Sources] '{name}' désactivée (SOURCES_CONFIG)")
                continue

            # Filtre CLI --source <parent> : ne garder que ce parent
            if self.source_filter and self.source_filter not in (name, None):
                # Vérifier si le filtre est une sous-source de ce parent
                # (ex: --source bodacc doit activer open_data)
                parent_keys = [
                    k for k in config
                    if k not in _NON_SUBSOURCE_KEYS
                ]
                if self.source_filter not in parent_keys:
                    continue

            sources[name] = scraper_cls()

        return sources

    def _active_subsources(self) -> Dict[str, Tuple[str, Any]]:
        """
        Retourne TOUTES les sous-sources ACTIVES avec leur scraper parent.
        Le résultat détermine directement le nombre de threads au moment du run.

        La liste des sous-sources possibles est lue DEPUIS SOURCES_CONFIG
        (clés booléennes hors _NON_SUBSOURCE_KEYS), ce qui garantit que toute
        nouvelle sous-source ajoutée dans settings.py est automatiquement prise
        en compte — sans modifier l'orchestrateur.

        Filtre CLI --source :
          --source open_data   → uniquement les sous-sources de open_data
          --source bodacc      → uniquement bodacc (sous-source de open_data)
          --source directory   → uniquement les sous-sources de directory
          (sans --source)      → toutes les sous-sources actives

        Retour :
            {
                "sirene":        ("open_data", <OpenDataScraper instance>),
                "osm":           ("open_data", <OpenDataScraper instance>),
                "bodacc":        ("open_data", <OpenDataScraper instance>),
                "data_gouv_dir": ("directory", <DirectoryScraper instance>),
                "societe_com":   ("societe",   <SocieteScraper instance>),
            }
            → 5 threads seront lancés en parallèle
        """
        subsources: Dict[str, Tuple[str, Any]] = {}
        active_sources = self._active_sources()

        for parent_name, scraper in active_sources.items():
            parent_cfg = SOURCES_CONFIG.get(parent_name, {})

            for subsource_name, is_active in parent_cfg.items():
                # Ignorer les clés de configuration non-scraper
                if subsource_name in _NON_SUBSOURCE_KEYS:
                    continue

                # Ignorer les sous-sources désactivées (valeur False)
                if not is_active:
                    continue

                # Filtre CLI --source <subsource> ou <parent>
                if self.source_filter:
                    sf = self.source_filter
                    if sf != subsource_name and sf != parent_name:
                        continue

                subsources[subsource_name] = (parent_name, scraper)

        if subsources:
            logger.debug(
                f"[Sources] {len(subsources)} sous-source(s) active(s) : "
                f"{', '.join(subsources.keys())}"
            )
        else:
            logger.warning("[Sources] Aucune sous-source active détectée")

        return subsources

    def _get_active_source_names(self) -> List[str]:
        """Retourne la liste des noms de sources parents actives."""
        return [
            name for name in self._SOURCE_REGISTRY
            if SOURCES_CONFIG.get(name, {}).get("enabled", True)
        ]

    def _get_geo_qbuilder(self) -> SireneQueryBuilder:
        if self._geo_qbuilder is None:
            self._geo_qbuilder = SireneQueryBuilder()
        return self._geo_qbuilder

    @staticmethod
    def _norm_geo_text(value: Any) -> str:
        text = str(value or "").strip().lower()
        text = unicodedata.normalize("NFKD", text)
        return "".join(ch for ch in text if not unicodedata.combining(ch))

    @classmethod
    def _geo_text_match(cls, value: str, targets: List[str]) -> bool:
        v = cls._norm_geo_text(value)
        if not v:
            return False
        for t in targets:
            tv = cls._norm_geo_text(t)
            if not tv:
                continue
            if tv in v or v in tv:
                return True
        return False

    @staticmethod
    def _cp_match(code_postal: str, prefixes: List[str]) -> bool:
        cp = re.sub(r"\D", "", str(code_postal or ""))
        if not cp:
            return False
        for p in prefixes:
            pref = re.sub(r"\D", "", str(p or ""))
            if pref and cp.startswith(pref):
                return True
        return False

    def _enrich_region(self, prospects: List[Prospect]) -> List[Prospect]:
        """
        Enrichit les prospects avec leur région basée sur le code postal, puis la ville en fallback.
        Utilise le SireneQueryBuilder pour faire un reverse lookup : CP → région.
        Fallback: ville → région (cherche dans postal_by_geo.json).
        """
        qb = self._get_geo_qbuilder()
        enriched_cp = 0
        enriched_ville = 0
        
        for p in prospects:
            # Si région vide, chercher enrichissement
            if not p.region or not str(p.region).strip():
                # Tentative 1: depuis code postal
                if p.code_postal:
                    region = qb.get_region_for_postal(p.code_postal)
                    if region:
                        p.region = region
                        enriched_cp += 1
                        continue
                
                # Tentative 2 (fallback): depuis la ville
                if p.ville:
                    try:
                        # Cherche dans les données de région si cette ville est mentionnée
                        ville_lower = str(p.ville).lower().strip()
                        postal_data = qb._postal.get("regions", {})
                        
                        # Logique spéciale pour les grandes villes
                        ville_to_region = {
                            "paris": "Île-de-France",
                            "marseille": "Provence-Alpes-Côte d'Azur",
                            "lyon": "Auvergne-Rhône-Alpes",
                            "toulouse": "Midi-Pyrénées",
                            "nice": "Provence-Alpes-Côte d'Azur",
                            "nantes": "Pays de la Loire",
                            "strasbourg": "Grand Est",
                            "montpellier": "Occitanie",
                            "bordeaux": "Nouvelle-Aquitaine",
                            "lille": "Hauts-de-France",
                        }
                        
                        if ville_lower in ville_to_region:
                            p.region = ville_to_region[ville_lower]
                            enriched_ville += 1
                    except Exception:
                        pass
        
        if enriched_cp > 0:
            logger.debug(f"[EnrichRegion] {enriched_cp} prospects enrichis via code postal")
        if enriched_ville > 0:
            logger.debug(f"[EnrichRegion] {enriched_ville} prospects enrichis via ville (fallback)")
        
        return prospects

    def _validate_emails(self, prospects: List[Prospect]) -> List[Prospect]:
        """
        Valide tous les emails des prospects.
        Met à jour le champ email_valid avec une validation robuste:
          - Syntaxe regex
          - Domaine non bloqué (pas gmail, yahoo, etc.)
          - Pas de noreply, contact générique, etc.
        
        Note: MX check (DNS) n'est pas fait par défaut (trop lent pour batch).
              Pourrait être activé via settings si besoin.
        """
        from utils.email_validator import validate_email
        
        validated = 0
        invalid = 0
        
        for p in prospects:
            if p.email:
                result = validate_email(p.email, check_mx=False)
                p.email_valid = result["is_valid"]
                
                if result["is_valid"]:
                    validated += 1
                else:
                    invalid += 1
                    logger.debug(f"[EmailValidation] {p.nom_commercial}: {result['reason']}")
        
        if validated > 0 or invalid > 0:
            logger.info(f"[EmailValidation] {validated} valides, {invalid} invalides / {len([p for p in prospects if p.email])} emails")
        
        return prospects

    def _filter_by_geo(self, prospects: List[Prospect], criteria: Dict[str, Any]) -> List[Prospect]:
        loc = criteria.get("localisation") or {}
        target_pays_raw    = loc.get("pays", []) or []
        target_regions_raw = loc.get("regions", []) or []
        target_villes_raw  = loc.get("villes", []) or []

        target_pays    = [str(x).strip() for x in target_pays_raw if str(x).strip()]
        target_regions = [str(x).strip() for x in target_regions_raw if str(x).strip()]
        target_villes  = [str(x).strip() for x in target_villes_raw if str(x).strip()]

        if not any([target_pays, target_regions, target_villes]):
            return prospects

        city_cp_prefixes: List[str] = []
        region_cp_prefixes: List[str] = []
        try:
            qb = self._get_geo_qbuilder()

            def _canonicalize(values: List[str], catalog: List[str]) -> List[str]:
                out: List[str] = []
                for value in values:
                    nv = self._norm_geo_text(value)
                    chosen = value
                    for item in catalog:
                        ni = self._norm_geo_text(item)
                        if nv == ni or nv in ni or ni in nv:
                            chosen = item
                            break
                    out.append(chosen)
                return list(dict.fromkeys(out))

            canonical_villes = _canonicalize(target_villes, qb.list_villes()) if target_villes else []
            canonical_regions = _canonicalize(target_regions, qb.list_regions()) if target_regions else []

            if canonical_villes:
                city_cp_prefixes = qb._map_geo(regions=[], villes=canonical_villes)
            if canonical_regions:
                region_cp_prefixes = qb._map_geo(regions=canonical_regions, villes=[])
        except Exception as geo_err:
            logger.warning(f"[GeoFilter] Mapping geo indisponible: {geo_err}")

        filtered: List[Prospect] = []
        for p in prospects:
            if target_pays and not self._geo_text_match(p.pays, target_pays):
                continue

            # Priorité stricte : ville > région > pays.
            if target_villes:
                if self._geo_text_match(p.ville, target_villes):
                    filtered.append(p)
                    continue
                if self._cp_match(p.code_postal, city_cp_prefixes):
                    filtered.append(p)
                    continue
                continue

            if target_regions:
                if self._geo_text_match(p.region, target_regions):
                    filtered.append(p)
                    continue
                if self._cp_match(p.code_postal, region_cp_prefixes):
                    filtered.append(p)
                    continue
                continue

            filtered.append(p)

        return filtered

    # ──────────────────────────────────────────
    # Enrichissement contacts (step 5)
    # ──────────────────────────────────────────

    def _enrich_contacts(
        self, prospects: List[Prospect], max_enrich: int = 50
    ) -> List[Prospect]:
        """
        Enrichissement contacts PARALLELE — ThreadPoolExecutor + DomainRateLimiter.

        Améliorations v2 :
          ① Priorisation intelligente : les prospects avec le meilleur
            qualification_score sont traités en premier, optimisant le
            budget de requêtes HTTP sur les leads les plus prometteurs.
            Filtre optionnel par ENRICH_MIN_SCORE_PRIORITY (settings.py).
          ② Vérification MX : après génération d'emails par patterns, une
            résolution DNS MX valide que le domaine accepte des emails.
            Évite d'injecter des adresses invalides dans le CRM.
          ③ Patterns email + fallback DDG contact ciblé : si aucun email
            n'est trouvé par scraping, on génère contact@domaine,
            info@domaine, etc. avec vérification MX, puis on interroge
            DDG avec "inurl:contact" pour trouver la page de contact.
            Optionnellement, Hunter.io ou apilayer en dernier recours
            (ENRICH_EXTERNAL_API=True dans settings.py — quota limité).
          ④ Score d'enrichissement : chaque prospect reçoit un enrich_score
            (0-4) reflétant la qualité des données collectées :
              +1 email présent et valide (+ email_mx_verified si MX OK)
              +1 téléphone présent
              +1 website présent
              +1 linkedin_url présent
            Ce score est distinct du qualification_score (critères CRM)
            et permet de trier les exports par qualité de données contact.

        Etapes par prospect (A→B→C→D→E) :
          A. Site connu     → WebsiteScraper (email, tél, adresse, LinkedIn)
          B. Pas de site    → DDG "site officiel" → WebsiteScraper
          C. Email manquant → Patterns email + vérification MX
          D. Toujours vide  → DDG "inurl:contact" + site /contact direct
          E. Dernier recours→ Hunter.io / apilayer (si ENRICH_EXTERNAL_API)

        Thread-safety :
          - Chaque thread travaille sur un Prospect distinct.
          - enriched_count est protégé par count_lock.
          - DomainRateLimiter est entièrement thread-safe.
          - Un WebsiteScraper distinct par thread (threading.local).

        Ignoré si source_filter actif sur une autre source.
        Ignoré si source 'website' désactivée dans SOURCES_CONFIG.
        """
        if self.source_filter and self.source_filter not in ("website", None):
            return prospects

        if not SOURCES_CONFIG.get("website", {}).get("enabled", True):
            logger.info("[Enrich] Source 'website' desactivee dans settings.py — enrichissement ignore")
            return prospects

        # ── ① Priorisation intelligente ───────────────────────────────
        candidates = [
            p for p in prospects
            if p.nom_commercial
            and (not p.email or not p.telephone or not p.website)
            and p.qualification_score >= ENRICH_MIN_SCORE_PRIORITY
        ]
        candidates.sort(key=lambda p: p.qualification_score, reverse=True)
        to_enrich = candidates[:max_enrich]

        if not to_enrich:
            logger.info("[Enrich] Aucun prospect eligible a l'enrichissement")
            for p in prospects:
                p.enrich_score = self._compute_enrich_score(p)
            return prospects

        _N_WORKERS = min(ENRICH_N_WORKERS, len(to_enrich))
        rl         = DomainRateLimiter()
        count_lock = threading.Lock()
        total      = len(to_enrich)
        enriched_count = 0

        _thread_local = threading.local()

        def _get_ws() -> WebsiteScraper:
            if not hasattr(_thread_local, "ws"):
                _thread_local.ws = WebsiteScraper()
            return _thread_local.ws

        logger.info(
            f"[Enrich] {total}/{len(prospects)} prospects eligibles "
            f"(score>={ENRICH_MIN_SCORE_PRIORITY}, tries par score) "
            f"| {_N_WORKERS} threads | MX_verify={ENRICH_MX_VERIFY} "
            f"| ext_api={ENRICH_EXTERNAL_API}"
        )

        _DDG_URL = "https://html.duckduckgo.com/html/"

        def _enrich_one(args: tuple) -> None:
            """Enrichit un seul prospect dans un thread isolé."""
            nonlocal enriched_count
            idx, p   = args
            progress = f"[{idx}/{total}]"
            changed  = False

            try:
                # ── Step A : scraper le site déjà connu ───────────────
                if p.website and (not p.email or not p.telephone):
                    logger.debug(f"[Enrich] {progress} Scraping site connu: {p.website}")
                    with rl.acquire(p.website):
                        web = _get_ws().scrape_website(p.website)
                    if web:
                        changed = self._merge_contact(p, web)

                # ── Step B : trouver le site via DuckDuckGo ───────────
                if not p.website:
                    logger.debug(f"[Enrich] {progress} Recherche DDG site: {p.nom_commercial!r}")
                    with rl.acquire(_DDG_URL):
                        found_url = self._ddg_find_website(p)
                    if found_url:
                        p.website = found_url
                        with rl.acquire(found_url):
                            web = _get_ws().scrape_website(found_url)
                        if web:
                            changed = self._merge_contact(p, web) or changed

                # ── Step C : patterns email + vérification MX ─────────
                if not p.email and p.website:
                    mx_email = self._find_email_by_patterns(p)
                    if mx_email:
                        p.email       = mx_email
                        p.email_valid = True
                        changed       = True
                        logger.debug(
                            f"[Enrich] {progress} Email pattern: {mx_email!r}"
                        )

                # ── Step D : fallback DDG contact ciblé ───────────────
                if not p.telephone or not p.email:
                    if p.website and (not p.email or not p.telephone):
                        with rl.acquire(p.website):
                            pj_changed = self._pj_find_contact(p)
                    else:
                        with rl.acquire(_DDG_URL):
                            pj_changed = self._pj_find_contact(p)
                    changed = changed or pj_changed

                # ── Step E : APIs externes (Hunter / apilayer) ─────────
                if ENRICH_EXTERNAL_API and not p.email and p.website:
                    ext_email = self._find_email_external_api(p)
                    if ext_email:
                        p.email       = ext_email
                        p.email_valid = True
                        changed       = True
                        logger.debug(
                            f"[Enrich] {progress} Email API ext: {ext_email!r}"
                        )

                # ── ④ Score d'enrichissement ──────────────────────────
                p.enrich_score = self._compute_enrich_score(p)

                if changed:
                    with count_lock:
                        enriched_count += 1
                    logger.info(
                        f"[Enrich] {progress} OK {p.nom_commercial} "
                        f"enrich={p.enrich_score}/4 -- "
                        f"email={p.email!r} | tel={p.telephone!r} | web={p.website!r}"
                    )
                else:
                    logger.debug(
                        f"[Enrich] {progress} {p.nom_commercial} "
                        f"enrich={p.enrich_score}/4 -- aucun contact trouve"
                    )

            except Exception as e:
                logger.debug(f"[Enrich] Erreur '{p.nom_commercial}': {e}")
                try:
                    p.enrich_score = self._compute_enrich_score(p)
                except Exception:
                    pass

        # ── Lancement parallèle ───────────────────────────────────────
        with ThreadPoolExecutor(max_workers=_N_WORKERS) as executor:
            list(executor.map(_enrich_one, enumerate(to_enrich, 1)))

        # Calculer l'enrich_score pour les prospects non traités
        enriched_ids = {id(p) for p in to_enrich}
        for p in prospects:
            if id(p) not in enriched_ids:
                p.enrich_score = self._compute_enrich_score(p)

        # Stats enrich_score distribution
        scores = [p.enrich_score for p in prospects]
        score_dist = {s: scores.count(s) for s in range(5)}
        logger.info(
            f"[Enrich] Resultat : {enriched_count}/{total} prospects enrichis "
            f"| score dist: {score_dist} "
            f"| domaines vus : {rl.stats()}"
        )
        return prospects

    @staticmethod
    def _merge_contact(p: Prospect, web: Prospect) -> bool:
        """
        Copie les champs de contact manquants depuis web -> p.
        Retourne True si au moins un champ de contact (email/tel/website) a été ajouté.
        Rejette les websites qui pointent vers des annuaires tiers.
        """
        import re as _re
        from urllib.parse import urlparse as _urlparse
        from utils.email_validator import validate_email_syntax, validate_email_domain, validate_email_type
        
        _ANNUAIRE_DOMAINS = {
            "infonet.fr", "northdata.com", "lagazettefrance.fr", "societe.com",
            "infogreffe.fr", "pappers.fr", "verif.com", "bodacc.fr",
            "actulegales.fr", "manageo.fr", "societe.ninja", "corporama.com",
            "firmapi.com", "kompass.com", "europages.fr", "annuaire-startups.pro",
            "e-pro.fr", "infobel.com", "cylex.fr", "hoodspot.fr",
            "annuaire-entreprises.data.gouv.fr",
        }

        changed = False
        for attr, new_val in [
            ("email",        web.email),
            ("telephone",    web.telephone),
            ("website",      web.website),
            ("description",  web.description),
            ("adresse",      web.adresse),
            ("linkedin_url", web.linkedin_url),
        ]:
            if not getattr(p, attr) and new_val:
                if attr == "website":
                    domain = _urlparse(new_val).netloc.lower().lstrip("www.")
                    if any(ann in domain for ann in _ANNUAIRE_DOMAINS):
                        continue
                setattr(p, attr, new_val)
                if attr in ("email", "telephone", "website"):
                    changed = True

        # Email validation améliorée
        if p.email:
            p.email_valid = (
                validate_email_syntax(p.email) and
                validate_email_domain(p.email) and
                validate_email_type(p.email)
            )
            logger.debug(f"[MergeContact] Email {p.email}: valid={p.email_valid}")
        
        # Website validation
        if p.website:
            p.website_active = bool(_re.match(r"https?://[^\s]+\.[^\s]{2,}", p.website))
        
        return changed

    def _ddg_find_website(self, p: Prospect) -> str:
        """Cherche le site officiel via DuckDuckGo HTML (sans clé API)."""
        import re as _re
        from urllib.parse import quote_plus, urlparse as _urlparse, unquote as _unquote
        from sources.base_scraper import BaseScraper

        _SKIP = {
            "societe.com", "infogreffe.fr", "pappers.fr", "verif.com",
            "bodacc.fr", "annuaire-entreprises.data.gouv.fr",
            "actulegales.fr", "actu-juridique.fr", "legifrance.gouv.fr",
            "infonet.fr", "northdata.com", "lagazettefrance.fr",
            "societe.ninja", "corporama.com", "firmapi.com", "manageo.fr",
            "kompass.com", "europages.fr", "annuaire.certa.fr",
            "entreprises.gouv.fr", "annuaire-startups.pro",
            "e-pro.fr", "infobel.com", "cylex.fr", "hoodspot.fr",
            "parcourir.com", "118000.fr", "118712.fr", "lespagesjaunes.fr",
            "pagesjaunes.fr", "linkedin.com", "facebook.com", "twitter.com",
            "instagram.com", "youtube.com", "tiktok.com",
            "indeed.fr", "glassdoor.fr", "welcometothejungle.com",
            "monster.fr", "apec.fr", "pole-emploi.fr",
            "wikipedia.org", "leboncoin.fr", "amazon.fr", "google.com",
            "bing.com", "yahoo.com",
        }

        q    = f"{p.nom_commercial.strip()} {p.ville.strip() if p.ville else ''} site officiel".strip()
        html = self.client.get(
            f"https://html.duckduckgo.com/html/?q={quote_plus(q)}",
            headers={"Referer": "https://duckduckgo.com/"},
            timeout=20, retries=2,
        )
        if not html:
            return ""

        for a in BaseScraper.parse_html(html).select(
            "a.result__url, a[class*='result__a'], h2.result__title a"
        ):
            href = a.get("href", "")
            if not href.startswith("http"):
                m = _re.search(r"uddg=(https?[^&]+)", href)
                href = _unquote(m.group(1)) if m else ""
            if not href:
                continue
            domain = _urlparse(href).netloc.lower().lstrip("www.")
            if any(skip in domain for skip in _SKIP):
                continue
            url = BaseScraper.clean_url(href)
            if url:
                return url
        return ""

    def _pj_find_contact(self, p: Prospect) -> bool:
        """
        Fallback contact : si PagesJaunes est bloqué, tente une recherche DDG
        ciblée sur l'email/téléphone de l'entreprise, puis essaie le site
        déjà trouvé avec le chemin /contact directement.
        """
        from urllib.parse import quote_plus, urlparse as _urlparse
        from sources.base_scraper import BaseScraper
        import re as _re

        changed = False

        if p.website and (not p.email or not p.telephone):
            base = p.website.rstrip("/")
            for path in ["/contact", "/nous-contacter", "/contactez-nous", "/a-propos"]:
                try:
                    c_url  = base + path
                    c_html = self.client.get(c_url, timeout=12, retries=1)
                    if not c_html:
                        continue
                    c_text = BaseScraper.extract_text(c_html)
                    if not p.email:
                        emails = BaseScraper.extract_emails(c_text)
                        if emails:
                            p.email   = emails[0]
                            changed   = True
                    if not p.telephone:
                        phones = BaseScraper.extract_phones(c_text)
                        if phones:
                            p.telephone = phones[0]
                            changed     = True
                    if p.email and p.telephone:
                        break
                except Exception:
                    continue

        if not p.email or not p.telephone:
            q    = f"{p.nom_commercial.strip()} contact email téléphone {p.ville or ''}".strip()
            html = self.client.get(
                f"https://html.duckduckgo.com/html/?q={quote_plus(q)}",
                headers={"Referer": "https://duckduckgo.com/"},
                timeout=15, retries=1,
            )
            if html:
                text = BaseScraper.extract_text(html)
                if not p.email:
                    emails = BaseScraper.extract_emails(text)
                    _skip_domains = {
                        "duckduckgo.com", "infonet.fr", "actulegales.fr",
                        "northdata.com", "lagazettefrance.fr", "societe.com",
                        "pappers.fr", "verif.com", "infogreffe.fr",
                        "corporama.com", "manageo.fr", "societe.ninja",
                        "kompass.com", "europages.fr", "linkedin.com",
                        "facebook.com", "twitter.com", "google.com",
                        "gmail.com", "yahoo.fr", "yahoo.com", "hotmail.com",
                        "outlook.com", "orange.fr", "free.fr", "sfr.fr",
                    }
                    company_domain = ""
                    if p.website:
                        from urllib.parse import urlparse as _up
                        try:
                            company_domain = _up(p.website).netloc.lower().lstrip("www.")
                        except Exception:
                            pass

                    for e in emails:
                        e_domain = e.split("@")[-1].lower() if "@" in e else ""
                        if company_domain and e_domain == company_domain:
                            p.email = e
                            changed = True
                            break
                        elif not company_domain and e_domain not in _skip_domains:
                            p.email = e
                            changed = True
                            break

                if not p.telephone:
                    phones = BaseScraper.extract_phones(text)
                    if phones:
                        p.telephone = phones[0]
                        changed     = True

        return changed

    # ──────────────────────────────────────────
    # ② Vérification MX DNS
    # ──────────────────────────────────────────

    @staticmethod
    def _check_mx(domain: str, timeout: float = None) -> bool:
        """
        Vérifie qu'un domaine a des enregistrements MX valides (accepte des emails).
        Retourne True si au moins un MX est trouvé, False sinon.
        Nécessite : pip install dnspython
        Si dnspython n'est pas installé, retourne True (pas de filtre).
        """
        _timeout = timeout or ENRICH_MX_TIMEOUT
        try:
            import dns.resolver
            resolver = dns.resolver.Resolver()
            resolver.lifetime = _timeout
            resolver.timeout  = _timeout
            answers = resolver.resolve(domain, "MX")
            return len(answers) > 0
        except ImportError:
            logger.debug("[Enrich] dnspython absent — MX verify désactivé")
            return True
        except Exception:
            return False

    @staticmethod
    def _check_smtp(email: str, timeout: float = None) -> bool:
        """
        Vérifie qu'une adresse email est probablement valide via handshake SMTP.
        Retourne True si l'adresse est acceptée ou si le test est non concluant.
        Retourne False uniquement si le serveur rejette explicitement (5xx).
        """
        import smtplib
        import socket
        _timeout = timeout or ENRICH_SMTP_TIMEOUT

        if not email or "@" not in email:
            return False

        _, domain = email.rsplit("@", 1)
        domain    = domain.lower().strip()

        try:
            import dns.resolver
            resolver   = dns.resolver.Resolver()
            resolver.lifetime = _timeout
            answers    = resolver.resolve(domain, "MX")
            mx_records = sorted(answers, key=lambda r: r.preference)
            mx_host    = str(mx_records[0].exchange).rstrip(".")
        except ImportError:
            return True
        except Exception:
            return False

        try:
            smtp = smtplib.SMTP(timeout=_timeout)
            smtp.connect(mx_host, 25)
            smtp.ehlo_or_helo_if_needed()
            smtp.mail("probe@scraper-verify.local")
            code, _ = smtp.rcpt(email)
            smtp.quit()
            if code in (250, 251, 252):
                return True
            elif 500 <= code < 600:
                logger.debug(f"[Enrich] SMTP {code} pour {email} — adresse rejetée")
                return False
            else:
                return True
        except smtplib.SMTPConnectError:
            return True
        except smtplib.SMTPRecipientsRefused:
            logger.debug(f"[Enrich] SMTP recipients refused pour {email}")
            return False
        except (socket.timeout, ConnectionRefusedError, OSError):
            return True
        except Exception as e:
            logger.debug(f"[Enrich] SMTP error pour {email}: {e}")
            return True

    # ──────────────────────────────────────────
    # ③ Patterns email + MX verify
    # ──────────────────────────────────────────

    def _find_email_by_patterns(self, p: "Prospect") -> str:
        """
        Génère des emails candidats depuis ENRICH_EMAIL_PATTERNS et valide
        leur existence via DNS MX puis (optionnellement) SMTP RCPT TO.
        """
        from urllib.parse import urlparse
        from sources.base_scraper import BaseScraper

        if not p.website:
            return ""

        try:
            netloc = urlparse(p.website).netloc.lower()
            domain = netloc.lstrip("www.").split(":")[0]
        except Exception:
            return ""

        if not domain or "." not in domain:
            return ""

        # ── Étape 1 : vérification MX ────────────────────────────────
        if ENRICH_MX_VERIFY:
            if not self._check_mx(domain):
                logger.debug(f"[Enrich] Pas de MX pour {domain} — patterns ignorés")
                return ""

        # ── Étape 2 : tester les patterns ────────────────────────────
        for pattern in ENRICH_EMAIL_PATTERNS:
            candidate = pattern.replace("{domain}", domain)

            if not BaseScraper._is_valid_email(candidate):
                continue

            # ── Étape 3 : vérification SMTP (optionnelle) ─────────────
            if ENRICH_SMTP_VERIFY:
                if not self._check_smtp(candidate):
                    logger.debug(f"[Enrich] SMTP reject: {candidate!r} — pattern suivant")
                    continue
                p.email_mx_verified = True
                logger.debug(f"[Enrich] Pattern+SMTP validé: {candidate!r}")
                return candidate
            else:
                if ENRICH_MX_VERIFY:
                    p.email_mx_verified = True
                logger.debug(f"[Enrich] Pattern email retenu (MX only): {candidate!r}")
                return candidate

        return ""

    # ──────────────────────────────────────────
    # APIs externes (Hunter / apilayer) — step E
    # ──────────────────────────────────────────

    def _find_email_external_api(self, p: "Prospect") -> str:
        """
        Dernier recours : cherche l'email via Hunter.io ou apilayer Email Finder.
        N'est appelé que si ENRICH_EXTERNAL_API=True dans settings.py.
        """
        from urllib.parse import urlparse

        if not p.website:
            return ""

        try:
            netloc = urlparse(p.website).netloc.lower()
            domain = netloc.lstrip("www.").split(":")[0]
        except Exception:
            return ""

        # ── Hunter.io ────────────────────────────────────────────────
        if HUNTER_API_KEY:
            try:
                url  = f"https://api.hunter.io/v2/domain-search?domain={domain}&api_key={HUNTER_API_KEY}&limit=1"
                html = self.client.get(url, timeout=10, retries=1)
                if html:
                    import json as _json
                    data = _json.loads(html)
                    emails = data.get("data", {}).get("emails", [])
                    if emails:
                        candidate = emails[0].get("value", "")
                        if candidate:
                            logger.debug(f"[Enrich] Hunter email: {candidate!r}")
                            return candidate.lower()
            except Exception as e:
                logger.debug(f"[Enrich] Hunter API error: {e}")

        # ── apilayer Email Finder ─────────────────────────────────────
        if APILAYER_EMAIL_KEY:
            try:
                url  = f"https://api.apilayer.com/email_finder/search?domain={domain}&company={p.nom_commercial}"
                html = self.client.get(
                    url,
                    headers={"apikey": APILAYER_EMAIL_KEY},
                    timeout=10, retries=1,
                )
                if html:
                    import json as _json
                    data = _json.loads(html)
                    candidate = data.get("email", "")
                    if candidate:
                        logger.debug(f"[Enrich] apilayer email: {candidate!r}")
                        return candidate.lower()
            except Exception as e:
                logger.debug(f"[Enrich] apilayer API error: {e}")

        return ""

    # ──────────────────────────────────────────
    # ④ Score d'enrichissement contact
    # ──────────────────────────────────────────

    @staticmethod
    def _compute_enrich_score(p: "Prospect") -> int:
        """
        Calcule le score de qualité des données de contact (0-4) :
          +1  email présent et syntaxiquement valide
          +1  téléphone présent
          +1  website présent et actif
          +1  linkedin_url présent

        Distinct du qualification_score (critères CRM) — mesure uniquement
        la complétude des informations de contact après enrichissement.
        """
        score = 0
        if p.email and p.email_valid:
            score += 1
        if p.telephone:
            score += 1
        if p.website and p.website_active:
            score += 1
        if p.linkedin_url:
            score += 1
        return score

    # ──────────────────────────────────────────
    # Client HTTP lazy (partagé dans l'orchestrateur)
    # ──────────────────────────────────────────

    @property
    def client(self):
        if not hasattr(self, "_client"):
            from sources.curl_client import CurlClient
            self._client = CurlClient()
        return self._client

    # ──────────────────────────────────────────
    # Affichage récapitulatif
    # ──────────────────────────────────────────

    @staticmethod
    def _print_summary(scored: List[Prospect], stats: Dict, elapsed: float) -> None:
        qualified: List[Prospect] = [p for p in scored if p.qualification == "QUALIFIE"]
        sources_count: Dict[str, int] = {}
        for p in scored:
            sources_count[p.source_origin] = sources_count.get(p.source_origin, 0) + 1

        enrich_dist = {s: sum(1 for p in scored if p.enrich_score == s) for s in range(5)}
        avg_enrich  = round(sum(p.enrich_score for p in scored) / max(len(scored), 1), 2)
        full_enrich = enrich_dist.get(4, 0)

        print("\n" + "=" * 60)
        print(" RÉSULTATS DU SCRAPING")
        print("=" * 60)
        print(f"  Total collectés    : {stats['total']}")
        print(f"  Qualifiés          : {stats['qualified']} ({stats['qualification_rate_pct']}%)")
        print(f"  Non qualifiés      : {stats['non_qualified']}")
        print(f"  Score moyen        : {stats['avg_score']}/100")
        print(f"  Durée              : {elapsed}s")
        print(f"\n ENRICHISSEMENT CONTACTS :")
        print(f"  Score enrich moyen : {avg_enrich}/4")
        print(f"  Totalement enrichis: {full_enrich} (enrich=4/4)")
        print(f"  Distribution       : "
              f"0={enrich_dist[0]}  1={enrich_dist[1]}  "
              f"2={enrich_dist[2]}  3={enrich_dist[3]}  4={enrich_dist[4]}")

        if sources_count:
            print("\n RÉPARTITION PAR SOURCE :")
            for src, cnt in sorted(sources_count.items(), key=lambda x: -x[1]):
                print(f"  {src:<35} : {cnt}")

        if qualified:
            print("\n TOP 5 PROSPECTS :")
            for p in qualified[:5]:
                print(
                    f"  {p.nom_commercial:<30} "
                    f"score={p.qualification_score:>3}/100 "
                    f"enrich={p.enrich_score}/4 "
                    f"| {p.secteur_activite:<20} "
                    f"| {p.ville:<15}"
                    f"| email={p.email or '—':<30}"
                    f"| tel={p.telephone or '—'}"
                )

        pg_status = "activé" if PG_ENABLED else "désactivé"
        print(f"\n PERSISTANCE :")
        print(f"  CSV + JSON         : output/")
        print(f"  PostgreSQL         : {pg_status}")
        print("=" * 60)
        print(" Résultats -> output/")
        print("=" * 60 + "\n")

    @staticmethod
    def _print_timings_table(step_timings: Dict[str, float]) -> None:
        """Affiche une table des timings pour chaque étape du pipeline."""
        if not step_timings:
            return

        total_time = sum(step_timings.values())

        print("\n" + "=" * 80)
        print(" RÉSUMÉ DES TIMINGS")
        print("=" * 80)
        print(f"{'Étape':<25} {'Temps (s)':<15} {'Pourcentage':<20} {'Barre':<20}")
        print("-" * 80)

        for step, elapsed in step_timings.items():
            pct        = (elapsed / total_time) * 100 if total_time > 0 else 0
            bar_length = max(1, int(pct / 5))
            bar        = "" * bar_length + "" * (20 - bar_length)
            print(f"{step:<25} {elapsed:>8.2f}s      {pct:>6.1f}%        [{bar}]")

        print("-" * 80)
        print(f"{'TOTAL':<25} {total_time:>8.2f}s      {'100.0%':>6}")
        print("=" * 80 + "\n")


# ══════════════════════════════════════════════════════════════════════════════
# Alias backward-compat
# ══════════════════════════════════════════════════════════════════════════════

class Orchestrator(ProspectCollector):
    """Alias de compatibilité ascendante."""
    def run(self, target=None, max_enrich: int = 0):
        job = super().run(target, max_enrich=max_enrich)
        return {
            "total":                  job.total_scored,
            "qualified":              job.total_qualified,
            "non_qualified":          job.total_scored - job.total_qualified,
            "qualification_rate_pct": round(100 * job.total_qualified / max(job.total_scored, 1), 1),
            "job_id":                 job.id,
        }


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def main() -> int:
    parser = argparse.ArgumentParser(
        description="SCRAPING_V1 — Système de scraping CRM"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Pipeline complet sans sauvegarde des fichiers",
    )
    parser.add_argument(
        "--source",
        choices=[
            "open_data", "opendata", "directory", "societe", "website",
            "bodacc", "europages", "verif",
        ],
        default=None,
        help="Restreindre la collecte à une seule source ou sous-source",
    )
    parser.add_argument(
        "--target",
        choices=list(EXAMPLE_TARGETS.keys()),
        default=None,
        help="Cible prédéfinie (définie dans config/targets.py)",
    )
    parser.add_argument(
        "--stats-only", action="store_true",
        help="Afficher les stats des résultats existants sans lancer le scraping",
    )
    parser.add_argument(
        "--max-enrich", type=int, default=0,
        help="Nombre max de prospects à enrichir (défaut: valeur COLLECTION_LIMITS)",
    )
    parser.add_argument(
        "--list-sources", action="store_true",
        help="Lister les sources et sous-sources disponibles avec leur statut",
    )
    # ── Arguments bridge FastAPI / Spring (compatibilité AutoProspectionOrchestratorService) ──
    parser.add_argument(
        "--bridge-json-file",
        type=str,
        default=None,
        dest="bridge_json_file",
        help="Chemin vers un fichier JSON contenant le payload Spring (jobId + criteria)",
    )
    parser.add_argument(
        "--json-output",
        action="store_true",
        default=False,
        dest="json_output",
        help="Écrire le résultat final sur stdout en JSON pur (attendu par le bridge FastAPI)",
    )
    args = parser.parse_args()

    # ── Lecture du payload bridge (--bridge-json-file) ─────────────────────────
    # Priorité maximale : écrase les autres args si présent
    _bridge_job_id: Optional[str] = None
    _bridge_payload_cached: Optional[Dict[str, Any]] = None   # FIX: cache pour éviter la double lecture
    if args.bridge_json_file:
        import pathlib
        try:
            _bridge_payload_cached = json.loads(
                pathlib.Path(args.bridge_json_file).read_text(encoding="utf-8")
            )
            _bridge_job_id = _bridge_payload_cached.get("job_id") or _bridge_payload_cached.get("jobId")
            _criteria_raw  = _bridge_payload_cached.get("criteria") or {}

            # Mapper les champs de ProspectionCriteriaDTO vers les args CLI existants
            if _criteria_raw.get("source_filter") and not args.source:
                sf_raw = _criteria_raw["source_filter"]
                if sf_raw == "opendata":
                    sf_raw = "open_data"
                args.source = sf_raw
            if _criteria_raw.get("maxEnrich") and not args.max_enrich:
                args.max_enrich = int(_criteria_raw["maxEnrich"])
            if _criteria_raw.get("dryRun"):
                args.dry_run = bool(_criteria_raw["dryRun"])

            logger.info(f"[Bridge] Payload chargé depuis {args.bridge_json_file} — job_id={_bridge_job_id}")
        except Exception as _bridge_err:
            logger.error(f"[Bridge] Impossible de lire le fichier JSON bridge : {_bridge_err}")

    # ── --list-sources ─────────────────────────
    if args.list_sources:
        print("\nSources et sous-sources disponibles :")
        for parent_name in ProspectCollector._SOURCE_REGISTRY:
            parent_cfg = SOURCES_CONFIG.get(parent_name, {})
            enabled    = parent_cfg.get("enabled", True)
            status     = " actif" if enabled else " désactivé"
            print(f"\n  [{parent_name}]  {status}")
            for key, val in parent_cfg.items():
                if key in _NON_SUBSOURCE_KEYS:
                    continue
                sub_status = " actif" if val else " désactivé"
                print(f"     {key:<20} {sub_status}")
        print(f"\n  [postgresql]   {' activé' if PG_ENABLED else ' désactivé'}")
        print("\nPour activer/désactiver : config/settings.py -> SOURCES_CONFIG / PG_ENABLED")
        return 0

    # ── --stats-only ───────────────────────────
    if args.stats_only:
        repo   = ProspectRepository()
        scored = repo.load_scored()
        if not scored:
            print("Aucun résultat existant. Lancez d'abord le scraping.")
            return 0
        stats = ProspectScorer.get_stats(scored)
        print(json.dumps(stats, indent=2, ensure_ascii=False))

        # Afficher aussi les stats PostgreSQL si activé
        if PG_ENABLED:
            try:
                with PgRepository() as pg:
                    pg_stats = pg.get_stats()
                print("\n[PostgreSQL stats]")
                print(json.dumps(
                    {k: str(v) for k, v in pg_stats.items()},
                    indent=2, ensure_ascii=False,
                ))
            except Exception as e:
                print(f"\n[PostgreSQL] Indisponible : {e}")
        return 0

    def _emit_bridge_error(message: str) -> int:
        if args.json_output:
            print(json.dumps({"success": False, "error": message}, ensure_ascii=False))
        else:
            print(message)
        return 2

    # ── Lancement du pipeline ──────────────────
    sf = args.source
    if sf == "opendata":
        sf = "open_data"

    # Transmettre l'URL Spring au collector pour que SecteurResolver
    # puisse charger les secteurs connus depuis GET /api/crm/secteur-activite/secteurs_resolver.
    _spring_url = os.environ.get("SPRING_BASE_URL", "http://localhost:8080")
    collector = ProspectCollector(
        dry_run         = args.dry_run,
        source_filter   = sf,
        spring_base_url = _spring_url,
    )

    # Source unique des critères : payload backend (--bridge-json-file)
    if not args.bridge_json_file:
        return _emit_bridge_error(
            "Missing --bridge-json-file: backend criteria are mandatory."
        )

    _bridge_criteria_raw: Optional[Dict[str, Any]] = None
    try:
        # FIX: réutilise le payload déjà parsé (évite une double lecture du fichier)
        if _bridge_payload_cached is not None:
            _bp = _bridge_payload_cached
        else:
            import pathlib
            _bp = json.loads(pathlib.Path(args.bridge_json_file).read_text(encoding="utf-8"))
        _raw = _bp.get("criteria")
        if isinstance(_raw, dict):
            _bridge_criteria_raw = _raw
    except Exception as _bridge_read_err:
        return _emit_bridge_error(
            f"Invalid bridge payload file: {_bridge_read_err}"
        )

    if not _bridge_criteria_raw:
        return _emit_bridge_error(
            "Bridge payload must contain a non-empty 'criteria' object."
        )

    # Normaliser les critères Spring (camelCase) vers format interne (snake_case)
    # La fonction centralisée gère tous les fallbacks et variantes
    _bridge_criteria_normalized = normalize_criteria(_bridge_criteria_raw)

    # Construire un SearchTarget depuis les critères normalisés
    from config.targets import SearchTarget as _ST
    try:
        target = _ST(
            secteur_activite     = _bridge_criteria_normalized.get("secteurs_activite", []),
            secteurs_activite_id = _bridge_criteria_normalized.get("secteurs_activite_id", []),
            taille_entreprise    = _bridge_criteria_normalized.get("tailles_entreprise", []),
            types_entreprise     = _bridge_criteria_normalized.get("types_entreprise", []),
            pays                 = _bridge_criteria_normalized.get("pays", ["France"]),
            regions              = _bridge_criteria_normalized.get("regions", []),
            villes               = _bridge_criteria_normalized.get("villes", []),
            keywords             = _bridge_criteria_normalized.get("keywords", []),
            max_resultats        = int(_bridge_criteria_normalized.get("max_resultats", 700)),
            max_par_source       = int(_bridge_criteria_normalized.get("max_par_source", 100)),
        )
        logger.info("[Bridge] Critères normalisés convertis en SearchTarget")
    except Exception as _bt_err:
        return _emit_bridge_error(
            f"Unable to build SearchTarget from bridge criteria: {_bt_err}"
        )

    job = collector.run(target, max_enrich=args.max_enrich, external_job_id=_bridge_job_id)

    # ── Sortie JSON bridge (--json-output) ─────────────────────────────────────
    # Attendu par fastapi_app._parse_bridge_stdout() → Spring AutoProspectionOrchestratorService
    if args.json_output:
        def _to_int_or_none(v: Any) -> Optional[int]:
            try:
                if v is None or str(v).strip() == "":
                    return None
                return int(str(v).strip())
            except (ValueError, TypeError):
                return None

        def _to_iso_or_none(v: Any) -> Optional[str]:
            if v is None:
                return None
            s = str(v).strip()
            return s or None

        def _to_json_text(v: Any) -> str:
            if isinstance(v, str):
                return v
            try:
                return json.dumps(v if v is not None else {}, ensure_ascii=False, default=str)
            except Exception:
                return "{}"

        def _text_list_join(v: Any) -> str:
            if isinstance(v, list):
                return ",".join(str(x).strip() for x in v if str(x).strip())
            if v is None:
                return ""
            return str(v).strip()

        # Charger les prospects qualifiés depuis le repo pour les renvoyer à Spring
        _qualified_prospects: list = []
        try:
            _repo_out = ProspectRepository()
            _all_scored = _repo_out.load_scored()
            expected_job_id_raw = str(_bridge_job_id).strip() if _bridge_job_id else str(job.id)
            expected_job_id_num = _to_int_or_none(expected_job_id_raw)

            _qualified_prospects = []
            for p in _all_scored:
                p_dict = p.__dict__ if hasattr(p, "__dict__") else dict(p)
                # Support both old "statut" field and new "qualification" field
                statut = str(p_dict.get("qualification", p_dict.get("statut", ""))).upper()
                if statut != "QUALIFIE":
                    continue

                existing_job_id = str(p_dict.get("job_id", "")).strip()
                if _bridge_job_id and existing_job_id and existing_job_id != expected_job_id_raw:
                    continue

                prospect_out = {
                    # Aligné avec AutoProspectionEntity
                    "id": None,
                    "hash_dedup": p_dict.get("hash_dedup"),
                    "job_id": expected_job_id_num if expected_job_id_num is not None else expected_job_id_raw,
                    "nom_commercial": p_dict.get("nom_commercial", ""),
                    "raison_sociale": p_dict.get("raison_sociale", ""),
                    "email": p_dict.get("email", ""),
                    "telephone": p_dict.get("telephone", ""),
                    "website": p_dict.get("website", ""),
                    "linkedin_url": p_dict.get("linkedin_url", ""),
                    "adresse": p_dict.get("adresse", ""),
                    "ville": p_dict.get("ville", ""),
                    "region": p_dict.get("region", ""),
                    "pays": p_dict.get("pays", ""),
                    "code_postal": p_dict.get("code_postal", ""),
                    "secteur_activite":    p_dict.get("secteur_activite", ""),
                    # secteur_activite_scraped : valeur brute du scraper (avant résolution FK)
                    "secteur_activite_scraped": p_dict.get("secteur_activite_scraped", ""),
                    # secteur_activite_id : FK résolue par SecteurResolver (step 4B)
                    # None si le secteur scraped ne correspond à aucun secteur connu
                    # et qu'aucun secteur n'a été fourni dans les critères du job.
                    "secteur_activite_id": p_dict.get("secteur_activite_id"),
                    "type_entreprise":     p_dict.get("type_entreprise", ""),
                    "taille_entreprise": p_dict.get("taille_entreprise", ""),
                    "nombre_employes": p_dict.get("nombre_employes"),
                    "chiffre_affaires": p_dict.get("chiffre_affaires"),
                    "description": p_dict.get("description", ""),
                    "code_naf": p_dict.get("code_naf", ""),
                    "siren": p_dict.get("siren", ""),
                    "siret": p_dict.get("siret", ""),
                    "qualification_score": int(p_dict.get("qualification_score", 0) or 0),
                    "score_pct": p_dict.get("score_pct", 0),
                    # "qualification" = colonne DB Spring (QUALIFIE / NON_QUALIFIE)
                    "qualification": p_dict.get("qualification", p_dict.get("statut", "NON_QUALIFIE")),
                    "score_detail": _to_json_text(p_dict.get("score_detail", {})),
                    "enrich_score": int(p_dict.get("enrich_score", 0) or 0),
                    "criteria_met": int(p_dict.get("criteria_met", 0) or 0),
                    "criteria_total": int(p_dict.get("criteria_total", 0) or 0),
                    "email_valid": bool(p_dict.get("email_valid", False)),
                    "website_active": bool(p_dict.get("website_active", False)),
                    "email_mx_verified": bool(p_dict.get("email_mx_verified", False)),
                    # source = valeur fixe discriminant CRM
                    "source": "AUTOPROSPECTION",
                    # source_origin = source de données réelle (anciennement "source")
                    "source_origin": p_dict.get("source_origin", p_dict.get("source", "")),
                    # sector_confidence : qualité du mapping FK (SecteurResolver)
                    "sector_confidence": p_dict.get("sector_confidence", 0),
                    # nlp_confidence : qualité de la détection NLP du label textuel (distinct de sector_confidence)
                    "nlp_confidence": p_dict.get("nlp_confidence", 0),
                    "created_at": _to_iso_or_none(p_dict.get("created_at")),
                    "updated_at": _to_iso_or_none(p_dict.get("updated_at") or p_dict.get("created_at")),
                    "is_deleted": bool(p_dict.get("is_deleted", False)),
                    "deleted_token": p_dict.get("deleted_token"),
                }
                _qualified_prospects.append(prospect_out)
        except Exception as _repo_err:
            logger.warning(f"[Bridge] Impossible de charger les prospects qualifiés : {_repo_err}")

        _bridge_result = {
            "success": job.status == JobStatut.DONE.value,
            "canceled": False,
            "job": {
                # Aligné avec CollectionJobEntity (payload API, non entité persistée directe)
                "id": _to_int_or_none(_bridge_job_id) if _bridge_job_id else _to_int_or_none(job.id),
                "external_job_id": str(job.id),
                "name": job.name,
                "type": "SCRAPING",
                "status": job.status,
                "parameters_json": job.parameters_json,
                "started_at": _to_iso_or_none(job.started_at),
                "finished_at": _to_iso_or_none(job.finished_at),
                "created_at": _to_iso_or_none(job.started_at),
                "is_deleted": False,
                "deleted_token": None,
                "total_collected": int(job.total_collected or 0),
                "total_cleaned": int(job.total_cleaned or 0),
                "total_deduped": int(job.total_deduped or 0),
                "total_scored": int(job.total_scored or 0),
                "total_saved": int(job.total_saved or 0),
                "total_qualified": int(job.total_qualified or 0),
                "total_duplicates": int(job.total_duplicates or 0),
                "sources_used": job.sources_used if isinstance(job.sources_used, list) else [],
                "errors": job.errors if isinstance(job.errors, list) else [],
                "crit_secteurs_activite_txt": _text_list_join(_bridge_criteria_normalized.get("secteurs_activite", [])),
                "crit_types_entreprise_txt": _text_list_join(_bridge_criteria_normalized.get("types_entreprise", [])),
                "crit_tailles_entreprise_txt": _text_list_join(_bridge_criteria_normalized.get("tailles_entreprise", [])),
                "crit_pays_txt": _text_list_join(_bridge_criteria_normalized.get("pays", [])),
                "crit_regions_txt": _text_list_join(_bridge_criteria_normalized.get("regions", [])),
                "crit_villes_txt": _text_list_join(_bridge_criteria_normalized.get("villes", [])),
                "crit_keywords_txt": _text_list_join(_bridge_criteria_normalized.get("keywords", [])),
                "crit_max_resultats": _to_int_or_none(_bridge_criteria_normalized.get("max_resultats")),
            },
            "qualified_prospects": _qualified_prospects,
        }
        # Écrire sur stdout BRUT — bypass le wrapper UTF-8 (_stdout_utf8) du logger
        # pour que _parse_bridge_stdout() de fastapi_app lise une ligne JSON pure
        _json_bytes = (json.dumps(_bridge_result, ensure_ascii=False, default=str) + "\n").encode("utf-8")
        sys.stdout.buffer.write(_json_bytes)
        sys.stdout.buffer.flush()

    if job.status == JobStatut.DONE.value:
        return 0
    if job.status == JobStatut.CANCELED.value:
        return 3
    return 1

if __name__ == "__main__":
    raise SystemExit(main())