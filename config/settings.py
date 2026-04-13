"""
SCRAPING_V1 - Global Settings
Configuration technique centrale du système de scraping CRM.

Responsabilités de ce fichier :
  - Paramètres HTTP / curl / rate-limiting
  - Limites de collecte par source
  - Règles de scoring et seuil de qualification
  - Activation/désactivation des sources et sous-sources
  - Filtres techniques (exclusions, déduplication, NAF...)
  - URLs des APIs publiques  ← SOURCE UNIQUE pour toutes les URLs
  - Constantes de pagination par source  ← CENTRALISÉ ICI (était dispersé)
  - Clés API (lues depuis .env — jamais codées en dur)
  - NLP / embeddings / logging


CORRECTIONS v2 :
  - URLs dupliquées supprimées des scrapers (VERIF_BASE_URL, EUROPAGES_BASE_URL,
    PAPPERS_URL redéfinies localement dans les scrapers -> supprimées là-bas)
  - Ajout PAPPERS_SEARCH_URL, SOCIETE_SEARCH_URL, SOCIETE_FICHE_URL
  - Ajout GEO_COMMUNES_URL, GEO_REGIONS_URL (étaient codées en dur dans open_data_scraper)
  - Centralisation des constantes de pagination par source (SIRENE_*, ANNUAIRE_*, PAPPERS_*, BODACC_*)
  - Valeurs par défaut des sous-sources corrigées : False au lieu de True
    pour les sources désactivées dans SOURCES_CONFIG (évitait activation silencieuse)
"""
from pathlib import Path
from dotenv import load_dotenv
import os

# ─────────────────────────────────────────────
# CHARGEMENT DU .env
# ─────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

# ─────────────────────────────────────────────
# PATHS
# ─────────────────────────────────────────────
OUTPUT_DIR      = BASE_DIR / "output"
LOGS_DIR        = BASE_DIR / "logs"
SEARCH_CONFIG   = BASE_DIR / "config" / "search_config.json"   # fichier utilisateur

OUTPUT_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# ─────────────────────────────────────────────
# OUTPUT FILES
# ─────────────────────────────────────────────
OUTPUT_JSON    = OUTPUT_DIR / "prospects.json"
OUTPUT_CSV     = OUTPUT_DIR / "prospects.csv"
SCORED_JSON    = OUTPUT_DIR / "prospects_scored.json"
SCORED_CSV     = OUTPUT_DIR / "prospects_scored.csv"
QUALIFIED_JSON = OUTPUT_DIR / "qualified_only.json"
QUALIFIED_CSV  = OUTPUT_DIR / "qualified_only.csv"
JOBS_JSON      = OUTPUT_DIR / "jobs.json"

# Aliases for backward compatibility
PROSPECTS_CSV         = OUTPUT_CSV
PROSPECTS_JSON        = OUTPUT_JSON
PROSPECTS_SCORED_CSV  = SCORED_CSV
PROSPECTS_SCORED_JSON = SCORED_JSON

# ─────────────────────────────────────────────
# CURL / HTTP : Curl permet de telecharger des pages web plus rapidement et de manière plus robuste que requests.
# ─────────────────────────────────────────────
USE_CURL = True
#User agents permettant de simuler différents navigateurs et éviter les blocages anti-scraping basés sur l'UA :immiter différents navigateurs populaires pour réduire les risques de blocage basés sur l'User-Agent.
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
]

CURL_OPTIONS = {
    "connect_timeout":  30,     # max time to establish connection
    "max_time":         50,     # temps maximum total en secondes
    "retry":            3,      # nombre de tentatives en cas d'échec
    "retry_delay":      2,      # délai entre les tentatives en secondes
    "follow_redirects": True,   # suivre les redirections HTTP (3xx)
    "max_redirects":    5,      # nombre maximum de redirections à suivre
    "compressed":       True,   # accepter les réponses compressées (gzip, deflate)
    "insecure":         False,  # ne pas vérifier les certificats SSL (risqué, à utiliser avec précaution)
    "http1_1":         True,    # utiliser HTTP/1.1 (plus compatible que HTTP/2 pour certains sites)
}

# ─────────────────────────────────────────────
# RATE LIMITING GLOBAL : lors du collecte des données 
# Délais appliqués par le client HTTP (curl_client) sur l'ensemble des sources.
# ─────────────────────────────────────────────
MIN_DELAY_BETWEEN_REQUESTS          = 1.5   # minimum secondes entre requêtes sur un même domaine
MAX_DELAY_BETWEEN_REQUESTS          = 4.0   # maximum secondes entre requêtes (randomisé pour éviter les patterns)
MAX_REQUESTS_PER_DOMAIN_PER_SESSION = 10    # nb max de requêtes sur un même domaine avant pause longue
SCRAPE_INTERVAL_DAYS                = 7     # délai minimum entre deux scrapes du même url

# ─────────────────────────────────────────────
# RATE LIMITING PAR DOMAINE — Enrichissement contacts (Step 5)
#
# Utilisé par DomainRateLimiter dans orchestrator.py.
# Chaque domaine est rate-limité indépendamment : les threads qui ciblent
# des domaines différents s'exécutent en parallèle sans se bloquer ;
# ceux qui ciblent le même domaine sont sérialisés + espacés.
#
# ENRICH_DEFAULT_DELAY     : délai minimum (s) entre deux requêtes vers un
#                            domaine non listé dans ENRICH_DOMAIN_DELAYS.
# ENRICH_MAX_CONCURRENT    : nb max de requêtes simultanées vers le MÊME
#                            domaine. 1 = sérialisation totale (recommandé).
# ENRICH_JITTER_FACTOR     : fraction du délai ajoutée/soustraite aléatoirement
#                            pour éviter que les threads se synchronisent.
#                            Ex: 0.3 = délai +/-30 % de variation aléatoire.
# ENRICH_DOMAIN_DELAYS     : overrides par domaine (priorité sur DEFAULT_DELAY).
#                            Clé = domaine nu (sans www. ni https://).
#                            Valeur = délai minimum en secondes.
# ─────────────────────────────────────────────
ENRICH_DEFAULT_DELAY  = 2.0   # délai par défaut entre requêtes (domaines non listés)
ENRICH_MAX_CONCURRENT = 1     # requêtes simultanées max par domaine (1 = sérialisé)
ENRICH_JITTER_FACTOR  = 0.3   # variation aléatoire du délai (+/- 30 %)
ENRICH_N_WORKERS      = 8     # threads simultanés max pour l'enrichissement

# ─────────────────────────────────────────────
# ENRICHISSEMENT — Vérification MX, patterns email, priorisation, APIs externes
#
# ENRICH_MX_VERIFY         : active la vérification DNS MX sur les emails générés
#                            par patterns (nécessite : pip install dnspython)
# ENRICH_MX_TIMEOUT        : timeout (s) pour la résolution DNS MX
# ENRICH_EMAIL_PATTERNS    : patterns testés quand le scraping ne trouve pas d'email.
#                            {domain} est remplacé par le domaine de p.website.
#                            Ordre = priorité. Vérification MX filtre les invalides.
# ENRICH_EXTERNAL_API      : active Hunter.io / apilayer en dernier recours.
#                            Désactivé par défaut (quota très limité sur free tier).
# HUNTER_API_KEY           : clé Hunter.io — lue depuis .env (25 req/mois gratuit)
# APILAYER_EMAIL_KEY       : clé apilayer Email Finder — lue depuis .env (100 req/mois)
# ENRICH_MIN_SCORE_PRIORITY: score qualification minimum pour intégrer la file
#                            d'enrichissement. 0 = pas de filtre (tous les prospects).
#                            Ex: 30 = seuls les prospects avec score >= 30 sont enrichis.
# ─────────────────────────────────────────────
ENRICH_MX_VERIFY          = True
ENRICH_MX_TIMEOUT         = 3.0   # secondes
# ENRICH_SMTP_VERIFY : active la vérification SMTP RCPT TO après MX.
# Envoie une commande RCPT TO sans envoyer de message (SMTP handshake only).
# Beaucoup plus précis que MX seul, mais plus lent (~2-5s par adresse)
# et bloqué par certains serveurs qui refusent les VRFY/RCPT anonymes.
# Recommandation : True en production, False en tests rapides.
ENRICH_SMTP_VERIFY        = True
ENRICH_SMTP_TIMEOUT       = 5.0   # secondes par tentative SMTP
ENRICH_EMAIL_PATTERNS: list = [
    # Patterns génériques haute probabilité (ordre = priorité)
    "contact@{domain}",
    "info@{domain}",
    "bonjour@{domain}",
    "hello@{domain}",
    # Fonctions commerciales
    "commercial@{domain}",
    "vente@{domain}",
    "sales@{domain}",
    # Direction
    "direction@{domain}",
    "pdg@{domain}",
    "dg@{domain}",
    # Support / IT (contexte informatique)
    "support@{domain}",
    "dev@{domain}",
    "tech@{domain}",
    # Administration
    "admin@{domain}",
    "rh@{domain}",
    "recrutement@{domain}",
    # Fallbacks très courants pour TPE/PME
    "bureau@{domain}",
    "agence@{domain}",
    "secretariat@{domain}",
]
ENRICH_EXTERNAL_API       = False   # désactivé par défaut (quota limité)
HUNTER_API_KEY            = os.getenv("HUNTER_API_KEY", "")
APILAYER_EMAIL_KEY        = os.getenv("APILAYER_EMAIL_KEY", "")
ENRICH_MIN_SCORE_PRIORITY = 0       # 0 = tous, >0 = filtrage par score avant enrichissement

ENRICH_DOMAIN_DELAYS: dict = {
    # ── Moteurs de recherche ──────────────────────────────────────────
    "html.duckduckgo.com": 3.0,   # DDG HTML — utilisé pour trouver les sites
    "duckduckgo.com":      3.0,

    # ── Annuaires français à fort anti-scraping ───────────────────────
    "pagesjaunes.fr":      4.0,
    "societe.com":         5.0,
    "pappers.fr":          3.0,
    "verif.com":           4.0,
    "infogreffe.fr":       4.0,
    # FIX : l'ancienne clé "bodacc.fr" ne correspondait pas au domaine réel de
    # l'API BODACC (bodacc-datadila.opendatasoft.com) et n'était donc jamais
    # appliquée par DomainRateLimiter.  "bodacc.fr" est conservé en commentaire
    # pour mémoire — il redirige vers le portail public, pas l'API.
    "bodacc-datadila.opendatasoft.com": 2.5,   # API BODACC — annonces commerciales
    # "bodacc.fr": 2.5,  # portail public uniquement, pas l'API

    # ── Annuaires B2B européens ───────────────────────────────────────
    "kompass.com":         3.0,
    "europages.fr":        3.0,

    # ── Données légales / agrégateurs ────────────────────────────────
    "infonet.fr":          3.0,
    "societe.ninja":       3.0,
    "corporama.com":       3.5,
    "manageo.fr":          3.0,
    "northdata.com":       3.0,

    # ── APIs publiques (quota journalier) ─────────────────────────────
    # Sirene, data.gouv.fr, Pappers ont leur propre rate-limit dans
    # COLLECTION_LIMITS (sirene_sleep, pappers_sleep, annuaire_sleep).
    # Listés ici en backup si jamais WebsiteScraper les contacte.
    "api.pappers.fr":                        1.5,
    "recherche-entreprises.api.gouv.fr":     0.5,
    "annuaire-entreprises.data.gouv.fr":     0.5,
}

# Proxies : liste optionnelle lue depuis .env (format CSV) : changer l'@IP et le port pour utiliser un proxy et masquer votre adresse IP réelle.
USE_PROXIES = False
_proxy_env  = os.getenv("PROXY_LIST", "")
PROXY_LIST  = [p.strip() for p in _proxy_env.split(",") if p.strip()]

# ─────────────────────────────────────────────
# LIMITES DE COLLECTE
# Contrôle fin du volume par source et sous-source.
# max_prospects_total est défini par l'utilisateur dans search_config.json.
# ─────────────────────────────────────────────
COLLECTION_LIMITS = {
    "max_prospects_per_source":  300,   # limite par source principale (open_data, directory, societe)
    "max_pages_per_search":      20,    # max page parcourues par recherche dans les sources paginées (ex: Sirene, Pappers)
    "max_enrich_per_run":        200,   #max prospects enrichis via WebsiteScraper par run

    # Limites fines par sous-source
    "europages_max_par_run":     100,
    "europages_max_pages":       3,
    "verif_max_par_run":         100,
    "verif_max_detail_pages":    15,
    "open_corporates_max_par_run": 100,
    "osm_max_par_run":           100,

    # ── Constantes de pagination Sirene / data.gouv.fr ──────────────────
    # FIX : étaient codées en dur comme attributs de classe dans OpenDataScraper
    # SOURCE UNIQUE ici — OpenDataScraper les lit via settings.COLLECTION_LIMITS
    "sirene_max_naf":     8,     # Nombre max de codes NAF itérés par run Sirene
    "sirene_max_pages":   4,     # Pages max par appel API Sirene (25 résultats/page = 100 max)
    "sirene_per_page":    25,    # Max autorisé par l'API Sirene
    "sirene_sleep":       0.4,   # Délai entre requêtes Sirene (API publique)
    "sirene_geo_chunk":   20,    # Max codes postaux par requête Sirene

    # ── Constantes de pagination annuaire-entreprises.data.gouv.fr ──────
    # FIX : étaient codées en dur dans DirectoryScraper — même API que Sirene
    "annuaire_max_naf":   8,
    "annuaire_max_pages": 4,
    "annuaire_per_page":  25,
    "annuaire_sleep":     0.4,
    "annuaire_geo_chunk": 20,

    # ── Constantes de pagination Pappers API ─────────────────────────────
    # FIX : étaient codées en dur dans SocieteScraper
    "pappers_max_naf":    8,     # codes NAF itérés max
    "pappers_max_pages":  5,     # pages max par appel (20 résultats/page = 100 max)
    "pappers_per_page":   20,    # max autorisé par l'API gratuite Pappers
    "pappers_sleep":      0.6,   # délai entre requêtes (quota 1000 req/jour)
    "pappers_max_enrich": 30,    # prospects enrichis via societe.com par run Pappers

    # ── Constantes BODACC ────────────────────────────────────────────────
    "bodacc_max_par_run": 100,
    "bodacc_limit":       100,
    "bodacc_max_pages":   3,
}

# ─────────────────────────────────────────────
# SOURCES — activation/désactivation
# Source de vérité unique : modifier ici pour activer/désactiver une source.
#
# FIX : les valeurs par défaut dans les scrapers étaient True pour toutes les
# sous-sources. Si SOURCES_CONFIG n'était pas correctement chargé, des sources
# désactivées ici se déclenchaient silencieusement (ex: opencorporates=False
# ignoré car .get("opencorporates", True) retournait True).
# Les scrapers doivent utiliser .get("cle", False) pour les sous-sources.
# ─────────────────────────────────────────────
SOURCES_CONFIG = {
    "open_data": {
        "enabled": True,
        # Sous-sources actives dans OpenDataScraper
        "sirene":          True,    # data.gouv.fr — API SIRENE officielle
        "opencorporates":  False,   # OpenCorporates — données légales internationales
        "osm":             True,    # OpenStreetMap / Overpass — données géospatiales
        "bodacc":          True,    # BODACC — immatriculations récentes (leads chauds)
    },
    "directory": {
        "enabled": True,
        # Sous-sources actives dans DirectoryScraper
        "pagesjaunes":     False,
        "data_gouv_dir":   True,    # annuaire-entreprises.data.gouv.fr
        "kompass":         False,
        "europages":       False,
        "verif":           False,
    },
    "societe": {
        "enabled": True,
        # Sous-sources actives dans SocieteScraper
        "pappers":         False,    # Pappers API (1000 req/jour gratuit)
        "societe_com":     True,    # societe.com — enrichissement fallback : c'est la source la plus riche en contacts (email/tel) mais elle est lente et bloque souvent, à utiliser avec modération pour enrichir les prospects les plus prometteurs.
    },
    "website": {
        "enabled":            True,  # Enrichissement contacts (step 5 pipeline)
        "max_pages_per_site": 5,
    },
}

# ─────────────────────────────────────────────
# FILTRES TECHNIQUES
# ─────────────────────────────────────────────
FILTERS = {
    "exclure_entreprises_fermees": True,   # ignorer dissolution/radiation/liquidation
    "exclure_doublons":            True,   # déduplication active
    "exclure_sans_contact":        False,  # garder les prospects sans email/tel
}

# ─────────────────────────────────────────────
# SCORING & QUALIFICATION
# ─────────────────────────────────────────────
QUALIFICATION_THRESHOLD = 50    # score minimum pour être QUALIFIE (sur 100)
MAX_SCORE               = 100

SCORING_RULES = {
    "sector_match":   20,   # secteur correspond au ciblage
    "size_match":     15,   # taille entreprise correspond
    "type_match":     15,   # forme juridique correspond
    "geo_match":      10,   # zone géographique correspond
    "email_valid":    10,   # email valide présent
    "website_active": 10,   # site web présent
    "phone_present":  10,   # téléphone présent
    "name_complete":   5,   # nom commercial ≥ 3 caractères
    "raison_sociale":  5,   # raison sociale présente
}

# ─────────────────────────────────────────────
# NLP / EMBEDDINGS
# ─────────────────────────────────────────────
SBERT_MODEL    = "paraphrase-multilingual-MiniLM-L12-v2"
USE_EMBEDDINGS = True   # False = mode keyword uniquement (plus rapide, moins précis)

# ─────────────────────────────────────────────
# DÉDUPLICATION : website et email et nom_commercial sont les clés de déduplication principales.
# Les champs email, nom_commercial et téléphone sont utilisés pour calculer un hash de déduplication
# Un seuil de similarité sur le nom+ville est appliqué pour détecter les doublons proches (fuzzy deduplication).
# ─────────────────────────────────────────────
DEDUP_KEYS        = ["website", "email", "nom_commercial"]
DEDUP_HASH_FIELDS = ["email", "nom_commercial", "telephone"]
DEDUP_SIMILARITY_THRESHOLD = 0.85   # seuil similarité nom+ville pour fuzzy dedup

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
LOG_LEVEL     = "INFO"
LOG_FILE      = LOGS_DIR / "scraping.log"
LOG_ROTATION  = "10 MB"
LOG_RETENTION = "30 days"

# ─────────────────────────────────────────────
# OPEN DATA APIs — URLs publiques (pas de secret)
# SOURCE UNIQUE pour toutes les URLs — ne pas redéfinir dans les scrapers.
#
# FIX : les URLs suivantes étaient dupliquées dans les scrapers :
#   - VERIF_BASE_URL   : redéfinie dans directory_scraper.py (ignorait settings)
#   - EUROPAGES_BASE_URL : redéfinie dans directory_scraper.py sous le nom EUROPAGES_BASE
#   - PAPPERS_URL      : redéfinie dans societe_scraper.py sous PAPPERS_SEARCH_URL
#   - SOCIETE_SEARCH_URL / SOCIETE_FICHE_URL : absentes de settings, codées en dur
#   - GEO_COMMUNES_URL / GEO_REGIONS_URL : absentes de settings, codées en dur
# ─────────────────────────────────────────────

# Sirene / annuaire-entreprises
NOMINATIM_BASE_URL = "https://nominatim.openstreetmap.org"
OVERPASS_BASE_URL  = "https://overpass-api.de/api/interpreter"
DATA_GOUV_URL      = "https://recherche-entreprises.api.gouv.fr/search"
OPENCORPORATES_URL = "https://api.opencorporates.com/v0.4/companies/search"

# Pappers
PAPPERS_URL        = "https://api.pappers.fr/v2/recherche"   # alias historique
PAPPERS_SEARCH_URL = "https://api.pappers.fr/v2/recherche"   # FIX : ajouté pour societe_scraper

# Societe.com
SOCIETE_SEARCH_URL = "https://www.societe.com/cgi-bin/search"   # FIX : était codée en dur
SOCIETE_FICHE_URL  = "https://www.societe.com/societe"           # FIX : était codée en dur

# BODACC — Bulletin Officiel Des Annonces Civiles Et Commerciales
BODACC_API_URL = "https://bodacc-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/annonces-commerciales/records"

# Europages — annuaire B2B européen
EUROPAGES_BASE_URL = "https://www.europages.fr"   # FIX : était redéfini localement dans directory_scraper

# Verif.com — données légales françaises
VERIF_BASE_URL    = "https://www.verif.com"                  # FIX : était redéfini localement dans directory_scraper
VERIF_SEARCH_URL  = "https://www.verif.com/cgi-bin/search"   # FIX : était codée en dur

# Geo API gouvernement — résolution communes et régions
GEO_COMMUNES_URL = "https://geo.api.gouv.fr/communes"   # FIX : était codée en dur dans open_data_scraper
GEO_REGIONS_URL  = "https://geo.api.gouv.fr/regions"    # FIX : était codée en dur dans open_data_scraper

# ─────────────────────────────────────────────
# API KEYS — chargées exclusivement depuis .env
# ─────────────────────────────────────────────
PAPPERS_API_KEY        = os.getenv("PAPPERS_API_KEY", "")
OPENCORPORATES_API_KEY = os.getenv("OPENCORPORATES_API_KEY", "")
NOMINATIM_USER_AGENT   = os.getenv("NOMINATIM_USER_AGENT", "scraping_v1/1.0")


def _warn_missing_keys() -> None:
    """Avertit en dev si une clé API obligatoire est absente du .env."""
    required = {
        "PAPPERS_API_KEY":        PAPPERS_API_KEY,
        "OPENCORPORATES_API_KEY": OPENCORPORATES_API_KEY,
    }
    missing = [name for name, val in required.items() if not val]
    if missing:
        import warnings
        warnings.warn(
            f"[settings] Clés API manquantes dans .env : {', '.join(missing)}. "
            "Les sources correspondantes seront désactivées.",
            stacklevel=2,
        )

_warn_missing_keys()


# ─────────────────────────────────────────────
# HELPER FUNCTION : Get sources config with thread isolation support
# ─────────────────────────────────────────────

def get_sources_config(criteria: dict = None) -> dict:
    """
    Retourne SOURCES_CONFIG avec support d'isolation par thread.
    
    Priorité :
      1. criteria.get("_isolated_sources_config") — config isolée passée par le thread
      2. SOURCES_CONFIG global — config par défaut
    
    Utilisation normale (pas de parallélisation) :
      config = get_sources_config()
    
    Utilisation en parallélisation (chaque thread a sa propre config) :
      config = get_sources_config(criteria)  # criteria contient _isolated_sources_config
    """
    if criteria and "_isolated_sources_config" in criteria:
        return criteria["_isolated_sources_config"]
    return SOURCES_CONFIG

# ══════════════════════════════════════════════════════════════════════════════
# PostgreSQL — Persistance (NOUVEAU)
# ══════════════════════════════════════════════════════════════════════════════
#
# Activation :
#   Option A (recommandé production) : variables d'environnement
#     export PG_ENABLED=true
#     export DATABASE_URL="postgresql://user:password@host:5432/scraping_v1"
#
#   Option B : modifier directement les valeurs ci-dessous
#     PG_ENABLED = True
#     PG_DSN     = "postgresql://postgres:monmotdepasse@localhost:5432/scraping_v1"
#
# Notes :
#   - La sauvegarde CSV/JSON est TOUJOURS active (comportement inchangé).
#   - PostgreSQL est additif : une erreur PG ne bloque pas le pipeline.
#   - Installer le driver : pip install psycopg2-binary
# ─────────────────────────────────────────────────────────────────────────────

# Active/désactive la persistance PostgreSQL.
# False = comportement original (CSV + JSON uniquement).
PG_ENABLED: bool = os.getenv("PG_ENABLED", "true").lower() in ("true", "1", "yes")

# DSN complet (prioritaire sur les composants individuels ci-dessous).
# Format : postgresql://user:password@host:port/dbname
PG_DSN: str = os.getenv("DATABASE_URL", "postgresql://postgres:nouha@localhost:5433/CRM")

# Composants DSN (utilisés uniquement si DATABASE_URL est absent).
_PG_HOST:     str = os.getenv("PG_HOST",     "localhost")
_PG_PORT:     str = os.getenv("PG_PORT",     "5433")
_PG_DB:       str = os.getenv("PG_DB",       "CRM")
_PG_USER:     str = os.getenv("PG_USER",     "postgres")
_PG_PASSWORD: str = os.getenv("PG_PASSWORD", "nouha")

if not PG_DSN:
    if _PG_PASSWORD:
        PG_DSN = f"postgresql://{_PG_USER}:{_PG_PASSWORD}@{_PG_HOST}:{_PG_PORT}/{_PG_DB}"
    else:
        PG_DSN = f"postgresql://{_PG_USER}@{_PG_HOST}:{_PG_PORT}/{_PG_DB}"

# Schéma PostgreSQL cible (laisser "public" sauf besoin multi-tenant).
PG_SCHEMA: str = os.getenv("PG_SCHEMA", "public")

# Taille des batches pour execute_batch (upsert PostgreSQL).
PG_BATCH_SIZE: int = int(os.getenv("PG_BATCH_SIZE", "100"))

# Timeout de connexion PostgreSQL (secondes).
PG_CONNECT_TIMEOUT: int = int(os.getenv("PG_CONNECT_TIMEOUT", "10"))