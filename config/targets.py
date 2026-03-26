"""
SCRAPING_V1 - Targets Configuration
Contient :
  - SearchTarget : dataclass des critères utilisateur (chargé depuis search_config.json)
  - TARGETING_CRITERIA : valeurs par défaut si search_config.json absent
  - EXAMPLE_TARGETS : cibles prédéfinies pour le CLI (--target)
  - SECTOR_KEYWORDS / SIZE_RANGES : référentiels NLP

SOURCES_CONFIG et COLLECTION_LIMITS ont été déplacés dans config/settings.py.
"""
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ─────────────────────────────────────────────
# LOADER search_config.json
# Charge les critères utilisateur depuis l'interface CRM.
# Si le fichier est absent, TARGETING_CRITERIA fait office de fallback.
# ─────────────────────────────────────────────
import json as _json
from pathlib import Path as _Path

def load_search_config() -> dict:
    """
    Charge search_config.json depuis config/.
    Retourne un dict vide si le fichier est absent ou invalide.
    """
    cfg_path = _Path(__file__).resolve().parent / "search_config.json"
    if not cfg_path.exists():
        return {}
    try:
        with open(cfg_path, encoding="utf-8") as _f:
            data = _json.load(_f)
        # Ignorer les clés de métadonnées
        return {k: v for k, v in data.items() if not k.startswith("_")}
    except Exception as _e:
        import warnings
        warnings.warn(f"[targets] Impossible de lire search_config.json : {_e}")
        return {}


from dataclasses import dataclass, field
from typing import List, Dict, Optional


# ─────────────────────────────────────────────
# SearchTarget  — dataclass paramétrable
# ─────────────────────────────────────────────

@dataclass
class SearchTarget:
    """
    Encapsule les critères de recherche d'une campagne de prospection.
    Peut être instancié depuis le code ou depuis search_config.json.
    """
    secteur_activite:   List[str] = field(default_factory=list)
    taille_entreprise:  List[str] = field(default_factory=list)
    types_entreprise:   List[str] = field(default_factory=list)
    keywords:           List[str] = field(default_factory=list)

    pays:     List[str] = field(default_factory=lambda: ["France"])
    regions:  List[str] = field(default_factory=list)
    villes:   List[str] = field(default_factory=list)

    max_resultats:        int = 200
    max_par_source:       int = 50
    max_pages_annuaire:   int = 5

    sources: List[str] = field(default_factory=lambda: ["api", "scraping"])

    def to_query_string(self) -> str:
        parts = []
        if self.secteur_activite:
            parts.append(f"secteurs={','.join(self.secteur_activite)}")
        if self.pays:
            parts.append(f"pays={','.join(self.pays)}")
        if self.villes:
            parts.append(f"villes={','.join(self.villes)}")
        if self.taille_entreprise:
            parts.append(f"tailles={','.join(self.taille_entreprise)}")
        return " | ".join(parts) if parts else "tous secteurs / tous pays"

    def to_dict(self) -> dict:
        from dataclasses import asdict
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "SearchTarget":
        valid = {k: v for k, v in d.items() if k in cls.__dataclass_fields__}
        return cls(**valid)


# ─────────────────────────────────────────────
# TARGETING_CRITERIA  — dict brut (compatible orchestrateur)
# ─────────────────────────────────────────────

TARGETING_CRITERIA: Dict = {
    "secteurs_activite": [
        "Informatique",
        "Technologie",
        "Télécommunications",
        "Développement logiciel",
        "Intelligence artificielle",
        "Conseil IT",
        "Cybersécurité",
        "Cloud computing",
        "E-commerce",
    ],
    "tailles_entreprise": ["TPE", "PME", "ETI", "GE"],
    "employes_min": 1,
    "employes_max": None,   # None = pas de limite haute (couvre TPE → GE)
    "types_entreprise": ["SARL", "SAS", "SASU", "SA", "Startup"],
    "localisation": {
        "pays":    ["France", "Tunisie", "Maroc", "Belgique"],
        "regions": [],
        "villes":  [],
    },
}


# ─────────────────────────────────────────────
# SEARCH_KEYWORDS
# ─────────────────────────────────────────────

SEARCH_KEYWORDS = [
    "logiciel", "software", "informatique", "développement web",
    "application mobile", "ERP", "CRM", "cloud", "digital", "tech",
    "startup", "IT", "conseil numérique", "transformation digitale",
    "intelligence artificielle", "cybersécurité",
]



# ─────────────────────────────────────────────
# REQUIRED / OPTIONAL FIELDS
# ─────────────────────────────────────────────

REQUIRED_FIELDS = ["nom_commercial"]

OPTIONAL_FIELDS = [
    "raison_sociale", "email", "telephone", "website",
    "adresse", "ville", "region", "pays",
    "secteur_activite", "taille_entreprise", "type_entreprise",
    "description", "nombre_employes", "chiffre_affaires",
    "siren", "siret", "linkedin_url",
]

# ─────────────────────────────────────────────
# SECTOR KEYWORDS  (NLP)
# ─────────────────────────────────────────────

SECTOR_KEYWORDS: Dict[str, List[str]] = {
    # ── Tech & Numérique ──────────────────────────────────────────────
    "Informatique": [
        "informatique", "IT", "software", "logiciel", "développement",
        "programmation", "application", "système d'information", "réseau",
        "infrastructure IT", "infogérance", "ERP", "CRM",
    ],
    "Télécommunications": [
        "télécom", "télécommunications", "réseau mobile", "internet",
        "fibre", "opérateur", "4G", "5G", "VoIP", "FAI",
    ],
    "Développement logiciel": [
        "développement logiciel", "éditeur logiciel", "SaaS", "DevOps",
        "agile", "scrum", "API", "microservices", "backend", "frontend",
        "full stack", "mobile app", "web app",
    ],
    "Intelligence artificielle": [
        "intelligence artificielle", "IA", "machine learning", "deep learning",
        "NLP", "data science", "big data", "algorithme", "neural network",
        "LLM", "GPT", "computer vision",
    ],
    "Cybersécurité": [
        "cybersécurité", "sécurité informatique", "RSSI", "pentest",
        "SOC", "SIEM", "firewall", "vulnerability", "ISO 27001",
        "RGPD", "protection données",
    ],
    "Cloud computing": [
        "cloud", "AWS", "Azure", "GCP", "SaaS", "IaaS", "PaaS",
        "hébergement", "datacenter", "virtualisation", "kubernetes", "docker",
    ],
    "E-commerce": [
        "e-commerce", "boutique en ligne", "marketplace", "shopify",
        "woocommerce", "magento", "vente en ligne", "dropshipping",
    ],
    "Conseil IT": [
        "conseil IT", "ESN", "SSII", "consulting informatique",
        "prestataire IT", "transformation numérique", "MOA", "MOE",
        "audit SI", "schéma directeur",
    ],
    # ── Finance & Gestion ─────────────────────────────────────────────
    "Finance": [
        "banque", "assurance", "finance", "investissement", "capital",
        "comptabilité", "audit", "gestion de patrimoine",
        "fintech", "crédit", "épargne", "bourse", "fiscalité",
    ],
    "Comptabilité": [
        "comptabilité", "expert-comptable", "cabinet comptable",
        "bilan", "liasse fiscale", "paie", "social", "TVA",
    ],
    # ── Santé & Bien-être ─────────────────────────────────────────────
    "Santé": [
        "santé", "médical", "pharmacie", "biotechnologie", "hôpital",
        "clinique", "laboratoire", "médecine", "cabinet médical",
        "mutuelle", "prévention", "e-santé", "healthtech",
    ],
    "Bien-être": [
        "bien-être", "spa", "massage", "yoga", "naturopathie",
        "ostéopathie", "nutrition", "coaching santé", "sophrologie",
    ],
    # ── Commerce & Distribution ───────────────────────────────────────
    "Commerce de détail": [
        "commerce", "magasin", "boutique", "vente", "retail",
        "distribution", "grande surface", "supermarché", "épicerie",
        "franchise", "enseigne",
    ],
    "Commerce de gros": [
        "grossiste", "distributeur", "négoce", "import", "export",
        "centrale d'achat", "approvisionnement",
    ],
    "Restauration": [
        "restaurant", "restauration", "traiteur", "brasserie",
        "café", "hôtel-restaurant", "fast food", "livraison repas",
        "food", "gastronomie",
    ],
    # ── BTP & Industrie ───────────────────────────────────────────────
    "BTP": [
        "BTP", "construction", "bâtiment", "travaux publics", "génie civil",
        "immobilier", "architecture", "maçonnerie", "charpente",
        "électricité", "plomberie", "menuiserie",
    ],
    "Industrie": [
        "industrie", "fabrication", "manufacturing", "usine",
        "production", "mécanique", "métallurgie", "agroalimentaire",
        "automobile", "aéronautique", "chimie", "plasturgie",
    ],
    "Énergie": [
        "énergie", "électricité", "gaz", "renouvelable", "solaire",
        "photovoltaïque", "éolien", "nucléaire", "pétrole",
        "efficacité énergétique",
    ],
    # ── Services ──────────────────────────────────────────────────────
    "Transport & Logistique": [
        "transport", "logistique", "livraison", "fret", "camion",
        "maritime", "aérien", "messagerie", "entrepôt", "supply chain",
    ],
    "Éducation & Formation": [
        "éducation", "formation", "école", "université", "organisme de formation",
        "e-learning", "OPCO", "CPF", "apprentissage", "certification",
    ],
    "Immobilier": [
        "immobilier", "agence immobilière", "promotion immobilière",
        "gestion locative", "syndic", "copropriété", "foncier",
    ],
    "Juridique & Droit": [
        "avocat", "cabinet juridique", "droit", "notaire",
        "huissier", "juridique", "contentieux", "propriété intellectuelle",
    ],
    "Marketing & Communication": [
        "marketing", "communication", "publicité", "agence", "digital",
        "SEO", "SEA", "réseaux sociaux", "branding", "webmarketing",
        "relations presse", "événementiel",
    ],
    "Ressources humaines": [
        "ressources humaines", "RH", "recrutement", "intérim",
        "cabinet RH", "formation professionnelle", "paie", "SIRH",
    ],
    # ── Agriculture & Environnement ───────────────────────────────────
    "Agriculture": [
        "agriculture", "agricole", "élevage", "viticulture",
        "maraîchage", "agroécologie", "coopérative agricole",
    ],
    "Environnement": [
        "environnement", "écologie", "recyclage", "déchets",
        "eau", "traitement eau", "développement durable", "RSE",
    ],
}

# ─────────────────────────────────────────────
# SIZE RANGES
# ─────────────────────────────────────────────

SIZE_RANGES: Dict[str, tuple] = {
    "TPE": (1,    9),
    "PME": (10,   249),
    "ETI": (250,  4999),
    "GE":  (5000, float("inf")),
}

# ─────────────────────────────────────────────
# EXAMPLE TARGETS
# ─────────────────────────────────────────────

EXAMPLE_TARGETS: Dict[str, SearchTarget] = {
    "tech_france_pme": SearchTarget(
        secteur_activite  = ["Informatique", "Cloud computing", "Intelligence artificielle"],
        taille_entreprise = ["PME", "TPE"],
        types_entreprise  = ["SAS", "SARL", "SASU"],
        pays              = ["France"],
        villes            = ["Paris", "Lyon", "Marseille"],
        keywords          = ["logiciel", "cloud", "IA", "SaaS"],
        max_resultats     = 100,
    ),
    "it_maghreb": SearchTarget(
        secteur_activite  = ["Informatique", "Télécommunications"],
        taille_entreprise = ["TPE", "PME"],
        pays              = ["Tunisie", "Maroc"],
        keywords          = ["informatique", "logiciel", "développement web"],
        max_resultats     = 50,
    ),
    "conseil_belgique": SearchTarget(
        secteur_activite  = ["Conseil IT", "Cybersécurité"],
        taille_entreprise = ["PME"],
        pays              = ["Belgique"],
        keywords          = ["consulting", "cybersécurité", "ESN"],
        max_resultats     = 50,
    ),
    "generic_demo": SearchTarget(
        secteur_activite  = ["Informatique"],
        taille_entreprise = ["PME"],
        pays              = ["France"],
        villes            = ["Paris"],
        keywords          = ["logiciel", "informatique"],
        max_resultats     = 15,
    ),
    "sante_france": SearchTarget(
        secteur_activite  = ["Santé", "Bien-être"],
        taille_entreprise = ["TPE", "PME"],
        types_entreprise  = ["SAS", "SARL", "SASU"],
        pays              = ["France"],
        villes            = ["Paris", "Lyon", "Bordeaux"],
        max_resultats     = 100,
    ),
    "commerce_france": SearchTarget(
        secteur_activite  = ["Commerce de détail", "Commerce de gros", "E-commerce"],
        taille_entreprise = ["TPE", "PME"],
        types_entreprise  = ["SAS", "SARL", "SASU", "EI"],
        pays              = ["France"],
        villes            = ["Paris", "Lyon", "Marseille", "Bordeaux", "Lille"],
        max_resultats     = 100,
    ),
    "restauration_france": SearchTarget(
        secteur_activite  = ["Restauration"],
        taille_entreprise = ["TPE", "PME"],
        pays              = ["France"],
        villes            = ["Paris", "Lyon", "Marseille"],
        max_resultats     = 80,
    ),
    "btp_france": SearchTarget(
        secteur_activite  = ["BTP", "Industrie"],
        taille_entreprise = ["TPE", "PME", "ETI"],
        types_entreprise  = ["SAS", "SARL", "SA"],
        pays              = ["France"],
        max_resultats     = 100,
    ),
    "finance_france": SearchTarget(
        secteur_activite  = ["Finance", "Comptabilité"],
        taille_entreprise = ["TPE", "PME"],
        types_entreprise  = ["SAS", "SARL", "SA"],
        pays              = ["France"],
        villes            = ["Paris", "Lyon"],
        max_resultats     = 80,
    ),
    "formation_france": SearchTarget(
        secteur_activite  = ["Éducation & Formation"],
        taille_entreprise = ["TPE", "PME"],
        pays              = ["France"],
        max_resultats     = 80,
    ),
    "marketing_france": SearchTarget(
        secteur_activite  = ["Marketing & Communication"],
        taille_entreprise = ["TPE", "PME"],
        types_entreprise  = ["SAS", "SARL", "SASU"],
        pays              = ["France"],
        villes            = ["Paris", "Lyon", "Bordeaux"],
        max_resultats     = 80,
    ),
    "all_sectors_demo": SearchTarget(
        secteur_activite  = [],
        taille_entreprise = ["PME"],
        pays              = ["France"],
        villes            = ["Paris"],
        max_resultats     = 20,
    ),
    # ── Cibles exploitant spécifiquement les nouvelles sources ────────
    "nouveaux_creates_bodacc": SearchTarget(
        secteur_activite  = ["Informatique", "Conseil IT", "E-commerce"],
        taille_entreprise = ["TPE", "PME"],
        types_entreprise  = ["SAS", "SARL", "SASU"],
        pays              = ["France"],
        villes            = ["Paris", "Lyon", "Marseille", "Bordeaux"],
        keywords          = ["logiciel", "cloud", "digital", "IT"],
        max_resultats     = 150,
    ),
    "b2b_europe_industrie": SearchTarget(
        secteur_activite  = ["BTP", "Industrie", "Transport & Logistique"],
        taille_entreprise = ["PME", "ETI"],
        pays              = ["France", "Belgique"],
        keywords          = ["industrie", "fabrication", "transport"],
        max_resultats     = 100,
    ),
}