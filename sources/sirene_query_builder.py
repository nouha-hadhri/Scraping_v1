"""
SCRAPING_V1 - SireneQueryBuilder
Traduit search_config.json (termes humains) en paramètres API SIRENE.

Responsabilités :
  - Charger les fichiers de mapping (NAF, codes postaux)
  - Convertir secteurs → codes NAF
  - Convertir régions/villes → préfixes codes postaux
  - Convertir types d'entreprise → codes nature_juridique (dict inline)
  - Construire le dict de paramètres prêt à envoyer à l'API SIRENE

Usage :
    builder = SireneQueryBuilder()
    params  = builder.build_params(search_config)
    # params est un dict utilisable directement dans CurlClient.get()

Fichiers de mapping requis (config/mappings/) :
    - naf_by_sector.json
    - postal_by_geo.json
"""
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import logging
from pathlib import Path
from typing  import Dict, List, Any

logger = logging.getLogger(__name__)

# ── Chemins des fichiers de mapping ───────────────────────────────────────────
_MAPPINGS_DIR = Path(__file__).resolve().parent.parent / "config" / "mappings"
_NAF_FILE     = _MAPPINGS_DIR / "naf_by_sector.json"
_POSTAL_FILE  = _MAPPINGS_DIR / "postal_by_geo.json"

# ── Mapping inline nature_juridique ───────────────────────────────────────────
# Source : INSEE — Nomenclature des catégories juridiques niveau III (2024)
# Couvre 95%+ des entreprises B2B françaises — pas besoin de fichier externe.
_LEGAL_CODES: Dict[str, List[str]] = {
    "EI":           ["1000"],
    "EIRL":         ["1300"],
    "SARL":         ["5499", "5410", "5485"],
    "EURL":         ["5498"],
    "SAS":          ["5710"],
    "SASU":         ["5720"],
    "SA":           ["5596", "5599"],
    "SNC":          ["2110"],
    "SCS":          ["2120"],
    "SCA":          ["5306"],
    "GIE":          ["2310"],
    "GEIE":         ["2400"],
    "SCOP":         ["5458"],
    "SE":           ["5800"],
    "Association":  ["9210", "9220", "9230"],
}


def _load_json(path: Path) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"[SireneQueryBuilder] Fichier mapping introuvable : {path}")
        return {}
    except json.JSONDecodeError as e:
        logger.error(f"[SireneQueryBuilder] Erreur JSON dans {path} : {e}")
        return {}


class SireneQueryBuilder:
    """
    Construit les paramètres de requête pour l'API SIRENE
    à partir d'un search_config.json (termes utilisateur lisibles).

    L'utilisateur saisit :
        "Informatique", "Île-de-France", "SAS", "PME"...

    Ce builder traduit en :
        activite_principale  = ["62.01Z", "62.02A", ...]
        code_postal          = ["75001", "75002", ..., "77", "78"]
        categorie_entreprise = ["PME", "ETI"]
        nature_juridique     = ["5710", "5720"]
        q                    = "logiciel OR cloud OR startup"
        etat_administratif   = "A"
    """

    def __init__(self):
        self._naf    = _load_json(_NAF_FILE)
        self._postal = _load_json(_POSTAL_FILE)
        logger.info(f"[SireneQueryBuilder] NAF chargé : {len(self._naf)} secteurs depuis {_NAF_FILE}")
        logger.info(f"[SireneQueryBuilder] Postal chargé : {len(self._postal)} clés depuis {_POSTAL_FILE}")

    # ──────────────────────────────────────────
    # Point d'entrée principal
    # ──────────────────────────────────────────

    def build_params(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Entrée : search_config dict (chargé depuis search_config.json)
        Sortie : dict de paramètres prêt pour l'API SIRENE

        Exemple de sortie :
        {
            "activite_principale":  ["62.01Z", "62.02A", ...],
            "code_postal":          ["75001", "75002", ..., "77"],
            "categorie_entreprise": ["PME", "ETI"],
            "nature_juridique":     ["5710", "5720"],
            "q":                    "logiciel OR cloud OR startup",
            "etat_administratif":   "A",
        }
        """
        params: Dict[str, Any] = {}

        # ── 1. Codes NAF depuis secteurs ──────────────────────────────────
        naf_codes = self._map_sectors(config.get("secteurs_activite", []))
        if naf_codes:
            params["activite_principale"] = naf_codes

        # ── 2. Codes postaux depuis régions + villes ───────────────────────
        geo     = config.get("zone_geographique", {})
        cp_list = self._map_geo(
            regions = geo.get("regions", []),
            villes  = geo.get("villes",  []),
        )
        if cp_list:
            params["code_postal"] = cp_list

        # ── 3. Catégories entreprise (taille) — passées directement ────────
        # TPE / PME / ETI / GE sont les valeurs natives de l'API SIRENE,
        # aucune traduction nécessaire.
        taille = config.get("taille_entreprise", {})
        cats   = taille.get("categories", [])
        if cats:
            params["categorie_entreprise"] = cats

        # ── 4. Formes juridiques (dict inline, pas de fichier externe) ─────
        legal_codes = self._map_legal_forms(config.get("types_entreprise", []))
        if legal_codes:
            params["nature_juridique"] = legal_codes

        # ── 5. Mots-clés → liste brute (q= est itéré par _search_sirene) ──
        # IMPORTANT : l'API SIRENE n'accepte qu'une seule valeur par q=.
        # On ne construit PAS q= ici. _search_sirene itère sur chaque mot-clé
        # et émet une requête distincte par keyword (comme pour activite_principale).
        keywords = config.get("mots_cles", [])
        if keywords:
            params["keywords_list"] = keywords   # Transmis à _search_sirene, pas envoyé à l'API

        # ── 6. Filtres fixes ───────────────────────────────────────────────
        params["etat_administratif"] = "A"   # entreprises actives uniquement

        # ── 7. Tranches d'effectif (optionnel) ────────────────────────────
        nb_min = taille.get("nb_employes_min")
        nb_max = taille.get("nb_employes_max")
        if nb_min is not None:
            params["tranche_effectif_min"] = self._employes_to_tranche(nb_min)
        if nb_max is not None:
            params["tranche_effectif_max"] = self._employes_to_tranche(nb_max)

        self._log_summary(params)
        return params

    # ──────────────────────────────────────────
    # Mappers
    # ──────────────────────────────────────────

    def _map_sectors(self, secteurs: List[str]) -> List[str]:
        """Convertit une liste de secteurs humains en codes NAF uniques."""
        codes: List[str] = []

        for s in secteurs:
            entry = self._naf.get(s)
            if isinstance(entry, list):
                codes.extend(entry)
            else:
                # Recherche partielle insensible à la casse
                sl = s.lower()
                for key, val in self._naf.items():
                    if key.startswith("_"):
                        continue
                    if sl in key.lower() or key.lower() in sl:
                        if isinstance(val, list):
                            codes.extend(val)
                            break
                else:
                    logger.warning(f"[SireneQueryBuilder] Secteur inconnu : {s!r}")

        # Dédoublonner en préservant l'ordre
        seen   = set()
        result = []
        for c in codes:
            if c not in seen:
                seen.add(c)
                result.append(c)

        logger.debug(f"[SireneQueryBuilder] {len(result)} codes NAF pour {secteurs}")
        return result

    def _map_geo(self, regions: List[str], villes: List[str]) -> List[str]:
        """
        Convertit régions et villes en liste de préfixes codes postaux.
        Priorité métier : villes > régions > national.
        Si aucune région ni ville n'est renseignée, retourne [] (pas de filtre CP,
        l'API couvre tout le pays via etat_administratif=A).
        """
        prefixes: List[str] = []
        regions_data = self._postal.get("regions", {})
        villes_data  = self._postal.get("villes",  {})

        # Si une ville est fournie, on filtre strictement sur les villes.
        if villes:
            selected_regions: List[str] = []
        else:
            selected_regions = regions

        for region in selected_regions:
            entry = regions_data.get(region)
            if entry:
                prefixes.extend(entry.get("prefixes", []))
            else:
                rl = region.lower()
                for key, val in regions_data.items():
                    if rl in key.lower() or key.lower() in rl:
                        prefixes.extend(val.get("prefixes", []))
                        break
                else:
                    logger.warning(f"[SireneQueryBuilder] Région inconnue : {region!r}")

        for ville in (villes or []):
            entry = villes_data.get(ville)
            if entry:
                prefixes.extend(entry.get("prefixes", []))
            else:
                vl = ville.lower()
                for key, val in villes_data.items():
                    if vl in key.lower() or key.lower() in vl:
                        prefixes.extend(val.get("prefixes", []))
                        break
                else:
                    logger.warning(f"[SireneQueryBuilder] Ville inconnue : {ville!r}")

        # Dédoublonner en préservant l'ordre
        seen   = set()
        result = []
        for p in prefixes:
            if p not in seen:
                seen.add(p)
                result.append(p)

        return result

    @staticmethod
    def _map_legal_forms(types: List[str]) -> List[str]:
        """
        Convertit les types d'entreprise en codes nature_juridique INSEE.
        Utilise le dict inline _LEGAL_CODES — pas de fichier externe.
        """
        codes = []
        for t in types:
            found = _LEGAL_CODES.get(t.upper())
            if found:
                codes.extend(found)
            else:
                logger.warning(f"[SireneQueryBuilder] Type entreprise inconnu : {t!r}")

        # Dédoublonner en préservant l'ordre
        return list(dict.fromkeys(codes))

    @staticmethod
    def _employes_to_tranche(n: int) -> str:
        """
        Convertit un nombre d'employés en code tranche d'effectif INSEE.
        Référence : https://www.insee.fr/fr/information/2028129
        """
        tranches = [
            (0,     0,       "00"),
            (1,     1,       "01"),
            (2,     2,       "02"),
            (3,     5,       "03"),
            (6,     9,       "11"),
            (10,    19,      "12"),
            (20,    49,      "21"),
            (50,    99,      "22"),
            (100,   199,     "31"),
            (200,   249,     "32"),
            (250,   499,     "41"),
            (500,   999,     "42"),
            (1000,  1999,    "51"),
            (2000,  4999,    "52"),
            (5000,  9999,    "53"),
            (10000, 19999,   "54"),
            (20000, int(1e9),"55"),
        ]
        for lo, hi, code in tranches:
            if lo <= n <= hi:
                return code
        return "55"

    # ──────────────────────────────────────────
    # Introspection / utilitaires pour l'UI CRM
    # ──────────────────────────────────────────

    def list_sectors(self) -> List[str]:
        """Liste tous les secteurs disponibles (pour peupler un dropdown UI)."""
        return [k for k in self._naf.keys() if not k.startswith("_")]

    def list_regions(self) -> List[str]:
        """Liste toutes les régions disponibles."""
        return list(self._postal.get("regions", {}).keys())

    def list_villes(self) -> List[str]:
        """Liste toutes les villes disponibles."""
        return list(self._postal.get("villes", {}).keys())

    def list_legal_types(self) -> List[str]:
        """Liste tous les types d'entreprise supportés."""
        return list(_LEGAL_CODES.keys())

    def get_naf_for_sector(self, sector: str) -> List[str]:
        """Retourne les codes NAF pour un secteur donné (debug)."""
        entry = self._naf.get(sector, [])
        return entry if isinstance(entry, list) else []

    def get_postal_for_region(self, region: str) -> List[str]:
        """Retourne les préfixes CP pour une région donnée (debug)."""
        return self._postal.get("regions", {}).get(region, {}).get("prefixes", [])

    def get_postal_for_ville(self, ville: str) -> List[str]:
        """Retourne les CP pour une ville donnée (debug)."""
        return self._postal.get("villes", {}).get(ville, {}).get("prefixes", [])

    def get_region_for_postal(self, code_postal: str) -> Optional[str]:
        """
        Reverse lookup : retourne la région pour un code postal donné.
        Ex: "75001" -> "Île-de-France", "69001" -> "Auvergne-Rhône-Alpes"
        
        Utilisé pour enrichir les prospects avec leur région quand
        le scraper n'a fourni que le code postal.
        """
        if not code_postal:
            return None
        
        cp = str(code_postal).strip()
        if not cp:
            return None
        
        # Cherche ligne région dont les préfixes matchent
        regions_data = self._postal.get("regions", {})
        for region_name, region_info in regions_data.items():
            if isinstance(region_info, dict):
                prefixes = region_info.get("prefixes", [])
                for prefix in prefixes:
                    if cp.startswith(str(prefix)):
                        return region_name
        
        return None

    def get_region_insee_code(self, region: str) -> str:
        """
        Retourne le code INSEE d'une région (ex: 'Île-de-France' -> '11').
        Utilisé par _GeoResolver pour appeler geo.api.gouv.fr/communes?codeRegion=.
        Recherche exacte d'abord, puis insensible à la casse.

        Structure attendue dans postal_by_geo.json :
            { "regions": { "Île-de-France": { "insee_code": "11", ... } } }
        """
        regions_data = self._postal.get("regions", {})
        # Recherche exacte
        entry = regions_data.get(region, {})
        if isinstance(entry, dict) and entry.get("insee_code"):
            return entry["insee_code"]
        # Recherche insensible à la casse
        key = region.lower().strip()
        for r_name, r_val in regions_data.items():
            if r_name.lower().strip() == key and isinstance(r_val, dict):
                return r_val.get("insee_code", "")
        logger.warning(f"[SireneQueryBuilder] Code INSEE introuvable pour région : {region!r}")
        return ""

    def get_region_capital(self, region: str) -> str:
        """
        Retourne la capitale d'une région (ex: 'Bretagne' -> 'Rennes').
        Utilisé par OSM et BODACC qui travaillent ville par ville.
        Recherche exacte d'abord, puis insensible à la casse.

        Structure attendue dans postal_by_geo.json :
            { "regions": { "Bretagne": { "capital": "Rennes", ... } } }
        """
        regions_data = self._postal.get("regions", {})
        # Recherche exacte
        entry = regions_data.get(region, {})
        if isinstance(entry, dict) and entry.get("capital"):
            return entry["capital"]
        # Recherche insensible à la casse
        key = region.lower().strip()
        for r_name, r_val in regions_data.items():
            if r_name.lower().strip() == key and isinstance(r_val, dict):
                return r_val.get("capital", "")
        logger.warning(f"[SireneQueryBuilder] Capitale introuvable pour région : {region!r}")
        return ""

    def get_all_region_capitals(self, regions: list) -> list:
        """
        Retourne les capitales uniques pour une liste de régions.
        Utilisé par OSM/BODACC pour itérer sur les villes représentatives.
        """
        caps, seen = [], set()
        for r in regions:
            cap = self.get_region_capital(r)
            if cap and cap not in seen:
                seen.add(cap)
                caps.append(cap)
        return caps

    # ──────────────────────────────────────────
    # Logging
    # ──────────────────────────────────────────

    def _log_summary(self, params: Dict[str, Any]) -> None:
        naf   = params.get("activite_principale", [])
        cp    = params.get("code_postal", [])
        legal = params.get("nature_juridique", [])
        cats  = params.get("categorie_entreprise", [])
        kw_list = params.get("keywords_list", [])
        logger.info(
            f"[SireneQueryBuilder] Paramètres construits — "
            f"NAF:{len(naf)} codes | CP:{len(cp)} préfixes | "
            f"Legal:{legal} | Taille:{cats} | "
            f"keywords:{len(kw_list)} (1 requête q= par mot-clé)"
        )