"""
SCRAPING_V1 - Criteria Normalizer
Fonction centralisée de normalisation des critères de recherche.

Utilisée par :
  - fastapi_app.py (avant d'envoyer à orchestrator)
  - orchestrator.py (après lecture du fichier bridge)
  - scrapers (pour unifier les formats)

Transformations :
  - camelCase → snake_case (critères Spring Boot)
  - Aplatissement des structures imbriquées
  - Mapping des catégories de taille d'entreprise
  - Unicité de la source de vérité pour les variantes de clés
"""
import copy
from typing import Dict, Any, List, Optional


_CATEGORY_MAP = {
    "1-10":     "TPE",
    "11-50":    "PME",
    "51-250":   "ETI",
    "251-5000": "ETI",
    "251+":     "GE",
    "5000+":    "GE",
    "5000":     "GE",
    # Variantes possibles venant du frontend
    "tpe":      "TPE",
    "pme":      "PME",
    "eti":      "ETI",
    "ge":       "GE",
    "grande":   "GE",
}


def normalize_criteria(criteria: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalise les critères de recherche en un format canonique snake_case.

    Entrée : Peut être camelCase (Spring) ou snake_case (interne) ou mix.
    Sortie : Toujours snake_case unifié, structure aplatie, catégories normalisées.

    Clés normalisées (sortie) :
      - secteurs_activite, types_entreprise, tailles_entreprise
      - pays, regions, villes (aplatis, pas dans localisation)
      - employes_min, employes_max
      - keywords
      - max_resultats, max_par_source

    Args:
        criteria: Dict brut (peut venir de Spring, fichier bridge, ou API)

    Returns:
        Dict normalisé en snake_case pour utilisation interne
    """
    if not isinstance(criteria, dict):
        return criteria

    # Copie profonde pour ne pas modifier l'original
    crit = copy.deepcopy(criteria)

    # ────────────────────────────────────────────────────────────────────
    # 1. TAILLE D'ENTREPRISE → normalization + aplatissement
    # ────────────────────────────────────────────────────────────────────
    taille = crit.get("taille_entreprise") or crit.get("tailleEntreprise")
    if isinstance(taille, dict):
        # Catégories normalisées
        raw_categories = taille.get("categories") or taille.get("category") or []
        if isinstance(raw_categories, list):
            normalized_cats = [
                _CATEGORY_MAP.get(str(cat).strip(), str(cat).strip())
                for cat in raw_categories if cat
            ]
            crit["tailles_entreprise"] = normalized_cats
        
        # Nombres d'employés (support camelCase ET snake_case)
        crit["employes_min"] = (
            taille.get("nb_employes_min") 
            or taille.get("nbEmployesMin") 
            or 1
        )
        crit["employes_max"] = (
            taille.get("nb_employes_max") 
            or taille.get("nbEmployesMax")
        )
    
    # Si tailles_entreprise n'a pas été créée mais on a une clé directe
    if "tailles_entreprise" not in crit and "tailles_entreprise" in crit:
        pass  # Déjà là
    
    # ────────────────────────────────────────────────────────────────────
    # 2. SECTEURS D'ACTIVITÉ (camelCase → snake_case)
    # ────────────────────────────────────────────────────────────────────
    if "secteurActivite" in crit and "secteurs_activite" not in crit:
        crit["secteurs_activite"] = crit.pop("secteurActivite")
    # Support variante pluriel camelCase (moins courant)
    elif "secteursActivite" in crit and "secteurs_activite" not in crit:
        crit["secteurs_activite"] = crit.pop("secteursActivite")
    
    # ────────────────────────────────────────────────────────────────────
    # 3. TYPES D'ENTREPRISE (camelCase → snake_case)
    # ────────────────────────────────────────────────────────────────────
    if "typesEntreprise" in crit and "types_entreprise" not in crit:
        crit["types_entreprise"] = crit.pop("typesEntreprise")
    elif "typeEntreprise" in crit and "types_entreprise" not in crit:
        crit["types_entreprise"] = crit.pop("typeEntreprise")
    
    # ────────────────────────────────────────────────────────────────────
    # 4. KEYWORDS (motsCles/mots_cles → keywords)
    # ────────────────────────────────────────────────────────────────────
    if "motsCles" in crit and "keywords" not in crit:
        crit["keywords"] = crit.pop("motsCles")
    elif "mots_cles" in crit and "keywords" not in crit:
        crit["keywords"] = crit.pop("mots_cles")
    
    # ────────────────────────────────────────────────────────────────────
    # 5. GÉOGRAPHIE → aplatissement
    # ────────────────────────────────────────────────────────────────────
    # Priorité : zoneGeographique (Spring camelCase) > zone_geographique (snake_case)
    geo = None
    if "zoneGeographique" in crit:
        geo = crit.pop("zoneGeographique")
    elif "zone_geographique" in crit:
        geo = crit.pop("zone_geographique")
    elif "localisation" in crit:
        geo = crit.pop("localisation")
    
    if isinstance(geo, dict):
        # Aplatir les champs géographiques au même niveau
        crit["pays"] = (
            geo.get("pays") 
            or geo.get("zone_geographique")  # fallback ancien format
            or ["France"]
        )
        crit["regions"] = geo.get("regions") or []
        crit["villes"] = geo.get("villes") or []
    
    # Défaut si aucune géographie n'a été fournie
    if "pays" not in crit:
        crit["pays"] = ["France"]
    if "regions" not in crit:
        crit["regions"] = []
    if "villes" not in crit:
        crit["villes"] = []
    
    # ────────────────────────────────────────────────────────────────────
    # 6. MAX RÉSULTATS (maxResultats → max_resultats)
    # ────────────────────────────────────────────────────────────────────
    if "maxResultats" in crit and "max_resultats" not in crit:
        crit["max_resultats"] = crit.pop("maxResultats")
    elif "max_prospects_total" in crit and "max_resultats" not in crit:
        crit["max_resultats"] = crit.pop("max_prospects_total")
    
    # ────────────────────────────────────────────────────────────────────
    # 7. MAX PAR SOURCE (maxParSource → max_par_source)
    # ────────────────────────────────────────────────────────────────────
    if "maxParSource" in crit and "max_par_source" not in crit:
        crit["max_par_source"] = crit.pop("maxParSource")
    
    # ────────────────────────────────────────────────────────────────────
    # 8. NETTOYAGE : suppression des clés en doublon obsolètes
    # ────────────────────────────────────────────────────────────────────
    obsolete_keys = [
        "tailleEntreprise", "typeEntreprise",
        "secteursActivite", "typesEntreprise",
        "zoneGeographique", "zone_geographique", "localisation",
        "motsCles", "mots_cles",
        "maxResultats", "maxParSource"
    ]
    for key in obsolete_keys:
        crit.pop(key, None)
    
    return crit


def denormalize_criteria_for_logging(criteria: Dict[str, Any]) -> Dict[str, Any]:
    """
    Optionnel : convertit les critères normalisés (snake_case)
    en camelCase pour les logs/audit/affichage côté frontend.
    
    Inverse partiel de normalize_criteria().
    """
    if not isinstance(criteria, dict):
        return criteria
    
    out = copy.deepcopy(criteria)
    
    # Retro-mapping snake_case → camelCase
    reverse_map = {
        "secteurs_activite": "secteurActivite",
        "types_entreprise": "typesEntreprise",
        "tailles_entreprise": "tailleEntreprise",
        "employes_min": "nbEmployesMin",
        "employes_max": "nbEmployesMax",
        "max_resultats": "maxResultats",
        "max_par_source": "maxParSource",
    }
    
    geo_out = {}
    for snake_key, camel_key in reverse_map.items():
        if snake_key in out:
            out[camel_key] = out.pop(snake_key)
    
    # Géographie
    if any(k in out for k in ["pays", "regions", "villes"]):
        geo_out = {
            "pays": out.pop("pays", []),
            "regions": out.pop("regions", []),
            "villes": out.pop("villes", []),
        }
        out["zoneGeographique"] = geo_out
    
    return out
