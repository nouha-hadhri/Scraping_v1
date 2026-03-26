"""
test_pappers.py — Test standalone de l'API Pappers v2
Exécuter depuis la racine du projet :
    python test_pappers.py

Vérifie :
  - Que le token est bien lu depuis .env
  - Que l'URL est bien construite
  - Que l'API répond avec des résultats
  - Affiche les champs retournés pour chaque résultat
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import json
from urllib.parse import urlencode
from pathlib import Path
from dotenv import load_dotenv

# ── Chargement du .env ────────────────────────────────────────────────
load_dotenv(Path(__file__).resolve().parent / ".env")
PAPPERS_API_KEY = os.getenv("PAPPERS_API_KEY", "")
PAPPERS_URL     = "https://api.pappers.fr/v2/recherche"

# ── Paramètres de test (modifiables) ─────────────────────────────────
TEST_QUERY       = "software"         # mot-clé recherché
TEST_NAF         = "62.01Z"           # code NAF Informatique
TEST_CP          = "75001,75002,75003,75004,75005"  # Paris arrondissements
TEST_CAT         = None               # None = PME+ETI+GE tous ensemble
TEST_FORMES      = "SAS,SARL,SASU"   # formes juridiques
TEST_PER_PAGE    = 5                  # petit pour le test
TEST_PAGE        = 1


def build_url(query, naf=None, cat=None, cp=None, formes=None, per_page=5, page=1):
    params = {
        "api_token":         PAPPERS_API_KEY,
        "q":                 query,
        "par_page":          per_page,
        "page":              page,
        "precision":         "approximative",
        "entreprise_cessee": "false",
        "_fields": (
            "siren,nom_entreprise,forme_juridique,date_creation,"
            "libelle_code_naf,code_naf,tranche_effectif,"
            "siege.siret,siege.adresse_ligne_1,siege.code_postal,"
            "siege.ville,siege.region,siege.telephone,"
            "siege.email,siege.site_internet"
        ),
    }
    if naf:
        params["code_naf"] = naf
    if cat:
        params["categorie_entreprise"] = cat
    if cp:
        params["siege_code_postal"] = cp
    if formes:
        params["forme_juridique"] = formes

    return PAPPERS_URL + "?" + urlencode(params, doseq=True)


def call_api(url):
    """Appel HTTP via requests (ou curl subprocess en fallback)."""
    try:
        import requests
        resp = requests.get(url, timeout=15)
        return resp.status_code, resp.json()
    except ImportError:
        import subprocess, json as _json
        result = subprocess.run(
            ["curl", "-s", "--max-time", "15", url],
            capture_output=True, text=True
        )
        if result.returncode == 0 and result.stdout:
            return 200, _json.loads(result.stdout)
        return 0, {}


def print_separator(char="-", width=70):
    print(char * width)


def run_test():
    print_separator("=")
    print("  TEST PAPPERS API v2")
    print_separator("=")

    # ── Vérification token ────────────────────────────────────────────
    if not PAPPERS_API_KEY:
        print("[ERREUR] PAPPERS_API_KEY absent du .env !")
        print("         Ajouter : PAPPERS_API_KEY=votre_token dans .env")
        sys.exit(1)
    print(f"[OK] Token trouve : {PAPPERS_API_KEY[:8]}{'*' * (len(PAPPERS_API_KEY) - 8)}")

    # ── Test 1 : requête minimale (q= seul) ───────────────────────────
    print_separator()
    print("TEST 1 — Requete minimale : q= seul (sans filtres)")
    url1 = build_url(TEST_QUERY, per_page=TEST_PER_PAGE)
    print(f"URL : {url1[:100]}...")
    status, data = call_api(url1)
    print(f"HTTP status : {status}")

    if status != 200 or "error" in data:
        print(f"[ERREUR] {data}")
        sys.exit(1)

    total = data.get("total", 0)
    resultats = data.get("resultats", [])
    print(f"Total API : {total} | Recus : {len(resultats)}")

    if resultats:
        print("\nPremier resultat :")
        r = resultats[0]
        siege = r.get("siege", {}) or {}
        print(f"  Nom       : {r.get('nom_entreprise')}")
        print(f"  SIREN     : {r.get('siren')}")
        print(f"  NAF       : {r.get('code_naf')} — {r.get('libelle_code_naf')}")
        print(f"  Forme     : {r.get('forme_juridique')}")
        print(f"  Ville     : {siege.get('ville')} ({siege.get('code_postal')})")
        print(f"  Tel       : {siege.get('telephone') or '(vide en gratuit)'}")
        print(f"  Email     : {siege.get('email') or '(vide en gratuit)'}")
        print(f"  Site      : {siege.get('site_internet') or '(vide en gratuit)'}")

    # ── Test 2 : avec NAF + CP Paris ─────────────────────────────────
    print_separator()
    print(f"TEST 2 — Avec NAF={TEST_NAF} + CP={TEST_CP}")
    url2 = build_url(TEST_QUERY, naf=TEST_NAF, cp=TEST_CP, per_page=TEST_PER_PAGE)
    print(f"URL : {url2[:120]}...")
    status2, data2 = call_api(url2)
    print(f"HTTP status : {status2}")

    if status2 != 200 or "error" in data2:
        print(f"[ERREUR] {data2}")
    else:
        total2 = data2.get("total", 0)
        res2   = data2.get("resultats", [])
        print(f"Total API : {total2} | Recus : {len(res2)}")
        for r in res2[:3]:
            siege = r.get("siege", {}) or {}
            print(f"  - {r.get('nom_entreprise'):<40} {siege.get('ville')}")

    # ── Test 3 : avec formes juridiques ──────────────────────────────
    print_separator()
    print(f"TEST 3 — Avec formes={TEST_FORMES}")
    url3 = build_url(TEST_QUERY, naf=TEST_NAF, cp=TEST_CP,
                     formes=TEST_FORMES, per_page=TEST_PER_PAGE)
    print(f"URL : {url3[:120]}...")
    status3, data3 = call_api(url3)
    print(f"HTTP status : {status3}")

    if status3 != 200 or "error" in data3:
        print(f"[ERREUR] {data3}")
    else:
        total3 = data3.get("total", 0)
        res3   = data3.get("resultats", [])
        print(f"Total API : {total3} | Recus : {len(res3)}")
        for r in res3[:3]:
            siege = r.get("siege", {}) or {}
            print(f"  - {r.get('nom_entreprise'):<40} {r.get('forme_juridique')}")

    # ── Résumé quota ─────────────────────────────────────────────────
    print_separator("=")
    print("RESUME : 3 requetes effectuees (sur 1000/jour disponibles)")
    print("Tokens utilises environ : 3")
    print_separator("=")


if __name__ == "__main__":
    run_test()