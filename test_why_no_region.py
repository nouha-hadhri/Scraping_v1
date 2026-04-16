#!/usr/bin/env python3
"""
Test simple: vérifier pourquoi les prospects OSM manquent de région.
Deux causes possibles:
1. Overpass ne retourne pas 'center' 
2. Nominatim échoue
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import json
import logging

# Logs détaillés
logging.basicConfig(
    level=logging.DEBUG,
    format='%(name)s - %(levelname)s - %(message)s'
)

from sources.open_data_scraper import OpenDataScraper
from sources.curl_client import CurlClient

print("=" * 80)
print("TEST: Pourquoi pas tous les prospects OSM ont une région?")
print("=" * 80)

# CAUSE A: Vérifier si Overpass retourne des 'center'
print("\nÉTAPE 1: Vérifier requête Overpass brute")
print("-" * 80)

client = CurlClient()
from config.settings import NOMINATIM_BASE_URL, OVERPASS_BASE_URL

# Get Paris bbox via Nominatim
print("Récupération bbox Paris...")
query_bbox = {
    "q": "Paris, France",
    "format": "json",
    "limit": 1,
}
from urllib.parse import urlencode
url_bbox = f"{NOMINATIM_BASE_URL}/search?{urlencode(query_bbox)}"
data_bbox = client.get_json(url_bbox, timeout=10)

if data_bbox and len(data_bbox) > 0:
    bb = data_bbox[0].get("boundingbox", [])
    if bb and len(bb) >= 4:
        south, north, west, east = float(bb[0]), float(bb[1]), float(bb[2]), float(bb[3])
        print(f"✓ Paris bbox: south={south:.2f}, west={west:.2f}, north={north:.2f}, east={east:.2f}\n")
        
        # Requête Overpass
        query = (
            f'[out:json][timeout:25];\n'
            f'(\n'
            f'  nwr["office"]["name"]({south},{west},{north},{east});\n'
            f');\n'
            f'out center;'
        )
        
        print("Envoi requête Overpass avec `out center;`...\n")
        resp = client.post(
            OVERPASS_BASE_URL,
            data={"data": query},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        
        if resp:
            parsed = json.loads(resp)
            elements = parsed.get("elements", [])
            
            with_center = sum(1 for el in elements if "center" in el)
            without_center = len(elements) - with_center
            
            print(f"Résultats Overpass:")
            print(f"  - Total éléments: {len(elements)}")
            print(f"  - Avec 'center': {with_center}")
            print(f"  - Sans 'center': {without_center}")
            
            if without_center > 0:
                print(f"\n⚠️  {without_center} éléments N'ONT PAS de 'center'")
                print("   Ces prospects n'auront PAS de région via Nominatim!")
                print("\n   SOLUTION: Améliorer la requête Overpass")
                print("   - Utiliser: nwr[...] → way[...] + nwr[...] (ways séparés)")
                print("   - Ou: out geom;")
                print("   - Ou: out body; + post-processing")
        else:
            print("✗ Pas de réponse Overpass")
else:
    print("✗ Nominatim n'a pas trouvé Paris")

print("\n" + "=" * 80)
print("ÉTAPE 2: Test OpenDataScraper")
print("-" * 80)

scraper = OpenDataScraper()
results = scraper._search_overpass("Paris", office_filter=[])

if results:
    with_region = sum(1 for r in results if r.region)
    without_region = len(results) - with_region
    
    print(f"\nRésultats OpenDataScraper:")
    print(f"  - Total: {len(results)}")
    print(f"  - Avec région: {with_region} ({100*with_region//len(results)}%)")
    print(f"  - Sans région: {without_region} ({100*without_region//len(results)}%)")
    
    if without_region > 0:
        print(f"\n⚠️  RAISON: Combiner les problèmes ci-dessus")
        print("   1. Overpass ne retourne pas 'center' pour certains éléments")
        print("   2. Ou Nominatim échoue silencieusement")
        print("\n   PROCHAINES SOLUTIONS:")
        print("   A. Améliorer la requête Overpass")
        print("   B. Ajouter fallback: code_postal → région (postal_by_geo.json)")
        print("   C. Ajouter fallback: ville → région (directement)")
else:
    print("✗ Aucun résultat")

print("=" * 80)
