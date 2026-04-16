#!/usr/bin/env python3
"""
Analyser une réponse Overpass brute pour diagnostiquer les problèmes.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import json
from sources.curl_client import CurlClient
from config.settings import NOMINATIM_BASE_URL, OVERPASS_BASE_URL

client = CurlClient()

print("=" * 80)
print("DIAGNOSTIC: Réponse OVERPASS - vérifier les coordonnées")
print("=" * 80)

# Faire une requête Overpass directe
query = (
    '[out:json][timeout:25];\n'
    '(\n'
    'nwr["office"]["name"](48.8566,2.3522,48.8566,2.3522);\n'  # Paris center
    'nwr["amenity"~"^(coworking_space|office)$"]["name"](48.8566,2.3522,48.8566,2.3522);\n'
    ');\n'
    'out center;'
)

print("\n1. Envoi requête Overpass avec `out center;`...\n")

raw_resp = client.post(
    OVERPASS_BASE_URL,
    data={"data": query},
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    timeout=30,
)

if not raw_resp:
    print("✗ Pas de réponse Overpass!")
else:
    try:
        parsed = json.loads(raw_resp)
        elements = parsed.get("elements", [])
        
        print(f"✓ {len(elements)} éléments retournés\n")
        
        # Analyser la structure
        with_center = sum(1 for el in elements if "center" in el)
        without_center = len(elements) - with_center
        
        print(f"Statistiques:")
        print(f"  - Avec 'center': {with_center} ({100*with_center//len(elements) if elements else 0}%)")
        print(f"  - Sans 'center': {without_center} ({100*without_center//len(elements) if elements else 0}%)")
        print()
        
        # Afficher les 3 premiers avec center
        print("Exemples avec 'center' (comprendre la structure):")
        count = 0
        for el in elements:
            if "center" in el and count < 3:
                print(f"  - {el.get('tags', {}).get('name', 'N/A')}")
                print(f"    lat={el['center']['lat']}, lon={el['center']['lon']}")
                count += 1
        
        print()
        print("Exemples SANS 'center' (nœuds isolés?):")
        count = 0
        for el in elements:
            if "center" not in el and count < 3:
                print(f"  - {el.get('tags', {}).get('name', 'N/A')}")
                print(f"    type={el.get('type', 'N/A')}, id={el.get('id', 'N/A')}")
                count += 1
        
        print("\n" + "=" * 80)
        print("ANALYSE:")
        print("=" * 80)
        if without_center > 0:
            print(f"⚠️  {without_center} éléments N'ONT PAS de 'center'")
            print("   Cela inclut probablement:")
            print("   - Ways (routes, polygones) sans barycentre")
            print("   - Relations complexes")
            print("   - Nœuds isolés sans tag addr:*")
            print()
            print("   SOLUTION: Utilisez 'out center' + 'out body'")
            print("   ou forcez le centre avec '[center]'")
        
    except json.JSONDecodeError as e:
        print(f"✗ Erreur JSON: {e}")
