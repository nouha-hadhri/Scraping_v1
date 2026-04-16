#!/usr/bin/env python3
"""
Diagnostic: Analyser pourquoi pas tous les prospects OSM ont une région.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import json
import logging
from sources.open_data_scraper import OpenDataScraper

# Activer les logs DEBUG
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

print("=" * 80)
print("DIAGNOSTIC: OpenStreetMap + Nominatim Reverse Geocoding")
print("=" * 80)

scraper = OpenDataScraper()

# Petit test avec 10 résultats
print("\n1. Test Overpass query (small dataset)...\n")
results = scraper._search_overpass("Le Marais, Paris", office_filter=[])

if not results:
    print("✗ Aucun résultat retourné par Overpass!")
else:
    print(f"✓ {len(results)} résultats trouvés\n")
    
    # Analyser
    with_center = sum(1 for r in results if r.region)
    without_center = len(results) - with_center
    
    print(f"Statistiques:")
    print(f"  - Total: {len(results)}")
    print(f"  - Avec région: {with_center} ({100*with_center//len(results)}%)")
    print(f"  - Sans région: {without_center} ({100*without_center//len(results)}%)")
    print()
    
    # Afficher les 10 premiers avec diagnostic
    print("Détail des 10 premiers prospects:")
    print("-" * 80)
    
    for i, p in enumerate(results[:10], 1):
        print(f"{i}. {p.nom_commercial}")
        print(f"   Ville: {p.ville}")
        print(f"   Région: {p.region or '(VIDE)'}")
        print(f"   Source: {p.source}")
        print()

print("=" * 80)
print("\n2. Raisons possibles d'absence de région:")
print("   a) Overpass n'a pas retourné de coordonnées (pas de 'center')")
print("   b) Nominatim n'a pas trouvé la région pour ces coordonnées")
print("   c) Rate limiting ou timeout Nominatim")
print("   d) Erreur silencieuse (mauvais parsing)")
print()
print("Activez les logs DEBUG pour plus de détails (déjà activés ci-dessus)")
print("=" * 80)
