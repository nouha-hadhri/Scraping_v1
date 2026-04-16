#!/usr/bin/env python3
"""
Test de la nouvelle enrichissement région depuis OpenStreetMap.
Vérifie que les prospects OSM ont maintenant une région.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import json
from sources.open_data_scraper import OpenDataScraper

# Initialiser
scraper = OpenDataScraper()

# Test: recherche Overpass pour Paris
print("=" * 80)
print("TEST: OpenStreetMap + Nominatim Reverse Geocoding")
print("=" * 80)

results = scraper._search_overpass("Paris", office_filter=[])

print(f"\n✓ {len(results)} résultats trouvés à Paris\n")

# Afficher les 5 premiers
for i, prospect in enumerate(results[:5], 1):
    print(f"{i}. {prospect.nom_commercial}")
    print(f"   Ville: {prospect.ville}")
    print(f"   Région: {prospect.region or '(vide)'}")
    print(f"   CP: {prospect.code_postal or '(vide)'}")
    print()

# Statistiques
regions_count = sum(1 for p in results if p.region)
print(f"=" * 80)
print(f"Statistiques:")
print(f"  - Total prospects: {len(results)}")
print(f"  - Avec région: {regions_count} ({100*regions_count//len(results) if results else 0}%)")
print(f"  - Sans région: {len(results) - regions_count}")
print("=" * 80)
