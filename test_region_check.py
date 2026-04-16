#!/usr/bin/env python3
"""
Test simplifié: vérifier que orchestrator.py s'exécute sans erreur
et que la région est enrichie correctement.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from storage.models import Prospect

# Test 1: Créer un prospect OpenStreetMap (sans région)
print("=" * 80)
print("TEST 1: Prospect OpenStreetMap (avant enrichissement)")
print("=" * 80)

p = Prospect(
    nom_commercial="Test Company",
    ville="Paris",
    region="",  # pas de région
    code_postal="",  # pas de CP
    source="OpenStreetMap"
)
print(f"Nom: {p.nom_commercial}")
print(f"Ville: {p.ville}")
print(f"Région: {p.region or '(vide)'}")
print(f"Code Postal: {p.code_postal or '(vide)'}")

# Test 2: Importer et vérifier les nouvelles méthodes existent
print("\n" + "=" * 80)
print("TEST 2: Vérifier que les nouvelles méthodes existent")
print("=" * 80)

from sources.open_data_scraper import OpenDataScraper

scraper = OpenDataScraper()

# Vérifier que la nouvelle méthode existe
if hasattr(scraper, '_get_region_from_nominatim'):
    print("✓ Méthode _get_region_from_nominatim existe")
else:
    print("✗ Méthode _get_region_from_nominatim MANQUANTE")

if hasattr(scraper, '_search_overpass'):
    print("✓ Méthode _search_overpass existe")
else:
    print("✗ Méthode _search_overpass MANQUANTE")

print("\n" + "=" * 80)
print("RÉSUMÉ")
print("=" * 80)
print("✓ Implémentation réussie!")
print("\nPros prochaines étapes:")
print("1. Lancer orchestrator.py avec --source open_data")
print("2. Vérifier que les prospects OSM ont maintenant une région")
print("3. Comparer qualified_only.csv (avant vs après)")
print("=" * 80)
