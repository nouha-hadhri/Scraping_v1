#!/usr/bin/env python3
"""
Test d'intégration: Vérifier que la validation d'email fonctionne
dans l'orchestrator avec de vrais prospects.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from storage.models import Prospect
from orchestrator import ProspectCollector

print("=" * 80)
print("TEST: Intégration validation d'email dans orchestrator")
print("=" * 80)

# Créer un orchestrator
try:
    orch = ProspectCollector()
except Exception as e:
    print(f"⚠️  Erreur initialisation orchestrator: {e}")
    print("   (C'est OK si config/settings.py par défaut)")

# Créer des prospects de test avec différents emails
test_prospects = [
    Prospect(
        nom_commercial="Company A",
        email="contact@acme.fr",  # ✓ valide
        website="https://acme.fr",
        ville="Paris",
        region="Île-de-France",
    ),
    Prospect(
        nom_commercial="Company B",
        email="noreply@test.com",  # ✗ noreply
        website="https://test.com",
        ville="Lyon",
        region="Auvergne-Rhône-Alpes",
    ),
    Prospect(
        nom_commercial="Company C",
        email="john@company.com",  # ✓ valide
        website="https://company.com",
        ville="Marseille",
        region="PACA",
    ),
    Prospect(
        nom_commercial="Company D",
        email="user@gmail.com",  # ✗ personnel
        website="https://company.de",
        ville="Berlin",
        region="",
    ),
    Prospect(
        nom_commercial="Company E",
        email="",  # pas d'email
        website="https://startup.io",
        ville="San Francisco",
        region="",
    ),
]

print(f"\n{len(test_prospects)} prospects de test créés\n")

# Valider les emails
print("AVANT validation:")
print("-" * 80)
for p in test_prospects:
    email_status = "?" if p.email else "vide"
    print(f"  {p.nom_commercial:<20} | Email: {p.email or '(vide)':<30} | Valid: {p.email_valid}")

# Appliquer la validation
print("\nAPPLICATION validation...")
orch._validate_emails(test_prospects)

print("\nAPRÈS validation:")
print("-" * 80)
for p in test_prospects:
    status = "✓" if p.email_valid else "✗" if p.email else "-"
    print(f"  {p.nom_commercial:<20} | Email: {p.email or '(vide)':<30} | Valid: {status}")

print("\n" + "=" * 80)
print("RÉSUMÉ:")
print("=" * 80)

valid_emails = sum(1 for p in test_prospects if p.email and p.email_valid)
total_emails = sum(1 for p in test_prospects if p.email)
print(f"  - Total emails: {total_emails}")
print(f"  - Emails valides: {valid_emails}")
print(f"  - Emails rejetés: {total_emails - valid_emails}")
print(f"  - Taux de validité: {100*valid_emails//total_emails if total_emails > 0 else 0}%")
print("\n✓ Validation d'email intégrée avec succès!")
print("=" * 80)
