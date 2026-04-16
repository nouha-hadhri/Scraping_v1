#!/usr/bin/env python3
"""
Test: Validation d'email
Vérifier que tous les cas sont correctement gérés.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.email_validator import (
    validate_email, validate_email_syntax, validate_email_domain,
    validate_email_type, batch_validate_emails
)

print("=" * 80)
print("TEST: Email Validation")
print("=" * 80)

# Cas de test
test_cases = [
    # (email, should_be_valid, reason)
    ("contact@acme.fr", True, "Email valide standard"),
    ("john.doe@company.com", True, "Email valide avec point"),
    ("hello@pepite.paris", True, "Email valide TLD long"),
    
    # Emails invalides - syntaxe
    ("invalid", False, "Pas de @"),
    ("invalid@", False, "Domaine vide"),
    ("@invalid.com", False, "Local part vide"),
    ("test@test", False, "TLD trop court"),
    
    # Emails invalides - domaine bloqué
    ("user@gmail.com", False, "Gmail bloqué (personnel)"),
    ("user@yahoo.com", False, "Yahoo bloqué (personnel)"),
    ("user@example.com", False, "Example.com bloqué (test)"),
    
    # Emails invalides - type spam
    ("noreply@company.com", False, "Noreply détecté"),
    ("no-reply@company.com", False, "No-reply détecté"),
    ("donotreply@company.com", False, "Donotreply détecté"),
    
    # Edge cases
    ("", False, "Email vide"),
    (None, False, "Email None"),
    ("   ", False, "Email espaces seulement"),
    ("test@undefined", False, "Unicode dangereux"),
]

print("\nRésultats détaillés:")
print("-" * 80)

passed = 0
failed = 0

for email, expected_valid, reason in test_cases:
    result = validate_email(email, check_mx=False)
    is_valid = result["is_valid"]
    
    status = "✓" if is_valid == expected_valid else "✗"
    if is_valid == expected_valid:
        passed += 1
    else:
        failed += 1
    
    print(f"{status} {email or '(empty)':<30} | Expected: {expected_valid:<5} Got: {is_valid:<5}")
    if is_valid != expected_valid:
        print(f"  ↳ {reason} - Reason: {result['reason']}")

print("\n" + "=" * 80)
print(f"RÉSULTATS: {passed} passés, {failed} échoués")
print("=" * 80)

# Test batch
print("\nTest batch avec cache MX:")
print("-" * 80)

batch_emails = [
    "contact@acme.fr",
    "john@company.com",
    "noreply@test.com",
    "user@gmail.com",
]

batch_results = batch_validate_emails(batch_emails, check_mx=False)

for email, result in batch_results.items():
    status = "✓" if result["is_valid"] else "✗"
    print(f"{status} {email:<30} → {result['reason']}")

print("=" * 80)
