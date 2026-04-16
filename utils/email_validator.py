"""
Email Validation Utilities - Vérifier si un email est valide et réel.

Trois niveaux de vérification:
  1. Syntaxe (regex) - format correct
  2. Domaine MX (DNS)  - domaine existe et accepte les emails
  3. Typage (spam check) - pas de noreply, example.com, etc.
"""
import re
import logging

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════
# Patterns & Listes noires
# ══════════════════════════════════════════════════════════════

EMAIL_REGEX = re.compile(
    r"^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$",
    re.IGNORECASE
)

# Emails invalides / spam
INVALID_EMAILS = {
    "noreply", "no-reply", "donotreply", "do-not-reply",
    "contact-en", "support@", "info@",  # pièces statiques
    "example.com", "test@", "admin@",
    "postmaster", "mailer-daemon", "nobody",
    "undefined", "none", "na", "n/a",
}

# Domaines bloqués (publics, faux, etc.)
BLOCKED_DOMAINS = {
    "example.com", "example.fr", "example.net",
    "test.com", "test.fr",
    "localhost", "127.0.0.1", "0.0.0.0",
    "gmail.com",  # considérer comme personnel, pas pro
    "yahoo.com", "outlook.com", "hotmail.com",
    "aol.com", "protonmail.com", "temp-mail.org",
}


def validate_email_syntax(email: str) -> bool:
    """
    Valide la syntaxe d'un email.
    
    Args:
        email: Adresse email à valider
    
    Returns:
        True si la syntaxe est valide
    """
    if not email or not isinstance(email, str):
        return False
    
    email = email.strip().lower()
    
    # Vérifier min/max longueur
    if len(email) < 5 or len(email) > 254:
        return False
    
    # Vérifier format avec regex
    if not EMAIL_REGEX.match(email):
        return False
    
    # Vérifier domaine n'est pas vide
    parts = email.split("@")
    if len(parts) != 2:
        return False
    
    local, domain = parts
    if not local or not domain:
        return False
    
    # Au moins 1 point dans le domaine
    if "." not in domain:
        return False
    
    return True


def validate_email_domain(email: str) -> bool:
    """
    Valide le domaine d'un email (pas dans la blacklist, syntax OK).
    
    Args:
        email: Adresse email
    
    Returns:
        True si le domaine est valide (utilisable en prod)
    """
    if not email or not isinstance(email, str):
        return False
    
    email = email.strip().lower()
    
    # Extraire domaine
    if "@" not in email:
        return False
    
    domain = email.split("@")[-1]
    
    # Vérifier domaine ne soit pas bloqué
    if domain in BLOCKED_DOMAINS:
        return False
    
    # Vérifier sans domaine générique trop court
    parts = domain.split(".")
    if len(parts[-1]) < 2:  # TLD trop court
        return False
    
    return True


def validate_email_type(email: str) -> bool:
    """
    Détecte si c'est un email 'spam' ou 'noreply'.
    
    Args:
        email: Adresse email
    
    Returns:
        False si c'est spam/noreply, True sinon
    """
    if not email or not isinstance(email, str):
        return False
    
    email = email.strip().lower()
    
    # Vérifier noreply, contact générique, etc.
    for invalid in INVALID_EMAILS:
        if invalid in email:
            return False
    
    return True


def check_mx_records(domain: str, timeout: int = 5) -> bool:
    """
    Vérifie qu'un domaine a des enregistrements MX valides (DNS).
    
    IMPORTANT: Cette fonction fait un appel DNS réel.
    Peut être lente si appelée sur beaucoup de domaines.
    Recommandation: cache + rate limiter
    
    Args:
        domain: Domaine (ex: "example.com")
        timeout: Timeout DNS en secondes
    
    Returns:
        True si le domaine a des records MX
    """
    try:
        import dns.resolver
        import dns.exception
    except ImportError:
        logger.warning("[EmailValidation] dnspython pas installé, skipping MX check")
        return True  # On laisse passer si pas de lib DNS
    
    if not domain or "." not in domain:
        return False
    
    try:
        # Chercher les records MX du domaine
        mx_records = dns.resolver.resolve(domain, 'MX', lifetime=timeout)
        return bool(mx_records)
    except (dns.resolver.NXDOMAIN, dns.resolver.NoNameservers, dns.exception.Timeout):
        logger.debug(f"[EmailValidation] Domaine invalide ou timeout: {domain}")
        return False
    except Exception as e:
        logger.debug(f"[EmailValidation] Erreur MX check {domain}: {e}")
        return True  # Par défaut, on tolère les erreurs réseau


def validate_email(email: str, check_mx: bool = False, timeout: int = 5) -> dict:
    """
    Validation complète d'un email.
    
    Args:
        email: Adresse email à valider
        check_mx: Si True, vérifier les records MX (DNS) - LENT
        timeout: Timeout DNS si check_mx=True
    
    Returns:
        Dict avec:
        - is_valid: bool (True si tous les checks passent)
        - syntax: bool (format correct)
        - domain: bool (domaine valide)
        - type: bool (pas spam)
        - mx: bool ou None (MX records si check_mx=True)
        - reason: str (explication si invalide)
    """
    result = {
        "is_valid": False,
        "syntax": False,
        "domain": False,
        "type": False,
        "mx": None,
        "reason": None,
    }
    
    if not email or not isinstance(email, str):
        result["reason"] = "Email vide ou non-string"
        return result
    
    email = email.strip()
    
    # Check 1: Syntaxe
    if not validate_email_syntax(email):
        result["reason"] = "Syntaxe invalide"
        return result
    result["syntax"] = True
    
    # Check 2: Domaine
    if not validate_email_domain(email):
        result["reason"] = "Domaine bloqué ou invalide"
        return result
    result["domain"] = True
    
    # Check 3: Type (noreply, spam)
    if not validate_email_type(email):
        result["reason"] = "Adresse spam ou noreply"
        return result
    result["type"] = True
    
    # Check 4: MX Records (optionnel, LENT)
    if check_mx:
        domain = email.split("@")[-1]
        has_mx = check_mx_records(domain, timeout)
        result["mx"] = has_mx
        if not has_mx:
            result["reason"] = "Domaine n'a pas d'enregistrements MX"
            return result
    else:
        result["mx"] = None  # Non vérifié
    
    # Tout est OK
    result["is_valid"] = True
    result["reason"] = "Email valide"
    return result


def batch_validate_emails(emails: list, check_mx: bool = False) -> dict:
    """
    Valider plusieurs emails en batch (avec cache MX).
    
    Args:
        emails: Liste d'emails
        check_mx: Si True, vérifier MX (avec cache interne)
    
    Returns:
        Dict {email: validation_result}
    """
    import time
    
    results = {}
    mx_cache = {}  # Cache pour éviter requêtes DNS répétées
    
    for email in emails:
        if not email or not isinstance(email, str):
            continue
        
        email_lower = email.strip().lower()
        
        # Validation sans MX (rapide)
        result = validate_email(email_lower, check_mx=False)
        
        # Si OK et MX demandé, faire MX check avec cache
        if check_mx and result["syntax"] and result["domain"]:
            domain = email_lower.split("@")[-1]
            
            if domain not in mx_cache:
                # Rate limit: pas plus d'une requête DNS par 0.1s
                time.sleep(0.1)
                mx_cache[domain] = check_mx_records(domain)
            
            result["mx"] = mx_cache[domain]
            if not result["mx"]:
                result["is_valid"] = False
                result["reason"] = "Domaine n'a pas d'enregistrements MX"
        
        results[email_lower] = result
    
    return results
