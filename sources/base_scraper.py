"""
SCRAPING_V1 - BaseScraper
Classe abstraite commune à tous les scrapers.
Fournit: CurlClient, parse_html, extract_emails/phones, normalize_prospect, _make_hash.

FIXES v2:
  - Regex téléphone élargie : France + Tunisie + Maroc + Belgique + Suisse + international
  - Extraction email améliorée : déobfuscation [at] / (at) / [@]
  - Nouvelle méthode extract_phones_from_html() : cherche aussi dans les attributs href/data-*
"""
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import hashlib
import logging
import re
from abc    import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Set

from bs4 import BeautifulSoup

from sources.curl_client import CurlClient
from storage.models      import Prospect

logger = logging.getLogger(__name__)

# Pre-compiled, length-bounded email validator (avoids ReDoS on adversarial input).
# FIX – Regex DoS: the original pattern used nested quantifiers on the domain part
# (e.g. [a-zA-Z0-9.\-]+) which, combined with backtracking on invalid inputs, can
# run in exponential time.  The rewrite uses possessive-style boundaries via a
# length cap enforced *before* the regex, and removes ambiguous overlapping groups.
_EMAIL_VALIDATE_RE = re.compile(
    r"^[a-zA-Z0-9._%+\-]{1,64}@[a-zA-Z0-9\-]{1,63}(?:\.[a-zA-Z0-9\-]{1,63})*\.[a-zA-Z]{2,10}$"
)

# Broad scan regex — used only to *find* candidate emails in free text.
# Validation (length caps, structure) is then done by _EMAIL_VALIDATE_RE.
_EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+\-]{1,64}@[a-zA-Z0-9.\-]{1,255}"
)

# Patterns obfusqués courants sur les sites web pour contourner les spambots
# ex: "contact [at] societe [dot] com" ou "contact(at)societe.fr"
_EMAIL_OBFUSCATED_RE = re.compile(
    r"([a-zA-Z0-9._%+\-]+)\s*[\[\(]at[\]\)]\s*([a-zA-Z0-9.\-]+)\s*[\[\(]dot[\]\)]\s*([a-zA-Z]{2,})",
    re.IGNORECASE,
)
_EMAIL_OBFUSCATED2_RE = re.compile(
    r"([a-zA-Z0-9._%+\-]+)\s*[\[\(]@[\]\)]\s*([a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})",
    re.IGNORECASE,
)

_INVALID_EMAILS = {
    "noreply", "no-reply", "donotreply", "example.com",
    "test@", "info@info", "admin@admin", ".png", ".jpg",
    ".gif", ".svg", "wpcf7", "wordpress", "schema.org",
    "sentry.io", "w3.org", "example.org",
}

# ─────────────────────────────────────────────
# PHONE REGEX — France + Tunisie + Maroc + Belgique + Suisse + International
# ─────────────────────────────────────────────
#
# Formats couverts :
#   France     : 0X XX XX XX XX  /  +33 X XX XX XX XX  /  +33(0)X...
#   Tunisie    : +216 XX XXX XXX  (8 chiffres après indicatif)
#   Maroc      : +212 X XX XX XX XX  (9 chiffres après indicatif)
#   Belgique   : +32 X XX XX XX  /  0X XX XX XX
#   Suisse     : +41 XX XXX XX XX
#   International : +[1-9][X]{6,14}
#
_PHONE_PATTERNS = [
    # France : 0[1-9] suivi de 4 groupes de 2 chiffres (séparateurs optionnels)
    re.compile(
        r"(?<!\d)"
        r"(?:\+33\s?(?:\(0\)\s?)?|0033\s?)?0[1-9]"
        r"(?:[\s.\-]?\d{2}){4}"
        r"(?!\d)"
    ),
    # Tunisie : +216 ou 00216 suivi de 8 chiffres
    re.compile(
        r"(?<!\d)"
        r"(?:\+216|00216)\s?"
        r"\d{2}[\s.\-]?\d{3}[\s.\-]?\d{3}"
        r"(?!\d)"
    ),
    # Maroc : +212 ou 00212 suivi de 9 chiffres
    re.compile(
        r"(?<!\d)"
        r"(?:\+212|00212)\s?"
        r"[5-9]\d{1}[\s.\-]?\d{2}[\s.\-]?\d{2}[\s.\-]?\d{2}"
        r"(?!\d)"
    ),
    # Belgique : +32 ou 0032 ou 0[2-9]
    re.compile(
        r"(?<!\d)"
        r"(?:\+32\s?|0032\s?|0[2-9])"
        r"(?:\d{1,2}[\s.\-]?\d{2}[\s.\-]?\d{2}[\s.\-]?\d{2})"
        r"(?!\d)"
    ),
    # Suisse : +41
    re.compile(
        r"(?<!\d)"
        r"(?:\+41|0041)\s?"
        r"\d{2}[\s.\-]?\d{3}[\s.\-]?\d{2}[\s.\-]?\d{2}"
        r"(?!\d)"
    ),
    # International générique : +[indicatif][6-14 chiffres]
    re.compile(
        r"(?<!\d)"
        r"\+[1-9]\d{0,2}[\s.\-]?\d{6,14}"
        r"(?!\d)"
    ),
]

# Longueurs min/max de chiffres acceptables par indicatif
_PHONE_MIN_DIGITS = 7
_PHONE_MAX_DIGITS = 15


class ScraperError(Exception):
    pass


class BaseScraper(ABC):
    """
    Classe abstraite commune.
    Tous les scrapers héritent de cette classe et utilisent self.client (CurlClient).
    """

    def __init__(self):
        self.client = CurlClient()

    @property
    @abstractmethod
    def source_name(self) -> str:
        pass

    @abstractmethod
    def search(self, criteria: Dict[str, Any]) -> List[Prospect]:
        pass

    # ──────────────────────────────────────────
    # HTML parsing
    # ──────────────────────────────────────────

    @staticmethod
    def parse_html(html: str) -> BeautifulSoup:
        return BeautifulSoup(html, "html.parser")

    @staticmethod
    def extract_text(html: str) -> str:
        """Extrait le texte brut depuis du HTML."""
        # FIX – Resource Leak: cap input size so that a giant HTML page cannot
        # exhaust memory, and call soup.decompose() to release the DOM tree
        # immediately rather than waiting for the GC.
        MAX_HTML = 5 * 1024 * 1024  # 5 MB hard cap
        if len(html) > MAX_HTML:
            html = html[:MAX_HTML]
        soup = BeautifulSoup(html, "html.parser")
        try:
            for tag in soup(["script", "style", "noscript", "meta", "link"]):
                tag.decompose()
            return " ".join(soup.get_text(separator=" ").split())
        finally:
            soup.decompose()

    # ──────────────────────────────────────────
    # EMAIL extraction — VERSION AMÉLIORÉE
    # ──────────────────────────────────────────

    @classmethod
    def extract_emails(cls, text: str) -> List[str]:
        """
        Extrait et filtre les emails valides d'un texte.
        Gère aussi les formats obfusqués : [at], (at), [@], [dot].
        """
        seen:  Set[str] = set()
        clean: List[str] = []

        # 1. Emails standards
        for e in _EMAIL_RE.findall(text):
            e = e.lower().strip(".,;:\"'")
            if e not in seen and cls._is_valid_email(e):
                seen.add(e)
                clean.append(e)

        # 2. Emails obfusqués : "contact [at] societe [dot] com"
        for m in _EMAIL_OBFUSCATED_RE.finditer(text):
            e = f"{m.group(1)}@{m.group(2)}.{m.group(3)}".lower()
            if e not in seen and cls._is_valid_email(e):
                seen.add(e)
                clean.append(e)

        # 3. Emails obfusqués : "contact [@] societe.fr"
        for m in _EMAIL_OBFUSCATED2_RE.finditer(text):
            e = f"{m.group(1)}@{m.group(2)}".lower()
            if e not in seen and cls._is_valid_email(e):
                seen.add(e)
                clean.append(e)

        return clean

    @staticmethod
    def _is_valid_email(email: str) -> bool:
        if not email or "@" not in email:
            return False
        # FIX – Regex DoS: enforce hard length cap *before* running the regex so
        # that a multi-kilobyte adversarial string never reaches the engine.
        if len(email) > 254:
            return False
        if not _EMAIL_VALIDATE_RE.match(email):
            return False
        return not any(p in email for p in _INVALID_EMAILS)

    @classmethod
    def extract_emails_from_html(cls, soup: BeautifulSoup) -> List[str]:
        """
        Extraction emails depuis le HTML structuré :
        cherche aussi dans les attributs href="mailto:..." et data-email.
        """
        found: Set[str] = set()
        results: List[str] = []

        # 1. Liens mailto
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("mailto:"):
                e = href.replace("mailto:", "").split("?")[0].strip().lower()
                if e not in found and cls._is_valid_email(e):
                    found.add(e)
                    results.append(e)

        # 2. Attributs data-email / data-mail
        for el in soup.find_all(attrs={"data-email": True}):
            e = el["data-email"].strip().lower()
            if e not in found and cls._is_valid_email(e):
                found.add(e)
                results.append(e)
        for el in soup.find_all(attrs={"data-mail": True}):
            e = el["data-mail"].strip().lower()
            if e not in found and cls._is_valid_email(e):
                found.add(e)
                results.append(e)

        # 3. Texte brut de la page
        text = soup.get_text(separator=" ")
        for e in cls.extract_emails(text):
            if e not in found:
                found.add(e)
                results.append(e)

        return results

    # ──────────────────────────────────────────
    # PHONE extraction — VERSION AMÉLIORÉE
    # ──────────────────────────────────────────

    @classmethod
    def extract_phones(cls, text: str) -> List[str]:
        """
        Extrait les numéros de téléphone depuis un texte.
        Couvre France, Tunisie, Maroc, Belgique, Suisse, international.
        """
        seen:  Set[str] = set()
        clean: List[str] = []

        for pattern in _PHONE_PATTERNS:
            for match in pattern.finditer(text):
                raw    = match.group(0).strip()
                digits = re.sub(r"\D", "", raw)

                if not (_PHONE_MIN_DIGITS <= len(digits) <= _PHONE_MAX_DIGITS):
                    continue
                if digits in seen:
                    continue
                # Éviter les faux positifs (codes postaux, SIREN, etc.)
                if cls._is_likely_not_phone(raw, digits):
                    continue

                seen.add(digits)
                clean.append(cls._normalize_phone(raw))

        return clean

    @classmethod
    def extract_phones_from_html(cls, soup: BeautifulSoup) -> List[str]:
        """
        Extraction téléphones depuis le HTML structuré.
        Cherche dans href="tel:...", data-phone, data-tel, itemprop="telephone".
        Contourne le masquage JavaScript partiel de PagesJaunes et similaires.
        """
        found:   Set[str] = set()
        results: List[str] = []

        # 1. Liens tel:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("tel:"):
                raw    = href.replace("tel:", "").strip()
                digits = re.sub(r"\D", "", raw)
                if _PHONE_MIN_DIGITS <= len(digits) <= _PHONE_MAX_DIGITS:
                    if digits not in found:
                        found.add(digits)
                        results.append(cls._normalize_phone(raw))

        # 2. Attributs data-phone / data-tel / data-telephone
        for attr in ["data-phone", "data-tel", "data-telephone", "data-numero"]:
            for el in soup.find_all(attrs={attr: True}):
                raw    = el[attr].strip()
                digits = re.sub(r"\D", "", raw)
                if _PHONE_MIN_DIGITS <= len(digits) <= _PHONE_MAX_DIGITS:
                    if digits not in found:
                        found.add(digits)
                        results.append(cls._normalize_phone(raw))

        # 3. itemprop="telephone" (Schema.org)
        for el in soup.find_all(attrs={"itemprop": "telephone"}):
            raw    = el.get("content", "") or el.get_text(strip=True)
            digits = re.sub(r"\D", "", raw)
            if _PHONE_MIN_DIGITS <= len(digits) <= _PHONE_MAX_DIGITS:
                if digits not in found:
                    found.add(digits)
                    results.append(cls._normalize_phone(raw))

        # 4. Texte brut
        text = soup.get_text(separator=" ")
        for phone in cls.extract_phones(text):
            digits = re.sub(r"\D", "", phone)
            if digits not in found:
                found.add(digits)
                results.append(phone)

        return results

    @staticmethod
    def _normalize_phone(raw: str) -> str:
        """Normalise un numéro : supprime espaces superflus, garde le format."""
        raw = raw.strip()
        # Remplace les séparateurs multiples par un seul espace
        raw = re.sub(r"[\s.\-]{2,}", " ", raw)
        return raw

    @staticmethod
    def _is_likely_not_phone(raw: str, digits: str) -> bool:
        """
        Heuristique pour rejeter les faux positifs (SIREN, codes postaux, années...).
        """
        # SIREN = exactement 9 chiffres sans indicatif +
        if len(digits) == 9 and not raw.startswith("+"):
            return True
        # Codes postaux FR = exactement 5 chiffres
        if len(digits) == 5:
            return True
        # Années (1900-2099)
        if re.match(r"^(19|20)\d{2}$", digits):
            return True
        return False

    # ──────────────────────────────────────────
    # Normalization
    # ──────────────────────────────────────────

    def normalize_prospect(self, raw: Dict[str, Any]) -> Prospect:
        """
        Convertit un dict brut en Prospect normalisé.
        Calcule le hash de déduplication.
        """
        p = Prospect.from_dict(raw)
        p.source     = p.source or self.source_name
        p.hash_dedup = self._make_hash(
            p.email, p.website, p.telephone, p.nom_commercial
        )
        return p

    # ──────────────────────────────────────────
    # Deduplication hash
    # ──────────────────────────────────────────

    @staticmethod
    def _make_hash(email: str, website: str, phone: str, name: str) -> str:
        """
        Génère un hash de déduplication.
        Priorité: email > domaine web > téléphone > nom normalisé.
        """
        from urllib.parse import urlparse
        candidates = []

        if email:
            candidates.append(f"email:{email.lower().strip()}")

        if website:
            try:
                domain = urlparse(
                    website if "://" in website else "https://" + website
                ).netloc.lower().lstrip("www.")
                if domain:
                    candidates.append(f"domain:{domain}")
            except Exception:
                pass

        if phone:
            digits = re.sub(r"\D", "", phone)
            if len(digits) >= 7:
                candidates.append(f"phone:{digits[-9:]}")

        if name:
            norm_name = re.sub(r"\W+", "", name.lower())
            candidates.append(f"name:{norm_name}")

        # Utilise le candidat de plus haute priorité (email > domaine > téléphone > nom).
        # Les candidats sont ajoutés dans cet ordre, donc candidates[0] est toujours
        # le plus discriminant disponible.
        # FIX : l'ancienne écriture `"|".join(candidates[:1])` construisait une liste
        # de 4 candidats puis la tronquait à 1 — trompeur pour le lecteur.
        raw = candidates[0] if candidates else name.lower()
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    # ──────────────────────────────────────────
    # URL helpers
    # ──────────────────────────────────────────

    @staticmethod
    def clean_url(url: str) -> str:
        if not url:
            return ""
        url = url.strip().rstrip("/")
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        # FIX – Regex DoS: cap URL length before running the regex so a
        # multi-kilobyte adversarial string cannot cause catastrophic backtracking.
        if len(url) > 2048:
            return ""
        if not re.match(r"https?://[^\s]{2,2000}", url):
            return ""
        return url