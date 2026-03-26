"""
SCRAPING_V1 - Cleaner (ProspectCleaner)
Nettoyage, normalisation et validation des Prospect collectés.
nettoie les champs, rejette les entrées sans nom, valide les emails, formate les téléphones,
normalise les tailles d'entreprise, etc. Fournit un rapport de nettoyage.
eviter les risques de DoS sur les regex en limitant la longueur des champs avant validation.
eviter les valeurs par défaut trompeuses (ex: "PME") qui peuvent fausser les analyses ultérieures en utilisant des placeholders neutres (ex: "") pour les données inconnues.
protéger contre les faux positifs d'emails (ex: adresses techniques, domaines génériques, agrégateurs) en maintenant une liste de patterns interdits.
protéger contre les faux positifs de téléphones en imposant des formats stricts et en normalisant les numéros selon les indicatifs.
"""
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import re
import logging
from typing import List, Optional

from storage.models import Prospect

logger = logging.getLogger(__name__)

# FIX – Regex DoS: add explicit length bounds on each segment so the engine
# cannot backtrack exponentially on a crafted adversarial string.
_EMAIL_RE = re.compile(
    r"^[a-zA-Z0-9._%+\-]{1,64}@[a-zA-Z0-9\-]{1,63}(?:\.[a-zA-Z0-9\-]{1,63})*\.[a-zA-Z]{2,10}$"
)
_INVALID_EMAIL = [
    # Adresses techniques / robots
    "noreply", "no-reply", "donotreply", "dont-reply",
    # Domaines de test / génériques
    "example.com", "example.org", "example.net",
    "test@", "info@info", "admin@admin",
    # Extensions image/média (faux positifs du scraping)
    ".png", ".jpg", ".gif", ".svg", ".webp", ".ico",
    # Annuaires / agrégateurs qui polluent les résultats DDG
    "duckduckgo.com", "error-lite@", "noreply@duckduckgo",
    "contact@infonet.fr", "contact@actulegales.fr",
    "info@northdata.com", "contact@lagazettefrance.fr",
    "contact@societe.com", "info@pappers.fr",
    # Adresses techniques WordPress / CMS
    "wpcf7", "wordpress",
    # Standards web
    "schema.org", "w3.org", "sentry.io",
]

_SIZE_RANGES = {"TPE": (1, 9), "PME": (10, 249), "ETI": (250, 4999), "GE": (5000, 1e9)}

_DEFAULT_SECTOR  = "Non spécifié"
# FIX – Default Values Risk: using "PME" as a hard default silently overwrites
# genuinely unknown size data and inflates PME counts in reports.  An empty
# string signals "unknown" more honestly; callers that need a display value
# should substitute their own placeholder.
_DEFAULT_SIZE    = ""
_DEFAULT_COUNTRY = "France"


class ProspectCleaner:
    """
    Nettoie, normalise et valide des objets Prospect.

    Usage:
        cleaner = ProspectCleaner()
        cleaned = cleaner.clean_all(prospects)
        print(cleaner.get_report())
    """

    def __init__(self):
        self._n_input    = 0
        self._n_skipped  = 0
        self._n_cleaned  = 0

    def clean_all(self, prospects: List[Prospect]) -> List[Prospect]:
        """Nettoie une liste de Prospect. Rejette ceux sans nom."""
        self._n_input   = len(prospects)
        self._n_skipped = 0
        result          = []

        for p in prospects:
            cleaned = self._clean_one(p)
            if cleaned is None:
                self._n_skipped += 1
            else:
                result.append(cleaned)

        self._n_cleaned = len(result)
        logger.info(
            f"[Cleaner] {self._n_cleaned} nettoyés, "
            f"{self._n_skipped} rejetés sur {self._n_input}"
        )
        return result

    # Alias (backward compat with orchestrator)
    def clean_batch(self, prospects) -> List[Prospect]:
        # Accept list of dicts or Prospect
        objs = []
        for p in prospects:
            if isinstance(p, dict):
                objs.append(Prospect.from_dict(p))
            else:
                objs.append(p)
        return self.clean_all(objs)

    def _clean_one(self, p: Prospect) -> Optional[Prospect]:
        # ── Nom (obligatoire) ──────────────────
        name = self._clean_text(p.nom_commercial)
        if not name:
            return None
        p.nom_commercial = name

        # ── Raison sociale ─────────────────────
        p.raison_sociale = self._clean_text(p.raison_sociale) or name

        # ── Email ──────────────────────────────
        p.email       = self._clean_email(p.email)
        p.email_valid = self._is_valid_email(p.email)

        # ── Téléphone ──────────────────────────
        p.telephone = self._clean_phone(p.telephone)

        # ── Website ────────────────────────────
        p.website       = self._clean_url(p.website)
        p.website_active = bool(p.website)

        # ── Localisation ───────────────────────
        p.adresse = self._clean_text(p.adresse)
        p.ville   = self._title_place(self._clean_text(p.ville))
        p.region  = self._title_place(self._clean_text(p.region))
        p.pays    = self._title_place(self._clean_text(p.pays)) or _DEFAULT_COUNTRY

        # ── Secteur ────────────────────────────
        p.secteur_activite = self._clean_text(p.secteur_activite) or _DEFAULT_SECTOR

        # ── Type ───────────────────────────────
        p.type_entreprise = self._clean_text(p.type_entreprise)

        # ── Taille ─────────────────────────────
        p.taille_entreprise = self._normalize_size(
            p.taille_entreprise, p.nombre_employes
        ) or _DEFAULT_SIZE

        # ── CA ─────────────────────────────────
        if p.chiffre_affaires is not None:
            try:
                ca = float(p.chiffre_affaires)
                p.chiffre_affaires = ca if ca >= 0 else None
            except (ValueError, TypeError):
                p.chiffre_affaires = None

        # ── Identifiants légaux ─────────────────
        p.siren = re.sub(r"\D", "", str(p.siren or ""))[:9]
        p.siret = re.sub(r"\D", "", str(p.siret or ""))[:14]

        # ── Description ────────────────────────
        p.description = self._clean_text(p.description)[:500]

        # ── LinkedIn ───────────────────────────
        p.linkedin_url = self._clean_url(p.linkedin_url)

        return p

    # ──────────────────────────────────────────
    # Field cleaners
    # ──────────────────────────────────────────

    # Articles et prépositions français/courants qui ne doivent PAS être
    # capitalisés au milieu d'un nom de lieu composé.
    _LOWERCASE_PARTICLES = frozenset({
        "sur", "sous", "en", "de", "du", "des", "le", "la", "les",
        "au", "aux", "et", "à", "l", "d",
    })

    @classmethod
    def _title_place(cls, text: str) -> str:
        """
        Capitalise un nom de lieu en respectant les articles et prépositions.

        Contrairement à str.title() qui capitalise chaque mot sans distinction,
        cette méthode laisse les particules en minuscules sauf en début de nom.

        Exemples :
          "SAINT-DENIS"          → "Saint-Denis"
          "l'isle-sur-la-sorgue" → "L'Isle-sur-la-Sorgue"
          "aix-en-provence"      → "Aix-en-Provence"
          "île-de-france"        → "Île-de-France"
        """
        if not text:
            return text
        text = text.lower()
        # Sépare sur espace, tiret et apostrophe en conservant les séparateurs
        tokens = re.split(r"([\s\-']+)", text)
        result = []
        first_word = True
        for token in tokens:
            if re.match(r"[\s\-']+", token):
                result.append(token)
            else:
                if first_word or token not in cls._LOWERCASE_PARTICLES:
                    result.append(token.capitalize())
                else:
                    result.append(token)
                first_word = False
        return "".join(result)

    @staticmethod
    def _clean_text(value) -> str:
        if not value:
            return ""
        text = str(value).strip()
        text = re.sub(r"\s+", " ", text)
        text = text.replace("&amp;", "&").replace("&nbsp;", " ").replace("\xa0", " ")
        return text.strip()

    @classmethod
    def _clean_email(cls, email: str) -> str:
        if not email:
            return ""
        email = str(email).strip().lower()
        email = re.sub(r"^mailto:", "", email)
        return email if cls._is_valid_email(email) else ""

    @staticmethod
    def _is_valid_email(email: str) -> bool:
        if not email:
            return False
        # FIX – Regex DoS: enforce hard length cap before running the regex.
        if len(email) > 254:
            return False
        if not _EMAIL_RE.match(email):
            return False
        return not any(p in email for p in _INVALID_EMAIL)

    @staticmethod
    def _clean_phone(phone: str) -> str:
        if not phone:
            return ""
        # Supprimer le préfixe tel: (liens HTML)
        phone = re.sub(r"^tel:", "", str(phone).strip(), flags=re.IGNORECASE)
        # Garder uniquement chiffres, +, espaces, points, tirets, parenthèses
        phone = re.sub(r"[^\d+\s.\-()]", "", phone).strip()

        digits = re.sub(r"\D", "", phone)
        if len(digits) < 7 or len(digits) > 15:
            return ""

        # ── Normalisation E.164-friendly par indicatif ─────────────────
        # France : 0X... → +33 X XX XX XX XX
        if re.match(r"^0[1-9](\d{8})$", digits):
            return "+33 " + digits[1:2] + " " + " ".join(
                digits[2+i:2+i+2] for i in range(0, 8, 2)
            )
        # France déjà formatée +33...
        if digits.startswith("33") and len(digits) == 11:
            local = digits[2:]
            return "+33 " + local[0] + " " + " ".join(
                local[1+i:1+i+2] for i in range(0, 8, 2)
            )
        # Tunisie +216 : 8 chiffres après indicatif
        if digits.startswith("216") and len(digits) == 11:
            d = digits[3:]
            return "+216 " + d[:2] + " " + d[2:5] + " " + d[5:]
        # Maroc +212 : 9 chiffres après indicatif
        if digits.startswith("212") and len(digits) == 12:
            d = digits[3:]
            return "+212 " + d[0] + d[1:3] + " " + d[3:5] + " " + d[5:7] + " " + d[7:]
        # Belgique +32 : 8-9 chiffres après indicatif
        if digits.startswith("32") and 9 <= len(digits) <= 11:
            d = digits[2:]
            return "+32 " + " ".join(d[i:i+2] for i in range(0, len(d), 2))
        # Suisse +41 : 9 chiffres après indicatif
        if digits.startswith("41") and len(digits) == 11:
            d = digits[2:]
            return "+41 " + d[:2] + " " + d[2:5] + " " + d[5:7] + " " + d[7:]

        # Fallback : retourner tel quel nettoyé si longueur valide
        return phone if len(digits) >= 7 else ""

    @staticmethod
    def _clean_url(url: str) -> str:
        if not url:
            return ""
        url = url.strip().rstrip("/")
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        # FIX – Regex DoS: cap URL length before running the pattern to prevent
        # catastrophic backtracking on a crafted multi-kilobyte input string.
        if len(url) > 2048:
            return ""
        return url if re.match(r"https?://[^\s]{2,2000}", url) else ""

    @staticmethod
    def _normalize_size(taille: str, employes) -> str:
        t = str(taille or "").upper().strip()
        if t in _SIZE_RANGES:
            return t
        if employes:
            try:
                n = int(employes)
                for size, (lo, hi) in _SIZE_RANGES.items():
                    if lo <= n <= hi:
                        return size
            except (ValueError, TypeError):
                pass
        tl = t.lower()
        if any(x in tl for x in ["tpe", "micro", "1-9"]):    return "TPE"
        if any(x in tl for x in ["pme", "10-", "petite"]):   return "PME"
        if any(x in tl for x in ["eti", "250-"]):             return "ETI"
        if any(x in tl for x in ["ge", "grand", "5000"]):    return "GE"
        return ""

    # ──────────────────────────────────────────
    # Report
    # ──────────────────────────────────────────

    def get_report(self) -> str:
        rate = (
            round(100 * self._n_cleaned / self._n_input, 1)
            if self._n_input else 0
        )
        return (
            f"\n[Cleaner Report]\n"
            f"  Input   : {self._n_input}\n"
            f"  Cleaned : {self._n_cleaned} ({rate}%)\n"
            f"  Skipped : {self._n_skipped} (no name)\n"
        )