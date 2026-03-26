"""
SCRAPING_V1 - WebsiteScraper
Scraping direct des sites web d'entreprises :
  email, téléphone, adresse, description, réseaux sociaux, schema.org.
"""
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import logging
import re
from typing import List, Dict, Any, Optional, Set
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from sources.base_scraper import BaseScraper
from storage.models       import Prospect

logger = logging.getLogger(__name__)

_CONTACT_PATHS = [
    "/contact", "/contact-us", "/contact.html", "/contact.php",
    "/nous-contacter", "/contactez-nous",
    "/about", "/about-us", "/a-propos", "/qui-sommes-nous",
    "/mentions-legales", "/legal",
]

_CONTACT_KEYWORDS = {
    "contact", "about", "a-propos", "qui-sommes", "nous-contacter",
    "equipe", "team", "legal", "mentions", "coordonnees",
}


class WebsiteScraper(BaseScraper):
    source_name = "Website"

    def search(self, criteria: Dict[str, Any]) -> List[Prospect]:
        """Enrichit les prospects fournis via criteria['websites']."""
        enriched = []
        for url in criteria.get("websites", []):
            p = self.scrape_website(url)
            if p:
                enriched.append(p)
        return enriched

    # ──────────────────────────────────────────
    # Main method
    # ──────────────────────────────────────────

    def scrape_website(self, url: str) -> Optional[Prospect]:
        """
        Scrape un site web :
          1. Page d'accueil (meta, schema.org, emails/phones)
          2. Pages contact / about (jusqu'à 3)
          3. Agrège et retourne un Prospect normalisé
        """
        url    = self.clean_url(url)
        domain = urlparse(url).netloc
        if not url:
            return None

        all_emails: Set[str] = set()
        all_phones: Set[str] = set()
        address = description = name = ""
        social: Dict[str, str] = {}

        logger.debug(f"[Website] Scraping {domain}")

        # ── Homepage ──────────────────────────
        html = self._fetch(url)
        if not html:
            return None

        soup        = self.parse_html(html)
        text        = self.extract_text(html)
        schema      = self._extract_schema_org(soup)

        all_emails.update(self.extract_emails(text))
        all_phones.update(self.extract_phones(text))
        name        = self._extract_name(soup, domain, schema)
        description = self._extract_description(soup) or schema.get("description", "")
        social      = self._extract_socials(soup)

        # Phone / email from schema
        if schema.get("telephone"):
            all_phones.add(schema["telephone"])
        if schema.get("email"):
            all_emails.add(schema["email"].lower())

        # ── Contact / About pages ──────────────
        # FIX – Suppression du time.sleep manuel : CurlClient._rate_limit()
        # applique déjà un délai aléatoire (MIN_DELAY..MAX_DELAY) avant chaque
        # requête HTTP vers le même domaine. Doubler ce délai ici ralentissait
        # inutilement le scraping sans apporter de protection supplémentaire.
        # En contexte parallèle (enrichissement), DomainRateLimiter gère de
        # plus le délai inter-domaines — un sleep fixe ici serait redondant
        # ET non thread-safe (il bloquerait le thread entier sans coordination).
        for c_url in self._discover_contact_pages(soup, url)[:3]:
            c_html = self._fetch(c_url)
            if c_html:
                c_text = self.extract_text(c_html)
                all_emails.update(self.extract_emails(c_text))
                all_phones.update(self.extract_phones(c_text))
                if not address:
                    address = self._extract_address(self.parse_html(c_html))

        raw = {
            "nom_commercial": name or schema.get("name", domain),
            "raison_sociale": schema.get("legalName", ""),
            "website":        url,
            "email":          next(iter(all_emails), ""),
            "telephone":      next(iter(all_phones), ""),
            "adresse":        address or schema.get("address", ""),
            "description":    description,
            "linkedin_url":   social.get("linkedin", ""),
            "raw_text":       text[:2000],
            "source":         "Website",
        }
        return self.normalize_prospect(raw)

    # ──────────────────────────────────────────
    # Fetch
    # ──────────────────────────────────────────

    def _fetch(self, url: str) -> Optional[str]:
        return self.client.get(
            url,
            headers={
                "Referer":          "https://www.google.com/",
                "Accept-Language":  "fr-FR,fr;q=0.9,en-US;q=0.8",
                "DNT":              "1",
                "Sec-GPC":          "1",
            },
            timeout=20,
            retries=2,
        )

    # ──────────────────────────────────────────
    # Extraction helpers
    # ──────────────────────────────────────────

    def _extract_name(self, soup: BeautifulSoup, domain: str, schema: dict) -> str:
        # 1. Schema.org
        if schema.get("name"):
            return schema["name"]
        # 2. og:site_name
        og = soup.find("meta", {"property": "og:site_name"})
        if og and og.get("content"):
            return og["content"].strip()
        # 3. <title> (first part)
        title = soup.find("title")
        if title:
            raw = title.get_text(strip=True)
            for sep in [" | ", " - ", " – ", " — ", " :: "]:
                if sep in raw:
                    return raw.split(sep)[0].strip()
            return raw.strip()
        # 4. Domain fallback
        return domain.replace("www.", "").split(".")[0].capitalize()

    @staticmethod
    def _extract_description(soup: BeautifulSoup) -> str:
        for attr in [
            {"name": "description"},
            {"property": "og:description"},
            {"name": "twitter:description"},
        ]:
            meta = soup.find("meta", attr)
            if meta and meta.get("content"):
                return meta["content"].strip()[:500]
        return ""

    @staticmethod
    def _extract_address(soup: BeautifulSoup) -> str:
        el = soup.find(attrs={"itemprop": "address"})
        if el:
            return el.get_text(strip=True)[:200]
        adr = soup.find(class_=re.compile(r"adr|address|adresse", re.I))
        if adr:
            return adr.get_text(separator=" ", strip=True)[:200]
        return ""

    @staticmethod
    def _extract_socials(soup: BeautifulSoup) -> Dict[str, str]:
        social: Dict[str, str] = {}
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "linkedin.com/company" in href:
                social["linkedin"] = href
            elif "twitter.com/" in href or "x.com/" in href:
                social["twitter"] = href
            elif "facebook.com/" in href:
                social["facebook"] = href
        return social

    @staticmethod
    def _extract_schema_org(soup: BeautifulSoup) -> Dict[str, str]:
        result: Dict[str, str] = {}
        for script in soup.find_all("script", {"type": "application/ld+json"}):
            try:
                raw = script.string or ""
                if not raw.strip():
                    continue
                data = json.loads(raw)
                if isinstance(data, list):
                    data = data[0]
                if isinstance(data, dict):
                    t = data.get("@type", "")
                    if any(x in t for x in ["Organization", "LocalBusiness", "Corporation"]):
                        result["name"]        = data.get("name", "")
                        result["legalName"]   = data.get("legalName", "")
                        result["description"] = data.get("description", "")
                        result["telephone"]   = data.get("telephone", "")
                        result["email"]       = data.get("email", "")
                        addr = data.get("address", {})
                        if isinstance(addr, dict):
                            result["address"] = " ".join(filter(None, [
                                addr.get("streetAddress", ""),
                                addr.get("postalCode", ""),
                                addr.get("addressLocality", ""),
                                addr.get("addressCountry", ""),
                            ]))
                        break
            # FIX – Silent Failure: catch only expected JSON errors; let
            # unexpected exceptions (MemoryError, KeyboardInterrupt) propagate.
            except json.JSONDecodeError as e:
                logger.debug(f"[Website] Malformed LD+JSON skipped: {e}")
            except (TypeError, AttributeError, IndexError) as e:
                logger.debug(f"[Website] LD+JSON structure error: {e}")
        return result

    def _discover_contact_pages(
        self, soup: BeautifulSoup, base_url: str
    ) -> List[str]:
        """Returns contact URLs found via <a> links in the HTML."""
        found: Set[str] = set()
        base_domain     = urlparse(base_url).netloc

        for a in soup.find_all("a", href=True):
            href = a["href"].lower()
            text = a.get_text(strip=True).lower()
            if any(kw in href or kw in text for kw in _CONTACT_KEYWORDS):
                full = urljoin(base_url, a["href"])
                if urlparse(full).netloc == base_domain:
                    found.add(full)

        # Fallback: if no links found in HTML, try _CONTACT_PATHS grouped by stem.
        # Stop at the first variant that responds per stem group.
        if not found:
            from collections import defaultdict
            groups = defaultdict(list)
            for path in _CONTACT_PATHS:
                stem = re.sub(r"\.[a-z]+$", "", path)  # /contact.html -> /contact
                groups[stem].append(path)
            for stem, variants in groups.items():
                for path in variants:
                    c_url = urljoin(base_url, path)
                    c_html = self._fetch(c_url)
                    if c_html:
                        found.add(c_url)
                        break  # skip remaining variants (.html, .php, etc.)

        return list(found)[:5]