"""
SCRAPING_V1 - DirectoryScraper
Scraping des annuaires publics et plateformes B2B :
  - annuaire-entreprises.data.gouv.fr (API officielle)
  - PagesJaunes.fr
  - Kompass (B2B international)
  - Europages (annuaire B2B européen)
  - Verif.com (données légales + contacts)

CORRECTIONS v3 (suite aux audits) :

  FIX A — URLs dupliquées supprimées
    AVANT : VERIF_BASE_URL et VERIF_SEARCH_URL redéfinis localement,
            EUROPAGES_BASE redéfini localement (différait de settings.EUROPAGES_BASE_URL).
            -> Toute modification dans settings.py était ignorée.
    APRÈS : importées exclusivement depuis config.settings (source unique).

  FIX B — Fallback zone_geographique manquant
    AVANT : criteria.get("localisation", {}).get("villes") uniquement.
            -> Si DirectoryScraper appelé sans orchestrator (search_config.json brut),
               les villes issues de zone_geographique n'étaient pas transmises.
    APRÈS : fallback identique à OpenDataScraper :
            _loc = criteria.get("localisation") or criteria.get("zone_geographique") or {}

  FIX C — Valeurs par défaut des sous-sources
    AVANT : _dir_cfg.get("kompass", True) — activait Kompass si SOURCES_CONFIG vide
    APRÈS : toutes les sous-sources désactivées dans settings utilisent False en fallback.

  FIX D — Erreurs Kompass loggées en DEBUG (invisibles en production)
    AVANT : logger.debug(f"[Dir/Kompass] Erreur...")
    APRÈS : logger.warning(f"[Dir/Kompass] Erreur...")

  FIX E — Constantes de pagination lues depuis settings.COLLECTION_LIMITS
    AVANT : ANNUAIRE_MAX_NAF = 8, etc. codées en dur comme attributs de classe
    APRÈS : propriétés déléguant à COLLECTION_LIMITS (source unique)
"""
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import random
import re
import time
from typing import List, Dict, Any, Optional
from urllib.parse import urlencode, quote_plus, urlparse, parse_qs, urljoin

from bs4 import BeautifulSoup

from sources.base_scraper         import BaseScraper
from sources.sirene_query_builder import SireneQueryBuilder
from storage.models               import Prospect
from config.settings              import (
    DATA_GOUV_URL,
    COLLECTION_LIMITS,
    SOURCES_CONFIG,
    # FIX A : URLs importées depuis settings — plus de redéfinition locale
    EUROPAGES_BASE_URL,
    VERIF_BASE_URL,
    VERIF_SEARCH_URL,
)

# ---------------------------------------------------------------------------
# FIX – Lazy singleton pour _qbuilder (même pattern que open_data_scraper.py)
# L'ancienne ligne `_qbuilder = SireneQueryBuilder()` chargeait les JSON de
# mapping au moment de l'import, même si DirectoryScraper n'était pas utilisé.
# Une erreur dans SireneQueryBuilder.__init__ plantait tout le module.
# ---------------------------------------------------------------------------
import threading as _threading

_qbuilder:      "SireneQueryBuilder | None" = None
_qbuilder_lock: _threading.Lock             = _threading.Lock()


def _get_qbuilder() -> "SireneQueryBuilder":
    global _qbuilder
    if _qbuilder is None:
        with _qbuilder_lock:
            if _qbuilder is None:
                _qbuilder = SireneQueryBuilder()
    return _qbuilder

logger = logging.getLogger(__name__)


def _is_france_target(pays: List[str]) -> bool:
    if not pays:
        return True
    return any(str(p).strip().lower() == "france" for p in pays)

_EXCLUDED_DOMAINS = {
    "pagesjaunes.fr", "facebook.com", "twitter.com", "instagram.com",
    "linkedin.com", "youtube.com", "google.com", "apple.com",
    "kompass.com", "europages.fr", "europages.com",
    "verif.com", "societe.com", "infogreffe.fr",
}

# ─────────────────────────────────────────────
# Constantes Europages
# FIX A : EUROPAGES_BASE supprimé (était un doublon de settings.EUROPAGES_BASE_URL)
# Le code utilise maintenant EUROPAGES_BASE_URL importé depuis settings.
# ─────────────────────────────────────────────

_EUROPAGES_SECTOR_SLUGS: Dict[str, str] = {
    "Informatique":              "informatique-logiciels",
    "Télécommunications":        "telecommunications",
    "Conseil IT":                "conseil-informatique",
    "E-commerce":                "commerce-electronique",
    "BTP":                       "construction-batiment",
    "Industrie":                 "industrie-manufacturiere",
    "Transport & Logistique":    "transport-logistique",
    "Commerce de gros":          "commerce-de-gros",
    "Finance":                   "services-financiers",
    "Marketing & Communication": "marketing-publicite",
    "Éducation & Formation":     "formation-education",
    "Santé":                     "sante-bien-etre",
    "Énergie":                   "energie",
    "Agriculture":               "agriculture",
}

_EUROPAGES_COUNTRY_CODES: Dict[str, str] = {
    "France": "FR", "Belgique": "BE", "Suisse": "CH",
    "Espagne": "ES", "Italie": "IT", "Allemagne": "DE",
    "Maroc": "MA", "Tunisie": "TN",
}

# ─────────────────────────────────────────────
# Constantes Verif
# FIX A : VERIF_BASE_URL et VERIF_SEARCH_URL supprimés (redéfinitions locales).
#         Importés depuis config.settings.
# ─────────────────────────────────────────────

_VERIF_EXCLUDED_DOMAINS = {
    "verif.com", "societe.com", "infogreffe.fr", "pappers.fr",
    "bodacc.fr", "annuaire-entreprises.data.gouv.fr",
    "facebook.com", "twitter.com", "linkedin.com",
    "instagram.com", "youtube.com", "google.com",
}


class DirectoryScraper(BaseScraper):
    source_name = "Directory"

    # ── FIX E : constantes de pagination via propriétés → settings.COLLECTION_LIMITS ──
    @property
    def ANNUAIRE_MAX_NAF(self) -> int:
        return COLLECTION_LIMITS.get("annuaire_max_naf", 8)

    @property
    def ANNUAIRE_MAX_PAGES(self) -> int:
        return COLLECTION_LIMITS.get("annuaire_max_pages", 4)

    @property
    def ANNUAIRE_PER_PAGE(self) -> int:
        return COLLECTION_LIMITS.get("annuaire_per_page", 25)

    @property
    def ANNUAIRE_GEO_CHUNK(self) -> int:
        return COLLECTION_LIMITS.get("annuaire_geo_chunk", 20)

    @property
    def ANNUAIRE_SLEEP(self) -> float:
        return COLLECTION_LIMITS.get("annuaire_sleep", 0.4)

    def search(self, criteria: Dict[str, Any]) -> List[Prospect]:
        prospects: List[Prospect] = []
        max_per  = COLLECTION_LIMITS.get("max_prospects_per_source", 100)
        secteurs = criteria.get("secteurs_activite", [])

        # FIX B : fallback zone_geographique (comme OpenDataScraper)
        # AVANT : criteria.get("localisation", {}).get("villes") seulement
        # APRÈS : lecture cohérente des deux formats (orchestrateur et brut)
        _loc  = criteria.get("localisation") or criteria.get("zone_geographique") or {}
        villes  = _loc.get("villes",  []) or criteria.get("villes",  [])
        regions = _loc.get("regions", []) or criteria.get("regions", [])
        pays    = _loc.get("pays",    []) or criteria.get("pays",    [])

        types    = criteria.get("types_entreprise", [])
        keywords = criteria.get("keywords", secteurs or ["informatique"])

        # Raccourci vers la config des sous-sources directory
        _dir_cfg = SOURCES_CONFIG.get("directory", {})

        # ── 1. annuaire-entreprises.data.gouv.fr ─────────────────────
        # FIX C : .get("data_gouv_dir", False) — source active uniquement si explicitement True
        if _dir_cfg.get("data_gouv_dir", False) and _is_france_target(pays):
            taille_cfg  = criteria.get("taille_entreprise", {})
            tailles_cat = (
                criteria.get("tailles_entreprise", [])
                or criteria.get("tailles_categorie", [])
                or taille_cfg.get("categories", [])
            )
            codes_naf = criteria.get("codes_naf") or _get_qbuilder()._map_sectors(secteurs)
            codes_cp  = _get_qbuilder()._map_geo(
                regions = regions,
                villes  = villes,
            )
            for kw in (keywords[:4] or [None]):
                try:
                    res = self._search_annuaire_gouv(
                        keyword       = kw,
                        codes_naf     = codes_naf,
                        tailles_cat   = tailles_cat,
                        codes_postaux = codes_cp,
                    )
                    prospects.extend(res[:max_per])
                    logger.info(f"[Dir/Gouv] kw='{kw}' -> {len(res)}")
                    time.sleep(random.uniform(0.8, 1.5))
                except Exception as e:
                    logger.warning(f"[Dir/Gouv] Erreur '{kw}': {e}")
        else:
            logger.info("[Dir/Gouv] desactive (SOURCES_CONFIG)")

        # ── 2. PagesJaunes ───────────────────────────────────────────
        # FIX C : .get("pagesjaunes", False)
        if _dir_cfg.get("pagesjaunes", False) and _is_france_target(pays):
            for kw in keywords[:3]:
                for ville in (villes[:2] or ["Paris"]):
                    try:
                        res = self._scrape_pagesjaunes(kw, ville)
                        prospects.extend(res[:max_per])
                        logger.info(f"[Dir/PJ] '{kw}' / {ville} -> {len(res)}")
                        time.sleep(random.uniform(2.0, 4.0))
                    except Exception as e:
                        logger.warning(f"[Dir/PJ] Erreur '{kw}': {e}")
        else:
            logger.info("[Dir/PJ] desactive (SOURCES_CONFIG)")

        # ── 3. Kompass ────────────────────────────────────────────────
        # FIX C : .get("kompass", False)
        if _dir_cfg.get("kompass", False):
            for kw in keywords[:2]:
                target_pays = pays[:1] if pays else ["France"]
                try:
                    res = self._scrape_kompass(kw, target_pays)
                    prospects.extend(res[:max_per])
                    logger.info(f"[Dir/Kompass] '{kw}' -> {len(res)}")
                    time.sleep(random.uniform(2.5, 5.0))
                except Exception as e:
                    # FIX D : WARNING au lieu de DEBUG (visible en production)
                    logger.warning(f"[Dir/Kompass] Erreur '{kw}': {e}")
        else:
            logger.info("[Dir/Kompass] desactive (SOURCES_CONFIG)")

        # ── 4. Europages ─────────────────────────────────────────────
        # FIX C : .get("europages", False)
        if _dir_cfg.get("europages", False):
            ep_max    = COLLECTION_LIMITS.get("europages_max_par_run", 100)
            ep_pages  = COLLECTION_LIMITS.get("europages_max_pages", 3)
            for secteur in secteurs[:4]:
                slug = _EUROPAGES_SECTOR_SLUGS.get(secteur)
                if not slug:
                    continue
                for country in (pays or ["France"])[:2]:
                    cc = _EUROPAGES_COUNTRY_CODES.get(country, "FR")
                    try:
                        res = self._scrape_europages_sector(slug, cc, max_pages=ep_pages)
                        prospects.extend(res[:ep_max])
                        logger.info(f"[Dir/Europages] '{secteur}' / {country} -> {len(res)}")
                        time.sleep(random.uniform(2.5, 5.0))
                    except Exception as e:
                        logger.warning(f"[Dir/Europages] Erreur '{secteur}': {e}")

            for kw in keywords[:2]:
                for country in (pays or ["France"])[:2]:
                    cc = _EUROPAGES_COUNTRY_CODES.get(country, "FR")
                    try:
                        res = self._scrape_europages_keyword(kw, cc, max_pages=min(ep_pages, 2))
                        prospects.extend(res[:ep_max])
                        logger.info(f"[Dir/Europages] keyword '{kw}' / {country} -> {len(res)}")
                        time.sleep(random.uniform(2.0, 4.0))
                    except Exception as e:
                        logger.warning(f"[Dir/Europages] Erreur keyword '{kw}': {e}")
        else:
            logger.info("[Dir/Europages] desactive (SOURCES_CONFIG)")

        # ── 5. Verif.com (France uniquement) ─────────────────────────
        # FIX C : .get("verif", False)
        if _dir_cfg.get("verif", False) and _is_france_target(pays):
            vf_max    = COLLECTION_LIMITS.get("verif_max_par_run", 100)
            vf_detail = COLLECTION_LIMITS.get("verif_max_detail_pages", 15)
            search_terms = (keywords + secteurs)[:5]
            for term in search_terms[:3]:
                for loc in (villes[:2] or ["Paris"]):
                    try:
                        res = self._scrape_verif(
                            query=term, ville=loc,
                            types_forme=types, max_detail=vf_detail,
                        )
                        prospects.extend(res[:vf_max])
                        logger.info(f"[Dir/Verif] '{term}' / {loc} -> {len(res)}")
                        time.sleep(random.uniform(2.0, 4.0))
                    except Exception as e:
                        logger.warning(f"[Dir/Verif] Erreur '{term}': {e}")
        else:
            logger.info("[Dir/Verif] desactive (SOURCES_CONFIG)")

        logger.info(f"[Directory] Total brut: {len(prospects)}")
        return prospects

    # ══════════════════════════════════════════
    # annuaire-entreprises.data.gouv.fr
    # ══════════════════════════════════════════

    def _search_annuaire_gouv(
        self,
        keyword:        Optional[str]  = None,
        codes_naf:      List[str]      = None,
        tailles_cat:    List[str]      = None,
        codes_postaux:  List[str]      = None,
        per_page:       int            = 25,
    ) -> List[Prospect]:
        results:     List[Prospect] = []
        seen_sirens: set            = set()

        api_cats = [c for c in (tailles_cat or []) if c in ("PME", "ETI", "GE")]
        if not api_cats:
            api_cats = [None]

        naf_list = (codes_naf or [])[:self.ANNUAIRE_MAX_NAF] or [None]

        cp_complets  = [cp for cp in (codes_postaux or []) if len(cp) == 5 and cp.isdigit()]
        cp_prefixes  = [cp for cp in (codes_postaux or []) if len(cp) < 5]

        cp_chunks: List[Optional[str]] = []
        if cp_complets:
            for i in range(0, len(cp_complets), self.ANNUAIRE_GEO_CHUNK):
                cp_chunks.append(",".join(cp_complets[i:i + self.ANNUAIRE_GEO_CHUNK]))
        else:
            cp_chunks = [None]

        if cp_prefixes:
            logger.info(
                f"[Dir/Gouv] {len(cp_prefixes)} prefixe(s) court(s) ignores par l'API "
                f"(ex: {cp_prefixes[:3]}) -> filtre post-traitement active"
            )

        for naf in naf_list:
            for cat in api_cats:
                for cp_chunk in cp_chunks:
                    batch = self._annuaire_paginated(
                        keyword  = keyword,
                        naf      = naf,
                        cat      = cat,
                        cp_chunk = cp_chunk,
                        per_page = per_page,
                    )
                    nb_added = 0
                    for item in batch:
                        siren = item.get("siren", "")
                        if siren and siren in seen_sirens:
                            continue
                        if siren:
                            seen_sirens.add(siren)

                        if cp_prefixes and not cp_complets:
                            siege   = item.get("siege", {}) or {}
                            item_cp = siege.get("code_postal", "") or ""
                            if not any(item_cp.startswith(pfx) for pfx in cp_prefixes):
                                continue

                        p = self._parse_annuaire_item(item)
                        if p:
                            results.append(p)
                            nb_added += 1

                    logger.info(
                        f"[Dir/Gouv] NAF={naf or 'ALL'} cat={cat or 'ALL'} "
                        f"kw={keyword or 'NONE'} cp={cp_chunk or 'FR'} -> +{nb_added}"
                    )

        return results

    def _annuaire_paginated(
        self,
        keyword:  Optional[str],
        naf:      Optional[str],
        cat:      Optional[str],
        cp_chunk: Optional[str],
        per_page: int = 25,
    ) -> List[Dict]:
        all_items: List[Dict] = []
        for page in range(1, self.ANNUAIRE_MAX_PAGES + 1):
            params: Dict[str, Any] = {
                "etat_administratif": "A",
                "per_page":           per_page,
                "page":               page,
            }
            if keyword:
                params["q"] = keyword
            if naf:
                params["activite_principale"] = naf.upper()
            if cat:
                params["categorie_entreprise"] = cat
            if cp_chunk:
                params["code_postal"] = cp_chunk

            final_url = DATA_GOUV_URL + "?" + urlencode(params, doseq=True, safe=",")
            logger.info(f"[Dir/Gouv][DEBUG] URL -> {final_url}")

            data = self.client.get_json(final_url)
            time.sleep(self.ANNUAIRE_SLEEP)

            if not data:
                break

            page_items = data.get("results", [])
            all_items.extend(page_items)
            if len(page_items) < per_page:
                break

        return all_items

    def _parse_annuaire_item(self, item: Dict) -> Optional[Prospect]:
        try:
            siege = item.get("siege", {}) or {}
            nom   = item.get("nom_complet") or item.get("nom_raison_sociale", "")
            if not nom:
                return None
            raw = {
                "nom_commercial":   nom,
                "raison_sociale":   item.get("nom_raison_sociale", ""),
                "siren":            item.get("siren", ""),
                "siret":            siege.get("siret", ""),
                "adresse":          siege.get("adresse", ""),
                "ville":            siege.get("libelle_commune", ""),
                "region":           siege.get("libelle_region", ""),
                "code_postal":      siege.get("code_postal", ""),
                "pays":             "France",
                "secteur_activite": item.get("activite_principale_libelle", ""),
                "code_naf":         item.get("activite_principale", ""),
                "type_entreprise":  item.get("nature_juridique_libelle", ""),
                "website":          self.clean_url(siege.get("site_internet", "")),
                "source":           "annuaire-entreprises.data.gouv.fr",
            }
            return self.normalize_prospect(raw)
        except Exception as e:
            logger.debug(f"[Dir/Gouv] Parse error: {e}")
            return None

    # ══════════════════════════════════════════
    # PagesJaunes
    # ══════════════════════════════════════════

    def _scrape_pagesjaunes(
        self, query: str, location: str = "Paris", max_pages: int = 3
    ) -> List[Prospect]:
        results = []
        for page in range(1, max_pages + 1):
            url = (
                "https://www.pagesjaunes.fr/annuaire/chercherlespros?"
                + urlencode({"quoiqui": query, "ou": location, "page": page})
            )
            html = self.client.get(
                url, headers={"Referer": "https://www.pagesjaunes.fr/"},
            )
            if not html:
                break
            page_results = self._parse_pagesjaunes(html, location)
            results.extend(page_results)
            if len(page_results) < 5:
                break
            time.sleep(random.uniform(2.0, 4.0))
        return results

    def _parse_pagesjaunes(self, html: str, location: str) -> List[Prospect]:
        soup    = self.parse_html(html)
        results = []

        cards = []
        for sel in [
            "div[class*='bi-content']",
            "article[class*='result']",
            "li[class*='bi ']",
            "div[class*='listResults'] > div",
            "div.bi",
        ]:
            cards = soup.select(sel)
            if cards:
                break

        for card in cards:
            try:
                name_el = card.select_one(
                    "a[class*='denomination'], span[class*='denomination'], "
                    "h2 a, h3 a, a[class*='company'], a.bi-denomination"
                )
                name = name_el.get_text(strip=True) if name_el else ""
                if not name:
                    continue

                addr_el = card.select_one(
                    "span[class*='address'], address, "
                    "p[class*='address'], span[class*='adresse']"
                )
                address = addr_el.get_text(strip=True) if addr_el else ""

                cat_el = card.select_one(
                    "span[class*='category'], span[class*='rubrique'], "
                    "p[class*='categorie'], span[class*='activite']"
                )
                category = cat_el.get_text(strip=True) if cat_el else ""

                phone   = self._extract_pj_phone(card)
                email   = self._extract_pj_email(card)
                website = self._extract_pj_website(card)

                if not phone or not website or not email:
                    detail_url = self._get_pj_detail_url(card)
                    if detail_url:
                        time.sleep(random.uniform(1.5, 3.0))
                        enriched = self._scrape_pj_detail(detail_url)
                        if not phone   and enriched.get("telephone"): phone   = enriched["telephone"]
                        if not email   and enriched.get("email"):     email   = enriched["email"]
                        if not website and enriched.get("website"):   website = enriched["website"]
                        if not address and enriched.get("adresse"):   address = enriched["adresse"]

                raw = {
                    "nom_commercial":   name,
                    "telephone":        phone,
                    "email":            email,
                    "website":          self.clean_url(website),
                    "adresse":          address,
                    "ville":            location,
                    "pays":             "France",
                    "secteur_activite": category,
                    "source":           "PagesJaunes",
                }
                results.append(self.normalize_prospect(raw))

            except Exception as e:
                logger.debug(f"[PJ] Parse error: {e}")

        return results

    def _extract_pj_phone(self, card: BeautifulSoup) -> str:
        tel_el = card.select_one("a[href^='tel:']")
        if tel_el:
            ph = tel_el["href"].replace("tel:", "").strip()
            if ph:
                return ph
        for attr in ["data-phone", "data-tel", "data-telephone", "data-numero", "data-pj-phone"]:
            el = card.find(attrs={attr: True})
            if el:
                val    = el[attr].strip()
                digits = re.sub(r"\D", "", val)
                if 7 <= len(digits) <= 15:
                    return val
        for el in card.find_all(True):
            for attr, val in el.attrs.items():
                if not isinstance(val, str):
                    continue
                m = re.search(r"0[1-9](?:[\s.\-]?\d{2}){4}", val)
                if m:
                    return m.group(0)
                m = re.search(r"\+\d{10,15}", val)
                if m:
                    return m.group(0)
        phones = self.extract_phones(card.get_text(separator=" "))
        return phones[0] if phones else ""

    def _extract_pj_email(self, card: BeautifulSoup) -> str:
        mailto_el = card.select_one("a[href^='mailto:']")
        if mailto_el:
            em = mailto_el["href"].replace("mailto:", "").split("?")[0].strip()
            if em:
                return em.lower()
        for el in card.find_all(attrs={"data-email": True}):
            return el["data-email"].strip().lower()
        emails = self.extract_emails(card.get_text(separator=" "))
        return emails[0] if emails else ""

    def _extract_pj_website(self, card: BeautifulSoup) -> str:
        web_el = card.select_one(
            "a[data-pj-event*='site'], a[data-pj-event*='web'], a[data-pj-event*='url']"
        )
        if web_el:
            href = web_el.get("href", "") or web_el.get("data-href", "")
            if href and "pagesjaunes" not in href:
                return href
        for a in card.find_all("a", href=True):
            href = a["href"]
            if "redirect" in href.lower() and "pagesjaunes" in href:
                qs     = parse_qs(urlparse(href).query)
                target = qs.get("url", qs.get("to", qs.get("href", [""])))
                if isinstance(target, list):
                    target = target[0] if target else ""
                if target and str(target).startswith("http"):
                    return str(target)
        for a in card.find_all("a", href=True):
            href   = a["href"]
            if not href.startswith("http"):
                continue
            domain = urlparse(href).netloc.lower().lstrip("www.")
            if not any(excl in domain for excl in _EXCLUDED_DOMAINS):
                if not href.startswith(("tel:", "mailto:")):
                    return href
        for attr in ["data-website", "data-url", "data-site", "data-href"]:
            val = card.get(attr, "")
            if val and val.startswith("http"):
                return val
        return ""

    def _get_pj_detail_url(self, card: BeautifulSoup) -> str:
        link = card.select_one(
            "a[href*='/annuaire/'], a[href*='/pro/'], a[class*='denomination']"
        )
        if link:
            href = link.get("href", "")
            if href.startswith("/"):
                return "https://www.pagesjaunes.fr" + href
            if "pagesjaunes.fr" in href:
                return href
        return ""

    def _scrape_pj_detail(self, url: str) -> Dict[str, str]:
        result: Dict[str, str] = {}
        html = self.client.get(
            url, headers={"Referer": "https://www.pagesjaunes.fr/"},
            timeout=20, retries=2,
        )
        if not html:
            return result
        soup = self.parse_html(html)
        phones = self.extract_phones_from_html(soup)
        if phones:
            result["telephone"] = phones[0]
        emails = self.extract_emails_from_html(soup)
        if emails:
            result["email"] = emails[0]
        web_el = soup.select_one(
            "a[data-pj-event*='site'], a[class*='website'], "
            "a[title*='site'], a[aria-label*='site']"
        )
        if web_el:
            href = web_el.get("href", "")
            if href and "pagesjaunes" not in href and href.startswith("http"):
                result["website"] = href
        if not result.get("website"):
            for a in soup.find_all("a", href=True):
                href   = a["href"]
                if not href.startswith("http"):
                    continue
                domain = urlparse(href).netloc.lower().lstrip("www.")
                if not any(excl in domain for excl in _EXCLUDED_DOMAINS):
                    result["website"] = href
                    break
        addr_el = soup.find(attrs={"itemprop": "address"})
        if addr_el:
            result["adresse"] = addr_el.get_text(separator=" ", strip=True)
        return result

    # ══════════════════════════════════════════
    # Kompass
    # ══════════════════════════════════════════

    def _scrape_kompass(
        self, query: str, pays: List[str], max_pages: int = 2
    ) -> List[Prospect]:
        cc_map = {"France": "FR", "Tunisie": "TN", "Maroc": "MA", "Belgique": "BE"}
        cc     = cc_map.get(pays[0] if pays else "France", "FR")

        results = []
        for page in range(1, max_pages + 1):
            url = (
                f"https://fr.kompass.com/searchCompany?"
                f"text={quote_plus(query)}&country={cc}&page={page}"
            )
            html = self.client.get(url, headers={"Referer": "https://fr.kompass.com/"})
            if not html:
                break
            page_res = self._parse_kompass(html, pays[0] if pays else "France")
            results.extend(page_res)
            if len(page_res) < 3:
                break
            time.sleep(random.uniform(2.5, 5.0))
        return results

    def _parse_kompass(self, html: str, country: str) -> List[Prospect]:
        soup    = self.parse_html(html)
        results = []
        for card in soup.select(
            "div.companyCard, li[class*='company-item'], div[class*='result-item']"
        ):
            try:
                name_el = card.select_one(
                    "a[class*='company-name'], h2 a, h3 a, span[class*='name']"
                )
                name = name_el.get_text(strip=True) if name_el else ""
                if not name:
                    continue
                addr_el   = card.select_one("span[class*='address'], p[class*='address']")
                address   = addr_el.get_text(strip=True) if addr_el else ""
                sector_el = card.select_one("span[class*='activity'], span[class*='sector']")
                sector    = sector_el.get_text(strip=True) if sector_el else ""
                size_el   = card.select_one("span[class*='employee'], span[class*='size']")
                size_text = size_el.get_text(strip=True) if size_el else ""

                website = ""
                web_el  = card.select_one(
                    "a[class*='website'], a[class*='web-link'], a[class*='url'], "
                    "a[itemprop='url'], li[class*='website'] a"
                )
                if web_el:
                    website = web_el.get("href", "")
                if not website:
                    for a in card.find_all("a", href=True):
                        href   = a["href"]
                        domain = urlparse(href).netloc.lower().lstrip("www.")
                        if (href.startswith("http")
                                and "kompass.com" not in domain
                                and not any(excl in domain for excl in _EXCLUDED_DOMAINS)
                                and not href.startswith(("tel:", "mailto:"))):
                            website = href
                            break
                if not website:
                    for attr in ["data-website", "data-url", "data-href"]:
                        website = card.get(attr, "")
                        if website:
                            break

                phones = self.extract_phones_from_html(card)
                emails = self.extract_emails_from_html(card)

                raw = {
                    "nom_commercial":    name,
                    "adresse":           address,
                    "pays":              country,
                    "secteur_activite":  sector,
                    "taille_entreprise": self._infer_size(size_text),
                    "telephone":         phones[0] if phones else "",
                    "email":             emails[0] if emails else "",
                    "website":           self.clean_url(website),
                    "source":            "Kompass",
                }
                results.append(self.normalize_prospect(raw))
            except Exception as e:
                logger.debug(f"[Kompass] Parse error: {e}")
        return results

    # ══════════════════════════════════════════
    # Europages
    # FIX A : utilise EUROPAGES_BASE_URL importé depuis settings
    # ══════════════════════════════════════════

    def _scrape_europages_sector(
        self,
        slug:         str,
        country_code: str = "FR",
        max_pages:    int = 3,
    ) -> List[Prospect]:
        results: List[Prospect] = []
        for page in range(1, max_pages + 1):
            # FIX A : EUROPAGES_BASE_URL (depuis settings) remplace EUROPAGES_BASE (local)
            url = (
                f"{EUROPAGES_BASE_URL}/entreprises/{slug}/nc-{country_code}"
                if page == 1
                else f"{EUROPAGES_BASE_URL}/entreprises/{slug}/nc-{country_code}/{page}"
            )
            html = self._fetch_europages(url)
            if not html:
                break
            page_results = self._parse_europages_listing(html, country_code)
            results.extend(page_results)
            if len(page_results) < 5:
                break
            time.sleep(random.uniform(2.0, 4.5))
        return results

    def _scrape_europages_keyword(
        self,
        keyword:      str,
        country_code: str = "FR",
        max_pages:    int = 2,
    ) -> List[Prospect]:
        results: List[Prospect] = []
        for page in range(1, max_pages + 1):
            params: Dict[str, Any] = {"q": keyword, "country": country_code}
            if page > 1:
                params["page"] = page
            # FIX A : EUROPAGES_BASE_URL depuis settings
            url  = f"{EUROPAGES_BASE_URL}/entreprises?{urlencode(params)}"
            html = self._fetch_europages(url)
            if not html:
                break
            page_results = self._parse_europages_listing(html, country_code)
            results.extend(page_results)
            if len(page_results) < 4:
                break
            time.sleep(random.uniform(2.5, 5.0))
        return results

    def _parse_europages_listing(
        self, html: str, country_code: str
    ) -> List[Prospect]:
        soup    = self.parse_html(html)
        results: List[Prospect] = []

        cards: List[BeautifulSoup] = []
        for selector in [
            "div[class*='company-card']",
            "article[class*='company']",
            "li[class*='company-item']",
            "div[class*='result-item']",
            "div[class*='listing-item']",
            "div[data-type='company']",
        ]:
            cards = soup.select(selector)
            if cards:
                break
        if not cards:
            anchors = soup.find_all("a", href=re.compile(r"/entreprise/[^/]+"))
            seen_parents: set = set()
            for a in anchors:
                parent = a.find_parent(["article", "div", "li"])
                if parent and id(parent) not in seen_parents:
                    seen_parents.add(id(parent))
                    cards.append(parent)

        pays_map = {v: k for k, v in _EUROPAGES_COUNTRY_CODES.items()}
        pays     = pays_map.get(country_code, "France")

        for card in cards:
            try:
                name_el = card.select_one(
                    "a[class*='company-name'], h2 a, h3 a, "
                    "span[class*='company-name'], [class*='name'] a, "
                    "a[class*='title'], h2[class*='title']"
                )
                name = name_el.get_text(strip=True) if name_el else ""
                if not name:
                    continue

                addr_el = card.select_one(
                    "span[class*='address'], p[class*='address'], "
                    "div[class*='address'], span[class*='location']"
                )
                address = addr_el.get_text(strip=True) if addr_el else ""

                sector_el = card.select_one(
                    "span[class*='activity'], span[class*='sector'], "
                    "p[class*='activity'], div[class*='category']"
                )
                sector = sector_el.get_text(strip=True) if sector_el else ""

                phones = self.extract_phones_from_html(card)
                emails = self.extract_emails_from_html(card)

                website = ""
                for a in card.find_all("a", href=True):
                    href   = a["href"]
                    domain = urlparse(href).netloc.lower().lstrip("www.")
                    if (href.startswith("http")
                            and "europages" not in domain
                            and not any(excl in domain for excl in _EXCLUDED_DOMAINS)
                            and not href.startswith(("tel:", "mailto:"))):
                        website = href
                        break

                raw = {
                    "nom_commercial":   name,
                    "adresse":          address,
                    "pays":             pays,
                    "secteur_activite": sector,
                    "telephone":        phones[0] if phones else "",
                    "email":            emails[0] if emails else "",
                    "website":          self.clean_url(website),
                    "source":           "Europages",
                }
                results.append(self.normalize_prospect(raw))
            except Exception as e:
                logger.debug(f"[Europages] Parse error: {e}")
        return results

    def _fetch_europages(self, url: str) -> Optional[str]:
        # FIX A : EUROPAGES_BASE_URL depuis settings
        return self.client.get(
            url,
            headers={"Referer": EUROPAGES_BASE_URL + "/"},
            timeout=25, retries=2,
        )

    # ══════════════════════════════════════════
    # Verif.com
    # FIX A : VERIF_BASE_URL et VERIF_SEARCH_URL importés depuis settings
    # ══════════════════════════════════════════

    def _scrape_verif(
        self,
        query:       str,
        ville:       str       = "Paris",
        types_forme: List[str] = None,
        max_detail:  int       = 10,
    ) -> List[Prospect]:
        # FIX A : VERIF_SEARCH_URL depuis settings
        params: Dict[str, Any] = {"q": query, "ville": ville}
        url  = f"{VERIF_SEARCH_URL}?{urlencode(params)}"
        html = self._fetch_verif(url, referer=VERIF_BASE_URL)
        if not html:
            return []

        soup      = self.parse_html(html)
        prospects = self._parse_verif_listing(soup, types_forme or [])
        return self._verif_enrich_detail(prospects, max_detail=max_detail)

    def _parse_verif_listing(
        self, soup: BeautifulSoup, types_forme: List[str]
    ) -> List[Prospect]:
        results: List[Prospect] = []
        cards: List[BeautifulSoup] = []

        for selector in [
            "div[class*='company-result']",
            "article[class*='result']",
            "li[class*='result']",
            "div[class*='result-item']",
            "tr[class*='result']",
        ]:
            cards = soup.select(selector)
            if cards:
                break

        for card in cards:
            try:
                p = self._parse_verif_card(card, types_forme)
                if p:
                    results.append(p)
            except Exception as e:
                logger.debug(f"[Verif] Parse card error: {e}")

        return results

    def _parse_verif_card(
        self, card: BeautifulSoup, types_forme: List[str]
    ) -> Optional[Prospect]:
        name_el = card.select_one(
            "a[class*='company-name'], h2 a, h3 a, "
            "span[class*='denomination'], a[class*='name']"
        )
        name = name_el.get_text(strip=True) if name_el else ""
        if not name:
            return None

        addr_el = card.select_one(
            "span[class*='address'], p[class*='address'], "
            "div[class*='adresse'], span[class*='location']"
        )
        address = addr_el.get_text(strip=True) if addr_el else ""

        forme = self._verif_extract_forme(card.get_text())
        if types_forme and forme and forme not in types_forme:
            return None

        secteur_el = card.select_one(
            "span[class*='activity'], span[class*='naf'], "
            "p[class*='activity'], div[class*='sector']"
        )
        secteur = secteur_el.get_text(strip=True) if secteur_el else ""

        siren_text = card.get_text()
        siren = self._verif_extract_siren(siren_text)
        cp    = self._verif_extract_cp(address)
        ville = self._verif_extract_city(address)

        detail_url = ""
        link = card.select_one("a[href*='/societe/'], a[href*='/entreprise/']")
        if link:
            href = link.get("href", "")
            if href.startswith("/"):
                detail_url = VERIF_BASE_URL + href
            elif href.startswith("http") and "verif.com" in href:
                detail_url = href

        phones = self.extract_phones_from_html(card)
        emails = self.extract_emails_from_html(card)

        raw = {
            "nom_commercial":   name,
            "raison_sociale":   name,
            "siren":            siren,
            "adresse":          address,
            "ville":            ville,
            "code_postal":      cp,
            "pays":             "France",
            "secteur_activite": secteur,
            "type_entreprise":  forme,
            "telephone":        phones[0] if phones else "",
            "email":            emails[0] if emails else "",
            "website":          "",
            "source":           "Verif.com",
        }
        p = self.normalize_prospect(raw)
        if detail_url:
            p.raw_text = f"__detail_url:{detail_url}"
        return p

    def _verif_enrich_detail(
        self, prospects: List[Prospect], max_detail: int = 10
    ) -> List[Prospect]:
        to_enrich = [
            p for p in prospects
            if (not p.email or not p.telephone or not p.website)
            and p.raw_text and p.raw_text.startswith("__detail_url:")
        ][:max_detail]

        logger.info(f"[Verif] Enrichissement détail: {len(to_enrich)} fiches")

        for p in to_enrich:
            try:
                url    = p.raw_text.replace("__detail_url:", "")
                detail = self._scrape_verif_detail(url) if url else {}
                if detail:
                    if not p.telephone and detail.get("telephone"):
                        p.telephone = detail["telephone"]
                    if not p.email and detail.get("email"):
                        p.email = detail["email"]
                    if not p.website and detail.get("website"):
                        p.website = detail["website"]
                    if not p.adresse and detail.get("adresse"):
                        p.adresse = detail["adresse"]
                    if not p.siren and detail.get("siren"):
                        p.siren = detail["siren"]
                    if not p.secteur_activite and detail.get("secteur"):
                        p.secteur_activite = detail["secteur"]
                p.raw_text = ""
                time.sleep(random.uniform(1.5, 3.0))
            except Exception as e:
                logger.debug(f"[Verif] Erreur détail '{p.nom_commercial}': {e}")

        for p in prospects:
            if p.raw_text and p.raw_text.startswith("__detail_url:"):
                p.raw_text = ""

        return prospects

    def _scrape_verif_detail(self, url: str) -> Dict[str, str]:
        # FIX A : VERIF_SEARCH_URL depuis settings
        html = self._fetch_verif(url, referer=VERIF_SEARCH_URL)
        if not html:
            return {}

        soup   = self.parse_html(html)
        result: Dict[str, str] = {}

        phones = self.extract_phones_from_html(soup)
        if phones:
            result["telephone"] = phones[0]
        else:
            for block in soup.select(
                "div[class*='contact'], div[class*='coordonnee'], section[id*='contact']"
            ):
                block_phones = self.extract_phones(block.get_text(separator=" "))
                if block_phones:
                    result["telephone"] = block_phones[0]
                    break

        emails = self.extract_emails_from_html(soup)
        if emails:
            result["email"] = emails[0]

        website = self._verif_extract_website(soup)
        if website:
            result["website"] = website

        addr_el = soup.find(attrs={"itemprop": "address"})
        if addr_el:
            result["adresse"] = addr_el.get_text(separator=" ", strip=True)

        page_text = self.extract_text(html)
        siren     = self._verif_extract_siren(page_text)
        if siren:
            result["siren"] = siren

        naf_el = soup.select_one("span[class*='naf'], [itemprop='description']")
        if naf_el:
            result["secteur"] = naf_el.get_text(strip=True)

        return result

    def _verif_extract_website(self, soup: BeautifulSoup) -> str:
        for el in soup.find_all("a", attrs={"itemprop": "url"}):
            href = el.get("href", "")
            if href.startswith("http") and "verif.com" not in href:
                return self.clean_url(href)
        for block in soup.select(
            "div[class*='contact'], div[class*='coordonnee'], [class*='website']"
        ):
            for a in block.find_all("a", href=True):
                href   = a["href"]
                domain = urlparse(href).netloc.lower().lstrip("www.")
                if (href.startswith("http")
                        and not any(excl in domain for excl in _VERIF_EXCLUDED_DOMAINS)
                        and not href.startswith(("tel:", "mailto:"))):
                    return self.clean_url(href)
        for a in soup.find_all("a", href=True):
            href      = a["href"]
            link_text = a.get_text(strip=True).lower()
            if not href.startswith("http"):
                continue
            domain = urlparse(href).netloc.lower().lstrip("www.")
            if any(excl in domain for excl in _VERIF_EXCLUDED_DOMAINS):
                continue
            if any(kw in link_text for kw in ["site", "web", "www"]):
                return self.clean_url(href)
        return ""

    def _fetch_verif(self, url: str, referer: str = None) -> Optional[str]:
        # FIX A : VERIF_BASE_URL depuis settings comme referer par défaut
        return self.client.get(
            url,
            headers={"Referer": referer or VERIF_BASE_URL, "Accept-Language": "fr-FR,fr;q=0.9"},
            timeout=25, retries=2,
        )

    # ══════════════════════════════════════════
    # Helpers partagés
    # ══════════════════════════════════════════

    @staticmethod
    def _infer_size(text: str) -> str:
        t = text.lower()
        if any(x in t for x in ["1-9", "1 à 9", "micro", "tpe", "sole"]):  return "TPE"
        if any(x in t for x in ["10-", "50-", "pme", "small"]):             return "PME"
        if any(x in t for x in ["250-", "500-", "eti", "medium"]):          return "ETI"
        if any(x in t for x in ["5000", "grand", "large", "ge "]):          return "GE"
        m = re.search(r"(\d+)\s*(?:employees?|employés?|salariés?)", t)
        if m:
            n = int(m.group(1))
            if n <= 9:    return "TPE"
            if n <= 249:  return "PME"
            if n <= 4999: return "ETI"
            return "GE"
        return ""

    @staticmethod
    def _verif_extract_siren(text: str) -> str:
        m = re.search(r"SIREN\s*:?\s*(\d{3}[\s.]?\d{3}[\s.]?\d{3})", text, re.IGNORECASE)
        if m:
            return re.sub(r"\D", "", m.group(1))[:9]
        m = re.search(r"\b(\d{3}[\s.]?\d{3}[\s.]?\d{3}[\s.]?\d{5})\b", text)
        if m:
            return re.sub(r"\D", "", m.group(1))[:9]
        return ""

    @staticmethod
    def _verif_extract_city(address: str, fallback: str = "") -> str:
        for src in [address, fallback]:
            if not src:
                continue
            m = re.search(r"\d{5}\s+([A-ZÀ-Ü][a-zà-ü\s\-]+)", src)
            if m:
                return m.group(1).strip()
        if address:
            parts = [p.strip() for p in address.split(",") if p.strip()]
            if len(parts) >= 2:
                return parts[-1].strip()
        return ""

    @staticmethod
    def _verif_extract_cp(text: str) -> str:
        m = re.search(r"\b(0[1-9]\d{3}|[1-8]\d{4}|9[0-5]\d{3})\b", text)
        return m.group(1) if m else ""

    @staticmethod
    def _verif_extract_forme(text: str) -> str:
        formes = ["SARL", "SAS", "SASU", "SA", "SNC", "EI", "EIRL", "SCI"]
        t_upper = text.upper()
        for f in formes:
            if re.search(rf"\b{f}\b", t_upper):
                return f
        return ""