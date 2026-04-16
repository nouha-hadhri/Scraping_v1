"""
SCRAPING_V1 - SocieteScraper
Collecte des données légales d'entreprises françaises :
  - Pappers API (données RNCS/INPI — gratuit jusqu'à 1000 req/jour)
  - societe.com (fallback scraping RNCS public — données complètes)

CORRECTIONS v3 (suite aux audits) :

  FIX A — URLs dupliquées supprimées
    AVANT : PAPPERS_SEARCH_URL, SOCIETE_SEARCH_URL, SOCIETE_FICHE_URL
            définies localement -> les modifier dans settings.py n'avait aucun effet.
    APRÈS : importées exclusivement depuis config.settings (source unique).

  FIX B — Valeurs par défaut des sous-sources
    AVANT : _soc_cfg.get("pappers", True) — activait Pappers si SOURCES_CONFIG vide
    APRÈS : .get("pappers", False) et .get("societe_com", False)

  FIX C — Enrichissement societe.com sans log quand liste vide
    AVANT : _enrich_via_societe() appelée même si prospects=[] sans log
    APRÈS : log explicite + early return si aucun prospect à enrichir.

  FIX D — Constantes de pagination lues depuis settings.COLLECTION_LIMITS
    AVANT : PAPPERS_MAX_NAF=8, PAPPERS_MAX_PAGES=5, etc. codées en dur
    APRÈS : propriétés déléguant à COLLECTION_LIMITS (source unique)
"""
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bs4 import BeautifulSoup

from config.settings import (
    PAPPERS_API_KEY,
    SOURCES_CONFIG,
    COLLECTION_LIMITS,
    # FIX A : URLs importées depuis settings — plus de définitions locales
    PAPPERS_SEARCH_URL,
    SOCIETE_SEARCH_URL,
    SOCIETE_FICHE_URL,
)

import logging
import re
import time
import random
from typing import List, Dict, Any, Optional
from urllib.parse import quote_plus, urlparse, urlencode

from sources.base_scraper         import BaseScraper
from sources.sirene_query_builder import SireneQueryBuilder
from storage.models               import Prospect

logger = logging.getLogger(__name__)


def _is_france_target(pays: List[str]) -> bool:
    if not pays:
        return True
    return any(str(p).strip().lower() == "france" for p in pays)

# ---------------------------------------------------------------------------
# FIX – Lazy singleton pour _qbuilder (même pattern que open_data_scraper.py)
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

# FIX A : ces constantes n'existent plus ici — elles sont dans settings.py
# PAPPERS_SEARCH_URL  = "https://api.pappers.fr/v2/recherche"   <- SUPPRIMÉ
# SOCIETE_SEARCH_URL  = "https://www.societe.com/cgi-bin/search" <- SUPPRIMÉ
# SOCIETE_FICHE_URL   = "https://www.societe.com/societe"        <- SUPPRIMÉ

_EXCLUDED_DOMAINS = {
    "societe.com", "infogreffe.fr", "pappers.fr", "verif.com",
    "facebook.com", "twitter.com", "linkedin.com", "instagram.com",
    "bodacc.fr", "annuaire-entreprises.data.gouv.fr",
}


class SocieteScraper(BaseScraper):
    source_name = "Societe"

    # ── FIX D : constantes de pagination via propriétés → settings.COLLECTION_LIMITS ──
    @property
    def PAPPERS_MAX_NAF(self) -> int:
        return COLLECTION_LIMITS.get("pappers_max_naf", 8)

    @property
    def PAPPERS_MAX_PAGES(self) -> int:
        return COLLECTION_LIMITS.get("pappers_max_pages", 5)

    @property
    def PAPPERS_PER_PAGE(self) -> int:
        return COLLECTION_LIMITS.get("pappers_per_page", 20)

    @property
    def PAPPERS_SLEEP(self) -> float:
        return COLLECTION_LIMITS.get("pappers_sleep", 0.6)

    @property
    def PAPPERS_MAX_ENRICH(self) -> int:
        return COLLECTION_LIMITS.get("pappers_max_enrich", 30)

    def search(self, criteria: Dict[str, Any]) -> List[Prospect]:
        prospects: List[Prospect] = []
        secteurs = criteria.get("secteurs_activite", [])

        # FIX B (alignement avec OpenDataScraper/DirectoryScraper)
        _loc     = criteria.get("localisation") or criteria.get("zone_geographique") or {}
        villes   = _loc.get("villes",  []) or criteria.get("villes",  [])
        regions  = _loc.get("regions", []) or criteria.get("regions", [])
        pays     = _loc.get("pays",    []) or criteria.get("pays",    [])

        types    = criteria.get("types_entreprise", [])
        keywords = criteria.get("keywords", secteurs or ["informatique"])

        taille_cfg  = criteria.get("taille_entreprise", {})
        tailles_cat = (
            criteria.get("tailles_entreprise", [])
            or criteria.get("tailles_categorie", [])
            or taille_cfg.get("categories", [])
        )

        # Pappers et societe.com = France uniquement
        if not _is_france_target(pays):
            return []

        _soc_cfg = SOURCES_CONFIG.get("societe", {})

        # ── 1. Pappers ────────────────────────────────────────────────
        # FIX B : .get("pappers", False) — valeur par défaut cohérente
        if _soc_cfg.get("pappers", False):
            if not PAPPERS_API_KEY:
                logger.warning("[Societe/Pappers] PAPPERS_API_KEY absent du .env — source ignoree")
            else:
                codes_naf = criteria.get("codes_naf") or _get_qbuilder()._map_sectors(secteurs)
                codes_cp  = _get_qbuilder()._map_geo(regions=regions, villes=villes)
                cp_complets = [cp for cp in codes_cp if len(cp) == 5 and cp.isdigit()]
                dep_prefixes = [cp for cp in codes_cp if len(cp) == 2]

                for kw in keywords[:4]:
                    try:
                        res = self._search_pappers(
                            query       = kw,
                            codes_naf   = codes_naf,
                            tailles_cat = tailles_cat,
                            types       = types,
                            cp_complets = cp_complets,
                            dep_prefixes= dep_prefixes,
                        )
                        logger.info(f"[Societe/Pappers] '{kw}' -> {len(res)}")
                        prospects.extend(res)
                        time.sleep(random.uniform(1.0, 1.5))
                    except Exception as e:
                        logger.warning(f"[Societe/Pappers] Erreur '{kw}': {e}")
        else:
            logger.info("[Societe/Pappers] desactive (SOURCES_CONFIG)")

        # ── 2. Enrichissement via societe.com ─────────────────────────
        # FIX B : .get("societe_com", False)
        # FIX C : log + early return si liste vide (évite appel inutile)
        if _soc_cfg.get("societe_com", False):
            if not prospects:
                logger.info("[Societe/societe.com] Aucun prospect a enrichir — enrichissement ignore")
            else:
                prospects = self._enrich_via_societe(prospects, max_enrich=self.PAPPERS_MAX_ENRICH)
        else:
            logger.info("[Societe/societe.com] desactive (SOURCES_CONFIG)")

        logger.info(f"[Societe] Total brut: {len(prospects)}")
        return prospects

    # ──────────────────────────────────────────
    # Pappers API
    # ──────────────────────────────────────────

    def _search_pappers(
        self,
        query:        str,
        codes_naf:    List[str]      = None,
        tailles_cat:  List[str]      = None,
        types:        List[str]      = None,
        cp_complets:  List[str]      = None,
        dep_prefixes: List[str]      = None,
        per_page:     int            = 20,
    ) -> List[Prospect]:
        """
        API Pappers v2 — légale, données RNCS publiques.
        Boucle NAF × categorie_entreprise × localisation.
        Token transmis dans l'URL via urlencode (méthode fiable).
        """
        results:     List[Prospect] = []
        seen_sirens: set            = set()

        api_cats = [c for c in (tailles_cat or []) if c in ("PME", "ETI", "GE")]
        if not api_cats:
            api_cats = [None]

        # FIX D : PAPPERS_MAX_NAF depuis settings via propriété
        naf_list = (codes_naf or [])[:self.PAPPERS_MAX_NAF] or [None]

        forme_map = {"SARL": "SARL", "SAS": "SAS", "SASU": "SASU", "SA": "SA", "EI": "EI"}
        formes_csv = ",".join([forme_map[t] for t in (types or []) if t in forme_map]) or None

        geo_list: List[Optional[Dict]] = []
        if cp_complets:
            for i in range(0, len(cp_complets), 5):
                geo_list.append({"siege_code_postal": ",".join(cp_complets[i:i+5])})
        elif dep_prefixes:
            for dep in dep_prefixes[:8]:
                geo_list.append({"siege_departement": dep})
        else:
            geo_list = [None]

        for naf in naf_list:
            for cat in api_cats:
                for geo in geo_list:
                    batch = self._pappers_paginated(
                        query    = query,
                        naf      = naf,
                        cat      = cat,
                        geo      = geo,
                        formes   = formes_csv,
                        per_page = per_page,
                    )
                    nb_added = 0
                    for item in batch:
                        siren = item.get("siren", "")
                        if siren and siren in seen_sirens:
                            continue
                        if siren:
                            seen_sirens.add(siren)
                        p = self._parse_pappers_item(item)
                        if p:
                            results.append(p)
                            nb_added += 1

                    geo_label = str(geo) if geo else "FR"
                    logger.info(
                        f"[Pappers] NAF={naf or 'ALL'} cat={cat or 'ALL'} "
                        f"kw={query} geo={geo_label} -> +{nb_added}"
                    )

        return results

    def _pappers_paginated(
        self,
        query:    str,
        naf:      Optional[str],
        cat:      Optional[str],
        geo:      Optional[Dict],
        formes:   Optional[str],
        per_page: int = 20,
    ) -> List[Dict]:
        """Appels paginés pour une combinaison (NAF, catégorie, geo)."""
        all_items: List[Dict] = []

        # FIX D : PAPPERS_MAX_PAGES et PAPPERS_SLEEP depuis settings via propriétés
        for page in range(1, self.PAPPERS_MAX_PAGES + 1):
            params: Dict[str, Any] = {
                "api_token":          PAPPERS_API_KEY,
                "q":                  query,
                "par_page":           per_page,
                "page":               page,
                "precision":          "approximative",
                "entreprise_cessee":  "false",
                "_fields": (
                    "siren,nom_entreprise,forme_juridique,date_creation,"
                    "libelle_code_naf,code_naf,tranche_effectif,chiffre_affaires,"
                    "siege.siret,siege.adresse_ligne_1,siege.adresse_ligne_2,"
                    "siege.code_postal,siege.ville,siege.region,"
                    "siege.telephone,siege.email,siege.site_internet"
                ),
            }
            if naf:
                params["code_naf"] = naf
            if cat:
                params["categorie_entreprise"] = cat
            if formes:
                params["forme_juridique"] = formes
            if geo:
                params.update(geo)

            # FIX A : PAPPERS_SEARCH_URL importé depuis settings
            final_url = PAPPERS_SEARCH_URL + "?" + urlencode(params, doseq=True)
            logger.info(f"[Pappers][DEBUG] URL -> {final_url[:120]}...")

            data = self.client.get_json(final_url)
            time.sleep(self.PAPPERS_SLEEP)

            if not data:
                logger.warning(f"[Pappers] Reponse vide — verifier PAPPERS_API_KEY dans .env")
                break

            page_items = data.get("resultats", [])
            all_items.extend(page_items)

            logger.info(
                f"[Pappers][DEBUG] page={page} total={data.get('total', '?')} "
                f"recu={len(page_items)}"
            )

            if len(page_items) < per_page:
                break

        return all_items

    def _parse_pappers_item(self, item: Dict) -> Optional[Prospect]:
        try:
            siege    = item.get("siege", {}) or {}
            employes = self._parse_tranche(item.get("tranche_effectif", ""))
            nom      = item.get("nom_entreprise", "")
            if not nom:
                return None
            raw = {
                "nom_commercial":   nom,
                "raison_sociale":   nom,
                "siren":            item.get("siren", ""),
                "siret":            siege.get("siret", ""),
                "adresse":          self._build_addr(siege),
                "ville":            siege.get("ville", ""),
                "code_postal":      siege.get("code_postal", ""),
                "region":           siege.get("region", ""),
                "pays":             "France",
                "secteur_activite": item.get("libelle_code_naf", ""),
                "code_naf":         item.get("code_naf", ""),
                "type_entreprise":  item.get("forme_juridique", ""),
                "telephone":        siege.get("telephone", "") or siege.get("tel", ""),
                "website":          self.clean_url(
                                        siege.get("site_internet", "")
                                        or siege.get("site_web", "")
                                        or siege.get("website", "")
                                    ),
                "email":            siege.get("email", "") or siege.get("mail", ""),
                "nombre_employes":  employes,
                "chiffre_affaires": item.get("chiffre_affaires") or item.get("ca"),
                "date_creation":    item.get("date_creation", ""),
                "source":           "Pappers/RNCS",
            }
            return self.normalize_prospect(raw)
        except Exception as e:
            logger.debug(f"[Pappers] Parse error: {e}")
            return None

    # ──────────────────────────────────────────
    # Enrichissement via societe.com
    # ──────────────────────────────────────────

    def _enrich_via_societe(
        self, prospects: List[Prospect], max_enrich: int = 30
    ) -> List[Prospect]:
        """
        Pour chaque prospect sans contact complet (email/tel/website),
        cherche et scrape la fiche societe.com correspondante.
        Utilise le SIREN comme clé si disponible, sinon le nom de l'entreprise.

        FIX C : early return si liste vide (évite traitement inutile)
        """
        # FIX C : vérification explicite avant la sélection des prospects à enrichir
        if not prospects:
            logger.info("[Societe/societe.com] Liste prospects vide — enrichissement ignoré")
            return prospects

        to_enrich = [
            p for p in prospects
            if p.nom_commercial and (not p.email or not p.telephone or not p.website)
        ][:max_enrich]

        logger.info(f"[Societe] Enrichissement societe.com : {len(to_enrich)} prospects")

        if not to_enrich:
            logger.info("[Societe/societe.com] Tous les prospects ont déjà leurs contacts — enrichissement ignoré")
            return prospects

        for p in to_enrich:
            try:
                enriched = None

                if p.siren and len(re.sub(r"\D", "", p.siren)) == 9:
                    enriched = self.scrape_by_siren(p.siren)

                if not enriched or not (enriched.telephone or enriched.email or enriched.website):
                    enriched = self._scrape_societe_by_name(p.nom_commercial, p.ville)

                if enriched:
                    changed = False
                    if not p.telephone and enriched.telephone:
                        p.telephone = enriched.telephone
                        changed = True
                    if not p.email and enriched.email:
                        p.email = enriched.email
                        changed = True
                    if not p.website and enriched.website:
                        p.website = enriched.website
                        changed = True
                    if not p.adresse and enriched.adresse:
                        p.adresse = enriched.adresse
                    if changed:
                        logger.debug(
                            f"[Societe]  {p.nom_commercial} enrichi : "
                            f"tel={p.telephone!r} email={p.email!r} web={p.website!r}"
                        )

                time.sleep(random.uniform(1.5, 3.0))

            except Exception as e:
                logger.debug(f"[Societe] Erreur enrichissement '{p.nom_commercial}': {e}")

        return prospects

    # ──────────────────────────────────────────
    # societe.com — Scraping par SIREN
    # ──────────────────────────────────────────

    def scrape_by_siren(self, siren: str) -> Optional[Prospect]:
        """Enrichit un prospect depuis societe.com en cherchant par SIREN."""
        siren_clean = re.sub(r"\D", "", siren)
        if len(siren_clean) != 9:
            return None

        # FIX A : SOCIETE_SEARCH_URL depuis settings
        url  = f"{SOCIETE_SEARCH_URL}?champs={siren_clean}"
        html = self.client.get(
            url,
            headers={
                "Referer":    "https://www.societe.com/",
                "Accept":     "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            timeout=25, retries=2,
        )
        if not html:
            return None

        return self._parse_societe_page(html, source=f"societe.com/SIREN:{siren_clean}")

    # ──────────────────────────────────────────
    # societe.com — Scraping par Nom
    # ──────────────────────────────────────────

    def _scrape_societe_by_name(
        self, name: str, ville: str = ""
    ) -> Optional[Prospect]:
        query = f"{name} {ville}".strip()
        # FIX A : SOCIETE_SEARCH_URL depuis settings
        url   = f"{SOCIETE_SEARCH_URL}?champs={quote_plus(query)}"
        html  = self.client.get(
            url,
            headers={"Referer": "https://www.societe.com/"},
            timeout=25, retries=2,
        )
        if not html:
            return None

        soup = self.parse_html(html)

        result_links = soup.select(
            "a[href*='/societe/'], "
            "h2.result-title a, "
            "div[class*='result'] a[href*='/societe/']"
        )

        name_words = [w.lower() for w in name.split() if len(w) > 2]

        for link in result_links[:3]:
            link_text = link.get_text(strip=True).lower()
            link_href = link.get("href", "")

            if name_words and not any(w in link_text for w in name_words[:2]):
                continue

            if link_href.startswith("/"):
                fiche_url = "https://www.societe.com" + link_href
            elif link_href.startswith("http"):
                fiche_url = link_href
            else:
                continue

            time.sleep(random.uniform(1.0, 2.0))
            fiche_html = self.client.get(
                fiche_url,
                headers={"Referer": url},
                timeout=25, retries=2,
            )
            if fiche_html:
                result = self._parse_societe_page(fiche_html, source="societe.com")
                if result and (result.telephone or result.email or result.website):
                    return result

        return self._parse_societe_page(html, source="societe.com")

    # ──────────────────────────────────────────
    # Parser societe.com
    # ──────────────────────────────────────────

    def _parse_societe_page(self, html: str, source: str = "societe.com") -> Optional[Prospect]:
        soup = self.parse_html(html)
        text = self.extract_text(html)
        raw: Dict[str, Any] = {"source": source}

        name_el = soup.select_one(
            "h1[class*='denomination'], "
            "span[itemprop='name'], "
            "h1.company-name, "
            "div[class*='company-title'] h1"
        )
        if name_el:
            raw["nom_commercial"] = name_el.get_text(strip=True)

        for el in soup.find_all(string=re.compile(r"\bSIREN\b|\bSIRET\b")):
            parent = el.parent
            if not parent:
                continue
            t      = parent.get_text()
            sirets = re.findall(r"\b\d{14}\b", t)
            sirens = re.findall(r"\b\d{9}\b", t)
            if sirets:
                raw.setdefault("siret", sirets[0])
                raw.setdefault("siren", sirets[0][:9])
            elif sirens:
                raw.setdefault("siren", sirens[0])

        addr_el = soup.find(attrs={"itemprop": "address"})
        if addr_el:
            raw["adresse"] = addr_el.get_text(separator=" ", strip=True)
        else:
            addr_el2 = soup.select_one(
                "span[class*='address'], div[class*='adresse'], "
                "p[class*='address'], span[itemprop='streetAddress']"
            )
            if addr_el2:
                raw["adresse"] = addr_el2.get_text(separator=" ", strip=True)

        city_el = soup.find(attrs={"itemprop": "addressLocality"})
        if city_el:
            raw["ville"] = city_el.get_text(strip=True)

        phones = self.extract_phones_from_html(soup)
        if phones:
            raw["telephone"] = phones[0]
        elif not phones:
            contact_blocks = soup.select(
                "div[class*='contact'], div[class*='coordonnee'], "
                "section[class*='contact'], div[id*='contact']"
            )
            for block in contact_blocks:
                block_phones = self.extract_phones(block.get_text(separator=" "))
                if block_phones:
                    raw["telephone"] = block_phones[0]
                    break

        emails = self.extract_emails_from_html(soup)
        if emails:
            raw["email"] = emails[0]

        website = self._extract_societe_website(soup)
        if website:
            raw["website"] = website

        if not raw.get("nom_commercial"):
            return None

        raw["pays"] = "France"
        return self.normalize_prospect(raw)

    def _extract_societe_website(self, soup: BeautifulSoup) -> str:
        for el in soup.find_all("a", attrs={"itemprop": "url"}):
            href = el.get("href", "")
            if href.startswith("http") and "societe.com" not in href:
                return href

        for block in soup.select(
            "div[class*='contact'], div[class*='coordonnee'], "
            "section[class*='contact'], div[id*='coordonnees']"
        ):
            for a in block.find_all("a", href=True):
                href   = a["href"]
                domain = urlparse(href).netloc.lower().lstrip("www.")
                if (href.startswith("http")
                        and not any(excl in domain for excl in _EXCLUDED_DOMAINS)
                        and not href.startswith(("tel:", "mailto:"))):
                    return href

        for a in soup.find_all("a", href=True):
            href      = a["href"]
            link_text = a.get_text(strip=True).lower()
            if not href.startswith("http"):
                continue
            domain = urlparse(href).netloc.lower().lstrip("www.")
            if any(excl in domain for excl in _EXCLUDED_DOMAINS):
                continue
            if any(kw in link_text for kw in ["site", "web", "www"]):
                return href

        return ""

    # ──────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────

    @staticmethod
    def _build_addr(siege: dict) -> str:
        return " ".join(filter(None, [
            siege.get("adresse_ligne_1", ""),
            siege.get("adresse_ligne_2", ""),
            siege.get("code_postal", ""),
            siege.get("ville", ""),
        ])).strip()

    @staticmethod
    def _parse_tranche(tranche: str) -> Optional[int]:
        # FIX : les valeurs retournées sont maintenant les bornes basses de chaque
        # tranche, identiques à _TRANCHE_MAP dans open_data_scraper.py.
        # L'ancienne version retournait des moyennes approximatives (ex: "01"→2,
        # "02"→5, "31"→225) ce qui produisait des nombre_employes différents pour
        # le même prospect selon sa source, faussant le scoring taille.
        mapping = {
            "NN": None,
            "00": 0,   "01": 1,   "02": 3,   "03": 6,
            "11": 10,  "12": 20,  "21": 50,  "22": 100,
            "31": 200, "32": 250, "41": 500, "42": 1000,
            "51": 2000, "52": 5000, "53": 10000,
        }
        return mapping.get(str(tranche))