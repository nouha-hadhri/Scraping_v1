"""
SCRAPING_V1 - CurlClient
Client HTTP robuste basé sur subprocess curl.
Gère : rotation User-Agent, proxies, retries, anti-bot headers, HTTP/2.
sanitisations de sécurité pour éviter les injections de commandes via les headers ou les proxies, validation stricte des URLs pour éviter les schémas dangereux, et un système de rate-limiting par domaine pour respecter les limites des sites cibles.
"""
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import io
import json
import random
import re
import subprocess
import threading
import time
import logging
from typing import Optional, Dict, Any
from urllib.parse import urlencode, urlparse

from config.settings import (
    USER_AGENTS,
    CURL_OPTIONS,
    USE_PROXIES,
    PROXY_LIST,
    MIN_DELAY_BETWEEN_REQUESTS,
    MAX_DELAY_BETWEEN_REQUESTS,
)

logger = logging.getLogger(__name__)


class CurlClient:
    """
    Client HTTP basé sur curl (subprocess).
    Utilise le binaire curl système pour bénéficier de TLS fingerprinting
    réaliste, HTTP/1.1, et une gestion fine des headers anti-bot.
    """
# Il mémorise s'il utilise des proxies, le nombre de requêtes effectuées, et les timestamps des dernières requêtes par domaine pour faire du rate limiting.
    def __init__(self, use_proxies: bool = USE_PROXIES):
        self.use_proxies          = use_proxies
        self._proxy_index         = 0
        self._request_count       = 0
        self._domain_timestamps: Dict[str, float] = {}
        # FIX – Thread-safety: _domain_timestamps is read and written by
        # _rate_limit() which is called from multiple threads when a single
        # CurlClient instance is shared across a ThreadPoolExecutor.
        # Without a lock, concurrent read-modify-write on the dict causes
        # race conditions : two threads can read the same `last` timestamp,
        # both decide no sleep is needed, and both fire simultaneously.
        self._rate_limit_lock: threading.Lock = threading.Lock()

    # ──────────────────────────────────────────
    # URL validation
    # ──────────────────────────────────────────

    @staticmethod
    def _validate_url(url: str) -> str:
        """
        FIX – URL Validation: reject any URL whose scheme is not http or https
        before it reaches curl.  This prevents file://, ftp://, dict://, gopher://
        and similar schemes that curl supports but are never legitimate scraping
        targets and could be used to read local files or reach internal services.
        Also enforces a 2 048-character hard cap to prevent curl argument overflow.
        Raises ValueError so callers can decide whether to log+skip or abort.
        """
        if not url or not isinstance(url, str):
            raise ValueError(f"URL must be a non-empty string, got {url!r}")
        url = url.strip()
        if len(url) > 2048:
            raise ValueError(f"URL exceeds 2 048-character limit: {url[:80]!r}…")
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            raise ValueError(
                f"Unsafe or missing URL scheme {parsed.scheme!r} in {url!r}. "
                "Only http:// and https:// are permitted."
            )
        if not parsed.netloc:
            raise ValueError(f"URL has no host: {url!r}")
        return url

    # ──────────────────────────────────────────
    # Public interface : get, post, get_json pour recueillir les données depuis les sites cibles et les sauvegarder dans la base de données ou des fichiers json/csv.
    # ──────────────────────────────────────────

    def get(
        self,
        url:     str,
        params:  Optional[Dict] = None,
        headers: Optional[Dict] = None,
        timeout: int = 30,
        retries: int = 3,
    ) -> Optional[str]:
        """HTTP GET via curl. Retourne le body HTML/JSON ou None."""
        try:
            url = self._validate_url(url)
        except ValueError as e:
            logger.error(f"[Curl] Invalid URL rejected: {e}")
            return None
        if params:
            url = f"{url}?{urlencode(params, doseq=True)}"
        return self._request(url, method="GET", headers=headers,
                             timeout=timeout, retries=retries)

    def post(
        self,
        url:       str,
        data:      Optional[Dict] = None,
        json_data: Optional[Dict] = None,
        headers:   Optional[Dict] = None,
        timeout:   int = 30,
        retries:   int = 3,
    ) -> Optional[str]:
        """HTTP POST via curl."""
        try:
            url = self._validate_url(url)
        except ValueError as e:
            logger.error(f"[Curl] Invalid URL rejected: {e}")
            return None
        return self._request(url, method="POST", data=data,
                             json_data=json_data, headers=headers,
                             timeout=timeout, retries=retries)

    def get_json(
        self, url: str, params: Optional[Dict] = None, **kwargs
    ) -> Optional[Dict]:
        """GET + JSON parse avec headers API corrects. Retourne dict ou None."""
        # Forcer les headers API (priorite JSON) en ecrasant les headers navigateur
        api_headers = {
            "Accept":                    "application/json, */*;q=0.8",
            "Sec-Fetch-Dest":            "empty",
            "Sec-Fetch-Mode":            "cors",
            "Sec-Fetch-Site":            "same-origin",
            "Upgrade-Insecure-Requests": "0",
        }
        existing = kwargs.pop("headers", {}) or {}
        api_headers.update(existing)  # les headers explicites ont priorite
        response = self.get(url, params=params, headers=api_headers, **kwargs)
        if response:
            # FIX – Silent JSON failure: strip BOM / leading whitespace and any
            # HTML preamble before attempting to parse, then surface the error
            # clearly rather than silently returning None.
            stripped = response.lstrip("\ufeff").strip()
            if not stripped.startswith(("{", "[")):
                logger.warning(
                    f"[Curl] Non-JSON response for {url} "
                    f"(starts with {stripped[:40]!r})"
                )
                return None
            try:
                return json.loads(stripped)
            except json.JSONDecodeError as e:
                logger.warning(
                    f"[Curl] JSON decode error for {url}: {e} — "
                    f"raw snippet: {stripped[:200]!r}"
                )
        return None

    # ──────────────────────────────────────────
    # Core request
    # ──────────────────────────────────────────

    def _request(
        self,
        url:       str,
        method:    str = "GET",
        data:      Optional[Dict] = None,
        json_data: Optional[Dict] = None,
        headers:   Optional[Dict] = None,
        timeout:   int = 30,
        retries:   int = 3,
    ) -> Optional[str]:
        """Construit et exécute curl avec retry exponentiel."""
        self._rate_limit(url)

        for attempt in range(retries):
            try:
                cmd = self._build_curl_command(
                    url, method, data, json_data, headers, timeout
                )
                logger.debug(f"[Curl] {method} {url} (attempt {attempt+1}/{retries})")

                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=timeout + 15,
                    encoding="utf-8",      #  FIX PRINCIPAL
                    errors="ignore"
                )

                if result.returncode == 0 and result.stdout.strip():
                    self._request_count += 1
                    return result.stdout

                stderr_snippet = result.stderr[:300] if result.stderr else ""
                logger.warning(
                    f"[Curl] Failed {url} — code={result.returncode} "
                    f"stderr={stderr_snippet}"
                )

            except subprocess.TimeoutExpired:
                logger.warning(f"[Curl] Timeout on {url} (attempt {attempt+1})")
            except FileNotFoundError:
                logger.error("[Curl] curl binary not found. Please install curl.")
                return None
            except Exception as e:
                logger.error(f"[Curl] Unexpected error on {url}: {e}")

            if attempt < retries - 1:
                wait = (2 ** attempt) + random.uniform(0.5, 2.0)
                logger.debug(f"[Curl] Retry in {wait:.1f}s...")
                time.sleep(wait)

        logger.error(f"[Curl] All {retries} attempts failed for {url}")
        return None

    # ──────────────────────────────────────────
    # Command builder : construit la liste d'arguments pour curl en fonction des options, des headers, des données, et de la rotation des User-Agents et des proxies: simule Chrome / Firefox / Safari pour éviter les blocages anti-bot.
    # ──────────────────────────────────────────

    # ──────────────────────────────────────────
    # Security helpers
    # ──────────────────────────────────────────

    @staticmethod
    def _sanitize_header_value(value: str) -> str:
        """
        FIX – Subprocess Command Injection:
        Strip newline / carriage-return characters from header values.
        A value containing \\r\\n could split the HTTP header and inject
        extra curl arguments or forge additional headers.
        """
        return re.sub(r"[\r\n]+", " ", str(value)).strip()

    @staticmethod
    def _sanitize_proxy(proxy: str) -> str:
        """
        FIX – Subprocess Command Injection:
        Allow only the characters that are valid in a proxy URL so that
        a poisoned PROXY_LIST entry cannot inject shell arguments.
        Accepted pattern: scheme://[user:pass@]host:port
        """
        # Keep only safe chars: alphanumeric, ://@._-
        sanitized = re.sub(r"[^\w://@.\-]", "", proxy)
        return sanitized

    def _build_curl_command(
        self,
        url:          str,
        method:       str,
        data:         Optional[Dict],
        json_data:    Optional[Dict],
        extra_headers: Optional[Dict],
        timeout:      int,
    ) -> list:
        """Construit la liste d'arguments curl avec headers anti-bot réalistes."""
        ua   = random.choice(USER_AGENTS)
        opts = CURL_OPTIONS

        cmd = [
            "curl",
            "--silent",
            "--show-error",
            "--location",
            "--max-redirs",      str(opts["max_redirects"]),
            "--connect-timeout", str(opts["connect_timeout"]),
            "--max-time",        str(timeout),
            "--compressed",
            "--user-agent",      ua,
        ]

        # FIX : la clé était "http" (ambiguë). Renommée "http1_1" dans settings.py.
        # Rétro-compatibilité : on accepte les deux pour ne pas casser les configs
        # existantes qui auraient encore l'ancienne clé.
        if opts.get("http1_1") or opts.get("http"):
            cmd.append("--http1.1")

        if opts.get("insecure"):
            cmd.append("--insecure")

        # Anti-bot headers réalistes (imitent Chrome)
        default_headers = {
            "Accept":                  "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8,application/json",
            "Accept-Language":         "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding":         "gzip, deflate",
            "Connection":              "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest":          "document",
            "Sec-Fetch-Mode":          "navigate",
            "Sec-Fetch-Site":          "none",
            "Sec-Fetch-User":          "?1",
            "Cache-Control":           "max-age=0",
            "DNT":                     "1",
        }
        if extra_headers:
            default_headers.update(extra_headers)

        # FIX – Header Sanitization (Incomplete): sanitize every header key AND
        # value before passing them to curl.  Previously a key that became empty
        # after stripping non-word chars (e.g. a key that is purely punctuation)
        # would produce a malformed '-H ": value"' argument accepted silently by
        # curl.  We now skip such headers and log a warning.
        for key, value in default_headers.items():
            safe_key   = re.sub(r"[^\w\-]", "", key)
            if not safe_key:
                logger.warning(
                    f"[Curl] Header key {key!r} reduced to empty string after "
                    "sanitization — header skipped."
                )
                continue
            safe_value = self._sanitize_header_value(value)
            cmd.extend(["-H", f"{safe_key}: {safe_value}"])

        # Method & body
        if method == "POST":
            cmd.extend(["-X", "POST"])
            if json_data:
                cmd.extend([
                    "-H", "Content-Type: application/json",
                    "--data", json.dumps(json_data),
                ])
            elif data:
                for k, v in data.items():
                    cmd.extend(["--data-urlencode", f"{k}={v}"])

        # FIX – Subprocess Command Injection: sanitize proxy string
        if self.use_proxies and PROXY_LIST:
            cmd.extend(["--proxy", self._sanitize_proxy(self._get_next_proxy())])

        cmd.append(url)
        return cmd

    # ──────────────────────────────────────────
    # Rate limiting
    # ──────────────────────────────────────────

    def _rate_limit(self, url: str) -> None:
        """Respecte un délai minimum entre requêtes vers le même domaine.

        FIX – Thread-safety: toute la logique lecture/calcul/ecriture sur
        _domain_timestamps est protégée par _rate_limit_lock.
        Sans ce lock, deux threads partageant le même CurlClient lisent le
        même `last` timestamp, calculent tous les deux wait=0, et tirent
        simultanément vers le même domaine — le rate-limiting est illusoire.
        Le sleep se fait HORS du lock : on ne bloque pas les autres threads
        pendant l'attente, on libère le lock avant de dormir.
        """
        domain = urlparse(url).netloc

        with self._rate_limit_lock:
            now     = time.time()
            last    = self._domain_timestamps.get(domain, 0)
            elapsed = now - last
            delay   = random.uniform(MIN_DELAY_BETWEEN_REQUESTS, MAX_DELAY_BETWEEN_REQUESTS)
            sleep_time = max(0.0, delay - elapsed)
            # Marquer immédiatement le timestamp AVANT le sleep :
            # les autres threads verront ce domaine comme "récemment utilisé"
            # et calculeront leur propre délai correctement.
            self._domain_timestamps[domain] = now + sleep_time

            # FIX – Resource Leak: evict oldest entries beyond 1 000.
            if len(self._domain_timestamps) > 1_000:
                oldest = sorted(self._domain_timestamps, key=self._domain_timestamps.__getitem__)
                for old_key in oldest[:500]:
                    del self._domain_timestamps[old_key]
                logger.debug("[RateLimit] Evicted 500 stale domain timestamps.")

        # Sleep hors du lock — les autres threads ne sont pas bloqués pendant
        # l'attente de ce thread.
        if sleep_time > 0:
            logger.debug(f"[RateLimit] Sleeping {sleep_time:.2f}s for {domain}")
            time.sleep(sleep_time)

    def _get_next_proxy(self) -> str:
        proxy = PROXY_LIST[self._proxy_index % len(PROXY_LIST)]
        self._proxy_index += 1
        return proxy

    @property
    def request_count(self) -> int:
        return self._request_count

    def reset_rate_limits(self) -> None:
        self._domain_timestamps.clear()