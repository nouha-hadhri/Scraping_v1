"""
SCRAPING_V1 - Rate Limiter
Implémentation robuste du rate-limiting avec TokenBucket par domaine.
tocken bucket algorithm pour permettre des bursts contrôlés et un refill progressif : ex: 2 requêtes instantanées, puis 1 requête/seconde ensuite.
DomainRateLimiter pour gérer les limites spécifiques à chaque domaine (ex: Sirene, Pappers
Remplace l'ancien système de timestamps dans CurlClient.

Avantages:
  - Distribution régulière des requêtes (pas de burst)
  - Configuration par domaine
  - Récupération progressive des tokens
  - Thread-safe
"""

import time
import threading
import logging
from collections import defaultdict
from typing import Dict, Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class TokenBucket:
    """
    Rate limiter basé sur Token Bucket algorithm.
    Permet une burst initial, puis refill régulier.
    """

    def __init__(self, capacity: float, refill_rate: float):
        """
        Args:
            capacity: nombre de tokens disponibles instantanément (burst allowance)
            refill_rate: tokens/seconde regagnés après consommation
        
        Exemple:
            - capacity=2, refill_rate=1.0 → burst de 2, puis 1/sec
            - capacity=1, refill_rate=0.33 → 1 token/3s (strict)
        """
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = float(capacity)
        self.last_refill = time.time()
        self.lock = threading.Lock()

    def acquire(self, tokens: int = 1, timeout: Optional[float] = None) -> float:
        """
        Acquiert 'tokens' tokens, en attendant si nécessaire.
        
        Args:
            tokens: nombre de tokens à acquérir
            timeout: timeout en secondes (None = pas de timeout)
        
        Returns:
            Temps d'attente réel (en secondes)
        
        Raises:
            TimeoutError si timeout dépassé avant d'acquérir les tokens
        """
        start_time = time.time()

        with self.lock:
            while True:
                now = time.time()
                elapsed = now - self.last_refill

                # Remplir les tokens progressivement
                self.tokens = min(
                    self.capacity,
                    self.tokens + elapsed * self.refill_rate
                )
                self.last_refill = now

                if self.tokens >= tokens:
                    self.tokens -= tokens
                    wait_time = now - start_time
                    if wait_time > 0.01:  # log seulement si attente > 10ms
                        logger.debug(
                            f"[TokenBucket] Acquired {tokens} tokens "
                            f"(waited {wait_time:.3f}s)"
                        )
                    return wait_time

                # Calculer le temps d'attente nécessaire
                wait_needed = (tokens - self.tokens) / self.refill_rate

                if timeout is not None:
                    elapsed_total = time.time() - start_time
                    if elapsed_total + wait_needed > timeout:
                        raise TimeoutError(
                            f"Rate limiter timeout: need {wait_needed:.2f}s "
                            f"but only {timeout - elapsed_total:.2f}s remaining"
                        )

                # Attendre un peu avant de retry (pour ne pas spinner)
                actual_sleep = min(wait_needed, 0.1)
                time.sleep(actual_sleep)


class DomainRateLimiter:
    """
    Gestionnaire centralisé de rate-limiting pour plusieurs domaines.
    Chaque domaine a son propre TokenBucket.
    """

    def __init__(self, default_rps: float = 1.0):
        """
        Args:
            default_rps: requêtes/seconde par défaut pour domaines non configurés
        """
        self.default_rps = default_rps
        self.limiters: Dict[str, TokenBucket] = {}
        self.lock = threading.Lock()

        # Configuration par domaine (req/seconde)
        # À adapter selon les limites réelles des API 
        self.custom_limits: Dict[str, float] = {
            # Sirene / data.gouv.fr (public, pas de limite officielle)
            "data.gouv.fr": 2.0,        # 2 req/sec 

            # Pappers API (gratuit: 1000 req/jour = ~0.012 req/sec, soyons conservateurs)
            "api.pappers.fr": 1.0 / 3,  # 1 req/3s

            # societe.com (pas d'API, scraping, soyons prudents)
            "societe.com": 0.5,         # 1 req/2s

            # PagesJaunes (même requête)
            "pagesjaunes.fr": 0.3,      # 1 req/3s

            # Kompass
            "kompass.com": 0.5,         # 1 req/2s

            # Europages
            "europages.fr": 0.5,        # 1 req/2s

            # Verif.com
            "verif.com": 0.33,          # 1 req/3s

            # DuckDuckGo (HTML scraping, être respectueux)
            "html.duckduckgo.com": 0.5, # 1 req/2s
            "duckduckgo.com": 0.5,      # 1 req/2s

            # OpenCorporates
            "api.opencorporates.com": 0.5,  # 1 req/2s

            # Overpass (osm.wiki pas de limite stricte, mais prudence)
            "overpass-api.de": 0.2,     # 1 req/5s (très prudent)

            # BODACC (officiel, modéré)
            "bodacc.fr": 1.0,           # 1 req/sec
        }

    def get_limiter(self, domain: str) -> TokenBucket:
        """
        Obtient ou crée un TokenBucket pour un domaine.
        Thread-safe via double-checked locking.
        """
        if domain not in self.limiters:
            with self.lock:
                if domain not in self.limiters:
                    rps = self._get_rps_for_domain(domain)
                    self.limiters[domain] = TokenBucket(
                        capacity=2,           # Burst de 2 requests
                        refill_rate=rps       # Refill rate configuré
                    )
                    logger.debug(
                        f"[DomainRateLimiter] Created limiter for {domain} "
                        f"({rps} req/sec)"
                    )

        return self.limiters[domain]

    def _get_rps_for_domain(self, domain: str) -> float:
        """
        Retourne le rate limit (req/sec) pour un domaine.
        Cherche d'abord dans custom_limits, puis defaults.
        """
        # Recherche directe
        if domain in self.custom_limits:
            return self.custom_limits[domain]

        # Recherche par préfixe (ex: api.pappers.fr match pappers)
        for custom_domain, rps in self.custom_limits.items():
            if custom_domain in domain or domain in custom_domain:
                return rps

        # Default fallback
        logger.debug(f"[DomainRateLimiter] Using default RPS for {domain}")
        return self.default_rps

    def acquire(self, domain: str, tokens: int = 1, timeout: float = 30.0) -> float:
        """
        Acquiert les tokens pour un domaine.
        Bloque jusqu'à avoir les tokens disponibles ou timeout.
        
        Returns:
            Temps d'attente réel
        
        Raises:
            TimeoutError si timeout dépassé
        """
        limiter = self.get_limiter(domain)
        return limiter.acquire(tokens, timeout=timeout)

    def wait_for_domain(self, url: str) -> None:
        """
        Bloque si nécessaire avant de faire une requête sur une URL.
        Extrait automatiquement le domaine.
        """
        domain = urlparse(url).netloc
        try:
            wait_time = self.acquire(domain, tokens=1, timeout=30.0)
            if wait_time > 0.05:
                logger.debug(
                    f"[RateLimit] Waiting {wait_time:.3f}s before {domain}"
                )
        except TimeoutError as e:
            logger.warning(f"[RateLimit] Timeout for {domain}: {e}")
            raise

    def reset_for_testing(self) -> None:
        """Réinitialise tous les limiters (pour tests uniquement)."""
        with self.lock:
            self.limiters.clear()
        logger.debug("[DomainRateLimiter] Reset all limiters")

    def get_stats(self) -> Dict:
        """Retourne les stats actuelles des limiters."""
        with self.lock:
            stats = {}
            for domain, limiter in self.limiters.items():
                with limiter.lock:
                    stats[domain] = {
                        "tokens_available": round(limiter.tokens, 2),
                        "capacity": limiter.capacity,
                        "refill_rate": limiter.refill_rate,
                    }
        return stats