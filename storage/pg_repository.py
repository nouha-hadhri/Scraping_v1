"""
SCRAPING_V1 - PgRepository
Couche de persistance PostgreSQL pour les Prospect et CollectionJob.

Complète ProspectRepository (CSV/JSON) — les deux coexistent.
Activé via PG_ENABLED=True dans config/settings.py.

Tables créées automatiquement au premier run (auto-migration).
L'upsert est basé sur hash_dedup (UPDATE si doublon, INSERT sinon).

Usage:
    from storage.pg_repository import PgRepository
    pg = PgRepository()
    pg.upsert_prospects(scored)
    pg.save_job(job)
"""
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import logging
from contextlib import contextmanager
from datetime   import datetime
from typing     import List, Dict, Any, Optional, Generator

from storage.models import Prospect, CollectionJob

logger = logging.getLogger(__name__)

# ── Import psycopg2 (optionnel — désactivé si absent) ─────────────────────────
try:
    import psycopg2
    import psycopg2.extras
    _PSYCOPG2_AVAILABLE = True
except ImportError:
    _PSYCOPG2_AVAILABLE = False
    logger.warning(
        "[PgRepo] psycopg2 non installé. "
        "Installez-le avec : pip install psycopg2-binary"
    )


# ══════════════════════════════════════════════════════════════════════════════
# DDL — Schéma des tables
# ══════════════════════════════════════════════════════════════════════════════

_DDL_PROSPECTS = """
CREATE TABLE IF NOT EXISTS prospects (
    -- Clé de déduplication (SHA-256 tronqué, calculé par BaseScraper)
    hash_dedup          TEXT        PRIMARY KEY,

    -- Identité
    nom_commercial      TEXT        NOT NULL DEFAULT '',
    raison_sociale      TEXT        NOT NULL DEFAULT '',

    -- Contacts
    email               TEXT        NOT NULL DEFAULT '',
    telephone           TEXT        NOT NULL DEFAULT '',
    website             TEXT        NOT NULL DEFAULT '',
    linkedin_url        TEXT        NOT NULL DEFAULT '',

    -- Localisation
    adresse             TEXT        NOT NULL DEFAULT '',
    ville               TEXT        NOT NULL DEFAULT '',
    region              TEXT        NOT NULL DEFAULT '',
    pays                TEXT        NOT NULL DEFAULT '',
    code_postal         TEXT        NOT NULL DEFAULT '',

    -- Entreprise
    secteur_activite    TEXT        NOT NULL DEFAULT '',
    type_entreprise     TEXT        NOT NULL DEFAULT '',
    taille_entreprise   TEXT        NOT NULL DEFAULT '',
    nombre_employes     INTEGER,
    chiffre_affaires    NUMERIC(18,2),
    description         TEXT        NOT NULL DEFAULT '',
    code_naf            TEXT        NOT NULL DEFAULT '',

    -- Identifiants légaux
    siren               TEXT        NOT NULL DEFAULT '',
    siret               TEXT        NOT NULL DEFAULT '',

    -- Scoring & Qualification
    qualification_score INTEGER     NOT NULL DEFAULT 0,
    score_pct           NUMERIC(5,1) NOT NULL DEFAULT 0,
    statut              TEXT        NOT NULL DEFAULT 'NON_QUALIFIE',
    score_detail        JSONB       NOT NULL DEFAULT '{}',
    criteria_met        INTEGER     NOT NULL DEFAULT 0,
    criteria_total      INTEGER     NOT NULL DEFAULT 0,

    -- Métadonnées
    source              TEXT        NOT NULL DEFAULT 'SCRAPING',
    segment             TEXT        NOT NULL DEFAULT '',
    sector_confidence   NUMERIC(5,3) NOT NULL DEFAULT 0,
    email_valid         BOOLEAN     NOT NULL DEFAULT FALSE,
    website_active      BOOLEAN     NOT NULL DEFAULT FALSE,

    -- Enrichissement contacts
    enrich_score        INTEGER     NOT NULL DEFAULT 0,
    email_mx_verified   BOOLEAN     NOT NULL DEFAULT FALSE,

    -- Timestamps
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

_DDL_PROSPECTS_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_prospects_statut    ON prospects (statut);
CREATE INDEX IF NOT EXISTS idx_prospects_siren     ON prospects (siren) WHERE siren <> '';
CREATE INDEX IF NOT EXISTS idx_prospects_email     ON prospects (email) WHERE email <> '';
CREATE INDEX IF NOT EXISTS idx_prospects_ville     ON prospects (ville);
CREATE INDEX IF NOT EXISTS idx_prospects_secteur   ON prospects (secteur_activite);
CREATE INDEX IF NOT EXISTS idx_prospects_score     ON prospects (qualification_score DESC);
CREATE INDEX IF NOT EXISTS idx_prospects_enrich    ON prospects (enrich_score DESC);
CREATE INDEX IF NOT EXISTS idx_prospects_created   ON prospects (created_at DESC);
"""

_DDL_JOBS = """
CREATE TABLE IF NOT EXISTS collection_jobs (
    id                  TEXT        PRIMARY KEY,
    name                TEXT        NOT NULL DEFAULT '',
    type                TEXT        NOT NULL DEFAULT 'SCRAPING',
    status              TEXT        NOT NULL DEFAULT 'PENDING',
    parameters_json     TEXT        NOT NULL DEFAULT '{}',
    started_at          TIMESTAMPTZ,
    finished_at         TIMESTAMPTZ,
    total_collected     INTEGER     NOT NULL DEFAULT 0,
    total_cleaned       INTEGER     NOT NULL DEFAULT 0,
    total_deduped       INTEGER     NOT NULL DEFAULT 0,
    total_scored        INTEGER     NOT NULL DEFAULT 0,
    total_saved         INTEGER     NOT NULL DEFAULT 0,
    total_qualified     INTEGER     NOT NULL DEFAULT 0,
    total_duplicates    INTEGER     NOT NULL DEFAULT 0,
    sources_used        TEXT[]      NOT NULL DEFAULT '{}',
    errors              TEXT[]      NOT NULL DEFAULT '{}',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

# Upsert (INSERT … ON CONFLICT) pour éviter les doublons par hash_dedup.
# Les champs de scoring et d'enrichissement sont TOUJOURS mis à jour
# car ils peuvent évoluer entre deux runs (re-score, re-enrich).
_SQL_UPSERT_PROSPECT = """
INSERT INTO prospects (
    hash_dedup, nom_commercial, raison_sociale,
    email, telephone, website, linkedin_url,
    adresse, ville, region, pays, code_postal,
    secteur_activite, type_entreprise, taille_entreprise,
    nombre_employes, chiffre_affaires, description, code_naf,
    siren, siret,
    qualification_score, score_pct, statut, score_detail,
    criteria_met, criteria_total,
    source, segment, sector_confidence,
    email_valid, website_active,
    enrich_score, email_mx_verified,
    created_at, updated_at
) VALUES (
    %(hash_dedup)s, %(nom_commercial)s, %(raison_sociale)s,
    %(email)s, %(telephone)s, %(website)s, %(linkedin_url)s,
    %(adresse)s, %(ville)s, %(region)s, %(pays)s, %(code_postal)s,
    %(secteur_activite)s, %(type_entreprise)s, %(taille_entreprise)s,
    %(nombre_employes)s, %(chiffre_affaires)s, %(description)s, %(code_naf)s,
    %(siren)s, %(siret)s,
    %(qualification_score)s, %(score_pct)s, %(statut)s, %(score_detail)s,
    %(criteria_met)s, %(criteria_total)s,
    %(source)s, %(segment)s, %(sector_confidence)s,
    %(email_valid)s, %(website_active)s,
    %(enrich_score)s, %(email_mx_verified)s,
    %(created_at)s, NOW()
)
ON CONFLICT (hash_dedup) DO UPDATE SET
    nom_commercial      = EXCLUDED.nom_commercial,
    raison_sociale      = EXCLUDED.raison_sociale,
    email               = EXCLUDED.email,
    telephone           = EXCLUDED.telephone,
    website             = EXCLUDED.website,
    linkedin_url        = EXCLUDED.linkedin_url,
    adresse             = EXCLUDED.adresse,
    ville               = EXCLUDED.ville,
    region              = EXCLUDED.region,
    pays                = EXCLUDED.pays,
    code_postal         = EXCLUDED.code_postal,
    secteur_activite    = EXCLUDED.secteur_activite,
    type_entreprise     = EXCLUDED.type_entreprise,
    taille_entreprise   = EXCLUDED.taille_entreprise,
    nombre_employes     = EXCLUDED.nombre_employes,
    chiffre_affaires    = EXCLUDED.chiffre_affaires,
    description         = EXCLUDED.description,
    code_naf            = EXCLUDED.code_naf,
    siren               = EXCLUDED.siren,
    siret               = EXCLUDED.siret,
    qualification_score = EXCLUDED.qualification_score,
    score_pct           = EXCLUDED.score_pct,
    statut              = EXCLUDED.statut,
    score_detail        = EXCLUDED.score_detail,
    criteria_met        = EXCLUDED.criteria_met,
    criteria_total      = EXCLUDED.criteria_total,
    source              = EXCLUDED.source,
    segment             = EXCLUDED.segment,
    sector_confidence   = EXCLUDED.sector_confidence,
    email_valid         = EXCLUDED.email_valid,
    website_active      = EXCLUDED.website_active,
    enrich_score        = EXCLUDED.enrich_score,
    email_mx_verified   = EXCLUDED.email_mx_verified,
    updated_at          = NOW();
"""

_SQL_UPSERT_JOB = """
INSERT INTO collection_jobs (
    id, name, type, status, parameters_json,
    started_at, finished_at,
    total_collected, total_cleaned, total_deduped,
    total_scored, total_saved, total_qualified, total_duplicates,
    sources_used, errors
) VALUES (
    %(id)s, %(name)s, %(type)s, %(status)s, %(parameters_json)s,
    %(started_at)s, %(finished_at)s,
    %(total_collected)s, %(total_cleaned)s, %(total_deduped)s,
    %(total_scored)s, %(total_saved)s, %(total_qualified)s, %(total_duplicates)s,
    %(sources_used)s, %(errors)s
)
ON CONFLICT (id) DO UPDATE SET
    status          = EXCLUDED.status,
    finished_at     = EXCLUDED.finished_at,
    total_collected = EXCLUDED.total_collected,
    total_cleaned   = EXCLUDED.total_cleaned,
    total_deduped   = EXCLUDED.total_deduped,
    total_scored    = EXCLUDED.total_scored,
    total_saved     = EXCLUDED.total_saved,
    total_qualified = EXCLUDED.total_qualified,
    total_duplicates = EXCLUDED.total_duplicates,
    sources_used    = EXCLUDED.sources_used,
    errors          = EXCLUDED.errors;
"""


# ══════════════════════════════════════════════════════════════════════════════
# PgRepository
# ══════════════════════════════════════════════════════════════════════════════

class PgRepository:
    """
    Couche de persistance PostgreSQL.

    Cycle de vie de la connexion :
      - Une connexion est ouverte à la demande (_conn) et réutilisée.
      - Elle est refermée proprement par close() ou en sortie de contexte.
      - En cas d'erreur transactionnelle, un rollback est effectué et la
        connexion est remise dans un état propre (autocommit désactivé).

    Toutes les opérations d'écriture sont effectuées dans une transaction
    unique par appel (commit à la fin, rollback sur exception).

    Usage recommandé :
        with PgRepository() as pg:
            pg.upsert_prospects(scored)
            pg.save_job(job)
    """

    def __init__(self, dsn: Optional[str] = None):
        """
        Args:
            dsn: Connection string PostgreSQL. Si None, lu depuis config/settings.py.
        """
        if not _PSYCOPG2_AVAILABLE:
            raise RuntimeError(
                "[PgRepo] psycopg2 non disponible. "
                "Installez-le avec : pip install psycopg2-binary"
            )

        # Résolution du DSN
        if dsn is None:
            from config.settings import PG_DSN
            dsn = PG_DSN

        if not dsn:
            raise ValueError(
                "[PgRepo] PG_DSN non configuré. "
                "Définissez PG_DSN dans config/settings.py ou via la variable d'environnement DATABASE_URL."
            )

        self._dsn  = dsn
        self._conn = None
        self._ensure_schema()

    # ──────────────────────────────────────────
    # Context manager
    # ──────────────────────────────────────────

    def __enter__(self) -> "PgRepository":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.debug("[PgRepo] Connexion fermée.")

    # ──────────────────────────────────────────
    # Connexion (lazy, avec reconnexion auto)
    # ──────────────────────────────────────────

    def _connection(self):
        """Retourne la connexion active, en la (re)créant si nécessaire."""
        if self._conn is None or self._conn.closed:
            logger.debug("[PgRepo] Ouverture de la connexion PostgreSQL…")
            self._conn = psycopg2.connect(self._dsn)
            self._conn.autocommit = False
        return self._conn

    @contextmanager
    def _cursor(self) -> Generator:
        """
        Context manager qui ouvre un curseur dict, commit sur succès,
        rollback + reset sur erreur.
        """
        conn = self._connection()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            yield cur
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"[PgRepo] Transaction annulée : {e}")
            raise
        finally:
            cur.close()

    # ──────────────────────────────────────────
    # Schema auto-creation
    # ──────────────────────────────────────────

    def _ensure_schema(self) -> None:
        """Crée les tables et index si absents (idempotent)."""
        try:
            with self._cursor() as cur:
                cur.execute(_DDL_PROSPECTS)
                cur.execute(_DDL_PROSPECTS_INDEXES)
                cur.execute(_DDL_JOBS)
            logger.info("[PgRepo] Schéma PostgreSQL vérifié/créé.")
        except Exception as e:
            logger.error(f"[PgRepo] Erreur lors de la création du schéma : {e}")
            raise

    # ──────────────────────────────────────────
    # Prospects — écriture
    # ──────────────────────────────────────────

    def upsert_prospects(self, prospects: List[Any]) -> Dict[str, int]:
        """
        Insère ou met à jour une liste de Prospect dans PostgreSQL.
        L'upsert est basé sur hash_dedup (clé primaire).

        Returns:
            {"upserted": N, "skipped": M}
            skipped = prospects sans hash_dedup (ne peuvent pas être upsertés)
        """
        if not prospects:
            return {"upserted": 0, "skipped": 0}

        rows    = [self._prospect_to_row(p) for p in prospects]
        valid   = [r for r in rows if r.get("hash_dedup")]
        skipped = len(rows) - len(valid)

        if skipped:
            logger.warning(
                f"[PgRepo] {skipped} prospect(s) ignorés (hash_dedup manquant)."
            )

        if not valid:
            return {"upserted": 0, "skipped": skipped}

        try:
            with self._cursor() as cur:
                psycopg2.extras.execute_batch(cur, _SQL_UPSERT_PROSPECT, valid, page_size=100)
            logger.info(f"[PgRepo] {len(valid)} prospects upsertés dans PostgreSQL.")
            return {"upserted": len(valid), "skipped": skipped}
        except Exception as e:
            logger.error(f"[PgRepo] Erreur upsert prospects : {e}")
            raise

    # ──────────────────────────────────────────
    # Jobs — écriture
    # ──────────────────────────────────────────

    def save_job(self, job: CollectionJob) -> None:
        """Insère ou met à jour un CollectionJob."""
        row = {
            "id":               job.id,
            "name":             job.name,
            "type":             job.type,
            "status":           job.status,
            "parameters_json":  job.parameters_json,
            "started_at":       _parse_dt(job.started_at),
            "finished_at":      _parse_dt(job.finished_at),
            "total_collected":  job.total_collected,
            "total_cleaned":    job.total_cleaned,
            "total_deduped":    job.total_deduped,
            "total_scored":     job.total_scored,
            "total_saved":      job.total_saved,
            "total_qualified":  job.total_qualified,
            "total_duplicates": job.total_duplicates,
            "sources_used":     job.sources_used or [],
            "errors":           job.errors or [],
        }
        try:
            with self._cursor() as cur:
                cur.execute(_SQL_UPSERT_JOB, row)
            logger.info(f"[PgRepo] Job {job.id!r} sauvegardé dans PostgreSQL.")
        except Exception as e:
            logger.error(f"[PgRepo] Erreur sauvegarde job : {e}")
            raise

    # ──────────────────────────────────────────
    # Prospects — lecture
    # ──────────────────────────────────────────

    def load_qualified(self, limit: int = 500) -> List[Dict]:
        """Retourne les prospects qualifiés triés par score décroissant."""
        sql = """
            SELECT * FROM prospects
            WHERE statut = 'QUALIFIE'
            ORDER BY qualification_score DESC
            LIMIT %(limit)s;
        """
        try:
            with self._cursor() as cur:
                cur.execute(sql, {"limit": limit})
                return [dict(r) for r in cur.fetchall()]
        except Exception as e:
            logger.error(f"[PgRepo] Erreur load_qualified : {e}")
            return []

    def load_by_sector(self, sector: str, limit: int = 200) -> List[Dict]:
        """Retourne les prospects d'un secteur donné."""
        sql = """
            SELECT * FROM prospects
            WHERE secteur_activite ILIKE %(sector)s
            ORDER BY qualification_score DESC
            LIMIT %(limit)s;
        """
        try:
            with self._cursor() as cur:
                cur.execute(sql, {"sector": f"%{sector}%", "limit": limit})
                return [dict(r) for r in cur.fetchall()]
        except Exception as e:
            logger.error(f"[PgRepo] Erreur load_by_sector : {e}")
            return []

    def load_recent(self, days: int = 7, limit: int = 500) -> List[Dict]:
        """Retourne les prospects collectés dans les N derniers jours."""
        sql = """
            SELECT * FROM prospects
            WHERE created_at >= NOW() - INTERVAL '%(days)s days'
            ORDER BY created_at DESC
            LIMIT %(limit)s;
        """
        try:
            with self._cursor() as cur:
                cur.execute(sql, {"days": days, "limit": limit})
                return [dict(r) for r in cur.fetchall()]
        except Exception as e:
            logger.error(f"[PgRepo] Erreur load_recent : {e}")
            return []

    def count(self, statut: Optional[str] = None) -> int:
        """Compte les prospects, optionnellement filtrés par statut."""
        if statut:
            sql = "SELECT COUNT(*) FROM prospects WHERE statut = %(statut)s;"
            params: Dict = {"statut": statut}
        else:
            sql    = "SELECT COUNT(*) FROM prospects;"
            params = {}
        try:
            with self._cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
                return int(row["count"]) if row else 0
        except Exception as e:
            logger.error(f"[PgRepo] Erreur count : {e}")
            return 0

    def get_stats(self) -> Dict[str, Any]:
        """Statistiques agrégées sur la table prospects."""
        sql = """
            SELECT
                COUNT(*)                                        AS total,
                COUNT(*) FILTER (WHERE statut = 'QUALIFIE')    AS qualified,
                ROUND(AVG(qualification_score), 1)             AS avg_score,
                MAX(qualification_score)                        AS max_score,
                ROUND(AVG(enrich_score), 2)                    AS avg_enrich_score,
                COUNT(*) FILTER (WHERE email <> '')            AS with_email,
                COUNT(*) FILTER (WHERE telephone <> '')        AS with_phone,
                COUNT(*) FILTER (WHERE website <> '')          AS with_website,
                COUNT(*) FILTER (WHERE email_mx_verified)      AS mx_verified,
                MIN(created_at)                                 AS oldest,
                MAX(created_at)                                 AS newest
            FROM prospects;
        """
        try:
            with self._cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
                return dict(row) if row else {}
        except Exception as e:
            logger.error(f"[PgRepo] Erreur get_stats : {e}")
            return {}

    # ──────────────────────────────────────────
    # Conversion Prospect → row dict
    # ──────────────────────────────────────────

    @staticmethod
    def _prospect_to_row(p: Any) -> Dict[str, Any]:
        """Convertit un Prospect (ou dict) en dict compatible psycopg2."""
        if isinstance(p, Prospect):
            d = p.to_dict()
        elif isinstance(p, dict):
            d = p
        else:
            d = {}

        def _str(v) -> str:
            return str(v).strip() if v is not None else ""

        def _int(v) -> Optional[int]:
            try:
                return int(v) if v is not None else None
            except (ValueError, TypeError):
                return None

        def _float(v) -> Optional[float]:
            try:
                return float(v) if v is not None else None
            except (ValueError, TypeError):
                return None

        def _bool(v) -> bool:
            if isinstance(v, bool):
                return v
            return str(v).lower() in ("true", "1", "yes", "oui")

        # score_detail : doit être du JSON sérialisé (psycopg2 gère le JSONB
        # via Json() ou via une string JSON valide selon le driver).
        score_detail = d.get("score_detail", {})
        if not isinstance(score_detail, str):
            score_detail = json.dumps(score_detail, ensure_ascii=False, default=str)

        created_at = _parse_dt(d.get("created_at")) or datetime.now()

        return {
            "hash_dedup":           _str(d.get("hash_dedup")),
            "nom_commercial":       _str(d.get("nom_commercial")),
            "raison_sociale":       _str(d.get("raison_sociale")),
            "email":                _str(d.get("email")),
            "telephone":            _str(d.get("telephone")),
            "website":              _str(d.get("website")),
            "linkedin_url":         _str(d.get("linkedin_url")),
            "adresse":              _str(d.get("adresse")),
            "ville":                _str(d.get("ville")),
            "region":               _str(d.get("region")),
            "pays":                 _str(d.get("pays")),
            "code_postal":          _str(d.get("code_postal")),
            "secteur_activite":     _str(d.get("secteur_activite")),
            "type_entreprise":      _str(d.get("type_entreprise")),
            "taille_entreprise":    _str(d.get("taille_entreprise")),
            "nombre_employes":      _int(d.get("nombre_employes")),
            "chiffre_affaires":     _float(d.get("chiffre_affaires")),
            "description":          _str(d.get("description")),
            "code_naf":             _str(d.get("code_naf")),
            "siren":                _str(d.get("siren")),
            "siret":                _str(d.get("siret")),
            "qualification_score":  _int(d.get("qualification_score")) or 0,
            "score_pct":            _float(d.get("score_pct")) or 0.0,
            "statut":               _str(d.get("statut")) or "NON_QUALIFIE",
            "score_detail":         score_detail,
            "criteria_met":         _int(d.get("criteria_met")) or 0,
            "criteria_total":       _int(d.get("criteria_total")) or 0,
            "source":               _str(d.get("source")) or "SCRAPING",
            "segment":              _str(d.get("segment")),
            "sector_confidence":    _float(d.get("sector_confidence")) or 0.0,
            "email_valid":          _bool(d.get("email_valid")),
            "website_active":       _bool(d.get("website_active")),
            "enrich_score":         _int(d.get("enrich_score")) or 0,
            "email_mx_verified":    _bool(d.get("email_mx_verified")),
            "created_at":           created_at,
        }


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def _parse_dt(value: Any) -> Optional[datetime]:
    """Convertit une string ISO 8601 en datetime, ou None si invalide."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except (ValueError, TypeError):
        return None