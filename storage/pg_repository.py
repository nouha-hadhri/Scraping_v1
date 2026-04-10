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

import hashlib
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

    -- Job d'origine
    job_id              TEXT        NOT NULL DEFAULT '',

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
CREATE INDEX IF NOT EXISTS idx_prospects_job_id    ON prospects (job_id);
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
    crit_secteurs_activite_txt TEXT NOT NULL DEFAULT '',
    crit_types_entreprise_txt  TEXT NOT NULL DEFAULT '',
    crit_tailles_entreprise_txt TEXT NOT NULL DEFAULT '',
    crit_pays_txt              TEXT NOT NULL DEFAULT '',
    crit_regions_txt           TEXT NOT NULL DEFAULT '',
    crit_villes_txt            TEXT NOT NULL DEFAULT '',
    crit_keywords_txt          TEXT NOT NULL DEFAULT '',
    crit_max_resultats     INTEGER,
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

_SQL_ALTER_PROSPECTS_ADD_JOB_ID = """
ALTER TABLE prospects
    ADD COLUMN IF NOT EXISTS job_id TEXT NOT NULL DEFAULT '';
"""

_SQL_ALTER_JOBS_ADD_CRITERIA_FIELDS = """
ALTER TABLE collection_jobs
    ADD COLUMN IF NOT EXISTS crit_secteurs_activite_txt TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS crit_types_entreprise_txt TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS crit_tailles_entreprise_txt TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS crit_pays_txt TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS crit_regions_txt TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS crit_villes_txt TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS crit_keywords_txt TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS crit_max_resultats INTEGER;
"""

_SQL_DROP_JOB_ARRAY_CRITERIA_COLUMNS = """
ALTER TABLE collection_jobs
    DROP COLUMN IF EXISTS crit_secteurs_activite,
    DROP COLUMN IF EXISTS crit_types_entreprise,
    DROP COLUMN IF EXISTS crit_tailles_entreprise,
    DROP COLUMN IF EXISTS crit_pays,
    DROP COLUMN IF EXISTS crit_regions,
    DROP COLUMN IF EXISTS crit_villes,
    DROP COLUMN IF EXISTS crit_keywords;
"""

# Upsert (INSERT … ON CONFLICT) pour éviter les doublons par hash_dedup.
# Les champs de scoring et d'enrichissement sont TOUJOURS mis à jour
# car ils peuvent évoluer entre deux runs (re-score, re-enrich).
_SQL_UPSERT_PROSPECT = """
INSERT INTO prospects (
    hash_dedup, job_id, nom_commercial, raison_sociale,
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
    %(hash_dedup)s, %(job_id)s, %(nom_commercial)s, %(raison_sociale)s,
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
    job_id              = EXCLUDED.job_id,
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
    crit_secteurs_activite_txt, crit_types_entreprise_txt, crit_tailles_entreprise_txt,
    crit_pays_txt, crit_regions_txt, crit_villes_txt, crit_keywords_txt,
    crit_max_resultats,
    started_at, finished_at,
    total_collected, total_cleaned, total_deduped,
    total_scored, total_saved, total_qualified, total_duplicates,
    sources_used, errors
) VALUES (
    %(id)s, %(name)s, %(type)s, %(status)s, %(parameters_json)s,
    %(crit_secteurs_activite_txt)s, %(crit_types_entreprise_txt)s, %(crit_tailles_entreprise_txt)s,
    %(crit_pays_txt)s, %(crit_regions_txt)s, %(crit_villes_txt)s, %(crit_keywords_txt)s,
    %(crit_max_resultats)s,
    %(started_at)s, %(finished_at)s,
    %(total_collected)s, %(total_cleaned)s, %(total_deduped)s,
    %(total_scored)s, %(total_saved)s, %(total_qualified)s, %(total_duplicates)s,
    %(sources_used)s, %(errors)s
)
ON CONFLICT (id) DO UPDATE SET
    status          = EXCLUDED.status,
    parameters_json = EXCLUDED.parameters_json,
    crit_secteurs_activite_txt = EXCLUDED.crit_secteurs_activite_txt,
    crit_types_entreprise_txt = EXCLUDED.crit_types_entreprise_txt,
    crit_tailles_entreprise_txt = EXCLUDED.crit_tailles_entreprise_txt,
    crit_pays_txt = EXCLUDED.crit_pays_txt,
    crit_regions_txt = EXCLUDED.crit_regions_txt,
    crit_villes_txt = EXCLUDED.crit_villes_txt,
    crit_keywords_txt = EXCLUDED.crit_keywords_txt,
    crit_max_resultats = EXCLUDED.crit_max_resultats,
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
                cur.execute(_SQL_ALTER_PROSPECTS_ADD_JOB_ID)
                cur.execute(_DDL_PROSPECTS_INDEXES)
                cur.execute(_DDL_JOBS)
                cur.execute(_SQL_ALTER_JOBS_ADD_CRITERIA_FIELDS)
                cur.execute(_SQL_DROP_JOB_ARRAY_CRITERIA_COLUMNS)
            logger.info("[PgRepo] Schéma PostgreSQL vérifié/créé.")
        except Exception as e:
            logger.error(f"[PgRepo] Erreur lors de la création du schéma : {e}")
            raise

    # ──────────────────────────────────────────
    # Prospects — écriture
    # ──────────────────────────────────────────

    def upsert_prospects(self, prospects: List[Any], job_id: str = "") -> Dict[str, int]:
        """
        Insère ou met à jour les prospects qualifiés dans PostgreSQL.
        L'upsert est basé sur hash_dedup (clé primaire).
        Si job_id est fourni, le hash est "scopé job" pour éviter les collisions
        entre exécutions différentes.

        Returns:
            {"upserted": N, "skipped": M, "non_qualified": K}
            skipped = prospects qualifiés sans hash_dedup (ne peuvent pas être upsertés)
            non_qualified = prospects ignorés car statut != QUALIFIE
        """
        if not prospects:
            return {"upserted": 0, "skipped": 0, "non_qualified": 0}

        qualified = [p for p in prospects if self._is_qualified(p)]
        non_qualified = len(prospects) - len(qualified)

        if non_qualified:
            logger.info(f"[PgRepo] {non_qualified} prospect(s) non qualifiés ignorés.")

        rows    = [self._prospect_to_row(p, job_id=job_id) for p in qualified]
        valid   = [r for r in rows if r.get("hash_dedup")]
        skipped = len(rows) - len(valid)

        if skipped:
            logger.warning(
                f"[PgRepo] {skipped} prospect(s) qualifié(s) ignorés (hash_dedup manquant)."
            )

        if not valid:
            return {"upserted": 0, "skipped": skipped, "non_qualified": non_qualified}

        try:
            with self._cursor() as cur:
                psycopg2.extras.execute_batch(cur, _SQL_UPSERT_PROSPECT, valid, page_size=100)
            logger.info(f"[PgRepo] {len(valid)} prospects qualifiés upsertés dans PostgreSQL.")
            return {"upserted": len(valid), "skipped": skipped, "non_qualified": non_qualified}
        except Exception as e:
            logger.error(f"[PgRepo] Erreur upsert prospects : {e}")
            raise

    # ──────────────────────────────────────────
    # Jobs — écriture
    # ──────────────────────────────────────────

    def save_job(self, job: CollectionJob) -> None:
        """Insère ou met à jour un CollectionJob."""
        params = _parse_json_dict(job.parameters_json)
        criteria = _extract_job_criteria(params)

        row = {
            "id":               job.id,
            "name":             job.name,
            "type":             getattr(job, "type", "SCRAPING"),
            "status":           job.status,
            "parameters_json":  job.parameters_json,
            "crit_secteurs_activite_txt": _join_text_list(criteria["crit_secteurs_activite"]),
            "crit_types_entreprise_txt":  _join_text_list(criteria["crit_types_entreprise"]),
            "crit_tailles_entreprise_txt": _join_text_list(criteria["crit_tailles_entreprise"]),
            "crit_pays_txt": _join_text_list(criteria["crit_pays"]),
            "crit_regions_txt": _join_text_list(criteria["crit_regions"]),
            "crit_villes_txt": _join_text_list(criteria["crit_villes"]),
            "crit_keywords_txt": _join_text_list(criteria["crit_keywords"]),
            "crit_max_resultats": criteria["crit_max_resultats"],
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
    def _prospect_to_row(p: Any, job_id: str = "") -> Dict[str, Any]:
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
        resolved_job_id = _str(job_id) or _str(d.get("job_id"))
        hash_raw = _str(d.get("hash_dedup"))
        hash_scoped = PgRepository._scope_hash_with_job(hash_raw, resolved_job_id)

        return {
            "hash_dedup":           hash_scoped,
            "job_id":               resolved_job_id,
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

    @staticmethod
    def _scope_hash_with_job(hash_dedup: str, job_id: str) -> str:
        """
        Retourne un hash de déduplication contenant le job_id.

        - Sans job_id: conserve le hash d'origine.
        - Avec job_id: génère SHA-256(job_id|hash_dedup), tronqué à 16 hex.
        """
        base_hash = (hash_dedup or "").strip()
        if not base_hash:
            return ""

        scoped_job = (job_id or "").strip()
        if not scoped_job:
            return base_hash

        raw = f"{scoped_job}|{base_hash}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    @staticmethod
    def _is_qualified(p: Any) -> bool:
        """Retourne True uniquement pour les prospects qualifiés."""
        if isinstance(p, Prospect):
            statut = p.statut
        elif isinstance(p, dict):
            statut = p.get("statut", "")
        else:
            statut = getattr(p, "statut", "")

        return str(statut).upper() == "QUALIFIE"


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


def _parse_json_dict(value: Any) -> Dict[str, Any]:
    """Retourne un dict JSON valide (ou {} si invalide)."""
    if isinstance(value, dict):
        return value
    if value is None:
        return {}
    try:
        parsed = json.loads(str(value))
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}


def _extract_job_criteria(params: Dict[str, Any]) -> Dict[str, Any]:
    """Normalise les variantes de payload critères (interne + Spring/UI)."""
    if not isinstance(params, dict):
        params = {}

    secteurs = params.get("secteurs_activite", params.get("secteur_activite", []))
    types = params.get("types_entreprise", params.get("type_entreprise", []))
    keywords = (
        params.get("keywords")
        or params.get("mots_cles")
        or params.get("motsCles")
        or []
    )

    tailles = params.get("tailles_entreprise", [])
    if not tailles:
        taille_obj = params.get("taille_entreprise", [])
        if isinstance(taille_obj, dict):
            tailles = taille_obj.get("categories", [])
            if not tailles:
                min_emp = _safe_int(taille_obj.get("nb_employes_min"))
                max_emp = _safe_int(taille_obj.get("nb_employes_max"))
                if min_emp is not None or max_emp is not None:
                    label_min = str(min_emp) if min_emp is not None else "0"
                    label_max = str(max_emp) if max_emp is not None else "+"
                    tailles = [f"{label_min}-{label_max}"]
        else:
            tailles = taille_obj

    localisation = params.get("localisation") if isinstance(params.get("localisation"), dict) else {}
    if not localisation:
        zone = params.get("zone_geographique") or params.get("zoneGeographique")
        if isinstance(zone, dict):
            localisation = {
                "pays": zone.get("pays", zone.get("zone_geographique", [])),
                "regions": zone.get("regions", []),
                "villes": zone.get("villes", []),
            }

    # Supporte aussi les payloads plats ou variantes où zone_geographique est une liste.
    pays = localisation.get("pays", [])
    if not pays:
        pays = localisation.get("zone_geographique", [])
    if not pays:
        pays = params.get("pays", [])
    if not pays:
        zone_raw = params.get("zone_geographique", params.get("zoneGeographique", []))
        if not isinstance(zone_raw, dict):
            pays = zone_raw

    regions = localisation.get("regions", []) or params.get("regions", []) or params.get("region", [])
    villes = localisation.get("villes", []) or params.get("villes", []) or params.get("ville", [])

    max_resultats = params.get("max_resultats", params.get("max_prospects_total"))

    return {
        "crit_secteurs_activite": _as_text_list(secteurs),
        "crit_types_entreprise": _as_text_list(types),
        "crit_tailles_entreprise": _as_text_list(tailles),
        "crit_pays": _as_text_list(pays),
        "crit_regions": _as_text_list(regions),
        "crit_villes": _as_text_list(villes),
        "crit_keywords": _as_text_list(keywords),
        "crit_max_resultats": _safe_int(max_resultats),
    }


def _as_text_list(value: Any) -> List[str]:
    """Normalise une valeur (str/list/tuple/set) en liste de strings non vides."""
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if isinstance(value, (list, tuple, set)):
        out: List[str] = []
        for item in value:
            text = str(item).strip()
            if text:
                out.append(text)
        return out
    text = str(value).strip()
    return [text] if text else []


def _safe_int(value: Any) -> Optional[int]:
    """Convertit une valeur en int si possible, sinon None."""
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _join_text_list(values: List[str]) -> str:
    """Retourne une représentation texte lisible sans accolades PostgreSQL."""
    return ", ".join(v for v in values if str(v).strip())