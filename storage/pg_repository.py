"""
SCRAPING_V1 - PgRepository
Couche de persistance PostgreSQL pour les Prospect et CollectionJob.

Complète ProspectRepository (CSV/JSON) — les deux coexistent.
Activé via PG_ENABLED=True dans config/settings.py.

Corrections appliquées :
  1. source          : TEXT au lieu de SMALLINT dans le DDL auto-migration
  2. qualification   : "statut" renommé "qualification" dans _prospect_to_row
                       et dans les requêtes de lecture (load_qualified, count, get_stats)
  3. secteur_activite_id dans collection_jobs : BIGINT[] (tableau) cohérent avec le SQL Flyway
  4. hash_dedup      : ajout de la contrainte UNIQUE dans _ensure_schema pour ON CONFLICT
  5. is_converted    : champ présent dans le DDL auto-migration (cohérence avec Flyway)
  6. segment         : colonne supprimée du DDL auto-migration (absente du schéma Flyway)

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

from storage.models import Prospect, CollectionJob, TailleEntreprise, TypeEntreprise

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
# DDL — Python ne crée PAS les tables (Spring/Flyway est le seul DDL owner).
# Ces statements sont tous des ADD COLUMN IF NOT EXISTS / contraintes — sans
# effet si la colonne ou la contrainte existe déjà (idempotents).
# ══════════════════════════════════════════════════════════════════════════════

# Contrainte UNIQUE sur hash_dedup — obligatoire pour ON CONFLICT (hash_dedup)
_SQL_ADD_UNIQUE_HASH_DEDUP = """
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_prospect_hash_dedup'
    ) THEN
        ALTER TABLE prospects
            ADD CONSTRAINT uq_prospect_hash_dedup UNIQUE (hash_dedup);
    END IF;
END $$;
"""

# source_origin : stocke la source de données réelle (open_data, annuaire, sirene…)
# Colonne ajoutée par Python — absente du schéma Flyway initial.
_SQL_ALTER_PROSPECTS_ADD_SOURCE_ORIGIN = """
ALTER TABLE prospects
    ADD COLUMN IF NOT EXISTS source_origin TEXT NOT NULL DEFAULT '';
"""

# Indexes sur colonnes que Spring ne crée pas forcément
_DDL_PROSPECTS_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_prospects_qualification ON prospects (qualification);
CREATE INDEX IF NOT EXISTS idx_prospects_job_id        ON prospects (job_id);
CREATE INDEX IF NOT EXISTS idx_prospects_score         ON prospects (qualification_score DESC);
CREATE INDEX IF NOT EXISTS idx_prospects_enrich        ON prospects (enrich_score DESC);
CREATE INDEX IF NOT EXISTS idx_prospects_created       ON prospects (created_at DESC);
"""

# Indexes conditionnels : créés seulement si la colonne cible existe dans la table
# (certaines colonnes comme siren/email/ville peuvent être présentes ou non selon
#  la version du schéma Flyway déployée).
_DDL_PROSPECTS_CONDITIONAL_INDEXES = """
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_name='prospects' AND column_name='siren') THEN
        EXECUTE $idx$CREATE INDEX IF NOT EXISTS idx_prospects_siren
                    ON prospects (siren) WHERE siren <> ''$idx$;
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_name='prospects' AND column_name='email') THEN
        EXECUTE $idx$CREATE INDEX IF NOT EXISTS idx_prospects_email
                    ON prospects (email) WHERE email <> ''$idx$;
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_name='prospects' AND column_name='ville') THEN
        EXECUTE $idx$CREATE INDEX IF NOT EXISTS idx_prospects_ville
                    ON prospects (ville)$idx$;
    END IF;
END $$;
"""

# collection_jobs : colonnes critères ajoutées par Python si absentes
_SQL_ALTER_JOBS_ADD_CRITERIA_FIELDS = """
ALTER TABLE collection_jobs
    ADD COLUMN IF NOT EXISTS crit_secteurs_activite_txt  TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS crit_secteurs_activite_id   BIGINT[],
    ADD COLUMN IF NOT EXISTS crit_types_entreprise_txt   TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS crit_tailles_entreprise_txt TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS crit_pays_txt               TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS crit_regions_txt            TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS crit_villes_txt             TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS crit_keywords_txt           TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS crit_max_resultats          INTEGER;
"""


# FIX : "qualification" au lieu de "statut" — cohérence avec Java/Flyway
# NOTE : secteur_activite (TEXT) n'existe pas dans le schéma Spring — seul
#        secteur_activite_id (BIGINT FK) est présent. Python stocke le nom
#        textuel dans description ou le laisse null ; la résolution FK est
#        faite côté Spring.
_SQL_INSERT_PROSPECT = """
INSERT INTO prospects (
    hash_dedup, job_id, nom_commercial, raison_sociale,
    email, telephone, website, linkedin_url,
    adresse, ville, region, pays, code_postal,
    secteur_activite_id,
    type_entreprise, taille_entreprise,
    nombre_employes, chiffre_affaires, description, code_naf,
    siren, siret,
    qualification_score, score_pct, qualification, score_detail,
    criteria_met, criteria_total,
    source, source_origin, sector_confidence,
    email_valid, website_active,
    enrich_score, email_mx_verified,
    created_at, updated_at, is_deleted
) VALUES (
    %(hash_dedup)s, %(job_id)s, %(nom_commercial)s, %(raison_sociale)s,
    %(email)s, %(telephone)s, %(website)s, %(linkedin_url)s,
    %(adresse)s, %(ville)s, %(region)s, %(pays)s, %(code_postal)s,
    %(secteur_activite_id)s,
    %(type_entreprise)s, %(taille_entreprise)s,
    %(nombre_employes)s, %(chiffre_affaires)s, %(description)s, %(code_naf)s,
    %(siren)s, %(siret)s,
    %(qualification_score)s, %(score_pct)s, %(qualification)s, %(score_detail)s,
    %(criteria_met)s, %(criteria_total)s,
    %(source)s, %(source_origin)s, %(sector_confidence)s,
    %(email_valid)s, %(website_active)s,
    %(enrich_score)s, %(email_mx_verified)s,
    %(created_at)s, NOW(), false
)
ON CONFLICT (hash_dedup) DO UPDATE SET
    job_id               = EXCLUDED.job_id,
    nom_commercial       = EXCLUDED.nom_commercial,
    raison_sociale       = EXCLUDED.raison_sociale,
    email                = EXCLUDED.email,
    telephone            = EXCLUDED.telephone,
    website              = EXCLUDED.website,
    linkedin_url         = EXCLUDED.linkedin_url,
    adresse              = EXCLUDED.adresse,
    ville                = EXCLUDED.ville,
    region               = EXCLUDED.region,
    pays                 = EXCLUDED.pays,
    code_postal          = EXCLUDED.code_postal,
    secteur_activite_id  = EXCLUDED.secteur_activite_id,
    type_entreprise      = EXCLUDED.type_entreprise,
    taille_entreprise    = EXCLUDED.taille_entreprise,
    nombre_employes      = EXCLUDED.nombre_employes,
    chiffre_affaires     = EXCLUDED.chiffre_affaires,
    description          = EXCLUDED.description,
    code_naf             = EXCLUDED.code_naf,
    siren                = EXCLUDED.siren,
    siret                = EXCLUDED.siret,
    qualification_score  = EXCLUDED.qualification_score,
    score_pct            = EXCLUDED.score_pct,
    qualification        = EXCLUDED.qualification,
    score_detail         = EXCLUDED.score_detail,
    criteria_met         = EXCLUDED.criteria_met,
    criteria_total       = EXCLUDED.criteria_total,
    source               = EXCLUDED.source,
    source_origin        = EXCLUDED.source_origin,
    sector_confidence    = EXCLUDED.sector_confidence,
    email_valid          = EXCLUDED.email_valid,
    website_active       = EXCLUDED.website_active,
    enrich_score         = EXCLUDED.enrich_score,
    email_mx_verified    = EXCLUDED.email_mx_verified,
    updated_at           = NOW();
"""

_SQL_INSERT_JOB = """
INSERT INTO collection_jobs (
    id, name, type, status, parameters_json,
    crit_secteurs_activite_txt, crit_secteurs_activite_id,
    crit_types_entreprise_txt, crit_tailles_entreprise_txt,
    crit_pays_txt, crit_regions_txt, crit_villes_txt, crit_keywords_txt,
    crit_max_resultats,
    started_at, finished_at,
    total_collected, total_cleaned, total_deduped,
    total_scored, total_saved, total_qualified, total_duplicates,
    sources_used, errors, is_deleted
) VALUES (
    %(id)s, %(name)s, %(type)s, %(status)s, %(parameters_json)s,
    %(crit_secteurs_activite_txt)s, %(crit_secteurs_activite_id)s,
    %(crit_types_entreprise_txt)s, %(crit_tailles_entreprise_txt)s,
    %(crit_pays_txt)s, %(crit_regions_txt)s, %(crit_villes_txt)s, %(crit_keywords_txt)s,
    %(crit_max_resultats)s,
    %(started_at)s, %(finished_at)s,
    %(total_collected)s, %(total_cleaned)s, %(total_deduped)s,
    %(total_scored)s, %(total_saved)s, %(total_qualified)s, %(total_duplicates)s,
    %(sources_used)s, %(errors)s, false
) ON CONFLICT (id) DO UPDATE SET
    name                        = EXCLUDED.name,
    type                        = EXCLUDED.type,
    status                      = EXCLUDED.status,
    parameters_json             = EXCLUDED.parameters_json,
    crit_secteurs_activite_txt  = EXCLUDED.crit_secteurs_activite_txt,
    crit_secteurs_activite_id   = EXCLUDED.crit_secteurs_activite_id,
    crit_types_entreprise_txt   = EXCLUDED.crit_types_entreprise_txt,
    crit_tailles_entreprise_txt = EXCLUDED.crit_tailles_entreprise_txt,
    crit_pays_txt               = EXCLUDED.crit_pays_txt,
    crit_regions_txt            = EXCLUDED.crit_regions_txt,
    crit_villes_txt             = EXCLUDED.crit_villes_txt,
    crit_keywords_txt           = EXCLUDED.crit_keywords_txt,
    crit_max_resultats          = EXCLUDED.crit_max_resultats,
    started_at                  = EXCLUDED.started_at,
    finished_at                 = EXCLUDED.finished_at,
    total_collected             = EXCLUDED.total_collected,
    total_cleaned               = EXCLUDED.total_cleaned,
    total_deduped               = EXCLUDED.total_deduped,
    total_scored                = EXCLUDED.total_scored,
    total_saved                 = EXCLUDED.total_saved,
    total_qualified             = EXCLUDED.total_qualified,
    total_duplicates            = EXCLUDED.total_duplicates,
    sources_used                = EXCLUDED.sources_used,
    errors                      = EXCLUDED.errors,
    is_deleted                  = FALSE;
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
        if not _PSYCOPG2_AVAILABLE:
            raise RuntimeError(
                "[PgRepo] psycopg2 non disponible. "
                "Installez-le avec : pip install psycopg2-binary"
            )

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
        """
        Vérifie que les extensions Python sont présentes dans le schéma Flyway/Spring.

        IMPORTANT : Python ne crée PAS les tables — Spring/Flyway est le seul
        DDL owner.  Cette méthode se limite à :
          1. Ajouter les colonnes que Python utilise et que Flyway ne déclare pas
             (source_origin).
          2. Créer la contrainte UNIQUE sur hash_dedup (obligatoire pour ON CONFLICT).
          3. Ajouter les colonnes de critères sur collection_jobs si absentes.
          4. Créer des index utiles (conditionnels selon les colonnes présentes).
        Tous les statements sont idempotents (IF NOT EXISTS / DO $$ … END $$).
        """
        try:
            with self._cursor() as cur:
                # ── Colonne source_origin (Python-only) ───────────────────
                cur.execute(_SQL_ALTER_PROSPECTS_ADD_SOURCE_ORIGIN)

                # ── Contrainte UNIQUE hash_dedup (pour ON CONFLICT) ───────
                cur.execute(_SQL_ADD_UNIQUE_HASH_DEDUP)

                # ── Critères job (colonnes potentiellement absentes) ──────
                cur.execute(_SQL_ALTER_JOBS_ADD_CRITERIA_FIELDS)

                # ── Index utiles (colonnes garanties par Spring) ──────────
                cur.execute(_DDL_PROSPECTS_INDEXES)

                # ── Index conditionnels (colonnes selon version Flyway) ───
                cur.execute(_DDL_PROSPECTS_CONDITIONAL_INDEXES)

            logger.info("[PgRepo] Schéma PostgreSQL vérifié (extensions Python appliquées).")
        except Exception as e:
            logger.error(f"[PgRepo] Erreur lors de la création du schéma : {e}")
            raise

    # ──────────────────────────────────────────
    # Prospects — écriture
    # ──────────────────────────────────────────

    def upsert_prospects(self, prospects: List[Any], job_id: str = "") -> Dict[str, int]:
        print("[PgRepo][PRINT] upsert_prospects called with", len(prospects), "prospects, job_id=", job_id)
        """
        Insère ou met à jour les prospects qualifiés dans PostgreSQL.
        L'upsert est basé sur hash_dedup (contrainte UNIQUE).

        Seuls les prospects avec qualification QUALIFIE sont persistés.
        Retourne un dict {"upserted": N, "skipped": N, "non_qualified": N}.
        """
        qualified     = [p for p in prospects if self._is_qualified(p)]
        non_qualified = len(prospects) - len(qualified)

        if non_qualified:
            logger.info(f"[PgRepo] {non_qualified} prospect(s) non qualifiés ignorés.")

        rows = [self._prospect_to_row(p, job_id=job_id) for p in qualified]

        # Génération d'un hash de secours pour les lignes sans hash_dedup
        for r in rows:
            if not r.get("hash_dedup"):
                raw = "|".join([
                    str(r.get("nom_commercial") or ""),
                    str(r.get("ville") or ""),
                    str(r.get("siren") or ""),
                    str(r.get("email") or ""),
                    str(r.get("website") or ""),
                ])
                r["hash_dedup"] = hashlib.sha256(raw.encode()).hexdigest()[:16]
                logger.debug(f"[PgRepo] hash_dedup fallback généré pour {r.get('nom_commercial')!r}")

        valid   = [r for r in rows if r.get("hash_dedup") and r.get("job_id") is not None]
        skipped = len(rows) - len(valid)

        logger.info(f"[PgRepo][DEBUG] Tentative d'upsert de {len(rows)} prospects qualifiés pour job_id={job_id}")
        for i, r in enumerate(rows[:10]):
            logger.info(
                f"[PgRepo][DEBUG] Prospect {i}: hash_dedup={r.get('hash_dedup')}, "
                f"job_id={r.get('job_id')}, nom={r.get('nom_commercial')}, email={r.get('email')}"
            )

        if skipped:
            logger.warning(f"[PgRepo] {skipped} prospect(s) qualifié(s) ignorés (hash_dedup ou job_id non résolu).")

        if not valid:
            return {"upserted": 0, "skipped": skipped, "non_qualified": non_qualified}

        try:
            with self._cursor() as cur:
                psycopg2.extras.execute_batch(cur, _SQL_INSERT_PROSPECT, valid, page_size=100)
            logger.info(f"[PgRepo] {len(valid)} prospects qualifiés insérés dans PostgreSQL.")
            return {"upserted": len(valid), "skipped": skipped, "non_qualified": non_qualified}
        except Exception as e:
            logger.error(f"[PgRepo] Erreur insert prospects : {e}")
            raise

    # ──────────────────────────────────────────
    # Jobs — écriture
    # ──────────────────────────────────────────

    def save_job(self, job: CollectionJob) -> None:
        """Insère ou met à jour un CollectionJob."""
        params          = _parse_json_dict(job.parameters_json)
        criteria        = _extract_job_criteria(params)
        resolved_job_id = _coerce_job_id_bigint(job.id)

        row = {
            "id":               resolved_job_id,
            "name":             job.name,
            "type":             getattr(job, "type", "SCRAPING"),
            "status":           job.status,
            "parameters_json":  job.parameters_json,
            "crit_secteurs_activite_txt":  _join_text_list(criteria["crit_secteurs_activite"]),
            # FIX #3 : liste d'entiers (BIGINT[]) — psycopg2 la sérialise en tableau PostgreSQL
            "crit_secteurs_activite_id":   criteria["crit_secteurs_activite_id"] or None,
            "crit_types_entreprise_txt":   _join_text_list(criteria["crit_types_entreprise"]),
            "crit_tailles_entreprise_txt": _join_text_list(criteria["crit_tailles_entreprise"]),
            "crit_pays_txt":               _join_text_list(criteria["crit_pays"]),
            "crit_regions_txt":            _join_text_list(criteria["crit_regions"]),
            "crit_villes_txt":             _join_text_list(criteria["crit_villes"]),
            "crit_keywords_txt":           _join_text_list(criteria["crit_keywords"]),
            "crit_max_resultats":          criteria["crit_max_resultats"],
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

        if row["id"] is None:
            raise ValueError(
                f"[PgRepo] job.id invalide pour PostgreSQL BIGINT: {job.id!r}. "
                "Le backend Spring doit fournir un jobId numérique."
            )

        try:
            logger.info("[PG JOB INSERT] Données insérées :\n%s",
                        json.dumps(row, ensure_ascii=False, indent=2, default=str))
        except Exception as log_exc:
            logger.warning(f"[PG JOB INSERT] Impossible d'afficher row : {log_exc}")

        try:
            with self._cursor() as cur:
                cur.execute(_SQL_INSERT_JOB, row)
            logger.info(f"[PgRepo] Job {job.id!r} inséré dans PostgreSQL.")
        except Exception as e:
            logger.error(f"[PgRepo] Erreur insert job : {e}")
            raise

    # ──────────────────────────────────────────
    # Prospects — lecture
    # ──────────────────────────────────────────

    def load_qualified(self, limit: int = 500) -> List[Dict]:
        """Retourne les prospects qualifiés triés par score décroissant."""
        # FIX #2 : filtre sur "qualification" et non "statut"
        sql = """
            SELECT * FROM prospects
            WHERE qualification = 'QUALIFIE'
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
        """Retourne les prospects dont la description contient le secteur donné.
        NOTE : la colonne secteur_activite (TEXT) n'existe pas dans le schéma Spring —
        la recherche se fait sur description qui contient le texte du secteur.
        """
        sql = """
            SELECT * FROM prospects
            WHERE description ILIKE %(sector)s
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
            WHERE created_at >= NOW() - (%(days)s || ' days')::INTERVAL
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

    def count(self, qualification: Optional[str] = None) -> int:
        """Compte les prospects, optionnellement filtrés par qualification."""
        # FIX #2 : paramètre renommé de "statut" en "qualification"
        if qualification:
            sql    = "SELECT COUNT(*) FROM prospects WHERE qualification = %(qualification)s;"
            params: Dict = {"qualification": qualification}
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
        # FIX #2 : filtre sur "qualification" et non "statut"
        sql = """
            SELECT
                COUNT(*)                                              AS total,
                COUNT(*) FILTER (WHERE qualification = 'QUALIFIE')   AS qualified,
                ROUND(AVG(qualification_score), 1)                    AS avg_score,
                MAX(qualification_score)                              AS max_score,
                ROUND(AVG(enrich_score), 2)                           AS avg_enrich_score,
                COUNT(*) FILTER (WHERE email <> '')                   AS with_email,
                COUNT(*) FILTER (WHERE telephone <> '')               AS with_phone,
                COUNT(*) FILTER (WHERE website <> '')                 AS with_website,
                COUNT(*) FILTER (WHERE email_mx_verified)             AS mx_verified,
                MIN(created_at)                                       AS oldest,
                MAX(created_at)                                       AS newest
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

        def _enum(v, enum_cls):
            """
            Safe enum lookup against the Python enum class (which mirrors the
            Spring enum exactly).  Any value not present in the enum — including
            empty string, None, or an unrecognised scraper value — resolves to
            the UNKNOWN sentinel (""), which this function then converts to None
            so psycopg2 writes SQL NULL.  NULL is accepted by Hibernate's CHECK
            constraint on nullable enum columns; an empty string is not.
            """
            s = str(v).strip().upper() if v is not None else ""
            try:
                result = enum_cls(s)
            except ValueError:
                result = enum_cls.UNKNOWN
            # UNKNOWN sentinel value is "" — map to None (SQL NULL)
            return result.value or None

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

        score_detail = d.get("score_detail", {})
        if not isinstance(score_detail, str):
            score_detail = json.dumps(score_detail, ensure_ascii=False, default=str)

        created_at          = _parse_dt(d.get("created_at")) or datetime.now()
        resolved_job_id_raw = _str(job_id) or _str(d.get("job_id"))
        # FIX – job_id coercion: _int() silently returns None for non-numeric
        # strings such as the short-UUID fallback ("d59f99ef") generated when no
        # external_job_id is provided.  The guard on line 411 then drops every
        # row, logging the misleading "job_id manquant" warning.
        # Use _coerce_job_id_bigint() (same helper as save_job) so any string —
        # numeric or UUID-style — always resolves to a valid BIGINT via SHA-256.
        resolved_job_id     = _coerce_job_id_bigint(resolved_job_id_raw)
        hash_raw            = _str(d.get("hash_dedup"))
        hash_scoped         = PgRepository._scope_hash_with_job(
            hash_raw,
            str(resolved_job_id) if resolved_job_id is not None else "",
        )

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
            # secteur_activite (TEXT) absent du schéma Spring — seul secteur_activite_id (FK) est inséré
            "secteur_activite_id":  _int(d.get("secteur_activite_id")),
            "type_entreprise":      _enum(d.get("type_entreprise"),   TypeEntreprise),
            "taille_entreprise":    _enum(d.get("taille_entreprise"),  TailleEntreprise),
            "nombre_employes":      _int(d.get("nombre_employes")),
            "chiffre_affaires":     _float(d.get("chiffre_affaires")),
            "description":          _str(d.get("description")),
            "code_naf":             _str(d.get("code_naf")),
            "siren":                _str(d.get("siren")),
            "siret":                _str(d.get("siret")),
            "qualification_score":  _int(d.get("qualification_score")) or 0,
            "score_pct":            _float(d.get("score_pct")) or 0.0,
            # FIX #2 : clé "qualification" — lecture depuis "statut" ou "qualification" du modèle Python
            "qualification":        _str(d.get("qualification") or d.get("statut")) or "NON_QUALIFIE",
            "score_detail":         score_detail,
            "criteria_met":         _int(d.get("criteria_met")) or 0,
            "criteria_total":       _int(d.get("criteria_total")) or 0,
            # FIX – prospects_source_check: Hibernate generates the CHECK constraint
            # from LeadSource enum values.  The correct value is 'AUTOPROSPECTION'
            # (no underscore) — matches LeadSource.AUTOPROSPECTION in Spring.
            "source":               "AUTOPROSPECTION",
            # source_origin = source de données réelle (anciennement "source")
            "source_origin":        _str(d.get("source_origin") or d.get("source")) or "",
            "sector_confidence":    _float(d.get("sector_confidence")) or 0.0,
            "email_valid":          _bool(d.get("email_valid")),
            "website_active":       _bool(d.get("website_active")),
            "enrich_score":         _int(d.get("enrich_score")) or 0,
            "email_mx_verified":    _bool(d.get("email_mx_verified")),
            "created_at":           created_at,
            "updated_at":           created_at,
            "is_deleted":           False,
        }

    @staticmethod
    def _scope_hash_with_job(hash_dedup: str, job_id: str) -> str:
        """
        Retourne un hash de déduplication scopé au job.
        - Sans job_id : conserve le hash d'origine.
        - Avec job_id : génère SHA-256(job_id|hash_dedup), tronqué à 16 hex.
        """
        base_hash  = (hash_dedup or "").strip()
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
            # FIX #2 : lecture depuis .qualification en priorité, .statut en fallback
            statut = getattr(p, "qualification", None) or getattr(p, "statut", "")
        elif isinstance(p, dict):
            statut = p.get("qualification") or p.get("statut", "")
        else:
            statut = getattr(p, "qualification", "") or getattr(p, "statut", "")
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


def _safe_int(value: Any) -> Optional[int]:
    """Convertit une valeur en int, ou None si impossible."""
    try:
        return int(value) if value is not None else None
    except (ValueError, TypeError):
        return None


def _coerce_job_id_bigint(value: Any) -> Optional[int]:
    """
    Convertit un job_id en BIGINT PostgreSQL.
    - Si numérique : cast direct.
    - Si alphanumérique (UUID, etc.) : dérive un BIGINT via SHA-256 (63 bits signés positifs).
    - Si None/vide : retourne None.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except (ValueError, TypeError):
        pass
    derived = int.from_bytes(
        hashlib.sha256(text.encode("utf-8")).digest()[:8], "big"
    )
    derived &= (1 << 63) - 1
    return derived or 1


def _as_text_list(value: Any) -> List[str]:
    """
    Normalise une valeur (str / list / tuple / set / autre) en liste de strings non vides.
    """
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


def _as_int_list(value: Any) -> List[int]:
    """Normalise une valeur en liste d'entiers non nuls."""
    if value is None:
        return []
    if isinstance(value, int):
        return [value]
    if isinstance(value, (list, tuple, set)):
        return [int(x) for x in value if str(x).strip().isdigit()]
    if str(value).strip().isdigit():
        return [int(value)]
    return []


def _join_text_list(values: List[str]) -> str:
    """Retourne une représentation texte lisible sans accolades PostgreSQL."""
    return ", ".join(v for v in values if str(v).strip())


def _extract_job_criteria(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalise les variantes de payload critères (interne + Spring/UI)
    et retourne un dict prêt à être inséré dans collection_jobs.

    Gère les deux structures possibles dans parameters_json :
      - Format orchestrateur interne : localisation.{pays, regions, villes}
      - Format Spring/bridge          : zone_geographique.{zone_geographique, regions, villes}
    """
    if not isinstance(params, dict):
        params = {}

    # ── Secteurs ──────────────────────────────────────────────────────────────
    secteurs    = params.get("secteurs_activite", params.get("secteur_activite", []))
    secteurs_id = params.get("secteurs_activite_id", params.get("secteur_activite_id", []))

    # ── Types ─────────────────────────────────────────────────────────────────
    types = params.get("types_entreprise", params.get("type_entreprise", []))

    # ── Keywords ──────────────────────────────────────────────────────────────
    keywords = (
        params.get("keywords")
        or params.get("mots_cles")
        or params.get("motsCles")
        or []
    )

    # ── Tailles ───────────────────────────────────────────────────────────────
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
            tailles = taille_obj if isinstance(taille_obj, list) else []

    # ── Géographie ────────────────────────────────────────────────────────────
    localisation: Dict[str, Any] = {}
    if isinstance(params.get("localisation"), dict):
        localisation = params["localisation"]

    if not localisation:
        zone = params.get("zone_geographique") or params.get("zoneGeographique")
        if isinstance(zone, dict):
            localisation = {
                "pays":    zone.get("pays", zone.get("zone_geographique", [])),
                "regions": zone.get("regions", []),
                "villes":  zone.get("villes", []),
            }

    pays: List[str] = _as_text_list(localisation.get("pays", []))
    if not pays:
        pays = _as_text_list(localisation.get("zone_geographique", []))
    if not pays:
        pays = _as_text_list(params.get("pays", []))
    if not pays:
        zone_raw = params.get("zone_geographique", params.get("zoneGeographique", []))
        if not isinstance(zone_raw, dict):
            pays = _as_text_list(zone_raw)

    regions = _as_text_list(
        localisation.get("regions", []) or params.get("regions", []) or params.get("region", [])
    )
    villes = _as_text_list(
        localisation.get("villes", []) or params.get("villes", []) or params.get("ville", [])
    )

    # ── Max résultats ─────────────────────────────────────────────────────────
    max_resultats = params.get("max_resultats", params.get("max_prospects_total"))

    return {
        "crit_secteurs_activite":    _as_text_list(secteurs),
        "crit_secteurs_activite_id": _as_int_list(secteurs_id),
        "crit_types_entreprise":     _as_text_list(types),
        "crit_tailles_entreprise":   _as_text_list(tailles),
        "crit_pays":                 pays,
        "crit_regions":              regions,
        "crit_villes":               villes,
        "crit_keywords":             _as_text_list(keywords),
        "crit_max_resultats":        _safe_int(max_resultats),
    }