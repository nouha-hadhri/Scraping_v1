-- ============================================================================
-- SCRAPING_V1 — Migration PostgreSQL
-- Fichier : migrations/001_initial_schema.sql
--
-- Crée les tables prospects et collection_jobs.
-- Idempotent (CREATE TABLE IF NOT EXISTS + CREATE INDEX IF NOT EXISTS).
-- Exécuter manuellement ou via un outil de migration (Flyway, Alembic, etc.)
-- ============================================================================

-- Extensions utiles (optionnelles, décommentez si disponibles)
-- CREATE EXTENSION IF NOT EXISTS pg_trgm;   -- Pour les recherches fuzzy sur TEXT
-- CREATE EXTENSION IF NOT EXISTS unaccent;   -- Pour ignorer les accents dans les recherches

-- ── Table : prospects ─────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS prospects (

    -- Clé primaire : hash SHA-256 tronqué calculé par BaseScraper._make_hash
    hash_dedup          TEXT            PRIMARY KEY,

    -- Identité
    nom_commercial      TEXT            NOT NULL DEFAULT '',
    raison_sociale      TEXT            NOT NULL DEFAULT '',

    -- Contacts
    email               TEXT            NOT NULL DEFAULT '',
    telephone           TEXT            NOT NULL DEFAULT '',
    website             TEXT            NOT NULL DEFAULT '',
    linkedin_url        TEXT            NOT NULL DEFAULT '',

    -- Localisation
    adresse             TEXT            NOT NULL DEFAULT '',
    ville               TEXT            NOT NULL DEFAULT '',
    region              TEXT            NOT NULL DEFAULT '',
    pays                TEXT            NOT NULL DEFAULT '',
    code_postal         TEXT            NOT NULL DEFAULT '',

    -- Entreprise
    secteur_activite    TEXT            NOT NULL DEFAULT '',
    type_entreprise     TEXT            NOT NULL DEFAULT '',
    taille_entreprise   TEXT            NOT NULL DEFAULT '',   -- TPE | PME | ETI | GE
    nombre_employes     INTEGER,
    chiffre_affaires    NUMERIC(18, 2),
    description         TEXT            NOT NULL DEFAULT '',
    code_naf            TEXT            NOT NULL DEFAULT '',

    -- Identifiants légaux
    siren               TEXT            NOT NULL DEFAULT '',
    siret               TEXT            NOT NULL DEFAULT '',

    -- Scoring & Qualification
    qualification_score INTEGER         NOT NULL DEFAULT 0,
    score_pct           NUMERIC(5, 1)   NOT NULL DEFAULT 0.0,
    statut              TEXT            NOT NULL DEFAULT 'NON_QUALIFIE',
    -- score_detail : dict Python sérialisé en JSON
    -- Exemple : {"sector_match": {"points": 20, "max": 20, "note": "match:Informatique"}, ...}
    score_detail        JSONB           NOT NULL DEFAULT '{}',
    criteria_met        INTEGER         NOT NULL DEFAULT 0,
    criteria_total      INTEGER         NOT NULL DEFAULT 0,

    -- Métadonnées pipeline
    source              TEXT            NOT NULL DEFAULT 'SCRAPING',
    segment             TEXT            NOT NULL DEFAULT '',
    sector_confidence   NUMERIC(5, 3)   NOT NULL DEFAULT 0.0,
    email_valid         BOOLEAN         NOT NULL DEFAULT FALSE,
    website_active      BOOLEAN         NOT NULL DEFAULT FALSE,

    -- Enrichissement contacts (0-4)
    enrich_score        INTEGER         NOT NULL DEFAULT 0,
    email_mx_verified   BOOLEAN         NOT NULL DEFAULT FALSE,

    -- Timestamps
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()

);

-- Commentaires de table
COMMENT ON TABLE  prospects IS 'Prospects B2B collectés par SCRAPING_V1';
COMMENT ON COLUMN prospects.hash_dedup          IS 'Clé de déduplication SHA-256 (tronqué 16 chars)';
COMMENT ON COLUMN prospects.qualification_score IS 'Score de qualification 0-100';
COMMENT ON COLUMN prospects.statut              IS 'QUALIFIE | NON_QUALIFIE';
COMMENT ON COLUMN prospects.enrich_score        IS 'Complétude contacts 0-4 (email+tel+web+linkedin)';
COMMENT ON COLUMN prospects.score_detail        IS 'Détail des points par critère (JSONB)';

-- ── Index : prospects ─────────────────────────────────────────────────────────

-- Filtres fréquents CRM
CREATE INDEX IF NOT EXISTS idx_prospects_statut
    ON prospects (statut);

CREATE INDEX IF NOT EXISTS idx_prospects_siren
    ON prospects (siren)
    WHERE siren <> '';

CREATE INDEX IF NOT EXISTS idx_prospects_email
    ON prospects (email)
    WHERE email <> '';

CREATE INDEX IF NOT EXISTS idx_prospects_ville
    ON prospects (ville);

CREATE INDEX IF NOT EXISTS idx_prospects_secteur
    ON prospects (secteur_activite);

-- Tri des résultats
CREATE INDEX IF NOT EXISTS idx_prospects_score
    ON prospects (qualification_score DESC);

CREATE INDEX IF NOT EXISTS idx_prospects_enrich
    ON prospects (enrich_score DESC);

CREATE INDEX IF NOT EXISTS idx_prospects_created
    ON prospects (created_at DESC);

-- Index composite pour les exports CRM typiques (qualifiés + score)
CREATE INDEX IF NOT EXISTS idx_prospects_qualified_score
    ON prospects (qualification_score DESC)
    WHERE statut = 'QUALIFIE';

-- ── Table : collection_jobs ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS collection_jobs (

    id                  TEXT            PRIMARY KEY,        -- UUID tronqué
    name                TEXT            NOT NULL DEFAULT '', -- scraping_YYYYMMDD_HHMMSS
    type                TEXT            NOT NULL DEFAULT 'SCRAPING',
    status              TEXT            NOT NULL DEFAULT 'PENDING', -- PENDING | RUNNING | DONE | FAILED
    parameters_json     TEXT            NOT NULL DEFAULT '{}',

    -- Timestamps du job
    started_at          TIMESTAMPTZ,
    finished_at         TIMESTAMPTZ,

    -- Compteurs pipeline
    total_collected     INTEGER         NOT NULL DEFAULT 0,
    total_cleaned       INTEGER         NOT NULL DEFAULT 0,
    total_deduped       INTEGER         NOT NULL DEFAULT 0,
    total_scored        INTEGER         NOT NULL DEFAULT 0,
    total_saved         INTEGER         NOT NULL DEFAULT 0,
    total_qualified     INTEGER         NOT NULL DEFAULT 0,
    total_duplicates    INTEGER         NOT NULL DEFAULT 0,

    -- Sources utilisées et erreurs
    sources_used        TEXT[]          NOT NULL DEFAULT '{}',
    errors              TEXT[]          NOT NULL DEFAULT '{}',

    -- Timestamp de création
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()

);

COMMENT ON TABLE  collection_jobs IS 'Historique des jobs de collecte SCRAPING_V1';
COMMENT ON COLUMN collection_jobs.status IS 'PENDING | RUNNING | DONE | FAILED';

CREATE INDEX IF NOT EXISTS idx_jobs_status
    ON collection_jobs (status);

CREATE INDEX IF NOT EXISTS idx_jobs_created
    ON collection_jobs (created_at DESC);

-- ── Vue utilitaire : prospects qualifiés enrichis ────────────────────────────

CREATE OR REPLACE VIEW v_qualified_prospects AS
SELECT
    hash_dedup,
    nom_commercial,
    raison_sociale,
    email,
    telephone,
    website,
    linkedin_url,
    ville,
    region,
    pays,
    secteur_activite,
    taille_entreprise,
    siren,
    qualification_score,
    score_pct,
    enrich_score,
    email_valid,
    website_active,
    email_mx_verified,
    source,
    created_at,
    updated_at
FROM prospects
WHERE statut = 'QUALIFIE'
ORDER BY qualification_score DESC, enrich_score DESC;

COMMENT ON VIEW v_qualified_prospects IS
    'Prospects qualifiés triés par score — colonnes utiles pour export CRM';