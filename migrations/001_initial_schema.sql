-- ============================================================================
-- SCRAPING_V1 — Migration PostgreSQL
-- Fichier : migrations/001_initial_schema.sql
--
-- Crée les tables prospects et collection_jobs.
-- Idempotent (CREATE TABLE IF NOT EXISTS + ADD COLUMN IF NOT EXISTS).
-- ============================================================================

-- ── Table : prospects ─────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS prospects (

    hash_dedup          TEXT            PRIMARY KEY,
    job_id              BIGINT          NOT NULL,

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
    secteur_activite        TEXT        NOT NULL DEFAULT '',
    -- [AJOUT] FK vers la table secteurs_activite (chargés depuis la BD, non plus enum statique)
    secteur_activite_id     BIGINT,
    -- [AJOUT] Valeur brute issue du scraper, avant résolution FK par SecteurResolver.
    -- Permet au CRM d'afficher ce que le scraper a trouvé et de ré-évaluer le mapping.
    -- Jamais écrasée après l'étape cleaner (lecture seule en upsert).
    -- Exemples : "Cybersécurité", "Logiciels & SaaS", "Conseil en IT"
    secteur_activite_scraped TEXT        NOT NULL DEFAULT '',
    type_entreprise         TEXT        NOT NULL DEFAULT '',
    taille_entreprise       TEXT        NOT NULL DEFAULT '',
    nombre_employes         INTEGER,
    chiffre_affaires        NUMERIC(18, 2),
    description             TEXT        NOT NULL DEFAULT '',
    code_naf                TEXT        NOT NULL DEFAULT '',

    -- Identifiants légaux
    siren               TEXT            NOT NULL DEFAULT '',
    siret               TEXT            NOT NULL DEFAULT '',

    -- Scoring & Qualification
    qualification_score INTEGER         NOT NULL DEFAULT 0,
    score_pct           NUMERIC(5, 1)   NOT NULL DEFAULT 0.0,
    statut              TEXT            NOT NULL DEFAULT 'NON_QUALIFIE',
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

    -- Soft delete
    is_deleted          BOOLEAN         NOT NULL DEFAULT FALSE,

    -- Timestamps
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE  prospects IS 'Prospects B2B collectés par SCRAPING_V1';
COMMENT ON COLUMN prospects.hash_dedup          IS 'Clé de déduplication SHA-256 (tronqué 16 chars)';
COMMENT ON COLUMN prospects.secteur_activite_id IS 'FK vers secteurs_activite.id (BD CRM)';
COMMENT ON COLUMN prospects.qualification_score IS 'Score de qualification 0-100';
COMMENT ON COLUMN prospects.statut              IS 'QUALIFIE | NON_QUALIFIE';
COMMENT ON COLUMN prospects.enrich_score        IS 'Complétude contacts 0-4 (email+tel+web+linkedin)';
COMMENT ON COLUMN prospects.score_detail        IS 'Détail des points par critère (JSONB)';

-- ── Migration rétroactive : prospects ────────────────────────────────────────
-- Ces ALTER sont idempotents (IF NOT EXISTS) — sans effet si la colonne existe déjà.

ALTER TABLE prospects ADD COLUMN IF NOT EXISTS secteur_activite_id BIGINT;
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS secteur_activite_scraped TEXT NOT NULL DEFAULT '';
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS is_deleted          BOOLEAN NOT NULL DEFAULT FALSE;

-- ── Index : prospects ─────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_prospects_statut          ON prospects (statut);
CREATE INDEX IF NOT EXISTS idx_prospects_job_id          ON prospects (job_id);
CREATE INDEX IF NOT EXISTS idx_prospects_siren           ON prospects (siren)           WHERE siren <> '';
CREATE INDEX IF NOT EXISTS idx_prospects_email           ON prospects (email)           WHERE email <> '';
CREATE INDEX IF NOT EXISTS idx_prospects_ville           ON prospects (ville);
CREATE INDEX IF NOT EXISTS idx_prospects_secteur         ON prospects (secteur_activite);
-- [AJOUT] Index sur la valeur brute scrapée (utile pour le débogage du mapping FK côté CRM)
CREATE INDEX IF NOT EXISTS idx_prospects_secteur_scraped ON prospects (secteur_activite_scraped) WHERE secteur_activite_scraped <> '';
-- [AJOUT] Index sur la FK secteur pour les jointures CRM
CREATE INDEX IF NOT EXISTS idx_prospects_secteur_id      ON prospects (secteur_activite_id) WHERE secteur_activite_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_prospects_score           ON prospects (qualification_score DESC);
CREATE INDEX IF NOT EXISTS idx_prospects_enrich          ON prospects (enrich_score DESC);
CREATE INDEX IF NOT EXISTS idx_prospects_created         ON prospects (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_prospects_qualified_score ON prospects (qualification_score DESC) WHERE statut = 'QUALIFIE';

-- ── Table : collection_jobs ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS collection_jobs (

    id                  BIGINT          PRIMARY KEY,
    name                TEXT            NOT NULL DEFAULT '',
    type                TEXT            NOT NULL DEFAULT 'SCRAPING',
    status              TEXT            NOT NULL DEFAULT 'PENDING',
    parameters_json     TEXT            NOT NULL DEFAULT '{}',

    -- Critères de ciblage — stockés en TEXT (lisibles, sans accolades PostgreSQL)
    -- [MODIFIÉ] Remplace les colonnes TEXT[] de l'ancienne version
    crit_secteurs_activite_txt  TEXT    NOT NULL DEFAULT '',
    -- [AJOUT] IDs BD des secteurs sélectionnés (BIGINT[] pour FK)
    crit_secteurs_activite_id   BIGINT[],
    crit_types_entreprise_txt   TEXT    NOT NULL DEFAULT '',
    crit_tailles_entreprise_txt TEXT    NOT NULL DEFAULT '',
    crit_pays_txt               TEXT    NOT NULL DEFAULT '',
    crit_regions_txt            TEXT    NOT NULL DEFAULT '',
    crit_villes_txt             TEXT    NOT NULL DEFAULT '',
    crit_keywords_txt           TEXT    NOT NULL DEFAULT '',
    crit_max_resultats          INTEGER,

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

    sources_used        TEXT[]          NOT NULL DEFAULT '{}',
    errors              TEXT[]          NOT NULL DEFAULT '{}',

    is_deleted          BOOLEAN         NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE  collection_jobs IS 'Historique des jobs de collecte SCRAPING_V1';
COMMENT ON COLUMN collection_jobs.status                     IS 'PENDING | RUNNING | DONE | FAILED | CANCELLED';
COMMENT ON COLUMN collection_jobs.crit_secteurs_activite_txt IS 'Noms des secteurs (CSV lisible)';
COMMENT ON COLUMN collection_jobs.crit_secteurs_activite_id  IS 'IDs BD des secteurs (FK secteurs_activite)';

-- ── Migration rétroactive : collection_jobs ──────────────────────────────────
-- Supprime les anciennes colonnes TEXT[] et ajoute les nouvelles colonnes TEXT.
-- Idempotent (DROP IF EXISTS + ADD IF NOT EXISTS).

-- Supprimer les anciennes colonnes tableau si elles existent encore
ALTER TABLE collection_jobs DROP COLUMN IF EXISTS crit_secteurs_activite;
ALTER TABLE collection_jobs DROP COLUMN IF EXISTS crit_types_entreprise;
ALTER TABLE collection_jobs DROP COLUMN IF EXISTS crit_tailles_entreprise;
ALTER TABLE collection_jobs DROP COLUMN IF EXISTS crit_pays;
ALTER TABLE collection_jobs DROP COLUMN IF EXISTS crit_regions;
ALTER TABLE collection_jobs DROP COLUMN IF EXISTS crit_villes;
ALTER TABLE collection_jobs DROP COLUMN IF EXISTS crit_keywords;

-- Ajouter les nouvelles colonnes si absentes
ALTER TABLE collection_jobs ADD COLUMN IF NOT EXISTS crit_secteurs_activite_txt  TEXT    NOT NULL DEFAULT '';
ALTER TABLE collection_jobs ADD COLUMN IF NOT EXISTS crit_secteurs_activite_id   BIGINT[];
ALTER TABLE collection_jobs ADD COLUMN IF NOT EXISTS crit_types_entreprise_txt   TEXT    NOT NULL DEFAULT '';
ALTER TABLE collection_jobs ADD COLUMN IF NOT EXISTS crit_tailles_entreprise_txt TEXT    NOT NULL DEFAULT '';
ALTER TABLE collection_jobs ADD COLUMN IF NOT EXISTS crit_pays_txt               TEXT    NOT NULL DEFAULT '';
ALTER TABLE collection_jobs ADD COLUMN IF NOT EXISTS crit_regions_txt            TEXT    NOT NULL DEFAULT '';
ALTER TABLE collection_jobs ADD COLUMN IF NOT EXISTS crit_villes_txt             TEXT    NOT NULL DEFAULT '';
ALTER TABLE collection_jobs ADD COLUMN IF NOT EXISTS crit_keywords_txt           TEXT    NOT NULL DEFAULT '';
ALTER TABLE collection_jobs ADD COLUMN IF NOT EXISTS crit_max_resultats          INTEGER;
ALTER TABLE collection_jobs ADD COLUMN IF NOT EXISTS is_deleted                  BOOLEAN NOT NULL DEFAULT FALSE;

-- ── Index : collection_jobs ───────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_jobs_status  ON collection_jobs (status);
CREATE INDEX IF NOT EXISTS idx_jobs_created ON collection_jobs (created_at DESC);

-- ── Table : secteurs_activite ────────────────────────────────────────────────
-- Gérée principalement par Spring/Flyway — ce bloc est idempotent.
-- La colonne `keywords` est TEXT[] (tableau de mots-clés) et non TEXT (CSV).
-- Utilisée par SecteurResolver (Python) pour la résolution FK par mots-clés.

CREATE TABLE IF NOT EXISTS secteurs_activite (
    "SA_ID"             BIGSERIAL       PRIMARY KEY,
    nom                 TEXT            NOT NULL UNIQUE,
    description         TEXT,
    -- FIX : keywords est TEXT[] (liste) et non TEXT (CSV).
    -- Chaque élément est un synonyme/mot-clé distinct pour la résolution Python.
    -- Ex : ARRAY['cybersécurité', 'sécurité informatique', 'infosec', 'pentest']
    -- L'endpoint /secteurs_resolver retourne ce tableau comme array JSON,
    -- que SecteurResolver.load_from_spring() itère directement (plus de split(",")).
    keywords            TEXT[]          NOT NULL DEFAULT '{}',
    "SA_IS_DELETED"     BOOLEAN         NOT NULL DEFAULT FALSE,
    "SA_DELETED_TOKEN"  UUID
);

COMMENT ON TABLE  secteurs_activite IS 'Secteurs d''activité du CRM (référentiel)';
COMMENT ON COLUMN secteurs_activite.keywords IS
    'Synonymes/mots-clés pour la résolution Python (SecteurResolver step 4B). '
    'TEXT[] — chaque élément est un mot-clé distinct (pas de CSV). '
    'Ex : ARRAY[''cybersécurité'', ''infosec'', ''pentest'']';

-- Migration rétroactive : ajouter keywords si la table existe déjà sans cette colonne.
ALTER TABLE secteurs_activite
    ADD COLUMN IF NOT EXISTS keywords TEXT[] NOT NULL DEFAULT '{}';

-- ── Vue utilitaire : prospects qualifiés enrichis ────────────────────────────

CREATE OR REPLACE VIEW v_qualified_prospects AS
SELECT
    hash_dedup,
    job_id,
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
    secteur_activite_scraped,
    secteur_activite_id,
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