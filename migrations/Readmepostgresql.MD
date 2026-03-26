# PostgreSQL Integration — SCRAPING_V1

Intégration PostgreSQL au pipeline existant, **en complément** du système CSV/JSON (non remplacé).

---

## Architecture

```
Pipeline SCRAPING_V1
│
├── Step 1-6  (inchangés)
│
└── Step 7 — Sauvegarde
    ├── 7a  CSV + JSON         ← comportement original, toujours actif
    └── 7b  PostgreSQL (upsert) ← nouveau, activé via PG_ENABLED=True
```

---

## Fichiers ajoutés / modifiés

| Fichier | Rôle |
|---|---|
| `storage/pg_repository.py` | Couche PostgreSQL (upsert, lecture, stats) |
| `config/settings_pg_additions.py` | Bloc de config à ajouter dans `settings.py` |
| `orchestrator_pg_patch.py` | Instructions de patch de `orchestrator.py` |
| `migrations/001_initial_schema.sql` | Schéma SQL (tables + index + vue) |

---

## Installation

### 1. Installer le driver PostgreSQL

```bash
pip install psycopg2-binary
```

### 2. Créer la base de données

```sql
CREATE DATABASE scraping_v1;
```

### 3. Appliquer la migration SQL

```bash
psql -U postgres -d scraping_v1 -f migrations/001_initial_schema.sql
```

### 4. Configurer la connexion

**Option A — Variable d'environnement (recommandé production)**

```bash
export DATABASE_URL="postgresql://user:password@localhost:5432/scraping_v1"
export PG_ENABLED=true
```

**Option B — Éditer `config/settings.py`**

Copiez le contenu de `config/settings_pg_additions.py` à la fin de `settings.py`, puis adaptez :

```python
PG_ENABLED = True
PG_DSN     = "postgresql://postgres:monmotdepasse@localhost:5432/scraping_v1"
```

### 5. Patcher `orchestrator.py`

Suivez les instructions dans `orchestrator_pg_patch.py` :

**a) Ajout des imports** (vers ligne 91 et 121) :

```python
# Dans le bloc from config.settings import (...)
PG_ENABLED,

# Après from storage.repository import ProspectRepository
from storage.pg_repository import PgRepository
```

**b) Remplacement du Step 7** (vers ligne 396) :

```python
# AVANT
logger.info("\n [Step 7/7] Sauvegarde…")
step_7_start = time.time()
if not self.dry_run:
    info = self.repo.save_all(enriched)
    self.repo.save_scored(scored)
    self.repo.export_qualified_only()
    job.total_saved = info["n_saved"]
    logger.info("    Fichiers sauvegardés dans output/")
else:
    logger.info("    DRY-RUN — pas de sauvegarde")
    job.total_saved = 0
step_7_elapsed = time.time() - step_7_start
step_timings["7. Sauvegarde"] = step_7_elapsed
logger.info(f"    ( {step_7_elapsed:.2f}s)")

# APRÈS
logger.info("\n [Step 7/7] Sauvegarde (CSV/JSON + PostgreSQL)…")
step_7_start   = time.time()
step_7_elapsed = self._step7_sauvegarde(enriched, scored, job, self.dry_run)
step_timings["7. Sauvegarde"] = step_7_elapsed
logger.info(f"    ( {step_7_elapsed:.2f}s)")
```

**c) Ajouter la méthode** `_step7_sauvegarde` à la classe `ProspectCollector` (copiez depuis `orchestrator_pg_patch.py`).

---

## Comportement en production

| Scénario | Résultat |
|---|---|
| `PG_ENABLED=False` | CSV/JSON uniquement (comportement original) |
| `PG_ENABLED=True`, PG disponible | CSV/JSON **+** PostgreSQL (upsert) |
| `PG_ENABLED=True`, PG indisponible | CSV/JSON sauvegardés, erreur PG loggée (non bloquant) |
| `--dry-run` | Aucune sauvegarde (CSV, JSON, PG) |

---

## Upsert par hash_dedup

La clé primaire PostgreSQL est `hash_dedup` (calculé par `BaseScraper._make_hash`).

- **INSERT** si hash absent → nouveau prospect
- **UPDATE** si hash présent → mise à jour de tous les champs (re-score, re-enrich)
- Les prospects sans `hash_dedup` sont ignorés (loggés en warning)

---

## Requêtes utiles

```sql
-- Prospects qualifiés du jour
SELECT * FROM v_qualified_prospects
WHERE created_at >= CURRENT_DATE;

-- Distribution par secteur
SELECT secteur_activite, COUNT(*), AVG(qualification_score)
FROM prospects
GROUP BY secteur_activite
ORDER BY COUNT(*) DESC;

-- Prospects à fort potentiel (score élevé + contacts complets)
SELECT nom_commercial, email, telephone, website, qualification_score, enrich_score
FROM prospects
WHERE statut = 'QUALIFIE' AND enrich_score = 4
ORDER BY qualification_score DESC
LIMIT 50;

-- Historique des jobs
SELECT id, name, status, total_qualified, total_scored, started_at, finished_at
FROM collection_jobs
ORDER BY created_at DESC;
```

---

## Utilisation directe de PgRepository

```python
from storage.pg_repository import PgRepository

# Lecture des stats
with PgRepository() as pg:
    stats = pg.get_stats()
    print(f"Total: {stats['total']} | Qualifiés: {stats['qualified']}")

# Chargement des qualifiés
with PgRepository() as pg:
    prospects = pg.load_qualified(limit=100)

# Chargement par secteur
with PgRepository() as pg:
    tech = pg.load_by_sector("Informatique")
```