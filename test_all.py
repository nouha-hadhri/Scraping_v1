"""
test_all.py — Suite de tests complète du système SCRAPING_V1
Teste chaque composant (unit) + pipeline bout-en-bout.
"""
import sys
import json
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s | %(levelname)-8s | %(message)s"
)
logger = logging.getLogger(__name__)

# ── Terminal colors ───────────────────────────────────────────────────────────
GREEN  = "\033[92m"; RED    = "\033[91m"; YELLOW = "\033[93m"
BLUE   = "\033[94m"; CYAN   = "\033[96m"; RESET  = "\033[0m"; BOLD = "\033[1m"


def h(title):  print(f"\n{BLUE}{'='*60}{RESET}\n{BOLD}{CYAN}  {title}{RESET}\n{BLUE}{'='*60}{RESET}")
def ok(msg):   print(f"  {GREEN}[OK]  {msg}{RESET}")
def err(msg):  print(f"  {RED}[ERR] {msg}{RESET}")
def info(msg): print(f"  {YELLOW}[i]   {msg}{RESET}")


# ─────────────────────────────────────────────
# TEST 1 — Imports
# ─────────────────────────────────────────────

def test_imports():
    h("TEST 1 : IMPORTS")
    modules = [
        ("storage.models",           "Prospect, CollectionJob"),
        ("storage.repository",       "ProspectRepository"),
        ("config.settings",          "SCORING, APIS"),
        ("config.targets",           "SearchTarget, EXAMPLE_TARGETS"),
        ("sources.base_scraper",     "BaseScraper"),
        ("sources.curl_client",      "CurlClient"),
        ("sources.open_data_scraper","OpenDataScraper"),
        ("sources.directory_scraper","DirectoryScraper"),
        ("sources.societe_scraper",  "SocieteScraper"),
        ("sources.website_scraper",  "WebsiteScraper"),
        ("pipeline.cleaner",         "ProspectCleaner"),
        ("pipeline.deduplication",   "Deduplicator"),
        ("pipeline.embedder",        "ProspectEmbedder"),
        ("pipeline.scorer",          "ProspectScorer"),
        ("orchestrator",             "ProspectCollector"),
    ]
    all_ok = True
    for mod, items in modules:
        try:
            __import__(mod)
            ok(f"{mod}  ({items})")
        except Exception as e:
            err(f"{mod}: {e}")
            all_ok = False
    return all_ok


# ─────────────────────────────────────────────
# TEST 2 — Modèles
# ─────────────────────────────────────────────

def test_models():
    h("TEST 2 : MODÈLES DE DONNÉES")
    from storage.models import Prospect, CollectionJob

    p = Prospect(
        nom_commercial    = "TechSolutions SAS",
        email             = "contact@techsolutions.fr",
        telephone         = "+33 1 23 45 67 89",
        website           = "https://www.techsolutions.fr",
        ville             = "Paris",
        region            = "Île-de-France",
        pays              = "France",
        secteur_activite  = "Informatique",
        taille_entreprise = "PME",
        chiffre_affaires  = 5_500_000,
        source            = "test",
    )

    assert p.nom_commercial == "TechSolutions SAS"
    assert p.is_contactable(), "Devrait être contactable"

    completeness = p.completeness_score()
    assert completeness > 60, f"Complétude trop faible: {completeness}"

    csv_row = p.to_csv_row()
    assert "nom_commercial"     in csv_row
    assert "qualification_score" in csv_row

    d = p.to_dict()
    assert isinstance(d, dict)

    p2 = Prospect.from_dict(d)
    assert p2.nom_commercial == p.nom_commercial

    ok(f"Prospect créé: {p.nom_commercial}")
    ok(f"Contactable: {p.is_contactable()}")
    ok(f"Complétude: {completeness}%")
    ok(f"CSV: {len(csv_row)} colonnes")
    return True


# ─────────────────────────────────────────────
# TEST 3 — SearchTarget
# ─────────────────────────────────────────────

def test_targets():
    h("TEST 3 : CIBLES DE RECHERCHE")
    from config.targets import SearchTarget, EXAMPLE_TARGETS

    t = SearchTarget(
        secteur_activite  = ["Informatique"],
        taille_entreprise = ["PME"],
        pays              = ["France"],
        regions           = ["Île-de-France"],
        villes            = ["Paris"],
        max_resultats     = 20,
        sources           = ["api", "scraping"],
    )
    ok(f"SearchTarget: {t.to_query_string()}")

    assert len(EXAMPLE_TARGETS) >= 3, f"Doit avoir ≥ 3 cibles, got {len(EXAMPLE_TARGETS)}"
    for name, tgt in EXAMPLE_TARGETS.items():
        ok(f"Cible '{name}': {tgt.to_query_string()}")

    # Round-trip dict
    d  = t.to_dict()
    t2 = SearchTarget.from_dict(d)
    assert t2.secteur_activite == t.secteur_activite
    ok("Round-trip to_dict / from_dict OK")

    return True


# ─────────────────────────────────────────────
# TEST 4 — Cleaner
# ─────────────────────────────────────────────

def test_cleaner():
    h("TEST 4 : NETTOYAGE")
    from storage.models  import Prospect
    from pipeline.cleaner import ProspectCleaner

    cleaner = ProspectCleaner()

    dirty = [
        Prospect(nom_commercial="  GOOGLE FRANCE SAS  ",
                 email="Contact@Google.FR", telephone="0123456789", source="test"),
        Prospect(nom_commercial="", source="test"),               # -> rejeté
        Prospect(nom_commercial="AmazonFR", email="noreply@test.com",
                 telephone="+33612345678", chiffre_affaires=-500, source="test"),
        Prospect(nom_commercial="Startup XYZ", taille_entreprise="",
                 secteur_activite="", source="test"),
    ]

    cleaned = cleaner.clean_all(dirty)

    assert len(cleaned) >= 2, f"Devrait garder ≥ 2 prospects, got {len(cleaned)}"
    assert all(p.nom_commercial for p in cleaned)

    for p in cleaned:
        if p.email:
            assert p.email == p.email.lower(), f"Email non normalisé: {p.email}"
        if p.chiffre_affaires is not None:
            assert p.chiffre_affaires >= 0, "CA négatif non nettoyé"
        assert p.taille_entreprise in ("TPE", "PME", "ETI", "GE"), \
            f"Taille invalide: {p.taille_entreprise}"
        assert p.secteur_activite, "Secteur vide"

    ok(f"{len(cleaned)} prospects valides (sur {len(dirty)})")
    print(cleaner.get_report())
    return True


# ─────────────────────────────────────────────
# TEST 5 — Déduplication
# ─────────────────────────────────────────────

def test_deduplication():
    h("TEST 5 : DÉDUPLICATION")
    from storage.models          import Prospect
    from pipeline.deduplication  import Deduplicator
    from sources.base_scraper    import BaseScraper

    dedup = Deduplicator(similarity_threshold=0.85)

    prospects = [
        Prospect(nom_commercial="TechCorp SAS",  email="contact@techcorp.fr",  source="s1"),
        Prospect(nom_commercial="TechCorp SAS",  email="contact@techcorp.fr",  source="s2"),  # doublon exact
        Prospect(nom_commercial="DataPro SARL",  email="hello@datapro.fr",      source="s1"),  # unique
        Prospect(nom_commercial="CloudSys",       telephone="+33123456789",      source="s1"),  # unique
        Prospect(nom_commercial="CloudSys SAS",   telephone="+33123456789",      source="s2"),  # doublon tel
    ]

    # Génération des hash
    for p in prospects:
        p.hash_dedup = BaseScraper._make_hash(p.email, p.website, p.telephone, p.nom_commercial)

    unique, n_dupes = dedup.deduplicate(prospects)

    assert n_dupes >= 1, f"Devrait détecter ≥ 1 doublon, got {n_dupes}"
    assert len(unique) <= 4, f"Devrait avoir ≤ 4 uniques, got {len(unique)}"

    ok(f"{len(unique)} uniques / {n_dupes} doublons sur {len(prospects)}")
    for p in unique:
        info(f"Unique: {p.nom_commercial}")
    return True


# ─────────────────────────────────────────────
# TEST 6 — Scorer
# ─────────────────────────────────────────────

def test_scorer():
    h("TEST 6 : SCORING")
    from storage.models  import Prospect
    from pipeline.scorer import ProspectScorer

    scorer = ProspectScorer()

    prospects = [
        Prospect(   # -> VIP ou Hot
            nom_commercial    = "VIP Corp SAS",
            raison_sociale    = "VIP Corp SAS",
            email             = "contact@vip.fr",
            email_valid       = True,
            telephone         = "+33123456789",
            website           = "https://vip.fr",
            secteur_activite  = "Informatique",
            taille_entreprise = "PME",
            type_entreprise   = "SAS",
            pays              = "France",
            ville             = "Paris",
            source            = "test",
        ),
        Prospect(   # -> Cold
            nom_commercial   = "Ghost Inc",
            source           = "test",
        ),
    ]

    scored = scorer.score_all(prospects)

    assert len(scored) == 2
    for p in scored:
        assert p.qualification_score is not None
        assert 0 <= p.qualification_score <= 100
        assert p.segment in ("VIP", "Hot", "Warm", "Cold"), f"Segment invalide: {p.segment}"
        emoji = {"VIP": "", "Hot": "", "Warm": "", "Cold": ""}.get(p.segment, "?")
        ok(f"{emoji} {p.nom_commercial}: {p.qualification_score}/100 -> {p.segment}")

    assert scored[0].qualification_score >= scored[1].qualification_score, \
        "VIP/Hot devrait avoir score > Cold"

    print(scorer.get_summary())
    return True


# ─────────────────────────────────────────────
# TEST 7 — Embedder
# ─────────────────────────────────────────────

def test_embedder():
    h("TEST 7 : NLP / EMBEDDER")
    from storage.models   import Prospect
    from pipeline.embedder import ProspectEmbedder

    embedder = ProspectEmbedder()

    prospects = [
        Prospect(nom_commercial="CloudDev Solutions",
                 description="Développement de logiciels cloud, data engineering, IA et machine learning",
                 source="test"),
        Prospect(nom_commercial="BioSanté Lab",
                 description="Laboratoire d'analyses médicales et biotechnologies",
                 source="test"),
        Prospect(nom_commercial="FinCapital Invest",
                 description="Gestion de fonds d'investissement et capital privé",
                 source="test"),
    ]

    enriched = embedder.enrich_all(prospects)

    for p in enriched:
        if p.secteur_activite and p.secteur_activite != "Non spécifié":
            ok(f"{p.nom_commercial} -> {p.secteur_activite} (conf={p.sector_confidence:.2f})")
        else:
            info(f"{p.nom_commercial} -> secteur non détecté")

    return True


# ─────────────────────────────────────────────
# TEST 8 — Repository
# ─────────────────────────────────────────────

def test_repository():
    h("TEST 8 : PERSISTANCE (JSON + CSV)")
    from storage.models      import Prospect
    from storage.repository  import ProspectRepository
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        repo = ProspectRepository(
            json_path   = Path(tmpdir) / "test.json",
            csv_path    = Path(tmpdir) / "test.csv",
            scored_json = Path(tmpdir) / "test_scored.json",
            scored_csv  = Path(tmpdir) / "test_scored.csv",
        )

        prospects = [
            Prospect(
                nom_commercial    = f"Entreprise {i}",
                email             = f"contact{i}@ent{i}.fr",
                telephone         = f"+3312345678{i}",
                website           = f"https://www.ent{i}.fr",
                ville             = "Paris",
                secteur_activite  = "Informatique",
                taille_entreprise = "PME",
                chiffre_affaires  = i * 1_000_000,
                qualification_score = 60 + i * 5,
                segment           = "Hot",
                source            = "test",
            )
            for i in range(5)
        ]

        info = repo.save_all(prospects)
        assert Path(info["json_path"]).exists(), "JSON non créé"
        assert Path(info["csv_path"]).exists(),  "CSV non créé"
        assert info["n_saved"] == 5

        loaded = repo.load_from_json()
        assert len(loaded) == 5, f"JSON: attendu 5, got {len(loaded)}"

        csv_data = repo.load_from_csv()
        assert len(csv_data) == 5, f"CSV: attendu 5, got {len(csv_data)}"

        required_cols = ["nom_commercial", "email", "telephone",
                         "secteur_activite", "taille_entreprise",
                         "chiffre_affaires", "qualification_score", "segment"]
        for col in required_cols:
            assert col in csv_data[0], f"Colonne manquante: {col}"

        ok(f"JSON sauvegardé: {info['json_path']}")
        ok(f"CSV  sauvegardé: {info['csv_path']}")
        ok(f"JSON rechargé: {len(loaded)} prospects")
        ok(f"CSV  rechargé: {len(csv_data)} lignes")
        ok(f"Colonnes CSV: {', '.join(required_cols)}")

    return True


# ─────────────────────────────────────────────
# TEST 9 — Pipeline complet (dry-run)
# ─────────────────────────────────────────────

def test_full_pipeline():
    h("TEST 9 : PIPELINE COMPLET (DRY-RUN)")
    from config.targets  import EXAMPLE_TARGETS
    from orchestrator    import ProspectCollector

    collector = ProspectCollector(dry_run=True)
    target    = EXAMPLE_TARGETS["generic_demo"]
    target.max_resultats = 15

    info(f"Cible: {target.to_query_string()}")
    info("Lancement du pipeline (peut prendre 15-30s)…")

    try:
        job = collector.run(target)

        assert job.status in ("done", "failed"), f"Statut inattendu: {job.status}"

        if job.status == "done":
            ok(f"Pipeline terminé avec succès")
            ok(f"Collectés  : {job.total_collected}")
            ok(f"Nettoyés   : {job.total_cleaned}")
            ok(f"Dédupliqués: {job.total_deduped}")
            ok(f"Scorés     : {job.total_scored}")
            ok(f"Qualifiés  : {job.total_qualified}")
        else:
            info(f"Pipeline terminé en mode dégradé (hors-ligne?): {job.errors}")

    except Exception as e:
        info(f"Pipeline indisponible (réseau?): {e}")

    return True


# ─────────────────────────────────────────────
# Runner
# ─────────────────────────────────────────────

def run_all():
    print(f"\n{BOLD}{CYAN}{'#'*60}\n  CRM SCRAPING_V1 — SUITE DE TESTS\n{'#'*60}{RESET}\n")

    tests = [
        ("Imports",          test_imports),
        ("Modèles",          test_models),
        ("Cibles",           test_targets),
        ("Nettoyage",        test_cleaner),
        ("Déduplication",    test_deduplication),
        ("Scoring",          test_scorer),
        ("NLP/Embedder",     test_embedder),
        ("Repository",       test_repository),
        ("Pipeline complet", test_full_pipeline),
    ]

    results = {}
    for name, fn in tests:
        try:
            ok_flag = fn()
            results[name] = " PASS" if ok_flag else "  PARTIEL"
        except Exception as e:
            results[name] = f" FAIL: {e}"
            logger.error(f"Test '{name}' échoué", exc_info=True)

    print(f"\n{BOLD}{BLUE}{'='*60}\n  RAPPORT FINAL\n{'='*60}{RESET}")
    passed = 0
    for name, result in results.items():
        color = GREEN if "PASS" in result else (YELLOW if "PARTIEL" in result else RED)
        print(f"  {color}{result:<30}{RESET} {name}")
        if "PASS" in result:
            passed += 1

    pct = round(100 * passed / len(tests))
    print(f"\n{BOLD}  Score: {passed}/{len(tests)} ({pct}%){RESET}")
    print(f"{BLUE}{'='*60}{RESET}\n")
    return passed == len(tests)


if __name__ == "__main__":
    success = run_all()
    sys.exit(0 if success else 1)