# tests/test_seed_manifest.py
from services.discovery.docs.models import SearchResult
from services.discovery.docs.seed_manifest import SeedManifestBuilder, _extract_github_org

def test_extract_github_org_variants():
    assert _extract_github_org("https://github.com/Uniswap/v3-core/blob/main/SECURITY.md") \
        == "https://github.com/Uniswap"
    assert _extract_github_org("https://github.com/Uniswap/v3-core") \
        == "https://github.com/Uniswap"
    assert _extract_github_org("https://github.com/Uniswap") \
        == "https://github.com/Uniswap"

def _make_result(url, score, source_type):
    return SearchResult(url=url, content="x", score=score,
                        source_type=source_type, query_used="q")

def test_dedup_keeps_highest_score():
    results = [
        _make_result("https://docs.aave.com", 0.9, "docs"),
        _make_result("https://docs.aave.com", 0.7, "docs"),
    ]
    manifest = SeedManifestBuilder().build(results, "aave", "Aave", "0xabc")
    assert len(manifest.seeds) == 1
    assert manifest.seeds[0].confidence == 0.9

def test_defillama_fallback_added_when_no_docs_seed():
    results = [_make_result("https://github.com/aave", 0.8, "github")]
    manifest = SeedManifestBuilder().build(
        results, "aave", "Aave", "0xabc", defillama_url="https://aave.com"
    )
    assert any(s.seed_type == "docs_site" for s in manifest.seeds)

def test_defillama_fallback_skipped_when_docs_seed_exists():
    results = [_make_result("https://docs.aave.com", 0.9, "docs")]
    manifest = SeedManifestBuilder().build(
        results, "aave", "Aave", "0xabc", defillama_url="https://aave.com"
    )
    fallbacks = [s for s in manifest.seeds if s.metadata.get("source") == "defillama"]
    assert len(fallbacks) == 0

def test_github_url_normalised_to_org():
    results = [_make_result(
        "https://github.com/Uniswap/v3-core/blob/main/README.md", 0.85, "github"
    )]
    manifest = SeedManifestBuilder().build(results, "uniswap", "Uniswap", "0xabc")
    github_seeds = [s for s in manifest.seeds if s.seed_type == "github_org"]
    assert github_seeds[0].url == "https://github.com/Uniswap"