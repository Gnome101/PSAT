"""Guards against accidental migration branching.

When two PRs each add a revision with the same ``down_revision``, Alembic
silently produces a branched history. ``alembic upgrade head`` then errors
with "Multiple head revisions are present" — but only at deploy time. This
test catches it in CI instead.
"""

from __future__ import annotations

from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory


def _script_dir() -> ScriptDirectory:
    repo_root = Path(__file__).resolve().parents[1]
    cfg = Config(str(repo_root / "alembic.ini"))
    cfg.set_main_option("script_location", str(repo_root / "alembic"))
    return ScriptDirectory.from_config(cfg)


def test_single_head_revision():
    heads = _script_dir().get_heads()
    assert len(heads) == 1, (
        f"Alembic has multiple heads: {heads}. Two migrations share a "
        "down_revision — merge them with `alembic merge` or rebase one onto "
        "the other before this lands."
    )


def test_no_branched_revisions():
    script = _script_dir()
    branched = [r.revision for r in script.walk_revisions() if r.is_branch_point]
    assert not branched, f"Branched revisions found: {branched}. Each revision should have at most one child."
