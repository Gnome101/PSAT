"""DB-bridge contract for per-sub-contract artifact bundles.

The resolution stage emits one ``LoadedArtifacts`` bundle per nested
sub-contract discovered during recursive control-graph resolution. The
runtime-state slices (``snapshot``, ``effective_permissions``) are
persisted as ``artifacts`` rows keyed ``recursive.<address>.<kind>`` so
the policy stage can look them up without another on-chain roundtrip.

The static slices (``analysis``, ``tracking_plan``) used to live here
too but they're a pure function of bytecode and now live in the
cross-job ``contract_materializations`` table. Policy hydrates them
per-address from there so a re-run of an already-analysed protocol
skips the storage write entirely.

Separator is ``.`` (not ``:``) because ``db.storage._safe_name`` only
allows ``[A-Za-z0-9._-]`` in artifact names destined for S3-compatible
object storage. Hex addresses and snake_case kind values are unambiguous
under a dot-split.

Both workers share this module so the naming convention and the
set of kinds have a single source of truth.
"""

from __future__ import annotations

import logging
from typing import Any, Mapping

from sqlalchemy.orm import Session

from db.queue import store_artifact

logger = logging.getLogger(__name__)

ARTIFACT_KINDS: tuple[str, ...] = ("snapshot", "effective_permissions")
KEY_PREFIX = "recursive"


def artifact_key(address: str, kind: str) -> str:
    """Build the deterministic artifact key for a nested sub-contract bundle."""
    return f"{KEY_PREFIX}.{address.lower()}.{kind}"


def parse_key(name: str) -> tuple[str, str] | None:
    """Inverse of ``artifact_key``. Returns ``(address, kind)`` or ``None``."""
    if not name.startswith(f"{KEY_PREFIX}."):
        return None
    parts = name.split(".", 2)
    if len(parts) != 3:
        return None
    _, address, kind = parts
    return address, kind


def store_bundle(session: Session, job_id: Any, nested: Mapping[str, Mapping[str, Any]]) -> None:
    """Persist a map of per-address ``LoadedArtifacts`` bundles as DB artifacts.

    Logs a warning when an expected kind is missing (for example,
    ``effective_permissions`` is ``None`` when the sub-contract build failed)
    so absent authority enrichment is traceable at the policy stage.
    """
    for address, bundle in nested.items():
        for kind in ARTIFACT_KINDS:
            payload = bundle.get(kind)
            if payload is None:
                logger.warning("Recursive artifact missing: address=%s kind=%s", address, kind)
                continue
            store_artifact(session, job_id, artifact_key(address, kind), data=payload)
