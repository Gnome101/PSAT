"""FastAPI routers for the PSAT HTTP surface.

Each module owns a slice of the URL space and exposes a ``router`` attribute
that ``api.py`` registers via ``app.include_router(...)``. ``spa`` must be
included LAST — its ``/{full_path:path}`` catch-all swallows anything that
follows it.

This package init intentionally does NOT eagerly import its submodules.
Eager imports here would create a cycle with ``services.aggregations``,
which imports ``routers.deps`` for SessionLocal access — running an eager
``from . import analyses`` here pulls ``services.aggregations`` mid-load.
"""
