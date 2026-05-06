from __future__ import annotations

from typing import Any, Dict

from app.config.mongo_rules_loader import get_mongo_schema_hint


def mongo_schema_hint() -> Dict[str, Any]:
    """
    Keep this stable and explicit so the LLM does NOT invent keys.
    Use dot-paths for nested fields.

    Audited field facts (Feb 2026):
    - voyageNumber is stored as STRING (e.g. "1901")
    - voyages use vesselName (no top-level vesselImo on voyages)
    - remarks array is "remarks" (not "remarkList")
    - fixtures array is "fixtures" (not "fixtureList")
    - financials live under projected_results.*
    """
    return get_mongo_schema_hint()

