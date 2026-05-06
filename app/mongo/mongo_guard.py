from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Set

from app.config.mongo_rules_loader import (
    get_mongo_guard_default_limit,
    get_mongo_guard_max_limit,
    get_mongo_regex_options_allowed_value,
)


@dataclass(frozen=True)
class MongoGuardResult:
    ok: bool
    reason: str
    collection: str
    filter: Dict[str, Any]
    projection: Dict[str, int]
    sort: Optional[Dict[str, int]]
    limit: int


def _walk(obj: Any, allowed_ops: Set[str]) -> bool:
    """
    Returns True if safe.
    Disallows any keys starting with '$' that are not in allowed_ops.
    """
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(k, str) and k.startswith("$"):
                # Allow $options ONLY as a sibling of $regex and only for case-insensitive matching.
                # This prevents $options from being used as a generic operator elsewhere.
                if k == "$options":
                    if "$regex" not in obj:
                        return False
                    if not isinstance(v, str) or v.strip().lower() != get_mongo_regex_options_allowed_value():
                        return False
                elif k not in allowed_ops:
                    return False
            if not _walk(v, allowed_ops):
                return False
        return True
    if isinstance(obj, list):
        return all(_walk(x, allowed_ops) for x in obj)
    return True


def validate_mongo_spec(
    *,
    collection: str,
    filt: Dict[str, Any],
    projection: Dict[str, int],
    sort: Optional[Dict[str, int]],
    limit: int,
    allowed_collections: Set[str],
    allowed_ops: Set[str],
) -> MongoGuardResult:
    if collection not in allowed_collections:
        return MongoGuardResult(False, f"Collection not allowed: {collection}", "", {}, {}, None, 0)
    if not isinstance(filt, dict):
        return MongoGuardResult(False, "Filter must be an object.", "", {}, {}, None, 0)
    if not _walk(filt, allowed_ops):
        return MongoGuardResult(False, "Filter contains forbidden operators.", "", {}, {}, None, 0)
    if not isinstance(projection, dict) or not projection:
        return MongoGuardResult(False, "Projection must be a non-empty object.", "", {}, {}, None, 0)

    proj_clean: Dict[str, int] = {"_id": 0}
    for k, v in projection.items():
        if k == "_id":
            continue
        try:
            proj_clean[str(k)] = 1 if int(v) == 1 else 0
        except Exception:
            proj_clean[str(k)] = 0

    lim = max(1, min(int(limit or get_mongo_guard_default_limit()), get_mongo_guard_max_limit()))

    sort_clean = None
    if sort is not None:
        if not isinstance(sort, dict):
            return MongoGuardResult(False, "Sort must be object or null.", "", {}, {}, None, 0)
        sort_clean = {str(k): (1 if int(v) >= 0 else -1) for k, v in sort.items()}

    return MongoGuardResult(True, "OK", collection, filt, proj_clean, sort_clean, lim)

