import logging
from typing import Optional

logger = logging.getLogger(__name__)

_CACHE_RESOLVABLE = {"filter", "sort", "slice", "rerank"}


def resolve_followup(structured_intent: dict, redis_session: dict) -> Optional[dict]:
    if structured_intent.get("scope") != "follow_up":
        return None

    action = structured_intent.get("follow_up_action")
    last_rows = redis_session.get("last_result_rows", [])
    carried_slots = redis_session.get("resolved_slots", {})

    if not last_rows:
        _enrich_with_carried_slots(structured_intent, carried_slots)
        return None

    if action in _CACHE_RESOLVABLE:
        result = _apply_in_memory(last_rows, action, structured_intent)
        if result is not None:
            return {"resolved_rows": result, "source": "cache"}

    _enrich_with_carried_slots(structured_intent, carried_slots)
    return None


def _enrich_with_carried_slots(intent: dict, slots: dict) -> None:
    if not slots:
        return
    existing = {f.get("field") for f in (intent.get("filters") or []) if isinstance(f, dict)}
    for field, value in slots.items():
        if field not in existing:
            intent.setdefault("filters", []).append(
                {"field": field, "operator": "eq", "value": value}
            )


def _apply_in_memory(rows: list, action: str, intent: dict) -> Optional[list]:
    if not rows:
        return []
    try:
        agg = intent.get("aggregation") or {}
        filters = intent.get("filters") or []

        if action == "slice":
            limit = agg.get("limit")
            return rows[: int(limit)] if limit else rows

        if action in ("sort", "rerank"):
            order_by = (agg.get("order_by") or "").strip().split()
            field = order_by[0] if order_by else None
            desc = len(order_by) > 1 and order_by[1].upper() == "DESC"
            if field and rows and isinstance(rows[0], dict) and field in rows[0]:
                return sorted(rows, key=lambda r: _safe_float(r.get(field, 0)), reverse=desc)
            return rows

        if action == "filter":
            result = list(rows)
            for f in filters:
                if not isinstance(f, dict):
                    continue
                field = f.get("field")
                op = f.get("operator")
                val = f.get("value")
                if not field or (rows and isinstance(rows[0], dict) and field not in rows[0]):
                    continue
                if op == "eq":
                    result = [r for r in result if str(r.get(field, "")) == str(val)]
                elif op == "gt":
                    result = [r for r in result if _safe_float(r.get(field)) > _safe_float(val)]
                elif op == "lt":
                    result = [r for r in result if _safe_float(r.get(field)) < _safe_float(val)]
                elif op == "gte":
                    result = [r for r in result if _safe_float(r.get(field)) >= _safe_float(val)]
                elif op == "lte":
                    result = [r for r in result if _safe_float(r.get(field)) <= _safe_float(val)]
            return result

    except Exception as e:
        logger.error(f"FollowUpResolver in-memory failed: {e}")
    return None


def _safe_float(val) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0

