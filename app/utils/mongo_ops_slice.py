"""
Operational Mongo slices for ops-only RBAC tenants (customer_ops_only).

Strip commercial/financial fixture fields but keep ports, grades, and legs for summarization.
"""

from __future__ import annotations

from typing import Any

from app.utils.ops_llm_shrink import (
    _json_list,
    grade_strings_from_grades_json,
    port_names_from_ports_json,
)


def port_names_from_activities_json(activities_blob: Any, *, max_unique: int = 15) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in _json_list(activities_blob)[:40]:
        if not isinstance(raw, dict):
            continue
        for key in ("portName", "port_name", "port", "name", "fromPort", "toPort", "loadPort", "dischargePort"):
            val = raw.get(key)
            if val in (None, "", [], {}):
                continue
            s = str(val).strip()
            if not s or len(s) > 120:
                continue
            lk = s.casefold()
            if lk in seen:
                continue
            seen.add(lk)
            out.append(s)
            if len(out) >= max_unique:
                return out
    return out


def operational_fixtures_for_summary(fixtures: Any, *, max_fixtures: int = 3) -> list[dict]:
    """Keep fixture ports/grades only — no hire rates, commissions, or revenue lines."""
    if not isinstance(fixtures, list):
        return []
    out: list[dict] = []
    for fx in fixtures[:max_fixtures]:
        if not isinstance(fx, dict):
            continue
        ports_raw = fx.get("fixturePorts") or fx.get("ports") or []
        grades_raw = fx.get("fixtureGrades") or fx.get("grades") or []
        port_slice = []
        if isinstance(ports_raw, list):
            for p in ports_raw[:15]:
                if not isinstance(p, dict):
                    continue
                port_slice.append(
                    {
                        "portName": p.get("portName") or p.get("port_name") or p.get("name"),
                        "activityType": p.get("activityType") or p.get("activity_type"),
                    }
                )
        grade_slice = []
        if isinstance(grades_raw, list):
            for g in grades_raw[:12]:
                if isinstance(g, dict):
                    gn = g.get("gradeName") or g.get("grade_name") or g.get("name") or g.get("grade")
                    if gn:
                        grade_slice.append({"gradeName": str(gn).strip()})
                elif g not in (None, "", [], {}):
                    grade_slice.append({"gradeName": str(g).strip()})
        if port_slice or grade_slice:
            out.append({"fixturePorts": port_slice, "fixtureGrades": grade_slice})
    return out


def _cap_legs(legs: Any, *, max_legs: int = 12) -> list[dict]:
    if not isinstance(legs, list):
        return []
    slim: list[dict] = []
    for leg in legs[:max_legs]:
        if not isinstance(leg, dict):
            continue
        slim.append(
            {
                "portName": leg.get("portName") or leg.get("port_name") or leg.get("name"),
                "activityType": leg.get("activityType") or leg.get("activity_type"),
                "startDate": leg.get("startDate") or leg.get("start_date"),
                "endDate": leg.get("endDate") or leg.get("end_date"),
            }
        )
    return slim


def ports_from_mongo_doc(doc: dict, *, max_ports: int = 20) -> list[str]:
    if not isinstance(doc, dict):
        return []
    seen: set[str] = set()
    out: list[str] = []

    def _add(name: str) -> None:
        s = (name or "").strip()
        if not s or len(s) > 120:
            return
        lk = s.casefold()
        if lk in seen:
            return
        seen.add(lk)
        out.append(s)

    for leg in _cap_legs(doc.get("legs"), max_legs=30):
        _add(str(leg.get("portName") or ""))
    for fx in operational_fixtures_for_summary(doc.get("fixtures") or [], max_fixtures=5):
        for p in fx.get("fixturePorts") or []:
            if isinstance(p, dict):
                _add(str(p.get("portName") or ""))
    if len(out) >= max_ports:
        return out[:max_ports]
    return out


def grades_from_mongo_doc(doc: dict, *, max_grades: int = 15) -> list[str]:
    if not isinstance(doc, dict):
        return []
    seen: set[str] = set()
    out: list[str] = []

    def _add(g: str) -> None:
        s = (g or "").strip()
        if not s:
            return
        lk = s.casefold()
        if lk in seen:
            return
        seen.add(lk)
        out.append(s)

    for fx in operational_fixtures_for_summary(doc.get("fixtures") or [], max_fixtures=5):
        for g in fx.get("fixtureGrades") or []:
            if isinstance(g, dict):
                _add(str(g.get("gradeName") or ""))
            else:
                _add(str(g))
    return out[:max_grades]


def build_mongo_llm_row_for_ops_tenant(mongo_safe: dict) -> dict:
    """Single mongo.rows[0] payload for ops-only voyage.summary summarization."""
    if not isinstance(mongo_safe, dict):
        return {}
    fixtures = operational_fixtures_for_summary(mongo_safe.get("fixtures") or [])
    legs = _cap_legs(mongo_safe.get("legs") or [])
    remarks = mongo_safe.get("remarks") or []
    if isinstance(remarks, list) and len(remarks) > 8:
        remarks = remarks[:8]
    return {
        "voyageId": mongo_safe.get("voyage_id") or mongo_safe.get("voyageId"),
        "voyageNumber": mongo_safe.get("voyage_number") or mongo_safe.get("voyageNumber"),
        "vesselName": mongo_safe.get("vessel_name") or mongo_safe.get("vesselName"),
        "vesselImo": mongo_safe.get("vessel_imo") or mongo_safe.get("vesselImo"),
        "remarks": remarks,
        "fixtures": fixtures,
        "legs": legs,
    }


def build_voyage_summary_merged_row(
    *,
    ops_row: dict | None,
    mongo_safe: dict | None,
    slots: dict | None,
) -> dict:
    """Flatten ops + mongo into one merged_rows item for the summarizer."""
    ops_row = ops_row if isinstance(ops_row, dict) else {}
    mongo_safe = mongo_safe if isinstance(mongo_safe, dict) else {}
    slots = slots if isinstance(slots, dict) else {}

    mongo_doc = build_mongo_llm_row_for_ops_tenant(mongo_safe)
    ports: list[str] = []
    if isinstance(ops_row.get("ports"), list):
        ports.extend(str(p).strip() for p in ops_row["ports"] if str(p).strip())
    elif ops_row.get("ports_json"):
        ports.extend(port_names_from_ports_json(ops_row.get("ports_json")))
    if ops_row.get("activities_json"):
        ports.extend(port_names_from_activities_json(ops_row.get("activities_json")))
    ports.extend(ports_from_mongo_doc(mongo_doc))

    grades: list[str] = []
    if isinstance(ops_row.get("cargo_grade_names"), list):
        grades.extend(str(g).strip() for g in ops_row["cargo_grade_names"] if str(g).strip())
    elif ops_row.get("grades_json"):
        grades.extend(grade_strings_from_grades_json(ops_row.get("grades_json")))
    grades.extend(grades_from_mongo_doc(mongo_doc))

    # De-dupe preserve order
    def _dedupe(items: list[str]) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for x in items:
            lk = x.casefold()
            if lk in seen:
                continue
            seen.add(lk)
            out.append(x)
        return out

    ports = _dedupe(ports)
    grades = _dedupe(grades)

    remarks_text = None
    raw_remarks = mongo_safe.get("remarks") or mongo_doc.get("remarks")
    if isinstance(raw_remarks, list) and raw_remarks:
        parts = []
        for item in raw_remarks[:6]:
            if isinstance(item, dict):
                body = str(item.get("remark") or item.get("text") or "").strip()
                if body:
                    parts.append(body[:300])
            elif str(item).strip():
                parts.append(str(item).strip()[:300])
        remarks_text = " | ".join(parts) if parts else None

    row = {
        "voyage_id": mongo_safe.get("voyage_id") or slots.get("voyage_id") or ops_row.get("voyage_id"),
        "voyage_number": mongo_safe.get("voyage_number") or slots.get("voyage_number") or ops_row.get("voyage_number"),
        "vessel_name": mongo_safe.get("vessel_name") or slots.get("vessel_name") or ops_row.get("vessel_name"),
        "vessel_imo": mongo_safe.get("vessel_imo") or slots.get("imo") or ops_row.get("vessel_imo"),
        "offhire_days": ops_row.get("offhire_days"),
        "is_delayed": ops_row.get("is_delayed"),
        "delay_reason": ops_row.get("delay_reason") or ops_row.get("delay_reasons"),
        "module_type": ops_row.get("module_type"),
        "key_ports": ports[:20],
        "cargo_grades": grades[:15],
        "remarks": remarks_text,
    }
    return {k: v for k, v in row.items() if v not in (None, "", [], {})}
