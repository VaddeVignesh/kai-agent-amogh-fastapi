"""
Normalize bulky ops_voyage_summary JSON columns before LLM summarization.

postgres/json drivers may return ports_json / grades_json as str or list; cap and
extract stable labels so the model does not receive megabyte payloads or hallucinate vessel names as ports.
"""

from __future__ import annotations

import json
import re
from typing import Any


def _json_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return []
        try:
            parsed = json.loads(s)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


_VESSEL_NAMEISH = re.compile(
    r"(?i)(stena|tanker|\bmt\s|m\.?v\.?|carrier|polemer|crown|crystal|blue\s+sky|clear\s+sky)"
)


def port_names_from_ports_json(ports_blob: Any, *, max_unique: int = 15, scan_cap: int = 80) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in _json_list(ports_blob)[:scan_cap]:
        name = ""
        if isinstance(raw, dict):
            name = (
                str(raw.get("portName") or raw.get("port_name") or raw.get("name") or raw.get("port") or "").strip()
            )
        elif isinstance(raw, str):
            name = raw.strip()
        else:
            continue
        if not name or len(name) > 120:
            continue
        lk = name.lower()
        # Heuristic: fixtures / noisy rows sometimes carry charterer or vessel-ish labels.
        if _VESSEL_NAMEISH.search(name):
            continue
        if lk in seen:
            continue
        seen.add(lk)
        out.append(name)
        if len(out) >= max_unique:
            break
    return out


def grade_strings_from_grades_json(grades_blob: Any, *, max_unique: int = 12, scan_cap: int = 60) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in _json_list(grades_blob)[:scan_cap]:
        g = ""
        if isinstance(raw, dict):
            g = str(raw.get("grade_name") or raw.get("gradeName") or raw.get("name") or raw.get("grade") or "").strip()
        elif isinstance(raw, str):
            g = raw.strip()
        else:
            continue
        if not g or len(g) > 80:
            continue
        lk = g.lower()
        if lk in seen:
            continue
        seen.add(lk)
        out.append(g)
        if len(out) >= max_unique:
            break
    return out


def shrink_ops_row_json_fields(row: dict, *, voyage_summary: bool) -> dict:
    """Return a shallow-updated dict safe for prompt injection (caller may deepcopy first)."""
    if not isinstance(row, dict):
        return row
    ports = port_names_from_ports_json(row.get("ports_json"))
    grades = grade_strings_from_grades_json(row.get("grades_json"))
    row["ports"] = ports
    row["cargo_grade_names"] = grades
    for k in ("ports_json", "grades_json"):
        row.pop(k, None)
    if voyage_summary:
        row.pop("activities_json", None)
    remarks = row.get("remarks_json")
    rlist = _json_list(remarks)
    if rlist:
        short: list[str] = []
        for x in rlist[:5]:
            if isinstance(x, dict):
                t = str(x.get("remark") or x.get("text") or "").strip()
                if t:
                    short.append(t[:400])
            elif x not in (None, "", [], {}):
                short.append(str(x).strip()[:400])
        row["remarks_preview"] = short
    if "remarks_json" in row:
        row.pop("remarks_json", None)
    return row
