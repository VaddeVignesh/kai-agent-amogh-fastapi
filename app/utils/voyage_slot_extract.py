"""
Voyage-number helpers.

Primary slot extraction is the LLM intent classifier (prompt_rules.yaml).
Regex here is a fallback only when the LLM omits or truncates voyage_numbers.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List


def normalize_query_text(text: str) -> str:
    """Normalize apostrophe variants for reliable regex matching."""
    t = text or ""
    return (
        t.replace("\u2019", "'")
        .replace("\u2018", "'")
        .replace("\u00e2\u20ac\u2122", "'")
        .replace("\u00e2\u20ac\u02dc", "'")
    )


def extract_voyage_numbers_from_text(text: str) -> List[int]:
    """
    Extract ordered unique voyage numbers (typically 3–5 digits) from user text.

    Handles patterns such as:
      - "voyage 1901 and 1902"
      - "tell me about voyage 2117 and 2312"
      - "tell me about 2302 and 2306"
      - "voyages 1901, 1902, 1903"
    """
    text_norm = normalize_query_text(text)
    if not text_norm.strip():
        return []

    found: List[int] = []
    seen: set[int] = set()

    def _add(raw_values: list) -> None:
        for raw in raw_values:
            try:
                n = int(float(str(raw).strip()))
            except (TypeError, ValueError):
                continue
            if n < 100 or n > 99999:
                continue
            if n not in seen:
                seen.add(n)
                found.append(n)

    # All voyage-labeled numbers (every occurrence).
    for m in re.finditer(
        r"\bvoyages?\s*(?:number\s+|no\.?\s+|#\s*)?(\d{3,5})\b",
        text_norm,
        flags=re.IGNORECASE,
    ):
        _add([m.group(1)])

    tl = text_norm.lower()
    voyage_context = (
        "voyage" in tl
        or "voyages" in tl
        or bool(re.search(r"\babout\s+\d{3,5}\b", tl, flags=re.IGNORECASE))
    )

    if voyage_context:
        # Explicit pairs/lists: "1901 and 1902", "1901, 1902"
        for m in re.finditer(
            r"\b(\d{3,5})\s*(?:(?:,\s*(?:and\s*)?)|and\s+|&\s*)(\d{3,5})\b",
            text_norm,
            flags=re.IGNORECASE,
        ):
            _add([m.group(1), m.group(2)])

        # Longer chains on the same match span.
        for m in re.finditer(
            r"\b(\d{3,5})(?:\s*(?:(?:,\s*(?:and\s*)?)|and\s+|&\s*)(\d{3,5}))+",
            text_norm,
            flags=re.IGNORECASE,
        ):
            _add(re.findall(r"\d{3,5}", m.group(0)))

        # Fallback: all standalone 3–5 digit tokens when voyage context exists.
        if not found:
            _add(re.findall(r"\b(\d{3,5})\b", text_norm))

    # Drop "top N" limit accidentally captured as a voyage number.
    top_m = re.search(r"\btop\s+(\d{1,2})\b", tl)
    if top_m:
        try:
            limit_n = int(top_m.group(1))
            found = [n for n in found if n != limit_n]
        except ValueError:
            pass

    return found


def _normalize_voyage_list(value: Any) -> List[int]:
    if value in (None, "", [], {}):
        return []
    raw = value if isinstance(value, list) else [value]
    out: List[int] = []
    seen: set[int] = set()
    for item in raw:
        try:
            n = int(float(item))
        except (TypeError, ValueError):
            continue
        if 100 <= n <= 99999 and n not in seen:
            seen.add(n)
            out.append(n)
    return out


def merge_voyage_slot_fields(slots: Dict[str, Any], text: str) -> Dict[str, Any]:
    """
    LLM-primary voyage slots. Regex fills gaps only when the model drops numbers.
    """
    merged = dict(slots or {})
    llm_list = _normalize_voyage_list(merged.get("voyage_numbers"))
    if not llm_list and merged.get("voyage_number") not in (None, "", [], {}):
        llm_list = _normalize_voyage_list(merged.get("voyage_number"))

    fallback = extract_voyage_numbers_from_text(text)

    if len(llm_list) >= 2:
        merged["voyage_numbers"] = llm_list
        merged.pop("voyage_number", None)
        return merged

    if len(llm_list) == 1 and len(fallback) <= 1:
        merged["voyage_numbers"] = llm_list
        merged["voyage_number"] = llm_list[0]
        return merged

    if llm_list and fallback and len(fallback) > len(llm_list):
        combined: List[int] = []
        seen: set[int] = set()
        for n in llm_list + fallback:
            if n not in seen:
                seen.add(n)
                combined.append(n)
        merged["voyage_numbers"] = combined
        if len(combined) == 1:
            merged["voyage_number"] = combined[0]
        else:
            merged.pop("voyage_number", None)
        return merged

    if fallback:
        merged["voyage_numbers"] = fallback
        if len(fallback) == 1:
            merged["voyage_number"] = fallback[0]
        else:
            merged.pop("voyage_number", None)
        return merged

    merged.pop("voyage_number", None)
    merged.pop("voyage_numbers", None)
    return merged
