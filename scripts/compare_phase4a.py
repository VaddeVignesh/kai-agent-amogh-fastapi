"""
Compare scripts/thing1_t1_t38_phase4a.json to scripts/thing1_t1_t38_baseline.json.
Supports baseline shape { "results": [ ... ] } or a bare list.
"""
from __future__ import annotations

import json
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def _rows(doc: object) -> list[dict]:
    if isinstance(doc, dict) and isinstance(doc.get("results"), list):
        return [r for r in doc["results"] if isinstance(r, dict)]
    if isinstance(doc, list):
        return [r for r in doc if isinstance(r, dict)]
    return []


def _tid(row: dict) -> str:
    return str(row.get("id") or row.get("t_number") or "?")


def main() -> int:
    base_path = REPO / "scripts" / "thing1_t1_t38_baseline.json"
    curr_path = REPO / "scripts" / "thing1_t1_t38_phase4a.json"

    base = json.loads(base_path.read_text(encoding="utf-8"))
    curr = json.loads(curr_path.read_text(encoding="utf-8"))

    base_rows = _rows(base)
    curr_rows = _rows(curr)

    status_match = intent_match = sql_match = 0
    failures: list[dict] = []

    if len(base_rows) != len(curr_rows):
        print(f"WARNING: row count mismatch baseline={len(base_rows)} current={len(curr_rows)}")

    for i, (b, c) in enumerate(zip(base_rows, curr_rows)):
        t = _tid(b)
        ok_status = b.get("status_code") == c.get("status_code")
        ok_intent = b.get("intent_key") == c.get("intent_key")
        ok_sql = (b.get("dynamic_sql_used") == c.get("dynamic_sql_used")) and (
            b.get("dynamic_sql_agents") == c.get("dynamic_sql_agents")
        )

        if ok_status:
            status_match += 1
        if ok_intent:
            intent_match += 1
        if ok_sql:
            sql_match += 1

        if not (ok_status and ok_intent and ok_sql):
            failures.append(
                {
                    "t": t,
                    "query": str(b.get("query", ""))[:60],
                    "status": f"{b.get('status_code')} -> {c.get('status_code')}",
                    "intent": f"{b.get('intent_key')} -> {c.get('intent_key')}",
                    "sql": (
                        f"{b.get('dynamic_sql_used')}/{b.get('dynamic_sql_agents')} -> "
                        f"{c.get('dynamic_sql_used')}/{c.get('dynamic_sql_agents')}"
                    ),
                }
            )

    n = len(base_rows)
    print(f"\n{'=' * 50}")
    print("PHASE 4A REGRESSION REPORT")
    print(f"{'=' * 50}")
    print(f"Compared rows     : {min(len(base_rows), len(curr_rows))} of baseline {n}")
    print(f"Status code parity: {status_match}/{n}")
    print(f"Intent parity     : {intent_match}/{n}")
    print(f"SQL routing parity: {sql_match}/{n}")

    if failures:
        print(f"\nFAILURES ({len(failures)}):")
        for fail in failures:
            print(f"  {fail['t']} | {fail['query']}")
            print(f"    status:  {fail['status']}")
            print(f"    intent:  {fail['intent']}")
            print(f"    sql:     {fail['sql']}")
    else:
        print("\nALL CHECKS PASSED — safe to proceed to Phase 4B")
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
