"""
Replay Thing 1 golden cases from scripts/thing1_t1_t38_baseline.json against /query.
Baseline format: { "results": [ { "id", "query", "session_id", ... }, ... ] }
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import requests

REPO = Path(__file__).resolve().parents[1]
BASELINE = REPO / "scripts" / "thing1_t1_t38_baseline.json"
OUTPUT = REPO / "scripts" / "thing1_t1_t38_phase4a.json"
URL = "http://127.0.0.1:8010/query"


def _result_rows(doc: object) -> list[dict]:
    if isinstance(doc, dict) and isinstance(doc.get("results"), list):
        return [r for r in doc["results"] if isinstance(r, dict)]
    if isinstance(doc, list):
        return [r for r in doc if isinstance(r, dict)]
    return []


def main() -> int:
    baseline_raw = json.loads(BASELINE.read_text(encoding="utf-8"))
    entries = _result_rows(baseline_raw)
    if not entries:
        print("No entries found under baseline['results']", flush=True)
        return 2

    n = len(entries)
    results: list[dict] = []
    for i, entry in enumerate(entries):
        tid = entry.get("id") or entry.get("t_number") or f"T{i+1}"
        try:
            resp = requests.post(
                URL,
                json={
                    "query": entry["query"],
                    "session_id": entry["session_id"],
                },
                timeout=120,
            )
            data = resp.json() if isinstance(resp.json(), dict) else {}
            row = {
                "id": tid,
                "query": entry["query"],
                "session_id": entry["session_id"],
                "status_code": resp.status_code,
                "elapsed_sec": None,
                "intent_key": data.get("intent_key") or data.get("intent"),
                "slots": data.get("slots"),
                "answer": data.get("answer") or data.get("response"),
                "data": data.get("data"),
                "clarification": data.get("clarification"),
                "dynamic_sql_used": data.get("dynamic_sql_used"),
                "dynamic_sql_agents": data.get("dynamic_sql_agents"),
                "trace": data.get("trace"),
                "error": data.get("error"),
                "canon_sha256": data.get("canon_sha256"),
            }
            results.append(row)
            print(
                f"  [{i + 1}/{n}] {tid} -> {resp.status_code} | intent={row['intent_key']!r}",
                flush=True,
            )
            time.sleep(0.3)
        except Exception as e:
            print(f"  [{i + 1}/{n}] {tid} FAILED: {e}", flush=True)
            results.append({"id": tid, "query": entry.get("query"), "error": str(e)})

    out_doc = {
        "generated_at": int(time.time()),
        "base_url": URL,
        "results": results,
    }
    OUTPUT.write_text(json.dumps(out_doc, indent=2), encoding="utf-8")
    print(f"\nSaved -> {OUTPUT}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
