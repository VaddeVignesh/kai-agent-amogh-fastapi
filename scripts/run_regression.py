import json
import sys
import time

import requests

BASELINE = "scripts/thing1_t1_t38_baseline.json"
PHASE = sys.argv[1] if len(sys.argv) > 1 else "phase"
OUTPUT = f"scripts/thing1_t1_t38_{PHASE}.json"
URL = "http://127.0.0.1:8010/query"

with open(BASELINE, encoding="utf-8") as f:
    _doc = json.load(f)

_entries = _doc.get("results") if isinstance(_doc, dict) else _doc
if not isinstance(_entries, list):
    print("BASELINE must be dict with 'results' list", file=sys.stderr)
    sys.exit(2)

results = []
for i, entry in enumerate(_entries):
    if not isinstance(entry, dict):
        continue
    tid = entry.get("id") or entry.get("t_number") or f"T{i+1}"
    try:
        resp = requests.post(
            URL,
            json={
                "query": entry["query"],
                "session_id": entry["session_id"],
            },
            timeout=90,
        )
        data = resp.json() if isinstance(resp.json(), dict) else {}
        results.append(
            {
                "t_number": tid,
                "query": entry["query"],
                "session_id": entry["session_id"],
                "status_code": resp.status_code,
                "intent_key": data.get("intent_key") or data.get("intent"),
                "dynamic_sql_used": data.get("dynamic_sql_used"),
                "dynamic_sql_agents": data.get("dynamic_sql_agents"),
            }
        )
        print(
            f"  [{i + 1}/{len(_entries)}] {tid} "
            f"-> {resp.status_code} | intent={results[-1]['intent_key']}"
        )
        time.sleep(0.3)
    except Exception as e:
        print(f"  [{i + 1}/{len(_entries)}] FAILED: {e}")
        results.append({"t_number": tid, "error": str(e)})

with open(OUTPUT, "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2)

with open(BASELINE, encoding="utf-8") as f:
    base_doc = json.load(f)

base_rows = base_doc.get("results") if isinstance(base_doc, dict) else base_doc
if not isinstance(base_rows, list):
    base_rows = []

status_match = intent_match = sql_match = 0
failures = []
for b, c in zip(base_rows, results):
    if not isinstance(b, dict):
        continue
    t = b.get("id") or b.get("t_number", "?")
    ok_s = b.get("status_code") == c.get("status_code")
    ok_i = b.get("intent_key") == c.get("intent_key")
    ok_q = (b.get("dynamic_sql_used") == c.get("dynamic_sql_used")) and (
        b.get("dynamic_sql_agents") == c.get("dynamic_sql_agents")
    )
    if ok_s:
        status_match += 1
    if ok_i:
        intent_match += 1
    if ok_q:
        sql_match += 1
    if not (ok_s and ok_i and ok_q):
        failures.append(
            {
                "t": t,
                "q": str(b.get("query", ""))[:70],
                "status": f"{b.get('status_code')} -> {c.get('status_code')}",
                "intent": f"{b.get('intent_key')} -> {c.get('intent_key')}",
                "sql": (
                    f"{b.get('dynamic_sql_used')}/{b.get('dynamic_sql_agents')} "
                    f"-> {c.get('dynamic_sql_used')}/{c.get('dynamic_sql_agents')}"
                ),
            }
        )

n = max(len(base_rows), 38)
print(f"\n{'=' * 55}")
print(f"REGRESSION REPORT — {PHASE.upper()}")
print(f"{'=' * 55}")
print(f"Status parity : {status_match}/{n}")
print(f"Intent parity : {intent_match}/{n}")
print(f"SQL parity    : {sql_match}/{n}")

if failures:
    print(f"\nFAILURES ({len(failures)}):")
    for f_ in failures:
        print(f"  {f_['t']} | {f_['q']}")
        print(f"    intent : {f_['intent']}")
        print(f"    sql    : {f_['sql']}")
    print("\nSTATUS: FAILED — do not proceed")
    sys.exit(1)
else:
    print("\nSTATUS: ALL PASS — safe to proceed")
    sys.exit(0)
