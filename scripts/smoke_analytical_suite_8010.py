"""
Comprehensive smoke: analytical / business questions (voyage identity, ops, commercial,
customer lens, multi-hop). POSTs each query to /query with a **fresh session per section**
so traces stay attributable and cross-turn slot bleed is minimized.

Outputs (local only; paths are gitignored so they are not committed):
  - scripts/smoke_analytical_suite_8010.json (full payloads incl. trace)
  - scripts/smoke_analytical_suite_8010_review.md (human-readable + trace flow)

Usage:
  python scripts/smoke_analytical_suite_8010.py
  python scripts/smoke_analytical_suite_8010.py --base-url http://127.0.0.1:8010/query
  python scripts/smoke_analytical_suite_8010.py --timeout 180
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from pathlib import Path
from typing import Any

import requests


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_BASE = "http://127.0.0.1:8010/query"
OUTPUT_JSON = SCRIPT_DIR / "smoke_analytical_suite_8010.json"
OUTPUT_MD = SCRIPT_DIR / "smoke_analytical_suite_8010_review.md"

SESSIONS: list[dict[str, Any]] = [
    {
        "name": "01_voyage_identity_details",
        "queries": [
            "Can you show full details for voyage 2304 on Maersk Cayman, including route and duration?",
            "Which vessel and IMO are assigned to voyage 2307, and what type of voyage is it (Spot vs TC)?",
            "What is the complete route for voyage 2105, including all intermediate ports?",
            "How long did each voyage take from start to end, and which one had the longest duration?",
            "Which voyages were executed under Spot contracts versus Time Charter voyages?",
        ],
    },
    {
        "name": "02_operational_performance",
        "queries": [
            "Which voyages experienced the highest waiting time before laycan or loading?",
            "Are there any voyages with significant port stay durations that could indicate congestion?",
            "Which ports (e.g., Houston, Tuxpan, San Francisco) are consistently associated with delays or higher turnaround time?",
            "Did any voyages include bunkering operations that increased port time or cost?",
            "Which voyages had operational inefficiencies based on extended port stays or multiple discharge points?",
            "Are there recurring operational remarks (e.g., demurrage adjustments, port cost issues) affecting voyage performance?",
        ],
    },
    {
        "name": "03_cargo_commercial",
        "queries": [
            "What cargo grades are associated with each voyage, and which grade is most frequently transported?",
            "Which voyage generated the highest revenue and what contributed most—freight, demurrage, or hire?",
            "Which voyage had the highest demurrage component, and what caused it operationally?",
            "How does PnL compare across voyages, and which voyage is underperforming commercially?",
            "What is the TCE performance across voyages, and which voyage delivered the highest earnings per day?",
            "Which voyages had significant miscellaneous or unexpected expenses impacting profitability?",
        ],
    },
    {
        "name": "04_customer_sales",
        "queries": [
            "Which customer (e.g., PMI, Shell, Vitol) is associated with each voyage, and who are our repeat customers?",
            "Did any voyage experience delays or operational issues that could impact customer satisfaction?",
            "Which voyages indicate strong repeat business potential based on customer and route consistency?",
            "Are there voyages where demurrage or delays could lead to customer disputes or renegotiations?",
            "Which voyages show strong commercial performance that can be used for upselling similar routes?",
            "Are there customers consistently associated with high-margin voyages?",
        ],
    },
    {
        "name": "05_multihop_analytical",
        "queries": [
            "Which voyages had high PnL but also high port costs or demurrage, indicating operational inefficiency?",
            "Which ports are most frequently linked to high revenue but also high delay or cost impact?",
            "Are there voyages where bunker costs significantly impacted profitability despite high revenue?",
            "Which voyages had multiple discharge ports, and how did that impact turnaround time and cost?",
            "Can we identify voyages where TCE is high but overall PnL is low, indicating cost inefficiency?",
            "Which voyages show strong revenue growth but declining efficiency, signaling operational risk?",
            "Are there voyages where customer, cargo, and route combination consistently produce high margins?",
        ],
    },
]


def _load_base_url(cli: str | None) -> str:
    if cli:
        return cli.rstrip("/")
    env = (os.getenv("KAI_API_URL") or "").strip().rstrip("/")
    if env.endswith("/query"):
        return env
    if env:
        return f"{env}/query" if not env.endswith("query") else env
    return DEFAULT_BASE


def _summarize_trace(trace: Any, *, max_events: int = 40) -> list[str]:
    if not isinstance(trace, list) or not trace:
        return ["(no trace)"]
    lines: list[str] = []
    for i, ev in enumerate(trace[:max_events]):
        if not isinstance(ev, dict):
            lines.append(f"{i + 1}. {type(ev).__name__}")
            continue
        phase = ev.get("phase")
        agent = ev.get("agent")
        ik = ev.get("intent_key")
        desc = ev.get("description")
        mode = ev.get("mode")
        rows = ev.get("rows")
        parts = [str(p) for p in (phase, agent, ik, mode) if p not in (None, "", [])]
        head = " | ".join(parts) if parts else "event"
        if isinstance(rows, int):
            head += f" | rows={rows}"
        if desc and isinstance(desc, str):
            ds = desc.replace("\n", " ")[:120]
            head += f" — {ds}"
        lines.append(f"{i + 1}. {head}")
    if len(trace) > max_events:
        lines.append(f"... ({len(trace) - max_events} more events)")
    return lines


def _extract_voyage_numbers(text: str) -> set[str]:
    return set(re.findall(r"\b\d{4}\b", text or ""))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default=None, help="Full POST URL ending in /query")
    ap.add_argument("--timeout", type=float, default=120.0)
    args = ap.parse_args()
    base = _load_base_url(args.base_url)

    results: list[dict[str, Any]] = []
    run_tag = int(time.time())
    intent_hist: dict[str, int] = {}
    stats = {"ok": 0, "http_err": 0, "exc": 0, "clarify": 0, "empty_answer": 0}

    for session in SESSIONS:
        sid = f"smoke-analytical-{run_tag}-{session['name']}"
        for index, query in enumerate(session["queries"], start=1):
            started = time.time()
            item: dict[str, Any] = {
                "session": session["name"],
                "index": index,
                "query": query,
                "session_id": sid,
            }
            try:
                r = requests.post(
                    base,
                    json={"query": query, "session_id": sid},
                    timeout=args.timeout,
                )
                data = r.json() if r.content else {}
                item.update(
                    {
                        "status_code": r.status_code,
                        "intent_key": data.get("intent_key"),
                        "answer": data.get("answer"),
                        "clarification": data.get("clarification"),
                        "trace": data.get("trace"),
                        "slots": data.get("slots"),
                        "dynamic_sql_used": data.get("dynamic_sql_used"),
                        "dynamic_sql_agents": data.get("dynamic_sql_agents"),
                    }
                )
                if r.status_code == 200:
                    stats["ok"] += 1
                else:
                    stats["http_err"] += 1
                ik = item.get("intent_key")
                if ik:
                    intent_hist[str(ik)] = intent_hist.get(str(ik), 0) + 1
                if item.get("clarification"):
                    stats["clarify"] += 1
                ans = item.get("answer") or ""
                if isinstance(ans, str) and len(ans.strip()) < 20:
                    stats["empty_answer"] += 1
            except Exception as exc:
                item["error"] = str(exc)
                stats["exc"] += 1
            item["elapsed_sec"] = round(time.time() - started, 2)
            results.append(item)
            print(
                json.dumps(
                    {
                        "session": item["session"],
                        "index": item["index"],
                        "intent_key": item.get("intent_key"),
                        "status": item.get("status_code"),
                        "err": item.get("error"),
                        "sec": item["elapsed_sec"],
                    },
                    ensure_ascii=True,
                ),
                flush=True,
            )

    OUTPUT_JSON.parent.mkdir(exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")

    md: list[str] = [
        "# Analytical suite smoke review",
        "",
        f"- Base URL: `{base}`",
        f"- Total queries: `{len(results)}`",
        f"- Stats: HTTP 200=`{stats['ok']}`, HTTP err=`{stats['http_err']}`, exceptions=`{stats['exc']}`, "
        f"clarifications=`{stats['clarify']}`, very short answers=`{stats['empty_answer']}`",
        "",
        "## Intent distribution",
        "",
        "```json",
        json.dumps(dict(sorted(intent_hist.items(), key=lambda x: (-x[1], x[0]))), indent=2),
        "```",
        "",
    ]

    cur: str | None = None
    for item in results:
        if item["session"] != cur:
            cur = item["session"]
            md.extend([f"## Session `{cur}`", ""])

        qnums = _extract_voyage_numbers(item["query"])
        slots = item.get("slots") or {}
        slot_nums_raw: set[str] = set()
        if isinstance(slots, dict):
            for k in ("voyage_number", "voyage_numbers"):
                v = slots.get(k)
                if isinstance(v, list):
                    slot_nums_raw.update(str(x) for x in v if x is not None)
                elif v not in (None, "", []):
                    slot_nums_raw.add(str(v))
        slot_norm: set[str] = set()
        for s in slot_nums_raw:
            try:
                slot_norm.add(str(int(float(s))))
            except Exception:
                t = str(s).strip()
                if t:
                    slot_norm.add(t)
        align_note = ""
        if qnums or slot_norm:
            overlap = qnums & slot_norm
            if overlap:
                align_note = f"Voyage number overlap (query ∩ slots): {sorted(overlap)}"
            elif qnums and slot_norm:
                align_note = f"Query voyages {sorted(qnums)} vs slot anchors {sorted(slot_norm)}"

        md.extend(
            [
                f"### Turn {item['index']}",
                f"- Query: {item['query']}",
                f"- Status: `{item.get('status_code')}` | Intent: `{item.get('intent_key')}` | "
                f"Dynamic SQL: `{item.get('dynamic_sql_used')}` | Agents: `{item.get('dynamic_sql_agents')}`",
                f"- Elapsed: `{item.get('elapsed_sec')}` s",
                f"- Slots: `{json.dumps(item.get('slots'), ensure_ascii=True)[:800]}`",
            ]
        )
        if align_note:
            md.append(f"- Heuristic: {align_note}")
        md.extend(
            [
                "",
                "#### Execution trace (compact)",
                "",
                "```text",
                "\n".join(_summarize_trace(item.get("trace"))),
                "```",
                "",
                "#### Response",
                "",
                "```text",
                str(item.get("answer") or "")[:12000],
                "```",
                "",
            ]
        )
        if item.get("clarification"):
            md.extend(
                [
                    "#### Clarification",
                    "",
                    "```text",
                    str(item.get("clarification")),
                    "```",
                    "",
                ]
            )
        if item.get("error"):
            md.extend(
                [
                    "#### Error",
                    "",
                    "```text",
                    str(item.get("error")),
                    "```",
                    "",
                ]
            )

    md.extend(
        [
            "## Validation notes",
            "",
            "- **Trace**: Steps list planner, finance/mongo/ops agents, multi-step boundaries, and row counts where emitted.",
            "- **Relevance**: Compare question nouns (voyage numbers, vessel names) to answer and `slots`; mismatches often mean routing or duplicate voyageNumber issues.",
            "- **Data consistency**: Cross-check finance figures in the narrative against any tabular SQL-backed blocks; multi-source intents should separate PnL (Postgres) from fixture/route (Mongo).",
            "- **Limits**: Fleet-wide ranking vs named-vessel questions may pull different scopes; customer/charterer fields depend on fixture data availability.",
            "",
        ]
    )

    OUTPUT_MD.write_text("\n".join(md), encoding="utf-8")
    print(f"WROTE {OUTPUT_JSON} ({len(results)} results)", flush=True)
    print(f"WROTE {OUTPUT_MD}", flush=True)


if __name__ == "__main__":
    main()
