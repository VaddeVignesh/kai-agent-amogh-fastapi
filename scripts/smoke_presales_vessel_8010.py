"""
Presales vessel Q&A smoke: POST queries to /query in small chunks with a **fresh session_id per chunk**
so multiturn memory does not accumulate across all 32 questions.

Golden checks (scripts/presales_vessel_golden_checks.json):
  - Substring rules aligned to the **uploaded-data reference answers** (30 numbered answers in the sales doc).
  - This script runs **32** ChatGPT-generated questions (`QUERIES`); reference doc order/count differs.
  - Each check includes `maps_to_reference`: which reference answer number(s) that turn is scored against.
  - Passing all checks means the **live answer text** contains the expected phrases/numbers — not identical
    marketing copy. Full parity (exact bullets, fleet scope = 3 demo vessels only) needs product + data work.

Usage:
  python scripts/smoke_presales_vessel_8010.py
  python scripts/smoke_presales_vessel_8010.py --base-url http://127.0.0.1:8010/query
  python scripts/smoke_presales_vessel_8010.py --chunk-size 3 --pause-between-chunks 1.5
  python scripts/smoke_presales_vessel_8010.py --single-session   # legacy: one session, all turns (not recommended)
  python scripts/smoke_presales_vessel_8010.py --no-validate

Writes JSON + review markdown under scripts/ (gitignored; regenerate locally).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import requests


DEFAULT_BASE = "http://127.0.0.1:8010/query"
# Default 3: avoids putting "Stena Imperial" scrubber (Q4) in the same session as Primorsk (Q2).
DEFAULT_CHUNK_SIZE = 3
DEFAULT_PAUSE_SEC = 1.0
SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_JSON = SCRIPT_DIR / "smoke_presales_vessel_8010.json"
OUTPUT_MD = SCRIPT_DIR / "smoke_presales_vessel_8010_review.md"
GOLDEN_PATH = SCRIPT_DIR / "presales_vessel_golden_checks.json"

QUERIES: list[str] = [
    "Can you show the full profile details for vessel Stena Imperial including its technical and commercial tags?",
    "What is the IMO number and operator entity for Stena Primorsk?",
    "Which pool and segment does Stenaweco Energy belong to?",
    "Does Stena Imperial have a scrubber installed, and how does that impact compliance?",
    "What are the key commercial tags associated with each vessel in this fleet?",
    "Can you provide historical metadata or changes in ownership for these vessels?",
    "Which vessels are currently marked as operational versus non-operational?",
    "How do the vessel profiles differ between Stena Imperial and Stena Primorsk in terms of segment and pool classification?",
    "What is the current head contract status for Stena Imperial, and when does it expire?",
    "Are there any active or historical contracts associated with Stena Primorsk?",
    "Has Stenaweco Energy ever been under a long-term charter agreement?",
    "Which vessels currently have firm long-term contracts versus no contract coverage?",
    "Can you show customer usage history linked to Stena Imperial's contract period?",
    "Are there any upcoming renewal opportunities for vessels with existing contracts?",
    "Which vessels are operating without any active charter contracts today?",
    "How does fuel consumption compare across vessels at standard speeds (e.g., 12.5–13 knots)?",
    "Which vessel is more fuel-efficient under laden conditions—Stena Imperial or Stena Primorsk?",
    "Are there any performance deviations between ballast and laden conditions for these vessels?",
    "Which vessel shows the highest fuel consumption at peak speed ranges?",
    "Are there any indicators of inefficiency based on non-passage consumption (loading, discharge, idle)?",
    "How does Stenaweco Energy's performance compare given that it is currently not operating?",
    "Are there any operational remarks or anomalies observed in vessel consumption profiles?",
    "Which vessels are most commercially active and suitable for repeat charter opportunities?",
    "Is Stena Imperial's long-term contract generating consistent customer engagement?",
    "Which vessel presents the best upsell opportunity based on fuel efficiency and performance?",
    "Are there vessels currently underutilized that can be repositioned for new customers?",
    "Which vessels are best suited for customers prioritizing ECO or fuel-efficient operations?",
    "Are there any vessels at risk of churn due to lack of active contracts or operational status?",
    "Which vessels align best with high-value customer segments (e.g., IMO pool vs ECO pool)?",
    "Can we identify vessels that can be marketed differently based on their segment (MR IMOIIMAX vs P-Max vs ECO)?",
    "Are there repeat customers associated with specific vessel segments or pools?",
    "Which vessel should be prioritized for upcoming commercial opportunities based on current utilization and readiness?",
]


def _load_golden() -> list[dict]:
    if not GOLDEN_PATH.is_file():
        return []
    data = json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))
    return list(data.get("checks") or [])


def _heuristic_report_issues(query: str, answer: str, slots: dict | None) -> list[str]:
    """Smoke-report hints only (not used by the agent)."""
    issues: list[str] = []
    q = (query or "").lower()
    a = (answer or "").lower()
    sl = slots if isinstance(slots, dict) else {}
    vn = str(sl.get("vessel_name") or "").lower()
    conflicts = (
        ("stena imperial", "stena primorsk"),
        ("stena primorsk", "stena imperial"),
        ("stenaweco energy", "stena primorsk"),
        ("stenaweco energy", "stena imperial"),
    )
    for primary, wrong in conflicts:
        if primary in q:
            if wrong in a and primary not in a:
                issues.append(f"answer_highlights_wrong_vessel:{wrong}")
            if vn and wrong in vn:
                issues.append(f"slot_has_wrong_vessel:{wrong}")
    if "no metadata available" in a or "not available in the dataset" in a:
        issues.append("answer_reports_missing_data")
    return issues


def _check_golden_rule(rule: dict, answer: str) -> list[str]:
    """Return human-readable failure reasons for one golden rule (empty = pass)."""
    failures: list[str] = []
    text = (answer or "").lower()
    turn = int(rule.get("turn", -1))
    label = str(rule.get("label") or f"turn {turn}")
    for s in rule.get("must_contain") or []:
        if s.lower() not in text:
            failures.append(f"{label}: missing substring {s!r}")
    for group in rule.get("must_contain_any") or []:
        if not any(g.lower() in text for g in group):
            failures.append(f"{label}: expected at least one of {group!r}")
    return failures


def _build_golden_scorecard(results: list[dict], checks: list[dict]) -> tuple[list[dict], list[str]]:
    by_turn = {int(c["turn"]): c for c in checks if "turn" in c}
    scorecard: list[dict] = []
    flat_failures: list[str] = []
    for r in sorted(results, key=lambda x: int(x.get("index") or 0)):
        idx = int(r.get("index") or 0)
        rule = by_turn.get(idx)
        if not rule:
            scorecard.append({"turn": idx, "label": "", "pass": None, "skipped": True, "failures": []})
            continue
        fails = _check_golden_rule(rule, str(r.get("answer") or r.get("error") or ""))
        for f in fails:
            flat_failures.append(f"Turn {idx}: {f}")
        ref = rule.get("maps_to_reference")
        scorecard.append(
            {
                "turn": idx,
                "label": rule.get("label", ""),
                "maps_to_reference": ref if isinstance(ref, list) else [],
                "pass": len(fails) == 0,
                "skipped": False,
                "failures": fails,
            }
        )
    return scorecard, flat_failures


def main() -> int:
    parser = argparse.ArgumentParser(description="Presales vessel smoke (chunked sessions by default).")
    parser.add_argument("--base-url", default=DEFAULT_BASE, help="Full URL to POST /query (e.g. http://127.0.0.1:8010/query)")
    parser.add_argument("--session-prefix", default="smoke-presales-vessel")
    parser.add_argument("--no-validate", action="store_true")
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help=f"Queries per fresh session (default {DEFAULT_CHUNK_SIZE}). Reduces cross-turn memory bleed.",
    )
    parser.add_argument(
        "--pause-between-chunks",
        type=float,
        default=DEFAULT_PAUSE_SEC,
        help=f"Seconds to sleep between chunks (default {DEFAULT_PAUSE_SEC}).",
    )
    parser.add_argument(
        "--single-session",
        action="store_true",
        help="Use one session for all queries (legacy; can amplify memory bugs).",
    )
    args = parser.parse_args()

    golden = [] if args.no_validate else _load_golden()
    run_tag = int(time.time())
    results: list[dict] = []
    session_ids_used: list[str] = []

    chunk_size = max(1, int(args.chunk_size))
    queries = list(QUERIES)

    if args.single_session:
        chunks: list[list[tuple[int, str]]] = []
        for idx, q in enumerate(queries, start=1):
            if not chunks:
                chunks.append([])
            chunks[0].append((idx, q))
    else:
        chunks = []
        batch: list[tuple[int, str]] = []
        for idx, q in enumerate(queries, start=1):
            batch.append((idx, q))
            if len(batch) >= chunk_size:
                chunks.append(batch)
                batch = []
        if batch:
            chunks.append(batch)

    for chunk_index, chunk in enumerate(chunks):
        session_id = f"{args.session_prefix}-{run_tag}-c{chunk_index}"
        session_ids_used.append(session_id)

        for turn_in_chunk, (global_index, query) in enumerate(chunk, start=1):
            started = time.time()
            row: dict = {
                "index": global_index,
                "chunk_index": chunk_index,
                "turn_in_chunk": turn_in_chunk,
                "session_id": session_id,
                "query": query,
            }
            try:
                r = requests.post(
                    args.base_url,
                    json={"query": query, "session_id": session_id},
                    timeout=120,
                )
                data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
                row.update(
                    {
                        "status_code": r.status_code,
                        "intent_key": data.get("intent_key"),
                        "slots": data.get("slots"),
                        "answer": data.get("answer"),
                        "clarification": data.get("clarification"),
                        "dynamic_sql_used": data.get("dynamic_sql_used"),
                        "error": data.get("error"),
                    }
                )
                row["report_issues"] = _heuristic_report_issues(
                    query, str(row.get("answer") or ""), row.get("slots") if isinstance(row.get("slots"), dict) else {}
                )
            except Exception as exc:
                row["error"] = str(exc)
                row["report_issues"] = []
            row["elapsed_sec"] = round(time.time() - started, 2)
            results.append(row)
            print(
                json.dumps(
                    {
                        "chunk": chunk_index,
                        "index": global_index,
                        "status": row.get("status_code"),
                        "intent": row.get("intent_key"),
                        "elapsed_sec": row["elapsed_sec"],
                        "error": row.get("error"),
                    },
                    ensure_ascii=True,
                ),
                flush=True,
            )

        if chunk_index < len(chunks) - 1 and args.pause_between_chunks > 0:
            time.sleep(float(args.pause_between_chunks))

    results.sort(key=lambda x: int(x.get("index") or 0))
    golden_scorecard: list[dict] = []
    validation_failures: list[str] = []
    if golden:
        golden_scorecard, validation_failures = _build_golden_scorecard(results, golden)
    for r in results:
        if r.get("error"):
            validation_failures.append(f"Turn {r.get('index')}: HTTP/request error: {r.get('error')}")
    checked = [x for x in golden_scorecard if x.get("skipped") is False]
    golden_pass_count = sum(1 for x in checked if x.get("pass") is True)
    golden_fail_count = sum(1 for x in checked if x.get("pass") is False)
    golden_total = len(checked)

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    summary = {
        "turns": len(results),
        "chunks": len(chunks),
        "chunk_size_config": chunk_size if not args.single_session else len(queries),
        "single_session": bool(args.single_session),
        "pause_between_chunks_sec": float(args.pause_between_chunks),
        "golden_total_checked": golden_total,
        "golden_pass_count": golden_pass_count,
        "golden_fail_count": golden_fail_count,
        "golden_pass_rate_pct": round(100.0 * golden_pass_count / golden_total, 1) if golden_total else None,
        "golden_check_failures": len(validation_failures),
        "turns_with_missing_data_phrase": sum(
            1 for r in results if any(x == "answer_reports_missing_data" for x in (r.get("report_issues") or []))
        ),
        "turns_with_possible_wrong_vessel": sum(
            1
            for r in results
            if any(
                str(x).startswith("answer_highlights_wrong_vessel")
                or str(x).startswith("slot_has_wrong_vessel")
                for x in (r.get("report_issues") or [])
            )
        ),
    }
    payload = {
        "base_url": args.base_url,
        "run_tag": run_tag,
        "session_ids": session_ids_used,
        "summary": summary,
        "golden_scorecard": golden_scorecard,
        "validation_failures": validation_failures,
        "results": results,
    }
    OUTPUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    md = [
        "# Presales vessel smoke review",
        "",
        f"- Base URL: `{args.base_url}`",
        f"- Run tag: `{run_tag}`",
        f"- Chunks: `{len(chunks)}` (fresh `session_id` per chunk; size `{chunk_size}`)" if not args.single_session else f"- Mode: **single session** (all `{len(queries)}` turns)",
        f"- Session IDs: `{session_ids_used}`",
        f"- Summary: `{json.dumps(summary, ensure_ascii=True)}`",
        "",
    ]
    if golden and golden_scorecard:
        md.extend(
            [
                "## Golden vs reference (substring checks)",
                "",
                f"- **Pass:** {golden_pass_count} / {golden_total} ({summary.get('golden_pass_rate_pct')}%)",
                f"- **Fail:** {golden_fail_count} / {golden_total}",
                "",
                "| Turn | Ref answer #(s) | Label | Pass |",
                "| --- | --- | --- | --- |",
            ]
        )
        for row in golden_scorecard:
            if row.get("skipped"):
                continue
            p = "yes" if row.get("pass") else "no"
            ref = row.get("maps_to_reference") or []
            ref_s = ", ".join(str(x) for x in ref) if ref else "—"
            md.append(f"| {row.get('turn')} | {ref_s} | {row.get('label', '')} | {p} |")
        md.append("")
        if validation_failures:
            md.extend(["### Failure detail", ""] + [f"- {x}" for x in validation_failures] + [""])
    elif args.no_validate:
        md.extend(["## Golden checks", "", "(disabled: `--no-validate`)", ""])
    else:
        md.extend(["## Golden checks", "", "(no `presales_vessel_golden_checks.json`)", ""])

    current_chunk = None
    for item in results:
        ch = item.get("chunk_index")
        if ch != current_chunk:
            current_chunk = ch
            md.extend(
                [
                    f"## Chunk {ch} — session `{item.get('session_id')}`",
                    "",
                ]
            )
        md.extend(
            [
                f"### Turn {item['index']} (in-chunk {item.get('turn_in_chunk')})",
                f"- Intent: `{item.get('intent_key')}`",
                f"- Status: `{item.get('status_code')}`",
                f"- Elapsed: `{item.get('elapsed_sec')}` s",
                f"- Report issues: `{item.get('report_issues')}`",
                f"- Slots: `{json.dumps(item.get('slots'), ensure_ascii=True)}`",
                "",
                "```text",
                str(item.get("answer") or item.get("error") or ""),
                "```",
                "",
            ]
        )

    OUTPUT_MD.write_text("\n".join(md), encoding="utf-8")
    print(f"Wrote {OUTPUT_JSON}", flush=True)
    print(f"Wrote {OUTPUT_MD}", flush=True)
    if golden_total:
        pct = summary.get("golden_pass_rate_pct")
        print(
            f"GOLDEN (substring checks vs presales_vessel_golden_checks.json): "
            f"{golden_pass_count}/{golden_total} passed ({pct}%)",
            flush=True,
        )
    if validation_failures:
        print(f"VALIDATION: {len(validation_failures)} failure(s)", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
