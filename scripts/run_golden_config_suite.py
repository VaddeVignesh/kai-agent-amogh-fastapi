from __future__ import annotations

import argparse
import json
import time
import uuid
from collections import Counter
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import requests


REPO = Path(__file__).resolve().parents[1]
DEFAULT_SUITE = REPO / "scripts" / "golden_config_suite.json"
DEFAULT_BASELINE = REPO / "scripts" / "golden_config_baseline.json"
DEFAULT_CURRENT = REPO / "scripts" / "golden_config_current.json"
DEFAULT_BASE_URL = "http://127.0.0.1:8010/query"


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _normalise_text(text: Any) -> str:
    return " ".join(str(text or "").split())


def _answer_tokens(text: str) -> set[str]:
    cleaned = "".join(ch.lower() if ch.isalnum() else " " for ch in text)
    return {tok for tok in cleaned.split() if len(tok) >= 3}


def _similarity(a: str, b: str) -> dict[str, float]:
    a_norm = _normalise_text(a)
    b_norm = _normalise_text(b)
    seq = round(SequenceMatcher(None, a_norm, b_norm).ratio(), 3) if a_norm or b_norm else 1.0
    a_tokens = _answer_tokens(a_norm)
    b_tokens = _answer_tokens(b_norm)
    if not a_tokens and not b_tokens:
        overlap = 1.0
    elif not a_tokens or not b_tokens:
        overlap = 0.0
    else:
        overlap = round(len(a_tokens & b_tokens) / len(a_tokens), 3)
    return {"sequence_ratio": seq, "baseline_token_overlap": overlap}


def _walk_values(value: Any):
    if isinstance(value, dict):
        for child in value.values():
            yield from _walk_values(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_values(child)
    else:
        yield value


def _contains_case_insensitive(answer: str, needle: str) -> bool:
    return str(needle).lower() in answer.lower()


def _trace_summary(trace: Any) -> dict[str, Any]:
    trace_list = trace if isinstance(trace, list) else []
    agents: list[str] = []
    phases: list[str] = []
    likely_paths: list[str] = []
    row_counts: dict[str, int] = {}
    sql_generated = False
    mongo_collections: list[str] = []

    for item in trace_list:
        if not isinstance(item, dict):
            continue
        phase = item.get("phase")
        if phase:
            phases.append(str(phase))
        agent = item.get("agent")
        if agent:
            agents.append(str(agent))
        likely_path = item.get("likely_path")
        if likely_path:
            likely_paths.append(str(likely_path))
        if item.get("sql") or item.get("generated_sql") or item.get("dynamic_sql"):
            sql_generated = True
        collection = item.get("collection")
        if collection:
            mongo_collections.append(str(collection))
        rows = item.get("rows")
        row_count = item.get("row_count")
        if agent:
            if isinstance(rows, list):
                row_counts[str(agent)] = max(row_counts.get(str(agent), 0), len(rows))
            elif isinstance(row_count, int):
                row_counts[str(agent)] = max(row_counts.get(str(agent), 0), row_count)

    return {
        "trace_steps_count": len(trace_list),
        "phases": list(dict.fromkeys(phases)),
        "agents_used": list(dict.fromkeys(agents)),
        "likely_paths": list(dict.fromkeys(likely_paths)),
        "row_counts": row_counts,
        "sql_generated": sql_generated,
        "mongo_collections": list(dict.fromkeys(mongo_collections)),
    }


def _bad_phrase_hits(answer: str, bad_phrases: list[str]) -> list[str]:
    low = answer.lower()
    return [phrase for phrase in bad_phrases if phrase.lower() in low]


def _capture_run_id(args: argparse.Namespace) -> str:
    configured = str(getattr(args, "run_id", "") or "").strip()
    if configured:
        return configured
    return f"{int(time.time())}-{uuid.uuid4().hex[:8]}"


def _session_id_for_run(base_session_id: str, run_id: str) -> str:
    # Keep role/user shape intact while making each capture immune to old Redis memory.
    return f"{base_session_id}:{run_id}"


def _record_from_response(
    *,
    item_id: str,
    category: str,
    query: str,
    session_id: str,
    turn_index: int,
    must_contain: list[str],
    response: requests.Response | None,
    data: dict[str, Any] | None,
    error: str | None,
    elapsed_sec: float,
    bad_phrases: list[str],
) -> dict[str, Any]:
    answer = str((data or {}).get("answer") or "")
    trace = (data or {}).get("trace")
    missing_must = [needle for needle in must_contain if not _contains_case_insensitive(answer, needle)]
    trace_summary = _trace_summary(trace)
    return {
        "id": item_id,
        "category": category,
        "turn_index": turn_index,
        "query": query,
        "session_id": session_id,
        "status_code": response.status_code if response is not None else None,
        "elapsed_sec": round(elapsed_sec, 2),
        "intent_key": (data or {}).get("intent_key"),
        "clarification": (data or {}).get("clarification"),
        "dynamic_sql_used": (data or {}).get("dynamic_sql_used"),
        "dynamic_sql_agents": (data or {}).get("dynamic_sql_agents") or [],
        "answer": answer,
        "answer_length": len(answer),
        "answer_preview": answer[:300],
        "must_contain": must_contain,
        "missing_must_contain": missing_must,
        "bad_phrase_hits": _bad_phrase_hits(answer, bad_phrases),
        "has_error": bool(error),
        "error": error,
        "trace_summary": trace_summary,
        "trace": trace,
    }


def _iter_suite_items(suite: dict[str, Any], *, category: str | None = None, ids: set[str] | None = None):
    for item in suite.get("single_turn", []):
        item_id = str(item["id"])
        item_category = str(item.get("category") or "single_turn")
        if category and item_category != category:
            continue
        if ids and item_id not in ids:
            continue
        yield {
            "id": item_id,
            "category": item_category,
            "session_id": f"customer:golden:{item_id.lower()}",
            "turn_index": 1,
            "query": item["query"],
            "must_contain": list(item.get("must_contain") or []),
        }

    for session in suite.get("multi_turn", []):
        session_id = f"customer:golden:{str(session['id']).lower()}"
        session_category = str(session.get("category") or "multi_turn")
        if category and session_category != category:
            continue
        queries = list(session.get("queries", []))
        include_through = len(queries)
        if ids:
            selected_indexes = [
                idx
                for idx, item in enumerate(queries, start=1)
                if str(item["id"]) in ids
            ]
            if not selected_indexes:
                continue
            # Include earlier turns so selected follow-up IDs run with the same context as a full capture.
            include_through = max(selected_indexes)

        for idx, item in enumerate(queries[:include_through], start=1):
            item_id = str(item["id"])
            if ids and item_id not in ids:
                include_as_context = True
            else:
                include_as_context = False
            yield {
                "id": item_id,
                "category": session_category,
                "session_id": session_id,
                "turn_index": idx,
                "query": item["query"],
                "must_contain": list(item.get("must_contain") or []),
                "context_only": include_as_context,
            }


def capture(args: argparse.Namespace) -> int:
    suite = _load_json(Path(args.suite))
    bad_phrases = list(suite.get("bad_phrases") or [])
    ids = set(args.ids.split(",")) if args.ids else None
    items = list(_iter_suite_items(suite, category=args.category, ids=ids))
    if args.limit:
        items = items[: args.limit]

    run_id = _capture_run_id(args)
    results: list[dict[str, Any]] = []
    for idx, item in enumerate(items, start=1):
        session_id = _session_id_for_run(item["session_id"], run_id)
        started = time.time()
        response = None
        data = None
        error = None
        try:
            response = requests.post(
                args.base_url,
                json={
                    "query": item["query"],
                    "session_id": session_id,
                    "request_id": str(uuid.uuid4()),
                },
                timeout=args.timeout,
            )
            try:
                data = response.json()
            except Exception:
                data = {}
                error = f"non_json_response: {response.text[:300]}"
        except Exception as exc:
            error = str(exc)

        record = _record_from_response(
            item_id=item["id"],
            category=item["category"],
            query=item["query"],
            session_id=session_id,
            turn_index=item["turn_index"],
            must_contain=item["must_contain"],
            response=response,
            data=data,
            error=error,
            elapsed_sec=time.time() - started,
            bad_phrases=bad_phrases,
        )
        results.append(record)
        print(
            json.dumps(
                {
                    "n": f"{idx}/{len(items)}",
                    "id": record["id"],
                    "status": record["status_code"],
                    "intent": record["intent_key"],
                    "agents": record["trace_summary"]["agents_used"],
                    "missing_must": record["missing_must_contain"],
                    "bad_hits": record["bad_phrase_hits"],
                    "elapsed": record["elapsed_sec"],
                },
                ensure_ascii=True,
            ),
            flush=True,
        )
        if args.sleep_sec:
            time.sleep(args.sleep_sec)

    payload = {
        "suite": suite.get("name"),
        "generated_at": int(time.time()),
        "base_url": args.base_url,
        "run_id": run_id,
        "session_isolation": "unique_run_suffix",
        "count": len(results),
        "results": results,
    }
    _write_json(Path(args.output), payload)
    print(f"WROTE {args.output} ({len(results)} results)")
    return 0


def _rows_by_id(doc: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(row.get("id")): row
        for row in doc.get("results", [])
        if isinstance(row, dict) and row.get("id")
    }


def compare(args: argparse.Namespace) -> int:
    baseline = _rows_by_id(_load_json(Path(args.baseline)))
    selected_ids = set(args.ids.split(",")) if args.ids else None
    if selected_ids:
        baseline = {item_id: row for item_id, row in baseline.items() if item_id in selected_ids}
    if args.category:
        baseline = {
            item_id: row
            for item_id, row in baseline.items()
            if str(row.get("category") or "") == args.category
        }
    if args.limit:
        baseline = dict(list(baseline.items())[: args.limit])
    current_doc_path = Path(args.output)
    capture(args)
    current = _rows_by_id(_load_json(current_doc_path))

    failures: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    counters: Counter[str] = Counter()

    for item_id, base in baseline.items():
        curr = current.get(item_id)
        if not curr:
            failures.append({"id": item_id, "reason": "missing_current_result"})
            continue

        checks = {
            "status": base.get("status_code") == curr.get("status_code"),
            "intent": base.get("intent_key") == curr.get("intent_key"),
            "dynamic_sql": (
                base.get("dynamic_sql_used") == curr.get("dynamic_sql_used")
                and base.get("dynamic_sql_agents") == curr.get("dynamic_sql_agents")
            ),
            "clarification": bool(base.get("clarification")) == bool(curr.get("clarification")),
            "missing_must": set(curr.get("missing_must_contain") or []).issubset(
                set(base.get("missing_must_contain") or [])
            ),
            "bad_phrases": set(curr.get("bad_phrase_hits") or []).issubset(
                set(base.get("bad_phrase_hits") or [])
            ),
            "answer_present": bool(str(curr.get("answer") or "").strip()),
        }
        for key, ok in checks.items():
            counters[f"{key}_{'ok' if ok else 'fail'}"] += 1

        if not all(checks.values()):
            failures.append({
                "id": item_id,
                "query": str(base.get("query") or "")[:120],
                "checks": checks,
                "status": f"{base.get('status_code')} -> {curr.get('status_code')}",
                "intent": f"{base.get('intent_key')} -> {curr.get('intent_key')}",
                "dynamic_sql": (
                    f"{base.get('dynamic_sql_used')}/{base.get('dynamic_sql_agents')} -> "
                    f"{curr.get('dynamic_sql_used')}/{curr.get('dynamic_sql_agents')}"
                ),
                "missing_must": curr.get("missing_must_contain"),
                "baseline_missing_must": base.get("missing_must_contain"),
                "bad_hits": curr.get("bad_phrase_hits"),
                "baseline_bad_hits": base.get("bad_phrase_hits"),
            })

        sim = _similarity(str(base.get("answer") or ""), str(curr.get("answer") or ""))
        base_len = int(base.get("answer_length") or 0)
        curr_len = int(curr.get("answer_length") or 0)
        if base_len > 200 and curr_len < base_len * 0.35:
            warnings.append({"id": item_id, "reason": "answer_much_shorter", "baseline_len": base_len, "current_len": curr_len})
        if sim["baseline_token_overlap"] < args.min_token_overlap and base_len > 100:
            warnings.append({"id": item_id, "reason": "low_token_overlap", **sim})

        base_agents = set((base.get("trace_summary") or {}).get("agents_used") or [])
        curr_agents = set((curr.get("trace_summary") or {}).get("agents_used") or [])
        if base_agents and curr_agents and base_agents != curr_agents:
            warnings.append({"id": item_id, "reason": "agents_changed", "baseline": sorted(base_agents), "current": sorted(curr_agents)})

    print("\nGOLDEN CONFIG COMPARISON")
    print(f"Baseline rows : {len(baseline)}")
    print(f"Current rows  : {len(current)}")
    print(f"Failures      : {len(failures)}")
    print(f"Warnings      : {len(warnings)}")
    print(f"Status parity : {counters['status_ok']}/{len(baseline)}")
    print(f"Intent parity : {counters['intent_ok']}/{len(baseline)}")
    print(f"SQL parity    : {counters['dynamic_sql_ok']}/{len(baseline)}")

    if failures:
        print("\nFAILURES:")
        for fail in failures[:50]:
            print(json.dumps(fail, ensure_ascii=True))
    if warnings:
        print("\nWARNINGS:")
        for warn in warnings[:50]:
            print(json.dumps(warn, ensure_ascii=True))

    report_path = Path(args.report)
    _write_json(report_path, {"failures": failures, "warnings": warnings, "counters": dict(counters)})
    print(f"WROTE {report_path}")
    return 1 if failures else 0


def list_items(args: argparse.Namespace) -> int:
    suite = _load_json(Path(args.suite))
    ids = set(args.ids.split(",")) if args.ids else None
    items = list(_iter_suite_items(suite, category=args.category, ids=ids))
    if args.limit:
        items = items[: args.limit]
    for item in items:
        print(f"{item['id']} [{item['category']}] turn={item['turn_index']} :: {item['query']}")
    print(f"TOTAL {len(items)}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Capture and compare golden query outputs for config refactor phases.")
    parser.add_argument("mode", choices=["list", "capture", "compare"])
    parser.add_argument("--suite", default=str(DEFAULT_SUITE))
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--output", default=str(DEFAULT_CURRENT))
    parser.add_argument("--baseline", default=str(DEFAULT_BASELINE))
    parser.add_argument("--report", default=str(REPO / "scripts" / "golden_config_compare_report.json"))
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--sleep-sec", type=float, default=0.0)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--category", default="")
    parser.add_argument("--ids", default="", help="Comma-separated query IDs to run.")
    parser.add_argument("--min-token-overlap", type=float, default=0.35)
    parser.add_argument("--run-id", default="", help="Optional suffix for isolated golden sessions.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    args.category = args.category or None
    if args.mode == "list":
        return list_items(args)
    if args.mode == "capture":
        return capture(args)
    return compare(args)


if __name__ == "__main__":
    raise SystemExit(main())
