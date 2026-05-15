from __future__ import annotations

import argparse
import json
import re
import time
import urllib.request
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import urlparse, urlunparse


def _validate_url(url: str) -> str:
    try:
        if "/../" in url or re.search(r"/%2e%2e/", url, re.IGNORECASE):
            raise ValueError("Invalid path")
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            raise ValueError("Invalid protocol")
        if not parsed.hostname:
            raise ValueError("Invalid host")
        allowed_domains = ["example.com"]  # add your allowed domains here
        if parsed.hostname.lower() not in allowed_domains:
            raise ValueError("Invalid host")
        return urlunparse(parsed)
    except Exception:
        raise ValueError("Invalid URL")


def _post_json(url: str, payload: Dict[str, Any], timeout_sec: int) -> Dict[str, Any]:
    validated_url = _validate_url(url)
    req = urllib.request.Request(
        url=validated_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw) if raw else {}


def _dynamic_used(resp: Dict[str, Any]) -> bool:
    if bool(resp.get("dynamic_sql_used")):
        return True
    trace = resp.get("trace")
    if not isinstance(trace, list):
        return False
    for t in trace:
        if isinstance(t, dict) and t.get("operation") == "dynamic_sql" and t.get("ok") is True:
            return True
    return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Run KAI benchmark queries against /query API.")
    parser.add_argument("--api-url", default="http://127.0.0.1:8000/query")
    parser.add_argument("--benchmark", default="eval/benchmark_queries.json")
    parser.add_argument("--output", default="eval/results_raw.json")
    parser.add_argument("--timeout-sec", type=int, default=120)
    parser.add_argument("--sleep-sec", type=float, default=0.2)
    parser.add_argument("--session-prefix", default="eval")
    args = parser.parse_args()

    bench_path = Path(args.benchmark)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    benchmark = json.loads(bench_path.read_text(encoding="utf-8"))
    queries: List[Dict[str, Any]] = benchmark.get("queries", [])

    run_started = time.time()
    out: Dict[str, Any] = {
        "meta": {
            "run_started_ts": run_started,
            "api_url": args.api_url,
            "benchmark_file": str(bench_path),
            "query_count": len(queries),
        },
        "results": [],
    }

    for idx, q in enumerate(queries, start=1):
        qid = str(q.get("id") or f"Q{idx:02d}")
        query_text = str(q.get("query") or "").strip()
        session_id = f"{args.session_prefix}-{qid.lower()}"
        started = time.time()
        payload = {"query": query_text, "session_id": session_id}
        try:
            resp = _post_json(args.api_url, payload, timeout_sec=args.timeout_sec)
            elapsed_ms = int((time.time() - started) * 1000)
            out["results"].append(
                {
                    "id": qid,
                    "query": query_text,
                    "session_id": session_id,
                    "ok": True,
                    "elapsed_ms": elapsed_ms,
                    "intent_key": resp.get("intent_key"),
                    "slots": resp.get("slots"),
                    "answer": resp.get("answer"),
                    "clarification": resp.get("clarification"),
                    "trace": resp.get("trace"),
                    "dynamic_sql_used_detected": _dynamic_used(resp),
                    "dynamic_sql_used_flag": bool(resp.get("dynamic_sql_used")),
                    "dynamic_sql_agents": resp.get("dynamic_sql_agents"),
                }
            )
            print(f"[{idx:02d}/{len(queries):02d}] {qid} OK in {elapsed_ms} ms")
        except Exception as exc:
            elapsed_ms = int((time.time() - started) * 1000)
            out["results"].append(
                {
                    "id": qid,
                    "query": query_text,
                    "session_id": session_id,
                    "ok": False,
                    "elapsed_ms": elapsed_ms,
                    "error": str(exc),
                }
            )
            print(f"[{idx:02d}/{len(queries):02d}] {qid} ERROR in {elapsed_ms} ms -> {exc}")
        time.sleep(max(0.0, args.sleep_sec))

    out["meta"]["run_finished_ts"] = time.time()
    out["meta"]["elapsed_sec"] = round(out["meta"]["run_finished_ts"] - run_started, 3)
    out_path.write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nWrote raw run results to: {out_path}")


if __name__ == "__main__":
    main()
