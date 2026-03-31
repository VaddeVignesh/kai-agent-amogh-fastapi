from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple


NUM_RE = re.compile(r"[-+]?\d[\d,]*\.?\d*")


def _to_text(x: Any) -> str:
    return str(x or "").strip()


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", _to_text(s).lower()).strip()


def _extract_numbers(text: str) -> List[float]:
    out: List[float] = []
    for m in NUM_RE.findall(text):
        try:
            out.append(float(m.replace(",", "")))
        except Exception:
            continue
    return out


def _contains_any(text: str, terms: List[str]) -> Tuple[int, int]:
    if not terms:
        return (0, 0)
    ntext = _norm(text)
    hits = sum(1 for t in terms if _norm(t) in ntext)
    return hits, len(terms)


def _contains_all(text: str, terms: List[str]) -> Tuple[int, int]:
    if not terms:
        return (0, 0)
    ntext = _norm(text)
    hits = sum(1 for t in terms if _norm(t) in ntext)
    return hits, len(terms)


def _numeric_score(answer: str, numeric_targets: List[Dict[str, Any]]) -> Tuple[float, List[str]]:
    if not numeric_targets:
        return 1.0, []
    nums = _extract_numbers(answer)
    if not nums:
        return 0.0, [f"missing numeric value for {t.get('label','target')}" for t in numeric_targets]

    issues: List[str] = []
    hit = 0
    for target in numeric_targets:
        label = str(target.get("label") or "target")
        val = float(target.get("value"))
        tol = float(target.get("tolerance_pct", 0.02))
        lo = val * (1 - tol)
        hi = val * (1 + tol)
        ok = any(lo <= n <= hi for n in nums)
        if ok:
            hit += 1
        else:
            issues.append(f"{label} not within tolerance [{lo:.2f}, {hi:.2f}]")
    return hit / max(1, len(numeric_targets)), issues


def _intent_score(actual_intent: str, expected_intent: str) -> float:
    ai = _to_text(actual_intent)
    ei = _to_text(expected_intent)
    if not ei:
        return 1.0
    if ai == ei:
        return 1.0
    if ai.startswith(ei.split(".")[0]):
        return 0.6
    return 0.0


def score_one(query_cfg: Dict[str, Any], run_item: Dict[str, Any]) -> Dict[str, Any]:
    answer = _to_text(run_item.get("answer"))
    ok = bool(run_item.get("ok"))
    actual_intent = _to_text(run_item.get("intent_key"))

    required_terms = [str(x) for x in (query_cfg.get("required_terms") or [])]
    any_terms = [str(x) for x in (query_cfg.get("any_of_terms") or [])]
    forbidden_terms = [str(x) for x in (query_cfg.get("forbidden_terms") or [])]
    numeric_targets = list(query_cfg.get("numeric_targets") or [])
    require_dynamic = bool(query_cfg.get("require_dynamic_sql"))
    expected_intent = _to_text(query_cfg.get("intent_family"))

    facts_hits, facts_total = _contains_all(answer, required_terms)
    any_hits, any_total = _contains_any(answer, any_terms)
    bad_hits, bad_total = _contains_any(answer, forbidden_terms)
    num_ratio, num_issues = _numeric_score(answer, numeric_targets)
    intent_ratio = _intent_score(actual_intent, expected_intent)

    dynamic_ok = bool(run_item.get("dynamic_sql_used_detected")) if require_dynamic else True

    # Weighted components
    factual = 40.0 * ((facts_hits / facts_total) if facts_total else intent_ratio)
    relevance = 20.0 * ((any_hits / any_total) if any_total else 1.0)
    completeness = 20.0 * ((facts_hits / facts_total) if facts_total else 1.0)
    no_hallucination = 15.0 * (1.0 - ((bad_hits / bad_total) if bad_total else 0.0))
    execution = 5.0 * (1.0 if dynamic_ok else 0.0)

    # Blend numeric rigor into factual dimension when numeric targets exist.
    if numeric_targets:
        factual = 20.0 * ((facts_hits / facts_total) if facts_total else 1.0) + 20.0 * num_ratio

    # If request failed, hard cap
    if not ok:
        factual = 0.0
        relevance = 0.0
        completeness = 0.0
        no_hallucination = 0.0
        execution = 0.0

    total = factual + relevance + completeness + no_hallucination + execution
    total = max(0.0, min(100.0, total))

    issues: List[str] = []
    if not ok:
        issues.append(_to_text(run_item.get("error")) or "request failed")
    if facts_total and facts_hits < facts_total:
        issues.append(f"missing required terms: {facts_hits}/{facts_total}")
    if any_total and any_hits == 0:
        issues.append("missing thematic relevance terms")
    if bad_hits > 0:
        issues.append("contains forbidden fallback language")
    if numeric_targets and num_ratio < 1.0:
        issues.extend(num_issues)
    if require_dynamic and not dynamic_ok:
        issues.append("expected dynamic_sql but none detected")
    if expected_intent and intent_ratio == 0.0:
        issues.append(f"intent mismatch: expected ~{expected_intent}, got {actual_intent or 'none'}")

    return {
        "id": query_cfg.get("id"),
        "set": query_cfg.get("set"),
        "difficulty": query_cfg.get("difficulty"),
        "query": query_cfg.get("query"),
        "score_total": round(total, 2),
        "score_breakdown": {
            "factual": round(factual, 2),
            "relevance": round(relevance, 2),
            "completeness": round(completeness, 2),
            "no_hallucination": round(no_hallucination, 2),
            "execution": round(execution, 2),
        },
        "ok": ok,
        "intent_expected": expected_intent,
        "intent_actual": actual_intent,
        "dynamic_required": require_dynamic,
        "dynamic_detected": bool(run_item.get("dynamic_sql_used_detected")),
        "issues": issues,
    }


def _avg(items: List[float]) -> float:
    return (sum(items) / len(items)) if items else 0.0


def main() -> None:
    parser = argparse.ArgumentParser(description="Score raw benchmark run against golden expectations.")
    parser.add_argument("--benchmark", default="eval/benchmark_queries.json")
    parser.add_argument("--raw", default="eval/results_raw.json")
    parser.add_argument("--output", default="eval/results_scored.json")
    args = parser.parse_args()

    bench = json.loads(Path(args.benchmark).read_text(encoding="utf-8"))
    raw = json.loads(Path(args.raw).read_text(encoding="utf-8"))

    query_cfg = {str(q.get("id")): q for q in (bench.get("queries") or [])}
    run_map = {str(r.get("id")): r for r in (raw.get("results") or [])}

    scored: List[Dict[str, Any]] = []
    for qid, cfg in query_cfg.items():
        run_item = run_map.get(qid, {"id": qid, "ok": False, "error": "missing run result"})
        scored.append(score_one(cfg, run_item))

    overall = _avg([s["score_total"] for s in scored])
    by_set: Dict[str, List[float]] = {}
    by_diff: Dict[str, List[float]] = {}
    for s in scored:
        by_set.setdefault(str(s.get("set") or "unknown"), []).append(float(s["score_total"]))
        by_diff.setdefault(str(s.get("difficulty") or "unknown"), []).append(float(s["score_total"]))

    out = {
        "meta": {
            "target_score": bench.get("meta", {}).get("target_score", 80),
            "overall_score": round(overall, 2),
            "pass_overall": overall >= float(bench.get("meta", {}).get("target_score", 80)),
            "counts": {"total": len(scored), "failed_requests": sum(1 for s in scored if not s.get("ok"))},
        },
        "set_scores": {k: round(_avg(v), 2) for k, v in by_set.items()},
        "difficulty_scores": {k: round(_avg(v), 2) for k, v in by_diff.items()},
        "queries": sorted(scored, key=lambda x: x.get("id", "")),
    }

    Path(args.output).write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Overall score: {out['meta']['overall_score']:.2f}")
    print(f"Pass target:   {out['meta']['pass_overall']}")
    print(f"Wrote scored results to: {args.output}")


if __name__ == "__main__":
    main()
