from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List


def _fmt_pct(v: float) -> str:
    return f"{v:.2f}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Build markdown report from scored eval output.")
    parser.add_argument("--scored", default="eval/results_scored.json")
    parser.add_argument("--output", default="eval/report.md")
    args = parser.parse_args()

    data = json.loads(Path(args.scored).read_text(encoding="utf-8"))
    meta = data.get("meta", {})
    qrows: List[Dict[str, Any]] = data.get("queries") or []

    lines: List[str] = []
    lines.append("# KAI Demo Validation Report")
    lines.append("")
    lines.append(f"- Overall score: **{_fmt_pct(float(meta.get('overall_score', 0.0)))}**")
    lines.append(f"- Target score: **{meta.get('target_score', 80)}**")
    lines.append(f"- Pass overall: **{meta.get('pass_overall', False)}**")
    lines.append(f"- Total queries: **{meta.get('counts', {}).get('total', len(qrows))}**")
    lines.append(f"- Failed requests: **{meta.get('counts', {}).get('failed_requests', 0)}**")
    lines.append("")

    lines.append("## Scores By Set")
    for k, v in sorted((data.get("set_scores") or {}).items()):
        lines.append(f"- {k}: **{_fmt_pct(float(v))}**")
    lines.append("")

    lines.append("## Scores By Difficulty")
    for k, v in sorted((data.get("difficulty_scores") or {}).items()):
        lines.append(f"- {k}: **{_fmt_pct(float(v))}**")
    lines.append("")

    lines.append("## Low-Scoring Queries (< 80)")
    lows = [q for q in qrows if float(q.get("score_total", 0.0)) < 80.0]
    if not lows:
        lines.append("- None")
    else:
        for q in sorted(lows, key=lambda x: float(x.get("score_total", 0.0))):
            issues = q.get("issues") or []
            issue_text = "; ".join(str(i) for i in issues[:3]) if issues else "no explicit issue logged"
            lines.append(f"- {q.get('id')} ({q.get('difficulty')}, {q.get('set')}): **{_fmt_pct(float(q.get('score_total', 0.0)))}** — {issue_text}")
    lines.append("")

    lines.append("## Per Query")
    lines.append("| ID | Difficulty | Set | Score | Intent (exp -> actual) | Dynamic |")
    lines.append("| --- | --- | --- | ---: | --- | --- |")
    for q in qrows:
        dynamic = "Y" if q.get("dynamic_detected") else "N"
        lines.append(
            f"| {q.get('id')} | {q.get('difficulty')} | {q.get('set')} | {_fmt_pct(float(q.get('score_total', 0.0)))} | "
            f"{q.get('intent_expected','')} -> {q.get('intent_actual','')} | {dynamic} |"
        )

    out_text = "\n".join(lines) + "\n"
    Path(args.output).write_text(out_text, encoding="utf-8")
    print(f"Wrote report: {args.output}")


if __name__ == "__main__":
    main()
