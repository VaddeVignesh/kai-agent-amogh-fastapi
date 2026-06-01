from __future__ import annotations

from typing import Any, Dict

from app.orchestration.handlers.router_context import (
    bind_graph_router_globals,
    build_router_context,
)


def handle_run_single(router: Any, state: Dict[str, Any]) -> Dict[str, Any]:
    bind_graph_router_globals(globals())
    ctx = build_router_context(router)
    self = router

    """Standard execution flow using intent registry."""
    intent_key = state.get("intent_key") or state.get("plan", {}).get("intent_key", "out_of_scope")
    cfg = INTENT_REGISTRY.get(intent_key, {})
    session_context = state.get("session_ctx") or {}
    slots = self._merge_slots(
        intent_key,
        session_context,
        state.get("slots") or {},
        state.get("user_input") or state.get("raw_user_input") or "",
    )
    user_input = state.get("user_input") or ""
    plan_type = str(state.get("plan_type") or (state.get("plan") or {}).get("plan_type") or "single").lower()

    _dprint(f"\nSingle execution: {intent_key}")

    if intent_key == "out_of_scope":
        if state.get("answer"):
            return state
        from app.config.prompt_rules_loader import get_out_of_scope_response_template

        if is_read_only_mutation_request(user_input):
            state["answer"] = get_out_of_scope_response_template("read_only_mutation").strip()
        return state

    def _registry_sql_from_result(res: Any) -> str | None:
        if not isinstance(res, dict):
            return None
        if str(res.get("mode") or "").strip().lower() != "registry_sql":
            return None
        qk = res.get("query_key")
        if not isinstance(qk, str) or not qk.strip():
            return None
        spec = SQL_REGISTRY.get(qk)
        if not spec:
            return None
        try:
            return str(spec.sql).strip()
        except Exception:
            return None


    # =========================================================
    # Result-set follow-ups (deterministic, uses Redis memory)
    # =========================================================
    if intent_key == "followup.result_set":
        rs = (session_context or {}).get("last_result_set") if isinstance(session_context, dict) else None
        rows = (rs or {}).get("rows") if isinstance(rs, dict) else None
        rows = rows if isinstance(rows, list) else []
        # Optional in-memory refinement from structured follow-up before action handlers.
        si_fu = (session_context or {}).get("_structured_intent") if isinstance(session_context, dict) else None
        if isinstance(si_fu, dict) and si_fu.get("confidence") in ("high", "medium"):
            scp = str(si_fu.get("scope") or "").strip().lower()
            if scp in ("follow_up", "followup"):
                rs_slots = (session_context or {}).get("last_focus_slots") if isinstance(session_context, dict) else {}
                if not isinstance(rs_slots, dict):
                    rs_slots = {}
                fr = resolve_followup(si_fu, {"last_result_rows": list(rows), "resolved_slots": rs_slots})
                if fr and isinstance(fr.get("resolved_rows"), list):
                    rows = fr["resolved_rows"]
                    logger.info(
                        "CACHE_RESOLVED|phase=4a|follow_up_action=%s|row_count=%s",
                        si_fu.get("follow_up_action"),
                        len(rows),
                    )
        action = (slots.get("action") or "").strip().lower()
        ul = (user_input or "").lower()

        def _num(x: Any) -> float | None:
            try:
                if x is None:
                    return None
                if isinstance(x, (int, float)):
                    return float(x)
                s = str(x).replace(",", "").strip()
                return float(s)
            except Exception:
                return None

        def _extract_ports(val: Any) -> list[str]:
            if val in (None, "", [], {}):
                return []
            if isinstance(val, list):
                out = []
                for p in val[:30]:
                    if p in (None, "", [], {}):
                        continue
                    if isinstance(p, dict):
                        s = str(
                            p.get("port_name")
                            or p.get("port")
                            or p.get("name")
                            or p.get("portName")
                            or ""
                        ).strip()
                    else:
                        s = str(p).strip()
                    if s:
                        out.append(s)
                return out
            s = str(val).strip()
            if not s:
                return []
            parts = [x.strip() for x in re.split(r"\s*[|,;]\s*", s) if x.strip()]
            cleaned = []
            for p in parts[:30]:
                pl = p.lower()
                if "grade_name" in pl and "none" in pl:
                    continue
                cleaned.append(p)
            return cleaned

        def _extract_grades(val: Any) -> list[str]:
            if val in (None, "", [], {}):
                return []
            if isinstance(val, list):
                out = []
                for g in val[:30]:
                    if g in (None, "", [], {}):
                        continue
                    if isinstance(g, dict):
                        s = str(
                            g.get("grade")
                            or g.get("cargo_grade")
                            or g.get("grade_name")
                            or g.get("name")
                            or ""
                        ).strip()
                    else:
                        s = str(g).strip()
                        sl = s.lower()
                        if "grade_name" in sl and "none" in sl:
                            s = ""
                    if s:
                        out.append(s)
                return out
            s = str(val).strip()
            if not s:
                return []
            parts = [x.strip() for x in re.split(r"\s*[|,;]\s*", s) if x.strip()]
            return parts[:30]

        def _format_remarks_value(val: Any) -> str:
            if val in (None, "", [], {}):
                return _result_set_text("no_remarks_recorded")
            items = val if isinstance(val, list) else [val]
            lines: list[str] = []
            for item in items[:10]:
                if item in (None, "", [], {}):
                    continue
                if isinstance(item, dict):
                    date = item.get("modifiedDate") or item.get("date") or item.get("createdAt")
                    who = item.get("modifiedByFull") or item.get("modifiedBy") or item.get("user")
                    text = item.get("remark") or item.get("text") or item.get("note")
                else:
                    parsed = None
                    text_raw = str(item).strip()
                    if text_raw.startswith("{") and text_raw.endswith("}"):
                        try:
                            parsed = ast.literal_eval(text_raw)
                        except Exception:
                            parsed = None
                    if isinstance(parsed, dict):
                        date = parsed.get("modifiedDate") or parsed.get("date") or parsed.get("createdAt")
                        who = parsed.get("modifiedByFull") or parsed.get("modifiedBy") or parsed.get("user")
                        text = parsed.get("remark") or parsed.get("text") or parsed.get("note")
                    else:
                        date = None
                        who = None
                        text = text_raw
                parts = [str(x).strip() for x in (date, who, text) if x not in (None, "", [], {})]
                line = " | ".join(parts).strip()
                if line:
                    lines.append(f"- {line}")
            return "\n".join(lines) if lines else _result_set_text("no_remarks_recorded")

        def _persist_result_set(new_rows: list[dict], *, primary_metric: str | None = None) -> None:
            try:
                prev_meta = (rs or {}).get("meta") if isinstance(rs, dict) and isinstance((rs or {}).get("meta"), dict) else {}
                meta = dict(prev_meta)
                if primary_metric:
                    latest_source_intent = (rs or {}).get("source_intent") if isinstance(rs, dict) else None
                    latest_rs = {
                        "source_intent": latest_source_intent,
                        "rows": new_rows[:50],
                        "meta": meta,
                    }
                    meta["primary_metric"] = primary_metric
                else:
                    latest_source_intent = (rs or {}).get("source_intent") if isinstance(rs, dict) else None
                    latest_rs = {
                        "source_intent": latest_source_intent,
                        "rows": new_rows[:50],
                        "meta": meta,
                    }
                self.redis.save_session(
                    state["session_id"],
                    {
                        **(session_context or {}),
                        "last_result_set": latest_rs,
                        "last_user_input": user_input,
                    },
                )
                sctx = state.get("session_ctx") if isinstance(state.get("session_ctx"), dict) else {}
                state["session_ctx"] = {**(sctx or {}), "last_result_set": latest_rs, "last_user_input": user_input}
            except Exception:
                pass

        def _persist_focus(row: dict | None) -> None:
            if not isinstance(row, dict):
                return
            focus_slots = self._row_focus_slots(row)
            if not focus_slots:
                return
            try:
                self.redis.save_session(
                    state["session_id"],
                    {
                        **(session_context or {}),
                        "last_focus_slots": focus_slots,
                        "last_user_input": user_input,
                    },
                )
                sctx = state.get("session_ctx") if isinstance(state.get("session_ctx"), dict) else {}
                state["session_ctx"] = {**(sctx or {}), "last_focus_slots": focus_slots, "last_user_input": user_input}
            except Exception:
                pass

        def _format_metric_value(metric: str, row: dict) -> str:
            metric = str(metric or "").strip().lower()
            if metric in ("vessel_name", "port_name"):
                value = row.get(metric)
                return str(value).strip() if value not in (None, "", [], {}) else _result_set_text("not_available")
            if metric == "port_calls":
                value = self._result_row_metric_value(row, metric)
                return f"{int(value):,}" if value is not None else _result_set_text("not_available")
            value = self._result_row_metric_value(row, metric)
            if value is None:
                return _result_set_text("not_available")
            return f"{value:,.2f}"

        def _selected_row(selector: str | None = None) -> dict | None:
            selector = str(selector or "").strip().lower()
            if selector == "first":
                return rows[0] if rows else None
            if selector == "last":
                return rows[-1] if rows else None
            focus_slots = (session_context or {}).get("last_focus_slots") if isinstance(session_context, dict) else None
            if isinstance(focus_slots, dict) and focus_slots:
                for r in rows:
                    if not isinstance(r, dict):
                        continue
                    if focus_slots.get("voyage_number") and str(r.get("voyage_number")) == str(focus_slots.get("voyage_number")):
                        return r
                    if focus_slots.get("voyage_id") and str(r.get("voyage_id")) == str(focus_slots.get("voyage_id")):
                        return r
                    if focus_slots.get("vessel_name") and str(r.get("vessel_name")) == str(focus_slots.get("vessel_name")):
                        return r
                    if focus_slots.get("port_name") and str(r.get("port_name")) == str(focus_slots.get("port_name")):
                        return r
                    if focus_slots.get("module_type") and str(r.get("module_type")) == str(focus_slots.get("module_type")):
                        return r
                    if focus_slots.get("time_bucket") and str(r.get("time_bucket")) == str(focus_slots.get("time_bucket")):
                        return r
                    if focus_slots.get("month") and str(r.get("month")) == str(focus_slots.get("month")):
                        return r
            prev_q = str((session_context or {}).get("last_user_input") or "").strip().lower()
            if prev_q and rows:
                metric = "pnl"
                if "expense ratio" in prev_q or "expense-to-revenue ratio" in prev_q or "expense to revenue ratio" in prev_q:
                    metric = "expense_to_revenue_ratio"
                elif "avg revenue" in prev_q or "average revenue" in prev_q:
                    metric = "avg_revenue"
                elif "avg pnl" in prev_q or "average pnl" in prev_q:
                    metric = "avg_pnl"
                elif "voyage count" in prev_q or re.search(r"\bcount\b", prev_q):
                    metric = "voyage_count"
                elif "pnl variance" in prev_q or "changed the most" in prev_q:
                    metric = "pnl_variance" if any(self._result_row_metric_value(r, "pnl_variance") is not None for r in rows if isinstance(r, dict)) else "variance"
                elif "tce variance" in prev_q:
                    metric = "tce_variance"
                elif "revenue" in prev_q:
                    metric = "revenue"
                elif "expense" in prev_q or "cost" in prev_q:
                    metric = "total_expense"
                scored = []
                for r in rows:
                    if not isinstance(r, dict):
                        continue
                    val = self._result_row_metric_value(r, metric)
                    if val is None:
                        continue
                    scored.append((val, r))
                if scored:
                    high_is_worse = metric in {
                        "expense_to_revenue_ratio",
                        "total_expense",
                        "avg_total_expense",
                        "offhire_days",
                        "avg_offhire_days",
                        "pnl_variance",
                        "tce_variance",
                        "variance",
                        "variance_diff",
                    }
                    if "worst" in prev_q:
                        return (max(scored, key=lambda t: t[0]) if high_is_worse else min(scored, key=lambda t: t[0]))[1]
                    if "best" in prev_q:
                        return (min(scored, key=lambda t: t[0]) if high_is_worse else max(scored, key=lambda t: t[0]))[1]
                    if any(k in prev_q for k in ("lowest", "least", "bottom")):
                        return min(scored, key=lambda t: t[0])[1]
                    if any(k in prev_q for k in ("highest", "most", "top")):
                        return max(scored, key=lambda t: t[0])[1]
            return rows[0] if rows else None

        # Generic refinements: top/bottom N by a metric
        if action in ("top_n", "bottom_n"):
            metric = str(slots.get("metric") or "pnl")
            try:
                n = int(slots.get("n") or 5)
            except Exception:
                n = 5
            n = max(1, min(n, 20))

            scored = []
            for r in rows:
                if not isinstance(r, dict):
                    continue
                label = self._result_row_label(r)
                if label in (None, "", "N/A"):
                    continue
                val = self._result_row_metric_value(r, metric)
                if val is None and metric == "port_calls":
                    ports = _extract_ports(r.get("key_ports"))
                    val = float(len(ports)) if ports else None
                if val is None:
                    continue
                scored.append((val, r))

            if not scored:
                state["answer"] = f"I couldn’t rank the previous result set by **{metric}**. Please ask the base query again with that metric."
                return state

            scored = sorted(scored, key=lambda t: t[0], reverse=(action == "top_n"))
            picked = [r for _, r in scored[:n]]
            _persist_result_set(picked, primary_metric=metric)

            lines = [f"### {'Top' if action == 'top_n' else 'Bottom'} {len(picked)} by {metric} (from previous results)"]
            for i, r in enumerate(picked, start=1):
                v = self._result_row_metric_value(r, metric)
                if v is None and metric == "port_calls":
                    v = float(len(_extract_ports(r.get("key_ports"))))
                label = self._result_row_label(r)
                lines.append(f"- {i}. **{label}**: {v:,.2f}" if isinstance(v, float) else f"- {i}. **{label}**")
            _persist_focus(picked[0] if picked else None)
            state["answer"] = "\n".join(lines)
            return state

        # Per-row remarks projection from current result set.
        if action == "remarks_each":
            if not rows:
                state["answer"] = _result_set_text("no_previous_result_set_read_remarks")
                return state
            lines = [_result_set_text("remarks_each_heading")]
            for r in rows[:20]:
                if not isinstance(r, dict):
                    continue
                vnum = r.get("voyage_number") or "N/A"
                rem = r.get("remarks")
                txt = _format_remarks_value(rem)
                lines.append(f"- **Voyage {vnum}**:")
                lines.append(txt)
            state["answer"] = "\n".join(lines)
            return state

        if action == "filter_has_field":
            field = str(slots.get("field") or "").strip()
            filtered = [r for r in rows if isinstance(r, dict) and r.get(field) not in (None, "", [], {})]
            if not filtered:
                state["answer"] = _result_set_text("no_rows_field_populated", field=field)
                return state
            _persist_result_set(filtered)
            _persist_focus(filtered[0])
            lines = [f"### Rows with {field.replace('_', ' ')} (from previous results)"]
            for r in filtered[:20]:
                label = self._result_row_label(r)
                val = r.get(field)
                preview = str(val).strip() if val not in (None, "", [], {}) else _result_set_text("not_available")
                lines.append(f"- **{label}**: {preview}")
            state["answer"] = "\n".join(lines)
            return state

        if action == "project_selected_field":
            field = str(slots.get("field") or "").strip()
            chosen = _selected_row(str(slots.get("selector") or "last_focus"))
            if not isinstance(chosen, dict):
                state["answer"] = _result_set_text("selected_row_not_identified")
                return state
            _persist_focus(chosen)
            label = self._result_row_label(chosen)
            if field == "remarks":
                rem = chosen.get("remarks")
                state["answer"] = (
                    _result_set_text("remarks_for_label_heading", label=label) + "\n"
                    + _format_remarks_value(rem)
                )
                return state
            if field == "cargo_grades":
                grades = _extract_grades(
                    chosen.get("cargo_grades")
                    if chosen.get("cargo_grades") not in (None, "", [], {})
                    else chosen.get("most_common_grade")
                )
                state["answer"] = (
                    _result_set_text("cargo_grades_for_label_heading", label=label) + "\n"
                    + (", ".join(grades[:20]) if grades else _result_set_text("no_cargo_grades_previous_result_set"))
                )
                return state
            if field == "key_ports":
                ports = _extract_ports(chosen.get("key_ports"))
                state["answer"] = (
                    _result_set_text("key_ports_for_label_heading", label=label) + "\n"
                    + (", ".join(ports[:20]) if ports else _result_set_text("no_key_ports_previous_result_set"))
                )
                return state
            state["answer"] = _result_set_text("cannot_project_field", field=field)
            return state

        if action == "project_selected_metric":
            metric = str(slots.get("metric") or "revenue").strip().lower()
            chosen = _selected_row(str(slots.get("selector") or "last_focus"))
            if not isinstance(chosen, dict):
                state["answer"] = _result_set_text("selected_row_not_identified")
                return state
            _persist_result_set(rows, primary_metric=metric)
            _persist_focus(chosen)
            label = self._result_row_label(chosen)
            pretty_name = metric.replace("_", " ")
            state["answer"] = f"### {pretty_name.title()} for {label}\n{_format_metric_value(metric, chosen)}"
            return state

        if action == "project_extreme_field":
            field = str(slots.get("field") or "").strip()
            metric = str(slots.get("metric") or "pnl").strip().lower()
            extreme = str(slots.get("extreme") or "high").strip().lower()
            scored = []
            for r in rows:
                if not isinstance(r, dict):
                    continue
                val = self._result_row_metric_value(r, metric)
                if val is None:
                    continue
                scored.append((val, r))
            if not scored:
                state["answer"] = "I don’t have a previous result set to project from. Please run the base query again."
                return state
            chosen = min(scored, key=lambda t: t[0])[1] if extreme == "low" else max(scored, key=lambda t: t[0])[1]
            _persist_result_set(rows, primary_metric=metric)
            _persist_focus(chosen)
            label = self._result_row_label(chosen)
            if field == "remarks":
                rem = chosen.get("remarks")
                state["answer"] = (
                    _result_set_text("remarks_for_label_heading", label=label) + "\n"
                    + _format_remarks_value(rem)
                )
                return state
            if field == "cargo_grades":
                grades = _extract_grades(
                    chosen.get("cargo_grades")
                    if chosen.get("cargo_grades") not in (None, "", [], {})
                    else chosen.get("most_common_grade")
                )
                state["answer"] = (
                    _result_set_text("cargo_grades_for_label_heading", label=label) + "\n"
                    + (", ".join(grades[:20]) if grades else _result_set_text("no_cargo_grades_previous_result_set"))
                )
                return state
            if field == "key_ports":
                ports = _extract_ports(chosen.get("key_ports"))
                state["answer"] = (
                    _result_set_text("key_ports_for_label_heading", label=label) + "\n"
                    + (", ".join(ports[:20]) if ports else _result_set_text("no_key_ports_previous_result_set"))
                )
                return state
            state["answer"] = _result_set_text("cannot_project_selected_field", field=field)
            return state

        # Per-row ports projection from current result set.
        if action == "list_ports":
            if not rows:
                state["answer"] = _result_set_text("no_previous_result_set_list_ports")
                return state
            lines = [_result_set_text("key_ports_current_result_set_heading")]
            for r in rows[:20]:
                if not isinstance(r, dict):
                    continue
                vnum = r.get("voyage_number") or "N/A"
                ports = _extract_ports(r.get("key_ports"))
                if not ports:
                    lines.append(f"- **Voyage {vnum}**: {_result_set_text('no_key_ports_for_row')}")
                else:
                    preview = ", ".join(ports[:12])
                    suffix = " (+more)" if len(ports) > 12 else ""
                    lines.append(f"- **Voyage {vnum}**: {preview}{suffix}")
            state["answer"] = "\n".join(lines)
            return state

        if action == "list_cargo_grades":
            if not rows:
                state["answer"] = _result_set_text("no_previous_result_set_list_cargo_grades")
                return state
            lines = [_result_set_text("cargo_grades_current_result_set_heading")]
            for r in rows[:20]:
                if not isinstance(r, dict):
                    continue
                vnum = r.get("voyage_number") or "N/A"
                grades = _extract_grades(r.get("cargo_grades"))
                if not grades:
                    lines.append(f"- **Voyage {vnum}**: {_result_set_text('no_cargo_grades_for_row')}")
                else:
                    preview = ", ".join(grades[:12])
                    suffix = " (+more)" if len(grades) > 12 else ""
                    lines.append(f"- **Voyage {vnum}**: {preview}{suffix}")
            state["answer"] = "\n".join(lines)
            return state

        if action == "list_fields":
            if not rows:
                state["answer"] = "I don’t have a previous result set to project from. Please run the base query again."
                return state
            fields_raw = slots.get("fields")
            fields = [str(f).strip().lower() for f in fields_raw if str(f).strip()] if isinstance(fields_raw, list) else []
            if not fields:
                state["answer"] = "I couldn’t identify which fields to show from the previous result set."
                return state
            pretty = ", ".join(f.replace("_", " ") for f in fields)
            lines = [f"### {pretty.title()} for current result set"]
            for r in rows[:20]:
                if not isinstance(r, dict):
                    continue
                label = self._result_row_label(r)
                vals = []
                for f in fields:
                    v = r.get(f)
                    if f == "passage_types" and isinstance(v, list):
                        v = ", ".join(str(x) for x in v if x not in (None, ""))
                    if isinstance(v, list):
                        v = ", ".join(str(x) for x in v[:8]) if v else _result_set_text("not_available")
                    if v in (None, "", [], {}):
                        v = _result_set_text("not_available")
                    vals.append(f"{f.replace('_', ' ')} {v}")
                lines.append(f"- **{label}** - " + "; ".join(vals))
            state["answer"] = "\n".join(lines)
            return state

        # Generic metrics comparison for current selected set.
        if action == "compare_metrics":
            metrics = slots.get("metrics") if isinstance(slots.get("metrics"), list) else []
            metrics = [str(m).strip() for m in metrics if str(m).strip()]
            if not metrics:
                metrics = ["pnl", "tce"]
            if not rows:
                state["answer"] = "I don’t have a previous result set to compare. Please run the base list query again."
                return state
            lines = [f"### Comparison from previous results ({', '.join(metrics)})"]
            for r in rows[:20]:
                if not isinstance(r, dict):
                    continue
                label = self._result_row_label(r)
                vals = []
                for m in metrics:
                    raw = self._result_row_metric_value(r, m)
                    v = raw if isinstance(raw, (int, float)) else _num(r.get(m))
                    vals.append(f"{m}={v:,.2f}" if isinstance(v, float) else f"{m}=N/A")
                lines.append(f"- **{label}**: " + " | ".join(vals))
            state["answer"] = "\n".join(lines)
            return state

        if action == "filter_by_metric_threshold":
            if not rows:
                state["answer"] = "I don’t have a previous result set to filter. Please run the base query again."
                return state
            metric = str(slots.get("metric") or "pnl").strip().lower()
            operator = str(slots.get("operator") or ">").strip()
            try:
                threshold = float(slots.get("value"))
            except Exception:
                threshold = None
            if threshold is None:
                state["answer"] = "I couldn’t read the threshold value. Please specify a numeric threshold."
                return state

            def _passes(val: float) -> bool:
                if operator == ">":
                    return val > threshold
                if operator == "<":
                    return val < threshold
                if operator == ">=":
                    return val >= threshold
                if operator == "<=":
                    return val <= threshold
                return abs(val - threshold) < 1e-9

            filtered: list[dict] = []
            for r in rows:
                if not isinstance(r, dict):
                    continue
                v = self._result_row_metric_value(r, metric)
                if v is None:
                    continue
                if _passes(v):
                    filtered.append(r)

            if not filtered:
                state["answer"] = f"No rows matched **{metric} {operator} {threshold:g}** in the previous result set."
                return state

            _persist_result_set(filtered, primary_metric=metric)
            lines = [f"### Filtered results ({metric} {operator} {threshold:g}) from previous results"]
            for r in filtered[:20]:
                label = self._result_row_label(r)
                v = self._result_row_metric_value(r, metric)
                lines.append(f"- **{label}**: {metric}={v:,.2f}" if isinstance(v, float) else f"- **{label}**")
            if len(filtered) > 20:
                lines.append(f"- ... and {len(filtered) - 20} more rows")
            _persist_focus(filtered[0] if filtered else None)
            state["answer"] = "\n".join(lines)
            return state

        # If user asks for extremes (highest/lowest) among the last list.
        if action == "compare_extremes" or any(k in ul for k in ("highest", "lowest", "max", "min", "best", "worst")):
            metric = str(slots.get("metric") or "pnl").strip().lower()
            explicit_pnl_request = bool(re.search(r"\b(pnl|p&l|profit|loss)\b", ul))
            if not metric or (metric == "pnl" and not explicit_pnl_request):
                preferred_metric = str(((rs or {}).get("meta") or {}).get("primary_metric") or "").strip().lower()
                if preferred_metric and not any(
                    k in ul for k in ("expense ratio", "expense-to-revenue ratio", "expense to revenue ratio", "avg revenue", "average revenue", "avg pnl", "average pnl", "avg tce", "average tce", "voyage count", "count", "pnl variance", "tce variance", "variance", "total pnl", "revenue", "expense", "cost", "port count", "port calls", "tce", "commission")
                ):
                    metric = preferred_metric
                elif "expense ratio" in ul or "expense-to-revenue ratio" in ul or "expense to revenue ratio" in ul:
                    metric = "expense_to_revenue_ratio"
                elif "avg revenue" in ul or "average revenue" in ul:
                    metric = "avg_revenue"
                elif "avg pnl" in ul or "average pnl" in ul:
                    metric = "avg_pnl"
                elif "avg tce" in ul or "average tce" in ul:
                    metric = "avg_tce"
                elif "voyage count" in ul or re.search(r"\bcount\b", ul):
                    metric = "voyage_count"
                elif "pnl variance" in ul or "changed the most" in ul:
                    metric = "pnl_variance" if any(self._result_row_metric_value(r, "pnl_variance") is not None for r in rows if isinstance(r, dict)) else "variance"
                elif "tce variance" in ul:
                    metric = "tce_variance"
                elif "variance" in ul:
                    preferred_metric = str(((rs or {}).get("meta") or {}).get("primary_metric") or "").strip().lower()
                    metric = preferred_metric if preferred_metric else "variance"
                elif "total pnl" in ul:
                    metric = "total_pnl"
                elif "revenue" in ul:
                    metric = "revenue"
                elif "expense" in ul or "cost" in ul:
                    metric = "total_expense"
                elif "port count" in ul or "port calls" in ul:
                    metric = "port_calls"
                elif "tce" in ul:
                    metric = "tce"
                elif "commission" in ul:
                    metric = "total_commission"

            scored = []
            for r in rows:
                if not isinstance(r, dict):
                    continue
                label = self._result_row_label(r)
                val = self._result_row_metric_value(r, metric)
                if label in (None, "", "N/A") or val is None:
                    continue
                scored.append((val, label, r))
            if not scored:
                state["answer"] = "I don’t have a previous result set to compare. Please run the list query again."
                return state
            lo = min(scored, key=lambda t: t[0])
            hi = max(scored, key=lambda t: t[0])
            def _ensure_vessel_anchor(item: tuple[float, str, Dict[str, Any]]) -> tuple[float, str, Dict[str, Any]]:
                val, lbl, row = item
                if "vessel" not in ul:
                    return item
                if not isinstance(row, dict):
                    return item
                if row.get("vessel_name") not in (None, "", [], {}):
                    return (val, str(row.get("vessel_name")), row)
                vn = row.get("voyage_number")
                if vn in (None, "", [], {}):
                    return item
                try:
                    ctx = self.mongo_agent.fetch_full_voyage_context(voyage_number=int(vn), entity_slots=row)
                except Exception:
                    ctx = {}
                vname = (ctx or {}).get("vessel_name")
                vimo = (ctx or {}).get("vessel_imo")
                if vname not in (None, "", [], {}):
                    row = dict(row)
                    row["vessel_name"] = vname
                    if vimo not in (None, "", [], {}):
                        row["vessel_imo"] = vimo
                    return (val, str(vname), row)
                return item
            lo = _ensure_vessel_anchor(lo)
            hi = _ensure_vessel_anchor(hi)
            high_is_worse = metric in {
                "expense_to_revenue_ratio",
                "total_expense",
                "avg_total_expense",
                "offhire_days",
                "avg_offhire_days",
                "pnl_variance",
                "tce_variance",
                "variance",
                "variance_diff",
            }
            asks_high = any(k in ul for k in ("highest", "max", "most", "better", "higher", "changed the most"))
            asks_low = any(k in ul for k in ("lowest", "min", "least", "lower"))
            if metric == "pnl" and "loss" in ul and any(k in ul for k in ("highest", "most", "worst")):
                # "Highest loss" means the most negative PnL in-context.
                asks_high = False
                asks_low = True
            if "best" in ul:
                if high_is_worse:
                    asks_low = True
                else:
                    asks_high = True
            if "worst" in ul:
                if high_is_worse:
                    asks_high = True
                else:
                    asks_low = True
            if asks_high and not asks_low:
                _persist_result_set(rows, primary_metric=metric)
                _persist_focus(hi[2])
                state["answer"] = f"The highest **{metric}** in the previous results was **{hi[1]}** at **{hi[0]:,.2f}**."
            elif asks_low and not asks_high:
                _persist_result_set(rows, primary_metric=metric)
                _persist_focus(lo[2])
                state["answer"] = f"The lowest **{metric}** in the previous results was **{lo[1]}** at **{lo[0]:,.2f}**."
            else:
                _persist_result_set(rows, primary_metric=metric)
                _persist_focus(hi[2])
                state["answer"] = (
                    "### Among the previous results\n"
                    f"- **Highest {metric.upper()}**: **{hi[1]}** ({hi[0]:,.2f})\n"
                    f"- **Lowest {metric.upper()}**: **{lo[1]}** ({lo[0]:,.2f})"
                )
            return state

        # Explain remarks: require a voyage selection from the last result set.
        if action == "explain_remarks":
            # If the user already named a voyage number in this message, use it.
            import re as _re
            m = _re.search(r"\b(\d{3,5})\b", user_input or "")
            if m:
                slots["voyage_number"] = int(m.group(1))
            vnum = slots.get("voyage_number")
            if not vnum:
                # Ask which voyage they mean, using the last result set as suggestions.
                sugg = []
                for r in rows[:8]:
                    if isinstance(r, dict) and r.get("voyage_number") is not None:
                        try:
                            sugg.append(int(r["voyage_number"]))
                        except Exception:
                            continue
                options = {"voyage_number": sugg} if sugg else {}
                state["clarification"] = (
                    "### Quick question\\n"
                    "- You said **explain remarks**, but didn’t specify **which voyage** from the previous list.\\n"
                    "- Reply with the voyage number (or pick a number from the suggestions)."
                )
                # Persist a pending clarification specifically for this follow-up
                try:
                    self.redis.save_session(
                        state["session_id"],
                        {
                            **(session_context or {}),
                            "pending_intent": "followup.result_set",
                            "missing_keys": ["voyage_number"],
                            "clarification_options": options,
                            "pending_question": user_input,
                            "pending_slots": {"action": "explain_remarks"},
                        },
                    )
                except Exception:
                    pass
                return state

            # Find the matching row and return its remarks.
            chosen = None
            for r in rows:
                if isinstance(r, dict) and str(r.get("voyage_number")) == str(vnum):
                    chosen = r
                    break
            rem = (chosen or {}).get("remarks") if isinstance(chosen, dict) else None
            _persist_focus(chosen if isinstance(chosen, dict) else None)
            state["answer"] = (
                f"### Remarks for voyage {vnum}\\n"
                + (str(rem).strip() if rem not in (None, "", [], {}) else "No remarks were available in the previous result set.")
            )
            return state


    # LangGraph may populate declared state keys with None. Ensure data is always a dict.
    if not isinstance(state.get("data"), dict):
        state["data"] = {}

    # =========================================================
    # Mixed voyage query: PostgreSQL financials + Mongo metadata
    # =========================================================
    if plan_type == "multi" and intent_key in ("voyage.summary", "voyage.metadata"):
        if not slots.get("voyage_number") and isinstance(slots.get("voyage_numbers"), list) and slots.get("voyage_numbers"):
            slots["voyage_number"] = slots.get("voyage_numbers")[0]

        # Mongo canonical identity first — one voyage → one vessel (never finance-led).
        canon: Dict[str, Any] = {}
        try:
            canon = self.mongo_agent.fetch_full_voyage_context(
                voyage_number=slots.get("voyage_number"),
                voyage_id=slots.get("voyage_id"),
                entity_slots=slots,
            )
        except Exception:
            canon = {}
        if isinstance(canon, dict) and canon:
            if canon.get("voyage_id"):
                slots["voyage_id"] = str(canon.get("voyage_id"))
            if canon.get("vessel_imo"):
                slots["imo"] = str(canon.get("vessel_imo"))
                slots["vessel_imo"] = str(canon.get("vessel_imo"))
            if canon.get("vessel_name"):
                slots["vessel_name"] = str(canon.get("vessel_name"))

        if slots.get("voyage_number") and not slots.get("voyage_id"):
            vn_disp = str(slots.get("voyage_number"))
            answer = _router_fallback("voyage_reference_ambiguous_or_not_found", voyage_ref=vn_disp)
            state["answer"] = answer
            state["slots"] = slots
            return state

        # Step 1: financials from Postgres (authoritative for PnL/revenue/expense/TCE/commission)
        self._trace(state, {
            "phase": "multi_step_start",
            "step_index": 1,
            "step_count": 2,
            "intent_key": "voyage.summary",
            "agent": "finance",
            "description": "Fetch actual PnL, revenue, expense, TCE from PostgreSQL",
        })
        try:
            finance_data = self.finance_agent.run(
                intent_key="voyage.summary",
                slots=slots,
                session_context=session_context,
                user_input=user_input,
            )
        except TypeError:
            finance_data = self.finance_agent.run(
                intent_key="voyage.summary",
                slots=slots,
                session_context={**session_context, "user_input": user_input},
            )
        except Exception as e:
            finance_data = {"mode": "error", "rows": [], "fallback_reason": f"Finance agent error: {e}"}
        finance_safe = _json_safe(finance_data)
        finance_rows = finance_safe.get("rows") if isinstance(finance_safe, dict) else []
        finance_rows = finance_rows if isinstance(finance_rows, list) else []

        def _norm_imo_multi(v: Any) -> str:
            if v in (None, ""):
                return ""
            s = str(v).strip()
            if s.endswith(".0"):
                s = s[:-2]
            return s

        mongo_imo_m = _norm_imo_multi(canon.get("vessel_imo")) if isinstance(canon, dict) else ""
        mongo_vname_m = str(canon.get("vessel_name") or "").strip().lower() if isinstance(canon, dict) else ""
        mongo_vid_m = str(canon.get("voyage_id") or "").strip() if isinstance(canon, dict) else ""
        if mongo_vid_m or mongo_imo_m or mongo_vname_m:
            fin_scoped = []
            for r in finance_rows:
                if not isinstance(r, dict):
                    continue
                row_vid = str(r.get("voyage_id") or "").strip()
                row_imo = _norm_imo_multi(r.get("vessel_imo"))
                row_vname = str(r.get("vessel_name") or "").strip().lower()
                if mongo_vid_m and row_vid and row_vid == mongo_vid_m:
                    fin_scoped.append(r)
                elif (mongo_imo_m and row_imo == mongo_imo_m) or (mongo_vname_m and row_vname == mongo_vname_m):
                    fin_scoped.append(r)
            if fin_scoped:
                finance_rows = fin_scoped[:1]
                finance_safe = {**finance_safe, "rows": finance_rows}

        finance_query_key = finance_safe.get("query_key") if isinstance(finance_safe, dict) else None
        finance_sql = None
        if finance_query_key in SQL_REGISTRY:
            finance_sql = SQL_REGISTRY[finance_query_key].sql
        self._trace(state, {
            "phase": "multi_step_result",
            "step_index": 1,
            "intent_key": "voyage.summary",
            "agent": "finance",
            "mode": finance_safe.get("mode") if isinstance(finance_safe, dict) else None,
            "query_key": finance_query_key,
            "params": finance_safe.get("params") if isinstance(finance_safe, dict) else None,
            "fallback_reason": finance_safe.get("fallback_reason") if isinstance(finance_safe, dict) else None,
            "sql_present": bool(finance_sql),
            "sql": finance_sql,
            "rows": len(finance_rows),
            "ok": True,
        })

        selected_fin = finance_rows[0] if finance_rows and isinstance(finance_rows[0], dict) else {}

        # Step 2: metadata from Mongo (single canonical doc only)
        self._trace(state, {
            "phase": "multi_step_start",
            "step_index": 2,
            "step_count": 2,
            "intent_key": "voyage.metadata",
            "agent": "mongo",
            "description": "Fetch remarks, ports, cargo, fixture from MongoDB",
        })
        projection = cfg.get("mongo_projection") or get_mongo_projection("voyage_metadata_context")
        doc: Dict[str, Any] = {}
        mongo_docs: list[Dict[str, Any]] = []
        mongo_filter = None
        mongo_fetch_error = False
        if slots.get("voyage_id") not in (None, ""):
            mongo_filter = {"voyageId": str(slots.get("voyage_id"))}
            try:
                fd = self.mongo_agent.adapter.fetch_voyage(str(slots.get("voyage_id")), projection=projection)
            except Exception:
                fd = None
                mongo_fetch_error = True
            if isinstance(fd, dict) and fd:
                doc = fd
                mongo_docs = [fd]
        if not mongo_docs and isinstance(canon, dict) and canon.get("voyage_id"):
            try:
                fd = self.mongo_agent.adapter.fetch_voyage(str(canon.get("voyage_id")), projection=projection)
                if isinstance(fd, dict) and fd:
                    doc = fd
                    mongo_docs = [fd]
                    mongo_filter = {"voyageId": str(canon.get("voyage_id"))}
            except Exception:
                mongo_fetch_error = True
        if not isinstance(doc, dict):
            doc = {}
        mongo_row_count = 1 if doc else 0
        self._trace(state, {
            "phase": "multi_step_result",
            "step_index": 2,
            "intent_key": "voyage.metadata",
            "agent": "mongo",
            "mode": "mongo_metadata",
            "collection": "voyages",
            "mongo_query": {
                "collection": "voyages",
                "filter": mongo_filter or {},
                "projection": self._mongo_projection_for_trace(projection),
                "sort": None,
                "limit": max(1, mongo_row_count) if mongo_row_count else 1,
                "pipeline": None,
            },
            "rows": mongo_row_count,
            "ok": True,
        })

        # Assemble source-separated context (financial values MUST come from PostgreSQL).
        mongo_vessel_name = None
        if isinstance(doc, dict) and doc.get("vesselName"):
            mongo_vessel_name = doc.get("vesselName")
        elif mongo_docs:
            mv0 = mongo_docs[0]
            if isinstance(mv0, dict) and mv0.get("vesselName"):
                mongo_vessel_name = mv0.get("vesselName")
        financial_data_postgres = {
            "voyage_id": selected_fin.get("voyage_id") or canon.get("voyage_id"),
            "voyage_number": selected_fin.get("voyage_number") or slots.get("voyage_number"),
            "vessel_name": mongo_vessel_name or selected_fin.get("vessel_name") or canon.get("vessel_name"),
            "pnl": selected_fin.get("pnl"),
            "revenue": selected_fin.get("revenue"),
            "total_expense": selected_fin.get("total_expense"),
            "tce": selected_fin.get("tce"),
            "total_commission": selected_fin.get("total_commission"),
            "scenario": selected_fin.get("scenario"),
        }
        if doc:
            metadata_mongodb = select_voyage_sections(user_input, doc, session_context)
        else:
            metadata_mongodb = {}

        finance_fallback_reason = ""
        if isinstance(finance_safe, dict):
            finance_fallback_reason = str(finance_safe.get("fallback_reason") or "")
        mongo_down = not mongo_docs and mongo_fetch_error
        postgres_down = (
            "Postgres is not available" in finance_fallback_reason
            or "connection refused" in finance_fallback_reason.lower()
        )
        if not finance_rows and not mongo_docs and (postgres_down or mongo_down):
            vn = slots.get("voyage_number") or slots.get("voyage_numbers") or "this voyage"
            unavailable_parts = []
            if postgres_down:
                unavailable_parts.append("Postgres on `localhost:5432`")
            if mongo_down:
                unavailable_parts.append("MongoDB on `localhost:27017`")
            backend_list = " and ".join(unavailable_parts) if unavailable_parts else "the data backends"
            answer = _router_fallback(
                "backend_unavailable_with_list",
                voyage_ref=vn,
                backend_list=backend_list,
            )
            mongo_safe = {"mode": "mongo_metadata", "ok": False, "rows": mongo_docs}
            merged = {
                "finance": finance_safe,
                "ops": {"mode": None, "rows": []},
                "mongo": mongo_safe,
                "artifacts": {
                    "intent_key": intent_key,
                    "slots": slots,
                    "financial_data_postgres": financial_data_postgres,
                    "metadata_mongodb": metadata_mongodb,
                },
                "dynamic_sql_used": False,
                "dynamic_sql_agents": [],
            }
            state["merged"] = merged
            state["answer"] = answer
            state["slots"] = slots
            state["finance"] = finance_safe
            state["ops"] = {"mode": None, "rows": []}
            state["mongo"] = mongo_safe
            state["data"]["finance"] = finance_safe
            state["data"]["ops"] = {"mode": None, "rows": []}
            state["data"]["mongo"] = mongo_safe
            state["data"]["artifacts"] = merged.get("artifacts") or {"intent_key": intent_key, "slots": slots}
            return state

        system_prompt = get_graph_router_multi_voyage_answer_system_prompt()
        user_prompt = (
            "Context JSON:\n"
            + json.dumps(
                {
                    "financial_data_postgres": financial_data_postgres,
                    "metadata_mongodb": metadata_mongodb,
                },
                default=str,
                indent=2,
            )
            + f"\n\nQuestion: {user_input}"
        )
        answer = self.llm._call_with_retry(
            system=system_prompt,
            user=user_prompt,
            operation="multi_voyage_answer",
            return_string=True,
        )
        if not answer:
            answer = _router_fallback("multi_voyage_formatting_failed")

        mongo_safe = {
            "mode": "mongo_metadata",
            "ok": True,
            "rows": mongo_docs if mongo_docs else ([doc] if doc else []),
        }
        merged = {
            "finance": finance_safe,
            "ops": {"mode": None, "rows": []},
            "mongo": mongo_safe,
            "artifacts": {
                "intent_key": intent_key,
                "slots": slots,
                "financial_data_postgres": financial_data_postgres,
                "metadata_mongodb": metadata_mongodb,
            },
            "dynamic_sql_used": False,
            "dynamic_sql_agents": [],
        }
        state["merged"] = merged
        state["answer"] = answer
        state["slots"] = slots
        state["finance"] = finance_safe
        state["ops"] = {"mode": None, "rows": []}
        state["mongo"] = mongo_safe
        state["data"]["finance"] = finance_safe
        state["data"]["ops"] = {"mode": None, "rows": []}
        state["data"]["mongo"] = mongo_safe
        state["data"]["artifacts"] = merged.get("artifacts") or {"intent_key": intent_key, "slots": slots}
        try:
            persisted_slots = self._build_persisted_slots(
                base=(session_context.get("slots") or {}),
                updates=slots,
            )
            remarks_preview = None
            ref_doc = doc if (isinstance(doc, dict) and doc) else (mongo_docs[0] if mongo_docs else {})
            if isinstance(ref_doc, dict):
                raw_remarks = ref_doc.get("remarks")
                if isinstance(raw_remarks, list):
                    remarks_preview = [str(x).strip() for x in raw_remarks[:3] if x not in (None, "", [], {})]
                elif raw_remarks not in (None, "", [], {}):
                    remarks_preview = str(raw_remarks).strip()
            single_result_row = {
                "voyage_id": persisted_slots.get("voyage_id") or selected_fin.get("voyage_id") or ref_doc.get("voyageId"),
                "voyage_number": persisted_slots.get("voyage_number") or selected_fin.get("voyage_number") or ref_doc.get("voyageNumber"),
                "vessel_name": persisted_slots.get("vessel_name") or selected_fin.get("vessel_name") or ref_doc.get("vesselName"),
                "vessel_imo": persisted_slots.get("vessel_imo") or persisted_slots.get("imo") or selected_fin.get("vessel_imo"),
                "pnl": selected_fin.get("pnl"),
                "revenue": selected_fin.get("revenue"),
                "total_expense": selected_fin.get("total_expense"),
                "tce": selected_fin.get("tce"),
                "total_commission": selected_fin.get("total_commission"),
                "remarks": remarks_preview,
            }
            single_result_row = {k: v for k, v in single_result_row.items() if v not in (None, "", [], {})}
            latest_result_set = {
                "source_intent": intent_key,
                "rows": [single_result_row] if single_result_row else [],
                "meta": {
                    "source_intent": intent_key,
                    "available_metrics": [
                        m for m in ("pnl", "revenue", "total_expense", "tce", "total_commission")
                        if single_result_row.get(m) not in (None, "", [], {})
                    ],
                    "primary_metric": "pnl" if single_result_row.get("pnl") not in (None, "", [], {}) else None,
                },
            }
            self.redis.save_session(
                state["session_id"],
                {
                    **(session_context or {}),
                    "last_intent": intent_key,
                    "last_intent_key": intent_key,
                    "memory_slots": self._extract_memory_slots(persisted_slots),
                    "param_slots": self._extract_param_slots(persisted_slots),
                    "slots": persisted_slots,
                    "last_result_set": latest_result_set,
                    "last_focus_slots": self._row_focus_slots(single_result_row),
                    "last_user_input": user_input,
                    "_turn_marker": uuid.uuid4().hex,
                    "_record_turn": self._build_turn_history_entry(
                        query=user_input,
                        raw_user_input=state.get("raw_user_input") or user_input,
                        intent_key=intent_key,
                        slots=persisted_slots,
                        answer=answer,
                        plan_type=state.get("plan_type") or (state.get("plan") or {}).get("plan_type"),
                    ),
                },
            )
            state["session_ctx"] = {
                **(session_context or {}),
                "last_intent": intent_key,
                "last_intent_key": intent_key,
                "memory_slots": self._extract_memory_slots(persisted_slots),
                "param_slots": self._extract_param_slots(persisted_slots),
                "slots": persisted_slots,
                "last_result_set": latest_result_set,
                "last_focus_slots": self._row_focus_slots(single_result_row),
                "last_user_input": user_input,
            }
        except Exception:
            pass
        return state

        # =========================================================
        # Voyage summary: full context (finance, ops, mongo)
        # =========================================================

    # =========================================================
    # Voyage summary: strict Mongo-led one voyage -> one vessel, with finance/ops scoped by voyage_id + IMO
    # =========================================================
    if intent_key == "voyage.summary":
        unique_vnums = self._unique_voyage_numbers_from_slots(slots)
        if len(unique_vnums) >= 2:
            voyage_entries: list[Dict[str, Any]] = []
            all_finance_rows: list[Dict[str, Any]] = []
            all_ops_rows: list[Dict[str, Any]] = []
            all_mongo_rows: list[Dict[str, Any]] = []
            not_found: list[int] = []

            for vnum in unique_vnums:
                bundle = self._collect_voyage_summary_for_number(
                    voyage_number=vnum,
                    base_slots=slots,
                    session_context=session_context,
                    user_input=user_input,
                    intent_key=intent_key,
                )
                if not bundle.get("ok"):
                    not_found.append(vnum)
                    continue
                voyage_entries.append(
                    {
                        "voyage_number": vnum,
                        "financial_data_postgres": bundle.get("financial_data_postgres") or {},
                        "metadata_mongodb": bundle.get("metadata_mongodb") or {},
                        "ops_summary": bundle.get("ops_summary") or {},
                    }
                )
                fin_safe = bundle.get("finance_safe") if isinstance(bundle.get("finance_safe"), dict) else {}
                ops_safe_b = bundle.get("ops_safe") if isinstance(bundle.get("ops_safe"), dict) else {}
                mongo_safe_b = bundle.get("mongo_safe") if isinstance(bundle.get("mongo_safe"), dict) else {}
                for r in (fin_safe.get("rows") or []):
                    if isinstance(r, dict):
                        all_finance_rows.append(r)
                for r in (ops_safe_b.get("rows") or []):
                    if isinstance(r, dict):
                        all_ops_rows.append(r)
                if mongo_safe_b:
                    all_mongo_rows.append(mongo_safe_b)

            if not voyage_entries:
                vn_disp = ", ".join(str(v) for v in unique_vnums)
                answer = _router_fallback("voyage_reference_ambiguous_or_not_found", voyage_ref=vn_disp)
                state["merged"] = {"finance": {}, "ops": {}, "mongo": {}, "artifacts": {"intent_key": intent_key, "slots": slots}}
                state["answer"] = answer
                state["slots"] = slots
                return state

            def _multi_entry_has_payload(entry: Dict[str, Any]) -> bool:
                fin = entry.get("financial_data_postgres") if isinstance(entry.get("financial_data_postgres"), dict) else {}
                meta = entry.get("metadata_mongodb") if isinstance(entry.get("metadata_mongodb"), dict) else {}
                ops = entry.get("ops_summary") if isinstance(entry.get("ops_summary"), dict) else {}
                if any(v not in (None, "", [], {}) for v in fin.values()):
                    return True
                return bool(meta) or bool(ops)

            if not any(_multi_entry_has_payload(e) for e in voyage_entries):
                vn_disp = ", ".join(str(v) for v in unique_vnums)
                answer = _router_fallback("voyage_reference_ambiguous_or_not_found", voyage_ref=vn_disp)
                state["merged"] = {"finance": {}, "ops": {}, "mongo": {}, "artifacts": {"intent_key": intent_key, "slots": slots}}
                state["answer"] = answer
                state["slots"] = slots
                return state

            if not session_may_access_finance_kpi(session_context):
                for entry in voyage_entries:
                    entry.pop("financial_data_postgres", None)

            system_prompt = get_graph_router_multi_voyage_answer_system_prompt()
            user_prompt = (
                "Context JSON:\n"
                + json.dumps({"voyages": voyage_entries}, default=str, indent=2)
                + f"\n\nQuestion: {user_input}"
            )
            try:
                answer = self.llm._call_with_retry(
                    system=system_prompt,
                    user=user_prompt,
                    operation="multi_voyage_answer",
                    return_string=True,
                )
            except Exception:
                answer = ""
            if not answer:
                answer = _router_fallback("multi_voyage_formatting_failed")
            if not_found:
                answer = (
                    f"{answer.rstrip()}\n\n"
                    f"Note: could not resolve voyage number(s) {', '.join(str(v) for v in not_found)}."
                )

            finance_safe = {"mode": "registry_sql", "rows": all_finance_rows}
            ops_safe = {"mode": "registry_sql", "rows": all_ops_rows}
            mongo_safe = {"mode": "mongo_multi", "ok": True, "rows": all_mongo_rows}
            merged = {
                "finance": finance_safe,
                "ops": ops_safe,
                "mongo": mongo_safe,
                "artifacts": {
                    "intent_key": intent_key,
                    "slots": slots,
                    "multi_voyage_single_path": True,
                    "voyage_numbers": unique_vnums,
                },
                "dynamic_sql_used": False,
                "dynamic_sql_agents": [],
            }
            state["merged"] = merged
            state["answer"] = answer
            state["slots"] = slots
            state["finance"] = finance_safe
            state["ops"] = ops_safe
            state["mongo"] = mongo_safe
            state["data"]["finance"] = finance_safe
            state["data"]["ops"] = ops_safe
            state["data"]["mongo"] = mongo_safe
            state["data"]["artifacts"] = merged.get("artifacts") or {"intent_key": intent_key, "slots": slots}
            persisted_slots = self._build_persisted_slots(base=(session_context.get("slots") or {}), updates=slots)
            state["session_ctx"] = {
                **(session_context or {}),
                "last_intent": intent_key,
                "memory_slots": self._extract_memory_slots(persisted_slots),
                "param_slots": self._extract_param_slots(persisted_slots),
                "slots": persisted_slots,
                "last_user_input": user_input,
            }
            return state

        voyage_number = slots.get("voyage_number")
        voyage_id = slots.get("voyage_id")

        # 1️⃣ Mongo canonical resolve first (so voyage-number-only queries can be disambiguated)
        try:
            mongo_data = self.mongo_agent.fetch_full_voyage_context(
                voyage_number=voyage_number,
                voyage_id=voyage_id,
                entity_slots=slots,
            )
        except Exception as e:
            _dprint(f"⚠️  Mongo failed: {e}")
            mongo_data = {}

        if not mongo_data and voyage_number not in (None, ""):
            try:
                adapter = getattr(self.mongo_agent, "adapter", None)
                getter = getattr(adapter, "get_voyage_by_number", None)
                doc = getter(voyage_number) if callable(getter) else None
            except Exception:
                doc = None
            if isinstance(doc, dict) and doc:
                mongo_data = {
                    "voyage_id": doc.get("voyageId") or doc.get("voyage_id"),
                    "voyage_number": doc.get("voyageNumber") or doc.get("voyage_number") or voyage_number,
                    "vessel_imo": doc.get("vesselImo") or doc.get("vessel_imo") or doc.get("imo"),
                    "vessel_name": doc.get("vesselName") or doc.get("vessel_name"),
                    "remarks": doc.get("remarks"),
                    "fixtures": doc.get("fixtures"),
                }

        if isinstance(mongo_data, dict) and mongo_data:
            canonical_vid = mongo_data.get("voyage_id")
            canonical_imo = mongo_data.get("vessel_imo")
            canonical_vname = mongo_data.get("vessel_name")
            if canonical_vid:
                slots["voyage_id"] = str(canonical_vid)
            if canonical_imo:
                slots["imo"] = str(canonical_imo)
                slots["vessel_imo"] = str(canonical_imo)
            if canonical_vname:
                slots["vessel_name"] = str(canonical_vname)
            voyage_id = slots.get("voyage_id")

        # One voyage → one vessel (Mongo canonical). Do not query finance/ops without Mongo identity.
        if (
            voyage_number
            and not voyage_id
            and not (isinstance(mongo_data, dict) and mongo_data.get("voyage_id"))
            and hasattr(self.mongo_agent, "adapter")
        ):
            vn_disp = str(voyage_number)
            answer = _router_fallback("voyage_reference_ambiguous_or_not_found", voyage_ref=vn_disp)
            state["merged"] = {"finance": {}, "ops": {}, "mongo": {}, "artifacts": {"intent_key": intent_key, "slots": slots}}
            state["answer"] = answer
            state["slots"] = slots
            return state

        # 2️⃣ Finance (scoped to Mongo voyage_id + vessel IMO only)
        self._trace(
            state,
            {
                "phase": "composite_step_start",
                "step_index": 1,
                "step_count": 2,
                "agent": "finance",
                "operation": "registry_sql",
                "inputs": {"intent_key": intent_key},
                "goal": "Run finance registry SQL in single plan mode.",
            },
        )
        try:
            finance_data = self.finance_agent.run(
                intent_key=intent_key,
                slots=slots,
                session_context=session_context,
                user_input=user_input,
            )
        except TypeError:
            finance_data = self.finance_agent.run(
                intent_key=intent_key,
                slots=slots,
                session_context={**session_context, "user_input": user_input},
            )
        except Exception as e:
            _dprint(f"⚠️  Finance failed: {e}")
            finance_data = {"mode": "error", "rows": []}
        try:
            fin_rows = finance_data.get("rows") if isinstance(finance_data, dict) else []
            fin_rows = fin_rows if isinstance(fin_rows, list) else []
            fin_vids = [r.get("voyage_id") for r in fin_rows if isinstance(r, dict) and r.get("voyage_id")]
            fin_vids = list(dict.fromkeys(fin_vids))
            fin_sql = _registry_sql_from_result(finance_data)
            self._trace(
                state,
                {
                    "phase": "composite_step_result",
                    "step_index": 1,
                    "agent": "finance",
                    "operation": "registry_sql",
                    "ok": True,
                    "mode": finance_data.get("mode") if isinstance(finance_data, dict) else None,
                    "rows": len(fin_rows),
                    "voyage_ids": len(fin_vids),
                    "extracted_voyage_ids": fin_vids,
                    "sql_present": bool(fin_sql),
                    "sql": fin_sql,
                    "summary": f"Finance(single): fetched {len(fin_rows)} rows using registry SQL.",
                },
            )
        except Exception:
            pass

        # 3️⃣ Ops
        ops_data = None
        finance_fr = ""
        if isinstance(finance_data, dict):
            finance_fr = str(finance_data.get("fallback_reason") or "")
        postgres_down = "Postgres is not available" in finance_fr or "connection refused" in finance_fr.lower()

        if postgres_down:
            ops_data = {"mode": "error", "rows": [], "fallback_reason": finance_fr}
        else:
            self._trace(
                state,
                {
                    "phase": "composite_step_start",
                    "step_index": 2,
                    "step_count": 2,
                    "agent": "ops",
                    "operation": "registry_sql",
                    "inputs": {"intent_key": intent_key},
                    "goal": "Run ops registry SQL in single plan mode.",
                },
            )
            try:
                ops_data = self.ops_agent.run(
                    intent_key=intent_key,
                    slots=slots,
                    session_context=session_context,
                    user_input=user_input,
                )
            except TypeError:
                ops_data = self.ops_agent.run(
                    intent_key=intent_key,
                    slots=slots,
                    session_context={**session_context, "user_input": user_input},
                )
            except Exception as e:
                _dprint(f"⚠️  Ops failed: {e}")
                ops_data = {"mode": "error", "rows": []}
        try:
            ops_rows = ops_data.get("rows") if isinstance(ops_data, dict) else []
            ops_rows = ops_rows if isinstance(ops_rows, list) else []
            ops_vids = [r.get("voyage_id") for r in ops_rows if isinstance(r, dict) and r.get("voyage_id")]
            ops_vids = list(dict.fromkeys(ops_vids))
            ops_sql = _registry_sql_from_result(ops_data)
            self._trace(
                state,
                {
                    "phase": "composite_step_result",
                    "step_index": 2,
                    "agent": "ops",
                    "operation": "registry_sql",
                    "ok": True,
                    "mode": ops_data.get("mode") if isinstance(ops_data, dict) else None,
                    "rows": len(ops_rows),
                    "voyage_ids": len(ops_vids),
                    "extracted_voyage_ids": ops_vids,
                    "sql_present": bool(ops_sql),
                    "sql": ops_sql,
                    "summary": f"Ops(single): fetched {len(ops_rows)} rows using registry SQL.",
                },
            )
        except Exception:
            pass

        finance_safe = _json_safe(finance_data)
        ops_safe = _json_safe(ops_data)
        mongo_safe = _json_safe(mongo_data)
        raw_finance_rows = []
        if isinstance(finance_safe, dict) and isinstance(finance_safe.get("rows"), list):
            raw_finance_rows = list(finance_safe.get("rows") or [])

        # Reconcile single-voyage SQL rows with Mongo's canonical vessel for this voyage_number.
        # In some datasets, voyage_number is reused across vessels; this keeps voyage.summary strict.
        def _norm_imo(v: Any) -> str:
            if v in (None, ""):
                return ""
            s = str(v).strip()
            if s.endswith(".0"):
                s = s[:-2]
            return s

        mongo_imo = ""
        mongo_vessel_name = ""
        if isinstance(mongo_safe, dict):
            mongo_imo = _norm_imo(mongo_safe.get("vessel_imo"))
            mongo_vessel_name = str(mongo_safe.get("vessel_name") or "").strip().lower()

        # Strict Mongo-led reconciliation: one voyage → one vessel; never surface multi-vessel finance rows.
        if (mongo_imo or mongo_vessel_name or voyage_id) and isinstance(finance_safe, dict) and isinstance(finance_safe.get("rows"), list):
            fin_rows = finance_safe.get("rows") or []
            fin_filtered = []
            for r in fin_rows:
                if not isinstance(r, dict):
                    continue
                row_imo = _norm_imo(r.get("vessel_imo"))
                row_vname = str(r.get("vessel_name") or "").strip().lower()
                row_vid = str(r.get("voyage_id") or "").strip()
                if voyage_id and row_vid and row_vid == str(voyage_id).strip():
                    fin_filtered.append(r)
                elif (mongo_imo and row_imo == mongo_imo) or (mongo_vessel_name and row_vname == mongo_vessel_name):
                    fin_filtered.append(r)
            if fin_filtered:
                finance_safe = {**finance_safe, "rows": fin_filtered[:1]}

        if (mongo_imo or mongo_vessel_name or voyage_id) and isinstance(ops_safe, dict) and isinstance(ops_safe.get("rows"), list):
            ops_rows = ops_safe.get("rows") or []
            ops_filtered = []
            for r in ops_rows:
                if not isinstance(r, dict):
                    continue
                row_imo = _norm_imo(r.get("vessel_imo"))
                row_vname = str(r.get("vessel_name") or "").strip().lower()
                row_vid = str(r.get("voyage_id") or "").strip()
                if voyage_id and row_vid and row_vid == str(voyage_id).strip():
                    ops_filtered.append(r)
                elif (mongo_imo and row_imo == mongo_imo) or (mongo_vessel_name and row_vname == mongo_vessel_name):
                    ops_filtered.append(r)
            if ops_filtered:
                ops_safe = {**ops_safe, "rows": ops_filtered[:1]}

        # If all backends are unavailable, avoid a slow/expensive LLM call and return a clear message.
        finance_ok = isinstance(finance_safe, dict) and finance_safe.get("rows")
        ops_ok = isinstance(ops_safe, dict) and ops_safe.get("rows")
        mongo_ok = isinstance(mongo_safe, dict) and (
            mongo_safe.get("remarks") or mongo_safe.get("fixtures") or mongo_safe.get("voyage_id") or mongo_safe.get("voyage_number")
        )
        if not finance_ok and not ops_ok and not mongo_ok:
            vn = voyage_number or slots.get("voyage_numbers") or voyage_id or "this voyage"
            vn_disp = str(vn)
            ops_fr = str((ops_safe or {}).get("fallback_reason") or "")
            fin_fr = str((finance_safe or {}).get("fallback_reason") or "")
            combined = f"{ops_fr} {fin_fr}".lower()
            # Empty SQL/Mongo rows often mean wrong/incomplete voyage ref — do not blame infra unless transport errors show up.
            transport_unavailable = postgres_down or any(
                needle in combined
                for needle in (
                    "connection refused",
                    "postgres is not available",
                    "could not connect",
                    "timed out",
                    "timeout",
                    "server selection timed out",
                    "operationalerror",
                    "lost connection",
                    "network is unreachable",
                    "name or service not known",
                )
            )
            if transport_unavailable:
                answer = _router_fallback("backend_unavailable_generic", voyage_ref=vn_disp)
            else:
                answer = _router_fallback("voyage_reference_ambiguous_or_not_found", voyage_ref=vn_disp)
            state["merged"] = {"finance": finance_safe, "ops": ops_safe, "mongo": {}, "artifacts": {"intent_key": intent_key, "slots": slots}}
            state["answer"] = answer
            state["slots"] = slots
            state["finance"] = finance_safe
            state["ops"] = ops_safe
            state["mongo"] = {}
            return state

        # Normalize single-path mongo payload into the same shape as dynamic NoSQL results
        # so validators/summarizer have a consistent contract.
        mongo_llm_like = mongo_safe
        if isinstance(mongo_safe, dict) and mongo_safe and mongo_safe.get("mode") != "mongo_llm":
            vid = mongo_safe.get("voyage_id")
            vnum = mongo_safe.get("voyage_number")
            remarks = mongo_safe.get("remarks") or []
            if not session_may_access_finance_kpi(session_context):
                mongo_row_payload = build_mongo_llm_row_for_ops_tenant(mongo_safe)
            else:
                fixtures = mongo_safe.get("fixtures") or []
                mongo_row_payload = {
                    "voyageId": str(vid) if vid is not None else None,
                    "voyageNumber": str(vnum) if vnum is not None else None,
                    "remarks": remarks,
                    "fixtures": fixtures,
                }
            mongo_llm_like = {
                "mode": "mongo_llm",
                "ok": True,
                "collection": "voyages",
                "filter": {"voyageId": str(vid)} if vid else ({"voyageNumber": str(vnum)} if vnum else {}),
                "projection": get_mongo_projection("single_path_mongo_payload"),
                "limit": 1,
                "rows": [mongo_row_payload] if (vid or vnum) and mongo_row_payload else [],
            }

        merged = {
            "finance": finance_safe,
            "ops": ops_safe,
            "mongo": mongo_llm_like,
            "artifacts": {
                "intent_key": intent_key,
                "slots": slots,
                "finance_kpi_unavailable": not session_may_access_finance_kpi(session_context),
            },
            "dynamic_sql_used": isinstance(finance_safe, dict) and finance_safe.get("mode") == "dynamic_sql",
            "dynamic_sql_agents": ["finance", "ops"] if isinstance(finance_safe, dict) and finance_safe.get("mode") == "dynamic_sql" else [],
        }

        # Keep state["data"] aligned with the response payload for validators.
        state["data"]["finance"] = finance_safe
        state["data"]["ops"] = ops_safe
        state["data"]["mongo"] = mongo_llm_like
        state["data"]["artifacts"] = merged.get("artifacts") or {"intent_key": intent_key, "slots": slots}

        # Guardrail: if the user asked for financial metrics on a single voyage but
        # finance has no rows, avoid LLM-generated "best effort" summaries that can
        # fabricate multi-voyage/multi-vessel tables.
        user_input_lower = (user_input or "").lower()
        asked_financials = any(
            k in user_input_lower
            for k in (
                "financial summary",
                "financials",
                "pnl",
                "profit",
                "revenue",
                "expense",
                "tce",
                "commission",
            )
        )
        # Ops-only / finance-KPI-disabled tenants: never run the LLM for explicit financial asks
        # when finance rows are empty — ops/mongo payloads can still contain hire/fixture text
        # that models misread as revenue/PnL.
        if asked_financials and not finance_ok and not session_may_access_finance_kpi(session_context):
            answer = get_finance_kpi_scope_restricted_user_message().strip()
            snap = GraphRouter._ops_only_voyage_snapshot_markdown(ops_safe if isinstance(ops_safe, dict) else {})
            if snap:
                answer = f"{answer}\n\n{snap}"
            state["merged"] = merged
            state["answer"] = answer
            state["slots"] = slots
            state["finance"] = finance_safe
            state["ops"] = ops_safe
            state["mongo"] = mongo_llm_like
            return state

        if asked_financials and not finance_ok and session_may_access_finance_kpi(session_context):
            vn = voyage_number or slots.get("voyage_numbers") or voyage_id or "this voyage"
            # Helpful diagnostics: finance had rows for this voyage number, but none
            # matched Mongo's canonical vessel after strict reconciliation.
            if raw_finance_rows:
                vessel_labels = []
                for r in raw_finance_rows:
                    if not isinstance(r, dict):
                        continue
                    imo = _norm_imo(r.get("vessel_imo"))
                    vname = str(r.get("vessel_name") or "").strip()
                    label = f"{vname} ({imo})" if (vname or imo) else ""
                    if label and label not in vessel_labels:
                        vessel_labels.append(label)
                preview = ", ".join(vessel_labels[:5]) if vessel_labels else "N/A"
                extra = (len(vessel_labels) - 5) if vessel_labels else 0
                suffix = f" (+{extra} more)" if extra > 0 else ""
                answer = _router_fallback(
                    "finance_identity_mismatch",
                    voyage_ref=vn,
                    candidate_preview=preview,
                    candidate_suffix=suffix,
                )
                state["merged"] = merged
                state["answer"] = answer
                state["slots"] = slots
                state["finance"] = finance_safe
                state["ops"] = ops_safe
                state["mongo"] = mongo_llm_like
                return state
            remarks_count = 0
            if isinstance(mongo_safe, dict) and isinstance(mongo_safe.get("remarks"), list):
                remarks_count = len(mongo_safe.get("remarks") or [])
            answer = _router_fallback(
                "missing_finance_records",
                voyage_ref=vn,
                remarks_count=remarks_count,
            )
            state["merged"] = merged
            state["answer"] = answer
            state["slots"] = slots
            state["finance"] = finance_safe
            state["ops"] = ops_safe
            state["mongo"] = mongo_llm_like
            return state

        # Ops-only tenants: give summarizer a flat merged_rows row (ports/grades/delays/remarks).
        if not session_may_access_finance_kpi(session_context):
            ops_rows_pre = ops_safe.get("rows") if isinstance(ops_safe, dict) else []
            ops_row_pre = None
            if isinstance(ops_rows_pre, list) and ops_rows_pre and isinstance(ops_rows_pre[0], dict):
                ops_row_pre = copy.deepcopy(ops_rows_pre[0])
                shrink_ops_row_json_fields(ops_row_pre, voyage_summary=True)
            mongo_for_row: Dict[str, Any] = dict(mongo_safe) if isinstance(mongo_safe, dict) else {}
            if isinstance(mongo_data, dict):
                for mk in ("legs", "fixtures", "remarks", "voyage_id", "voyage_number", "vessel_name", "vessel_imo"):
                    if mongo_data.get(mk) not in (None, "", [], {}) and not mongo_for_row.get(mk):
                        mongo_for_row[mk] = mongo_data.get(mk)
            flat_row = build_voyage_summary_merged_row(
                ops_row=ops_row_pre,
                mongo_safe=mongo_for_row,
                slots=slots,
            )
            if flat_row:
                art = merged.get("artifacts")
                if not isinstance(art, dict):
                    art = {"intent_key": intent_key, "slots": slots}
                    merged["artifacts"] = art
                art["merged_rows"] = [flat_row]
                art["finance_kpi_unavailable"] = True

        try:
            merged_llm = GraphRouter._sanitize_for_llm(compact_payload(merged))
            if hasattr(self.llm, "generate_final_answer"):
                answer = self.llm.generate_final_answer(
                    question=user_input,
                    merged_data=merged_llm,
                    session_context=session_context,
                )
            else:
                answer = self.llm.summarize_answer(
                    question=user_input,
                    plan=state.get("plan") or {"plan_type": "single", "intent_key": intent_key},
                    merged=merged_llm,
                    session_context=session_context,
                )
        except Exception as e:
            _dprint(f"⚠️  LLM generation failed: {e}")
            answer = _router_fallback("llm_summary_error", error=e)

        # Populate state so it naturally bypasses the rest via routing
        state["merged"] = merged
        state["answer"] = answer
        state["slots"] = slots
        state["finance"] = finance_safe
        state["ops"] = ops_safe
        state["mongo"] = mongo_llm_like

        # Save session immediately
        persisted_slots = self._build_persisted_slots(base=(session_context.get("slots") or {}), updates=slots)
        fin_rows_now = finance_safe.get("rows") if isinstance(finance_safe, dict) else []
        ops_rows_now = ops_safe.get("rows") if isinstance(ops_safe, dict) else []
        fin_row = fin_rows_now[0] if isinstance(fin_rows_now, list) and fin_rows_now and isinstance(fin_rows_now[0], dict) else {}
        ops_row = ops_rows_now[0] if isinstance(ops_rows_now, list) and ops_rows_now and isinstance(ops_rows_now[0], dict) else {}
        mongo_row = (mongo_llm_like.get("rows") or [None])[0] if isinstance(mongo_llm_like, dict) else None
        remarks_val = []
        if isinstance(mongo_row, dict):
            remarks_val = mongo_row.get("remarks") or []
        remarks_text = None
        if isinstance(remarks_val, list):
            remarks_text = " | ".join([str(x).strip() for x in remarks_val[:4] if x not in (None, "", [], {})]) or None
        elif remarks_val not in (None, "", [], {}):
            remarks_text = str(remarks_val).strip()
        single_result_row = {
            "voyage_id": mongo_safe.get("voyage_id") or slots.get("voyage_id") or fin_row.get("voyage_id") or ops_row.get("voyage_id"),
            "voyage_number": mongo_safe.get("voyage_number") or slots.get("voyage_number") or fin_row.get("voyage_number") or ops_row.get("voyage_number"),
            "vessel_name": mongo_safe.get("vessel_name") or slots.get("vessel_name") or fin_row.get("vessel_name") or ops_row.get("vessel_name"),
            "vessel_imo": mongo_safe.get("vessel_imo") or slots.get("imo") or slots.get("vessel_imo") or fin_row.get("vessel_imo") or ops_row.get("vessel_imo") or ops_row.get("imo"),
            "pnl": fin_row.get("pnl"),
            "revenue": fin_row.get("revenue"),
            "total_expense": fin_row.get("total_expense"),
            "tce": fin_row.get("tce"),
            "total_commission": fin_row.get("total_commission"),
            "offhire_days": ops_row.get("offhire_days"),
            "delay_reason": ops_row.get("delay_reason") or ops_row.get("delay_reasons"),
            "key_ports": ops_row.get("key_ports") or ops_row.get("ports") or ops_row.get("ports_json"),
            "cargo_grades": ops_row.get("cargo_grades") or fin_row.get("cargo_grades"),
            "port_calls": ops_row.get("port_calls") or fin_row.get("port_calls") or fin_row.get("port_count"),
            "remarks": remarks_text,
        }
        single_result_row = {k: v for k, v in single_result_row.items() if v not in (None, "", [], {})}
        single_result_set = {
            "source_intent": intent_key,
            "rows": [single_result_row] if single_result_row else [],
            "meta": {"source_intent": intent_key, "available_metrics": [m for m in ("pnl", "revenue", "total_expense", "tce", "offhire_days", "port_calls") if single_result_row.get(m) not in (None, "", [], {})], "primary_metric": "pnl" if single_result_row.get("pnl") not in (None, "", [], {}) else None},
        }
        self.redis.save_session(
            state["session_id"],
            {
                **session_context,
                "last_intent": intent_key,
                "last_intent_key": intent_key,
                "memory_slots": self._extract_memory_slots(persisted_slots),
                "param_slots": self._extract_param_slots(persisted_slots),
                "slots": persisted_slots,
                "last_result_set": single_result_set,
                "last_focus_slots": self._row_focus_slots(single_result_row),
                "last_user_input": user_input,
                "_turn_marker": uuid.uuid4().hex,
                "_record_turn": self._build_turn_history_entry(
                    query=user_input,
                    raw_user_input=state.get("raw_user_input") or user_input,
                    intent_key=intent_key,
                    slots=persisted_slots,
                    answer=answer,
                    plan_type=state.get("plan_type") or (state.get("plan") or {}).get("plan_type"),
                ),
            },
        )
        state["session_ctx"] = {
            **(session_context or {}),
            "last_intent": intent_key,
            "memory_slots": self._extract_memory_slots(persisted_slots),
            "param_slots": self._extract_param_slots(persisted_slots),
            "slots": persisted_slots,
            "last_result_set": single_result_set,
            "last_focus_slots": self._row_focus_slots(single_result_row),
            "last_user_input": user_input,
        }
        return state

        # =========================================================
        # Fleet-wide vessel metadata ranking/listing from Mongo
        # =========================================================

    # =========================================================
    # Fleet-wide vessel metadata ranking/listing from Mongo
    # =========================================================
    if intent_key == "ranking.vessel_metadata":
        def _extract_tag_pairs(doc: Dict[str, Any]) -> list[Dict[str, str]]:
            out: list[Dict[str, str]] = []
            raw_tags = doc.get("tags")
            if not isinstance(raw_tags, list):
                return out
            for tag in raw_tags:
                if not isinstance(tag, dict):
                    continue
                cat = str(tag.get("category") or "").strip()
                val = str(tag.get("value") or "").strip()
                if cat or val:
                    out.append({"category": cat, "value": val})
            return out

        def _pool_value(doc: Dict[str, Any]) -> str | None:
            for tag in _extract_tag_pairs(doc):
                cat = tag.get("category", "").lower()
                val = tag.get("value")
                if "pool" in cat and val:
                    return val
            for tag in _extract_tag_pairs(doc):
                val = tag.get("value", "")
                if "pool" in val.lower():
                    return val
            return None

        def _normalize_scrubber(val: Any, doc: Dict[str, Any]) -> str | None:
            if val not in (None, ""):
                return str(val).strip()
            for tag in _extract_tag_pairs(doc):
                txt = str(tag.get("value") or "").strip()
                if not txt:
                    continue
                tl = txt.lower()
                if "scrubber" in tl:
                    return txt
            return None

        def _default_speed(doc: Dict[str, Any], passage_type: str) -> float | None:
            cps = doc.get("consumption_profiles")
            if not isinstance(cps, list):
                return None
            wanted = str(passage_type or "").strip().lower()
            fallback: float | None = None
            for cp in cps:
                if not isinstance(cp, dict):
                    continue
                pp = cp.get("passageProfile")
                if not isinstance(pp, list):
                    continue
                for p in pp:
                    if not isinstance(p, dict):
                        continue
                    if str(p.get("passageType") or "").strip().lower() != wanted:
                        continue
                    cons = p.get("consumption")
                    if not isinstance(cons, list):
                        continue
                    for c in cons:
                        if not isinstance(c, dict):
                            continue
                        try:
                            speed = float(c.get("speed"))
                        except Exception:
                            speed = None
                        if speed is None:
                            continue
                        if c.get("isDefault") is True:
                            return speed
                        if fallback is None or speed > fallback:
                            fallback = speed
            return fallback

        def _current_contract(doc: Dict[str, Any]) -> Dict[str, Any] | None:
            history = (doc.get("contract_history") or {}).get("list")
            if not isinstance(history, list):
                return None
            for item in history:
                if isinstance(item, dict) and item.get("isCurrent") is True:
                    return item
            for item in history:
                if isinstance(item, dict):
                    return item
            return None

        def _contract_duration_days(contract: Dict[str, Any] | None) -> float | None:
            if not isinstance(contract, dict):
                return None
            try:
                dur = float(contract.get("duration"))
            except Exception:
                return None
            dtype = str(contract.get("durationType") or "").strip().lower()
            if "year" in dtype:
                return dur * 365.0
            if "month" in dtype:
                return dur * 30.0
            if "week" in dtype:
                return dur * 7.0
            return dur

        def _contract_duration_label(contract: Dict[str, Any] | None) -> str | None:
            if not isinstance(contract, dict):
                return None
            dur = contract.get("duration")
            dtype = contract.get("durationType")
            if dur in (None, ""):
                return None
            return f"{dur} {dtype}".strip() if dtype not in (None, "") else str(dur)

        def _fmt_num(v: Any, *, money: bool = False) -> str:
            if v in (None, "", [], {}):
                return "Not available"
            try:
                n = float(v)
                if money:
                    return f"${n:,.2f}"
                if abs(n - int(n)) < 1e-9:
                    return f"{int(n):,}"
                return f"{n:,.2f}"
            except Exception:
                return str(v)

        def _build_table(headers: list[str], body_rows: list[list[str]]) -> str:
            out = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
            for row in body_rows:
                out.append("| " + " | ".join(row) + " |")
            return "\n".join(out)

        projection = cfg.get("mongo_projection")
        try:
            res = self.mongo_agent.run(
                intent_key=cfg.get("mongo_intent", "vessel.list_all"),
                slots={},
                projection=projection,
                session_context=session_context,
            )
            mongo_safe = _json_safe(res)
        except Exception as e:
            _dprint(f"⚠️  Mongo failed (ranking.vessel_metadata): {e}")
            mongo_safe = {}

        vessel_docs = []
        if isinstance(mongo_safe, dict):
            doc = mongo_safe.get("document")
            if isinstance(doc, dict) and isinstance(doc.get("vessels"), list):
                vessel_docs = [v for v in doc.get("vessels") if isinstance(v, dict)]

        self._trace_single_mongo_query(
            state,
            intent_key=intent_key,
            operation="fleet_metadata_lookup",
            collection="vessels",
            filt={},
            projection=projection,
            limit=200,
            rows=len(vessel_docs),
            summary="Mongo: fetched fleet-wide vessel metadata from the `vessels` collection for ranking/listing.",
        )

        ql = (user_input or "").lower()
        wants_count = (
            any(k in ql for k in ("how many", "count", "total how many", "total number"))
            and not any(k in ql for k in ("highest", "lowest", "longest", "fastest", "top", "best", "worst", "least", "most"))
        )
        wants_operating = any(k in ql for k in ("operating", "operational", "active vessel", "active vessels"))
        wants_hire_rate = any(k in ql for k in ("hire rate", "hirerate", "hire_rate", "hire-rate"))
        wants_scrubber = "scrubber" in ql
        wants_non_scrubber = any(k in ql for k in ("non-scrubber", "non scrubber", "without scrubber"))
        wants_ballast_speed = ("ballast" in ql) and ("speed" in ql)
        wants_laden_speed = ("laden" in ql) and ("speed" in ql)
        wants_contract = "contract" in ql and any(k in ql for k in ("longest", "duration", "current", "owner", "delivery"))
        wants_short_pool = "short pool" in ql
        wants_long_pool = "long pool" in ql
        wants_market_type = "market type" in ql

        rows: list[Dict[str, Any]] = []
        seen_vessel_keys: set[str] = set()
        for doc in vessel_docs:
            contract = _current_contract(doc)
            pool = _pool_value(doc)
            scrubber = _normalize_scrubber(doc.get("scrubber"), doc)
            vessel_key = str(doc.get("imo") or doc.get("name") or "").strip().lower()
            if not vessel_key:
                continue
            if vessel_key in seen_vessel_keys:
                continue
            seen_vessel_keys.add(vessel_key)
            rows.append(
                {
                    "vessel_name": doc.get("name"),
                    "imo": doc.get("imo"),
                    "hire_rate": doc.get("hireRate"),
                    "scrubber": scrubber,
                    "market_type": doc.get("marketType"),
                    "is_operating": doc.get("isVesselOperating"),
                    "pool": pool,
                    "ballast_speed": _default_speed(doc, "ballast"),
                    "laden_speed": _default_speed(doc, "laden"),
                    "current_contract_duration_days": _contract_duration_days(contract),
                    "current_contract_duration": _contract_duration_label(contract),
                    "current_contract_owner": (contract or {}).get("owner") if isinstance(contract, dict) else None,
                    "current_contract_delivery": (contract or {}).get("deliveryDatetime") if isinstance(contract, dict) else None,
                }
            )

        def _scrubber_bucket(val: Any) -> str:
            txt = str(val or "").strip().lower()
            if not txt:
                return ""
            if "non" in txt and "scrubber" in txt:
                return "no"
            if txt in ("no", "false", "0"):
                return "no"
            if "yes" in txt or "scrubber" in txt:
                return "yes"
            return txt

        filtered = list(rows)
        if wants_operating:
            filtered = [r for r in filtered if r.get("is_operating") is True]
        if wants_non_scrubber:
            filtered = [r for r in filtered if _scrubber_bucket(r.get("scrubber")) == "no"]
        elif wants_scrubber and not wants_hire_rate:
            filtered = [r for r in filtered if _scrubber_bucket(r.get("scrubber")) == "yes"]
        if wants_short_pool:
            filtered = [r for r in filtered if "short pool" in str(r.get("pool") or "").lower()]
        if wants_long_pool:
            filtered = [r for r in filtered if "long pool" in str(r.get("pool") or "").lower()]

        metric = None
        if wants_hire_rate:
            metric = "hire_rate"
        elif wants_ballast_speed:
            metric = "ballast_speed"
        elif wants_laden_speed:
            metric = "laden_speed"
        elif wants_contract:
            metric = "current_contract_duration_days"

        descending = any(k in ql for k in ("highest", "top", "most", "best", "longest", "fastest"))
        ascending = any(k in ql for k in ("lowest", "least", "worst"))
        if metric:
            filtered = sorted(
                filtered,
                key=lambda r: (
                    r.get(metric) is None,
                    -(float(r.get(metric))) if (descending and r.get(metric) is not None) else (float(r.get(metric)) if r.get(metric) is not None else float("inf")),
                ),
            )
            if ascending and not descending:
                filtered = sorted(
                    filtered,
                    key=lambda r: (r.get(metric) is None, float(r.get(metric)) if r.get(metric) is not None else float("inf")),
                )
        else:
            filtered = sorted(filtered, key=lambda r: str(r.get("vessel_name") or ""))

        limit = max(1, min(int(slots.get("limit") or 10), 25))
        shown = filtered[:limit]

        if not rows:
            answer = _router_fallback("vessel_metadata_empty")
        elif wants_count:
            if wants_operating:
                answer = f"There are **{len(filtered)}** vessels currently operating."
            elif wants_non_scrubber:
                answer = f"There are **{len(filtered)}** non-scrubber vessels."
            elif wants_scrubber:
                answer = f"There are **{len(filtered)}** scrubber-fitted vessels."
            elif wants_short_pool:
                answer = f"There are **{len(filtered)}** vessels in the short pool."
            elif wants_long_pool:
                answer = f"There are **{len(filtered)}** vessels in the long pool."
            else:
                answer = f"There are **{len(filtered)}** matching vessels."
        else:
            headers = ["Vessel Name", "Vessel IMO"]
            body_rows: list[list[str]] = []
            if wants_hire_rate:
                headers.append("Hire Rate")
                for r in shown:
                    body_rows.append([str(r.get("vessel_name") or "Not available"), str(r.get("imo") or "Not available"), _fmt_num(r.get("hire_rate"), money=True)])
            elif wants_ballast_speed:
                headers.append("Default Ballast Speed")
                for r in shown:
                    body_rows.append([str(r.get("vessel_name") or "Not available"), str(r.get("imo") or "Not available"), _fmt_num(r.get("ballast_speed"))])
            elif wants_laden_speed:
                headers.append("Default Laden Speed")
                for r in shown:
                    body_rows.append([str(r.get("vessel_name") or "Not available"), str(r.get("imo") or "Not available"), _fmt_num(r.get("laden_speed"))])
            elif wants_contract:
                headers.extend(["Current Contract Duration", "Owner", "Delivery"])
                for r in shown:
                    body_rows.append([
                        str(r.get("vessel_name") or "Not available"),
                        str(r.get("imo") or "Not available"),
                        str(r.get("current_contract_duration") or "Not available"),
                        str(r.get("current_contract_owner") or "Not available"),
                        str(r.get("current_contract_delivery") or "Not available"),
                    ])
            elif wants_scrubber or wants_non_scrubber:
                headers.append("Scrubber")
                for r in shown:
                    body_rows.append([str(r.get("vessel_name") or "Not available"), str(r.get("imo") or "Not available"), str(r.get("scrubber") or "Not available")])
            elif wants_short_pool or wants_long_pool:
                headers.append("Pool")
                if wants_market_type:
                    headers.append("Market Type")
                for r in shown:
                    row = [str(r.get("vessel_name") or "Not available"), str(r.get("imo") or "Not available"), str(r.get("pool") or "Not available")]
                    if wants_market_type:
                        row.append(str(r.get("market_type") or "Not available"))
                    body_rows.append(row)
            elif wants_operating:
                headers.extend(["Operating", "Market Type"])
                for r in shown:
                    body_rows.append([
                        str(r.get("vessel_name") or "Not available"),
                        str(r.get("imo") or "Not available"),
                        "Yes" if r.get("is_operating") is True else ("No" if r.get("is_operating") is False else "Not available"),
                        str(r.get("market_type") or "Not available"),
                    ])
            else:
                headers.extend(["Operating", "Scrubber", "Hire Rate"])
                for r in shown:
                    body_rows.append([
                        str(r.get("vessel_name") or "Not available"),
                        str(r.get("imo") or "Not available"),
                        "Yes" if r.get("is_operating") is True else ("No" if r.get("is_operating") is False else "Not available"),
                        str(r.get("scrubber") or "Not available"),
                        _fmt_num(r.get("hire_rate"), money=True),
                    ])

            if not shown:
                answer = "No vessels matched the requested metadata filter."
            else:
                if wants_hire_rate and shown:
                    lead = f"The vessel with the {'highest' if descending and not ascending else 'lowest'} hire rate is **{shown[0].get('vessel_name') or 'Not available'}**."
                elif wants_ballast_speed and shown:
                    lead = f"The vessel with the highest default ballast speed is **{shown[0].get('vessel_name') or 'Not available'}**."
                elif wants_laden_speed and shown:
                    lead = f"The vessel with the highest default laden speed is **{shown[0].get('vessel_name') or 'Not available'}**."
                elif wants_contract and shown:
                    lead = f"The vessel with the longest current contract duration is **{shown[0].get('vessel_name') or 'Not available'}**."
                elif wants_operating:
                    lead = f"There are **{len(filtered)}** vessels currently operating; showing the first {len(shown)}."
                elif wants_non_scrubber:
                    lead = f"There are **{len(filtered)}** non-scrubber vessels; showing the first {len(shown)}."
                elif wants_scrubber:
                    lead = f"There are **{len(filtered)}** scrubber-fitted vessels; showing the first {len(shown)}."
                elif wants_short_pool:
                    lead = f"There are **{len(filtered)}** vessels in the short pool; showing the first {len(shown)}."
                elif wants_long_pool:
                    lead = f"There are **{len(filtered)}** vessels in the long pool; showing the first {len(shown)}."
                else:
                    lead = f"Showing **{len(shown)}** vessels that match the requested metadata view."
                answer = lead + "\n\n" + _build_table(headers, body_rows)

        current_trace = []
        if isinstance(state.get("artifacts"), dict) and isinstance((state.get("artifacts") or {}).get("trace"), list):
            current_trace = (state.get("artifacts") or {}).get("trace") or []

        merged_rows = shown if shown else filtered[:limit]
        merged = {
            "finance": {"mode": None, "rows": []},
            "ops": {"mode": None, "rows": []},
            "mongo": mongo_safe if isinstance(mongo_safe, dict) else {},
            "artifacts": {"intent_key": intent_key, "slots": slots, "merged_rows": merged_rows, "trace": current_trace},
            "dynamic_sql_used": False,
            "dynamic_sql_agents": [],
        }

        state["merged"] = merged
        state["answer"] = answer
        state["slots"] = slots
        state["mongo"] = mongo_safe
        state["finance"] = {"mode": None, "rows": []}
        state["ops"] = {"mode": None, "rows": []}
        state["data"]["mongo"] = mongo_safe
        state["data"]["finance"] = {"mode": None, "rows": []}
        state["data"]["ops"] = {"mode": None, "rows": []}
        state["data"]["artifacts"] = {"intent_key": intent_key, "slots": slots, "merged_rows": merged_rows, "trace": current_trace}
        return state

        # =========================================================
        # Vessel metadata: Mongo-only deterministic summary
        # =========================================================

    # =========================================================
    # Vessel metadata: Mongo-only deterministic summary
    # =========================================================
    if intent_key == "vessel.metadata":
        def _extract_passage_types(doc: Dict[str, Any]) -> list[str]:
            passage_types: list[str] = []
            cps = doc.get("consumption_profiles")
            if isinstance(cps, list):
                seen_pt = set()
                for cp in cps:
                    if not isinstance(cp, dict):
                        continue
                    pp = cp.get("passageProfile")
                    if not isinstance(pp, list):
                        continue
                    for p in pp:
                        if not isinstance(p, dict):
                            continue
                        pt = p.get("passageType")
                        if pt is None:
                            continue
                        spt = str(pt).strip()
                        if spt and spt not in seen_pt:
                            seen_pt.add(spt)
                            passage_types.append(spt)
            return passage_types

        def _extract_tags(doc: Dict[str, Any]) -> list[str]:
            tags: list[str] = []
            raw_tags = doc.get("tags")
            if isinstance(raw_tags, list):
                for t in raw_tags:
                    if isinstance(t, dict):
                        v = t.get("value")
                        if v is not None:
                            sv = str(v).strip()
                            if sv:
                                tags.append(sv)
            return tags[:8]

        def _extract_passage_consumption_rows(doc: Dict[str, Any]) -> list[Dict[str, Any]]:
            rows: list[Dict[str, Any]] = []
            cps = doc.get("consumption_profiles")
            if not isinstance(cps, list):
                return rows
            for cp in cps:
                if not isinstance(cp, dict):
                    continue
                profile_name = cp.get("profileName")
                pp = cp.get("passageProfile")
                if not isinstance(pp, list):
                    continue
                for p in pp:
                    if not isinstance(p, dict):
                        continue
                    ptype = p.get("passageType")
                    cons = p.get("consumption")
                    if not isinstance(cons, list):
                        continue
                    for c in cons:
                        if not isinstance(c, dict):
                            continue
                        rows.append(
                            {
                                "profile_name": profile_name,
                                "passage_type": str(ptype).strip() if ptype is not None else None,
                                "speed": c.get("speed"),
                                "ifo": c.get("ifo"),
                                "mgo": c.get("mgo"),
                                "rpm": c.get("rpm"),
                                "is_default": bool(c.get("isDefault")) if c.get("isDefault") is not None else False,
                            }
                        )
            return rows

        def _extract_non_passage_consumption(doc: Dict[str, Any]) -> Dict[str, Any] | None:
            cps = doc.get("consumption_profiles")
            if not isinstance(cps, list):
                return None
            for cp in cps:
                if not isinstance(cp, dict):
                    continue
                npp = cp.get("nonPassageProfile")
                if not isinstance(npp, list):
                    continue
                for np in npp:
                    if not isinstance(np, dict):
                        continue
                    cons = np.get("consumption")
                    if isinstance(cons, list) and cons and isinstance(cons[0], dict):
                        return cons[0]
            return None

        records: list[Dict[str, Any]] = []
        projection = cfg.get("mongo_projection")
        trace_queries: list[Dict[str, Any]] = []

        # Path A: direct vessel anchor (name/imo)
        if slots.get("vessel_name") or slots.get("imo"):
            try:
                res = self.mongo_agent.run(
                    intent_key=cfg.get("mongo_intent", "entity.vessel"),
                    slots=slots,
                    projection=projection,
                    session_context=session_context,
                )
                mongo_safe = _json_safe(res)
            except Exception as e:
                _dprint(f"⚠️  Mongo failed (vessel.metadata): {e}")
                mongo_safe = {}

            doc = (mongo_safe or {}).get("document") if isinstance(mongo_safe, dict) else {}
            if not isinstance(doc, dict):
                doc = {}
            if not doc and slots.get("vessel_name"):
                try:
                    doc = self.mongo_agent.adapter.fetch_vessel_by_name(str(slots.get("vessel_name")), projection=projection)
                except Exception:
                    doc = {}
                if not isinstance(doc, dict):
                    doc = {}
            if isinstance(doc, dict) and doc:
                records.append({
                    "voyage_number": slots.get("voyage_number"),
                    "vessel_name": doc.get("name") or slots.get("vessel_name"),
                    "imo": doc.get("imo") or slots.get("imo"),
                    "doc": doc,
                })
            resolved_imo = (
                (mongo_safe or {}).get("anchor_id")
                if isinstance(mongo_safe, dict)
                else None
            ) or doc.get("imo") or slots.get("imo")
            trace_queries.append(
                {
                    "collection": "vessels",
                    "filter": {"imo": str(resolved_imo)} if resolved_imo not in (None, "") else {},
                    "projection": self._mongo_projection_for_trace(projection),
                    "sort": None,
                    "limit": 1,
                    "pipeline": None,
                }
            )
        else:
            # Path B: small voyage-number list (1..3) -> resolve vessel(s) via Mongo voyages
            mongo_safe = {"mode": "mongo_metadata", "ok": True, "rows": []}
            vnums = slots.get("voyage_numbers")
            if not isinstance(vnums, list):
                vn = slots.get("voyage_number")
                if vn in (None, "", [], {}):
                    lfs = (session_context or {}).get("last_focus_slots") if isinstance(session_context, dict) else None
                    if isinstance(lfs, dict):
                        vn = lfs.get("voyage_number")
                vnums = [vn] if vn not in (None, "", [], {}) else []
            unique_vnums: list[int] = []
            for v in vnums:
                try:
                    iv = int(v)
                    if iv not in unique_vnums:
                        unique_vnums.append(iv)
                except Exception:
                    continue
            unique_vnums = unique_vnums[:3]

            for vnum in unique_vnums:
                try:
                    batch = self.mongo_agent.adapter.list_voyages_by_number(
                        vnum,
                        projection=get_mongo_projection("voyage_identity"),
                        limit=get_mongo_limit("voyage_identity_batch", 40),
                    )
                except Exception:
                    batch = []
                narrowed = narrow_voyage_rows_by_entity_slots(batch, slots)
                for vdoc in narrowed:
                    if not isinstance(vdoc, dict) or not vdoc:
                        continue

                    imo = vdoc.get("vesselImo")
                    vessel_name = vdoc.get("vesselName")
                    mdoc = None
                    if imo not in (None, ""):
                        try:
                            mdoc = self.mongo_agent.adapter.fetch_vessel(str(imo), projection=projection)
                        except Exception:
                            mdoc = None
                    if not isinstance(mdoc, dict):
                        mdoc = {}
                    if not mdoc:
                        # Keep minimal voyage-resolved identity even if vessel metadata is missing.
                        mdoc = {"imo": imo, "name": vessel_name}

                    records.append({
                        "voyage_number": vdoc.get("voyageNumber"),
                        "vessel_name": mdoc.get("name") or vessel_name,
                        "imo": mdoc.get("imo") or imo,
                        "doc": mdoc,
                    })
                    mongo_safe["rows"].append(vdoc)

            voyage_filters = [str(v) for v in unique_vnums if v not in (None, "")]
            if voyage_filters:
                trace_queries.append(
                    {
                        "collection": "voyages",
                        "filter": {"voyageNumber": {"$in": voyage_filters}},
                        "projection": self._mongo_projection_for_trace(
                            get_mongo_projection("voyage_identity")
                        ),
                        "sort": None,
                        "limit": len(voyage_filters),
                        "pipeline": None,
                    }
                )
            vessel_imos = []
            for rec in records:
                imo = rec.get("imo")
                if imo in (None, ""):
                    continue
                imo_s = str(imo)
                if imo_s not in vessel_imos:
                    vessel_imos.append(imo_s)
            if vessel_imos:
                trace_queries.append(
                    {
                        "collection": "vessels",
                        "filter": {"imo": {"$in": vessel_imos}},
                        "projection": self._mongo_projection_for_trace(projection),
                        "sort": None,
                        "limit": len(vessel_imos),
                        "pipeline": None,
                    }
                )

        ql = (user_input or "").lower()
        ask_identity = any(k in ql for k in ("identity", "imo", "vessel id", "account code", "who is vessel", "vessel details", "name"))
        ask_hire_rate = any(k in ql for k in ("hire rate", "hirerate", "hire_rate", "hire-rate"))
        ask_scrubber = "scrubber" in ql
        ask_market_type = "market type" in ql
        ask_operating = any(k in ql for k in ("is operating", "is vessel operating", "operational", "operating status", "is it operating", "is it operational"))
        ask_commercial = any((ask_hire_rate, ask_scrubber, ask_market_type, ask_operating))
        ask_passage_types = any(k in ql for k in ("passage type", "passage types"))
        ask_passage_consumption = any(k in ql for k in ("consumption profile", "consumption profiles", "consumption", "speed", "ifo", "mgo", "rpm"))
        ask_default_consumption = any(k in ql for k in ("default consumption", "default speed", "defaults", "isdefault", "default"))
        ask_ballast = "ballast" in ql
        ask_laden = "laden" in ql
        ask_non_passage_consumption = any(k in ql for k in ("non passage", "non-passage", "idle", "load", "discharge", "heat", "clean", "inert"))
        if ask_non_passage_consumption and not (ask_ballast or ask_laden):
            ask_passage_consumption = False
        ask_tags = any(k in ql for k in ("tags", "segment", "pool", "commercial tag", "sanction"))
        ask_contract_history = any(k in ql for k in ("contract", "owner", "duration", "cp date", "delivery"))
        ask_contract_owner = "owner" in ql
        ask_contract_duration = "duration" in ql
        ask_contract_cp_date = "cp date" in ql
        ask_contract_delivery = "delivery" in ql
        ask_contract_history_list = any(k in ql for k in ("contract history", "show contract", "list contract", "contracts"))
        ask_contract_fields_only = (
            ask_contract_history
            and not ask_contract_history_list
            and any((ask_contract_owner, ask_contract_duration, ask_contract_cp_date, ask_contract_delivery))
        )
        ask_extracted_at = "extracted at" in ql
        has_specific = any(
            (
                ask_identity,
                ask_commercial,
                ask_passage_types,
                ask_passage_consumption,
                ask_default_consumption,
                ask_ballast,
                ask_laden,
                ask_non_passage_consumption,
                ask_tags,
                ask_contract_history,
                ask_extracted_at,
            )
        )

        if not records:
            answer = _router_fallback("voyage_or_vessel_metadata_empty")
        elif len(records) == 1:
            rec = records[0]
            doc = rec.get("doc") if isinstance(rec.get("doc"), dict) else {}
            vessel_name = rec.get("vessel_name") or "this vessel"
            imo = rec.get("imo")
            hire_rate = doc.get("hireRate")
            scrubber = doc.get("scrubber")
            market_type = doc.get("marketType")
            account_code = doc.get("accountCode")
            is_operating = doc.get("isVesselOperating")
            passage_types = _extract_passage_types(doc)
            tags = _extract_tags(doc)
            passage_rows = _extract_passage_consumption_rows(doc)
            non_passage = _extract_non_passage_consumption(doc)
            contract_list = (doc.get("contract_history") or {}).get("list")
            current_contract = None
            if isinstance(contract_list, list):
                for item in contract_list:
                    if isinstance(item, dict) and item.get("isCurrent") is True:
                        current_contract = item
                        break
                if current_contract is None:
                    for item in contract_list:
                        if isinstance(item, dict):
                            current_contract = item
                            break
            contract_count = len(contract_list) if isinstance(contract_list, list) else None
            extracted_at = doc.get("extracted_at")

            answer_lines = ["### Summary"]
            answer_lines.append(f"- **Vessel**: {vessel_name}" + (f" (IMO: {imo})" if imo else ""))
            if has_specific:
                if ask_identity:
                    answer_lines.append(f"- **Vessel ID**: {doc.get('vesselId')}" if doc.get("vesselId") not in (None, "") else "- **Vessel ID**: Not available")
                    answer_lines.append(f"- **Account code**: {account_code}" if account_code not in (None, "") else "- **Account code**: Not available")
                if ask_commercial:
                    if ask_hire_rate:
                        if hire_rate is not None:
                            try:
                                answer_lines.append(f"- **Hire rate**: ${float(hire_rate):,.2f}")
                            except Exception:
                                answer_lines.append(f"- **Hire rate**: {hire_rate}")
                        else:
                            answer_lines.append("- **Hire rate**: Not available")
                    if ask_operating:
                        if is_operating is not None:
                            answer_lines.append(f"- **Operating**: {'Yes' if bool(is_operating) else 'No'}")
                        else:
                            answer_lines.append("- **Operating**: Not available")
                    if ask_scrubber:
                        answer_lines.append(f"- **Scrubber**: {scrubber}" if scrubber not in (None, "") else "- **Scrubber**: Not available")
                    if ask_market_type:
                        answer_lines.append(f"- **Market type**: {market_type}" if market_type not in (None, "") else "- **Market type**: Not available")
                if ask_passage_types:
                    answer_lines.append(f"- **Passage types**: {', '.join(passage_types)}" if passage_types else "- **Passage types**: Not available")
                if ask_passage_consumption or ask_default_consumption or ask_ballast or ask_laden:
                    filtered_rows = list(passage_rows)
                    if ask_default_consumption:
                        filtered_rows = [r for r in filtered_rows if r.get("is_default")]
                    if ask_ballast and not ask_laden:
                        filtered_rows = [r for r in filtered_rows if str(r.get("passage_type") or "").lower() == "ballast"]
                    if ask_laden and not ask_ballast:
                        filtered_rows = [r for r in filtered_rows if str(r.get("passage_type") or "").lower() == "laden"]
                    answer_lines.append("")
                    answer_lines.append("### Passage consumption")
                    if filtered_rows:
                        for r in filtered_rows[:8]:
                            ptype = r.get("passage_type") or "Unknown"
                            speed = r.get("speed")
                            ifo = r.get("ifo")
                            mgo = r.get("mgo")
                            dmark = " (default)" if r.get("is_default") else ""
                            answer_lines.append(f"- **{ptype}**{dmark}: speed={speed}, IFO={ifo}, MGO={mgo}")
                    else:
                        answer_lines.append("- Not available")
                if ask_non_passage_consumption:
                    answer_lines.append("")
                    answer_lines.append("### Non-passage consumption")
                    if isinstance(non_passage, dict) and non_passage:
                        keys = (
                            "ifoLoad", "ifoDischarge", "ifoIdle", "ifoHeat", "ifoClean", "ifoInert",
                            "mgoLoad", "mgoDischarge", "mgoIdle", "mgoHeat", "mgoClean", "mgoInert",
                        )
                        for k in keys:
                            if k in non_passage:
                                answer_lines.append(f"- **{k}**: {non_passage.get(k)}")
                    else:
                        answer_lines.append("- Not available")
                if ask_tags:
                    answer_lines.append(f"- **Tags**: {', '.join(tags)}" if tags else "- **Tags**: Not available")
                if ask_contract_history:
                    if ask_contract_fields_only:
                        if isinstance(current_contract, dict) and current_contract:
                            if ask_contract_owner:
                                answer_lines.append(f"- **Owner**: {current_contract.get('owner')}" if current_contract.get("owner") not in (None, "") else "- **Owner**: Not available")
                            if ask_contract_duration:
                                dur = current_contract.get("duration")
                                dtyp = current_contract.get("durationType")
                                if dur not in (None, ""):
                                    answer_lines.append(f"- **Duration**: {dur} {dtyp}".strip())
                                else:
                                    answer_lines.append("- **Duration**: Not available")
                            if ask_contract_cp_date:
                                answer_lines.append(f"- **CP date**: {current_contract.get('cpDate')}" if current_contract.get("cpDate") not in (None, "") else "- **CP date**: Not available")
                            if ask_contract_delivery:
                                answer_lines.append(f"- **Delivery**: {current_contract.get('deliveryDatetime')}" if current_contract.get("deliveryDatetime") not in (None, "") else "- **Delivery**: Not available")
                        else:
                            if ask_contract_owner:
                                answer_lines.append("- **Owner**: Not available")
                            if ask_contract_duration:
                                answer_lines.append("- **Duration**: Not available")
                            if ask_contract_cp_date:
                                answer_lines.append("- **CP date**: Not available")
                            if ask_contract_delivery:
                                answer_lines.append("- **Delivery**: Not available")
                    elif isinstance(contract_list, list) and contract_list:
                        answer_lines.append("")
                        answer_lines.append("### Contract history")
                        for c in contract_list[:5]:
                            if not isinstance(c, dict):
                                continue
                            cn = c.get("contractNumber")
                            owner = c.get("owner")
                            dur = c.get("duration")
                            dtyp = c.get("durationType")
                            cpd = c.get("cpDate")
                            dd = c.get("deliveryDatetime")
                            answer_lines.append(f"- Contract {cn} | Owner: {owner} | Duration: {dur} {dtyp} | CP Date: {cpd} | Delivery: {dd}")
                    else:
                        answer_lines.append("- **Contract history**: Not available")
                if ask_extracted_at:
                    answer_lines.append(f"- **Extracted at**: {extracted_at}" if extracted_at not in (None, "") else "- **Extracted at**: Not available")
            else:
                answer_lines.append(f"- **Passage types**: {', '.join(passage_types)}" if passage_types else "- **Passage types**: Not available")
                answer_lines.append("")
                answer_lines.append("### Metadata snapshot")
                if hire_rate is not None:
                    try:
                        answer_lines.append(f"- **Hire rate**: ${float(hire_rate):,.2f}")
                    except Exception:
                        answer_lines.append(f"- **Hire rate**: {hire_rate}")
                if scrubber not in (None, ""):
                    answer_lines.append(f"- **Scrubber**: {scrubber}")
                if market_type not in (None, ""):
                    answer_lines.append(f"- **Market type**: {market_type}")
                if account_code not in (None, ""):
                    answer_lines.append(f"- **Account code**: {account_code}")
                if is_operating is not None:
                    answer_lines.append(f"- **Operating**: {'Yes' if bool(is_operating) else 'No'}")
                if tags:
                    answer_lines.append(f"- **Tags**: {', '.join(tags)}")
                if extracted_at not in (None, ""):
                    answer_lines.append(f"- **Extracted at**: {extracted_at}")

            answer = "\n".join(answer_lines)
        else:
            # Multi-voyage metadata response (query-specific, compact).
            max_render_rows = 20
            render_records = records[:max_render_rows]
            answer_lines = [
                "### Summary",
                f"- Found metadata for {len(records)} voyage-linked vessel records.",
                (
                    f"- Showing first {len(render_records)} records for readability."
                    if len(records) > max_render_rows
                    else "- Showing all matched records."
                ),
                "",
                "### Results",
            ]
            for rec in render_records:
                doc = rec.get("doc") if isinstance(rec.get("doc"), dict) else {}
                vessel_name = rec.get("vessel_name") or "Unknown"
                imo = rec.get("imo")
                vnum = rec.get("voyage_number")
                prefix = f"- Voyage **{vnum}** - **{vessel_name}**" + (f" (IMO: {imo})" if imo else "")
                details: list[str] = []
                if ask_identity:
                    if doc.get("vesselId") not in (None, ""):
                        details.append(f"VesselId {doc.get('vesselId')}")
                    if doc.get("accountCode") not in (None, ""):
                        details.append(f"Account {doc.get('accountCode')}")
                if ask_commercial:
                    hr = doc.get("hireRate")
                    if ask_hire_rate and hr is not None:
                        try:
                            details.append(f"Hire rate ${float(hr):,.2f}")
                        except Exception:
                            details.append(f"Hire rate {hr}")
                    if ask_operating and doc.get("isVesselOperating") is not None:
                        details.append(f"Operating {'Yes' if bool(doc.get('isVesselOperating')) else 'No'}")
                    if ask_scrubber and doc.get("scrubber") not in (None, ""):
                        details.append(f"Scrubber {doc.get('scrubber')}")
                    if ask_market_type and doc.get("marketType") not in (None, ""):
                        details.append(f"Market {doc.get('marketType')}")
                if ask_passage_types:
                    pts = _extract_passage_types(doc)
                    details.append(f"Passage types {', '.join(pts)}" if pts else "Passage types Not available")
                if ask_passage_consumption or ask_default_consumption or ask_ballast or ask_laden:
                    p_rows = _extract_passage_consumption_rows(doc)
                    if ask_default_consumption:
                        p_rows = [r for r in p_rows if r.get("is_default")]
                    if ask_ballast and not ask_laden:
                        p_rows = [r for r in p_rows if str(r.get("passage_type") or "").lower() == "ballast"]
                    if ask_laden and not ask_ballast:
                        p_rows = [r for r in p_rows if str(r.get("passage_type") or "").lower() == "laden"]
                    if p_rows:
                        r0 = p_rows[0]
                        details.append(f"{r0.get('passage_type')} speed {r0.get('speed')} IFO {r0.get('ifo')} MGO {r0.get('mgo')}" + (" default" if r0.get("is_default") else ""))
                if ask_non_passage_consumption:
                    np = _extract_non_passage_consumption(doc)
                    if isinstance(np, dict) and np:
                        details.append("Non-passage available")
                    else:
                        details.append("Non-passage Not available")
                if ask_tags:
                    tgs = _extract_tags(doc)
                    if tgs:
                        details.append(f"Tags {', '.join(tgs[:4])}")
                if ask_contract_history:
                    cl = (doc.get("contract_history") or {}).get("list")
                    if isinstance(cl, list):
                        details.append(f"Contracts {len(cl)}")
                if ask_extracted_at and doc.get("extracted_at") not in (None, ""):
                    details.append(f"Extracted at {doc.get('extracted_at')}")
                if not details:
                    # No specific asks or missing fields: keep a small default.
                    hr = doc.get("hireRate")
                    pts = _extract_passage_types(doc)
                    if hr is not None:
                        try:
                            details.append(f"Hire rate ${float(hr):,.2f}")
                        except Exception:
                            details.append(f"Hire rate {hr}")
                    if pts:
                        details.append(f"Passage types {', '.join(pts)}")
                answer_lines.append(prefix + (" - " + "; ".join(details) if details else ""))
            if len(records) > max_render_rows:
                answer_lines.append(
                    f"- ... {len(records) - max_render_rows} additional matched records omitted for brevity."
                )
            answer = "\n".join(answer_lines)

        current_trace = []
        if isinstance(state.get("artifacts"), dict) and isinstance((state.get("artifacts") or {}).get("trace"), list):
            current_trace = (state.get("artifacts") or {}).get("trace") or []

        merged = {
            "finance": {"mode": None, "rows": []},
            "ops": {"mode": None, "rows": []},
            "mongo": mongo_safe if isinstance(mongo_safe, dict) else {},
            "artifacts": {"intent_key": intent_key, "slots": slots, "trace": current_trace},
            "dynamic_sql_used": False,
            "dynamic_sql_agents": [],
        }

        if trace_queries:
            self._trace_single_mongo_query(
                state,
                intent_key=intent_key,
                operation="metadata_lookup",
                collection=str(trace_queries[0].get("collection") or "vessels"),
                filt=trace_queries[0].get("filter") if isinstance(trace_queries[0].get("filter"), dict) else {},
                projection=trace_queries[0].get("projection") if isinstance(trace_queries[0].get("projection"), dict) else projection,
                limit=trace_queries[0].get("limit") if isinstance(trace_queries[0].get("limit"), int) else None,
                rows=len(records),
                mongo_queries=trace_queries,
                summary="Mongo: resolved vessel metadata from voyage/vessel anchors and fetched the requested vessel fields.",
            )

        state["merged"] = merged
        state["answer"] = answer
        state["slots"] = slots
        state["mongo"] = mongo_safe
        state["finance"] = {"mode": None, "rows": []}
        state["ops"] = {"mode": None, "rows": []}
        state["data"]["mongo"] = mongo_safe
        state["data"]["finance"] = {"mode": None, "rows": []}
        state["data"]["ops"] = {"mode": None, "rows": []}
        state["data"]["artifacts"] = {"intent_key": intent_key, "slots": slots, "trace": current_trace}
        persisted_slots = self._build_persisted_slots(base=(session_context.get("slots") or {}), updates=slots)
        meta_focus = records[0] if len(records) == 1 and isinstance(records[0], dict) else None
        focus_slots = {}
        if isinstance(meta_focus, dict):
            focus_slots = self._row_focus_slots(
                {
                    "voyage_number": meta_focus.get("voyage_number"),
                    "vessel_name": meta_focus.get("vessel_name"),
                    "imo": meta_focus.get("imo"),
                }
            )
        latest_result_rows: list[Dict[str, Any]] = []
        for rec in records[:200]:
            if not isinstance(rec, dict):
                continue
            doc = rec.get("doc") if isinstance(rec.get("doc"), dict) else {}
            item = {
                "voyage_number": rec.get("voyage_number"),
                "vessel_name": rec.get("vessel_name"),
                "imo": rec.get("imo"),
                "account_code": doc.get("accountCode"),
                "market_type": doc.get("marketType"),
                "operating": (
                    bool(doc.get("isVesselOperating"))
                    if doc.get("isVesselOperating") is not None
                    else None
                ),
                "passage_types": _extract_passage_types(doc),
            }
            item = {k: v for k, v in item.items() if v not in (None, "", [], {})}
            if item:
                latest_result_rows.append(item)
        latest_result_set = {
            "source_intent": intent_key,
            "rows": latest_result_rows,
            "meta": {
                "source_intent": intent_key,
                "available_metrics": [],
                "primary_metric": None,
            },
        }
        self.redis.save_session(
            state["session_id"],
            {
                **session_context,
                "last_intent": intent_key,
                "last_intent_key": intent_key,
                "memory_slots": self._extract_memory_slots(persisted_slots),
                "param_slots": self._extract_param_slots(persisted_slots),
                "slots": persisted_slots,
                "last_user_input": user_input,
                "last_result_set": latest_result_set,
                "last_focus_slots": focus_slots,
            },
        )
        state["session_ctx"] = {
            **(session_context or {}),
            "last_intent": intent_key,
            "last_intent_key": intent_key,
            "memory_slots": self._extract_memory_slots(persisted_slots),
            "param_slots": self._extract_param_slots(persisted_slots),
            "slots": persisted_slots,
            "last_user_input": user_input,
            "last_result_set": latest_result_set,
            "last_focus_slots": focus_slots,
        }
        return state

        # =========================================================
        # Voyage metadata: Mongo-only deterministic summary
        # =========================================================

    # =========================================================
    # Voyage metadata: Mongo-only deterministic summary
    # =========================================================
    if intent_key == "voyage.metadata":
        projection = cfg.get("mongo_projection") or get_mongo_projection("voyage_metadata_detail")
        records: list[Dict[str, Any]] = []
        mongo_rows: list[Dict[str, Any]] = []
        trace_queries: list[Dict[str, Any]] = []

        # Mongo-only metadata still resolves one canonical voyage → one vessel.
        try:
            canon_ctx = self.mongo_agent.fetch_full_voyage_context(
                voyage_number=slots.get("voyage_number"),
                voyage_id=slots.get("voyage_id"),
                entity_slots=slots,
            )
        except Exception:
            canon_ctx = {}
        if isinstance(canon_ctx, dict) and canon_ctx.get("voyage_id"):
            slots["voyage_id"] = str(canon_ctx.get("voyage_id"))
            if canon_ctx.get("vessel_imo"):
                slots["imo"] = str(canon_ctx.get("vessel_imo"))
                slots["vessel_imo"] = str(canon_ctx.get("vessel_imo"))
            if canon_ctx.get("vessel_name"):
                slots["vessel_name"] = str(canon_ctx.get("vessel_name"))

        # Primary anchors: voyage_id / voyage_number(s)
        if slots.get("voyage_id") not in (None, ""):
            voyage_id = str(slots.get("voyage_id"))
            trace_queries.append(
                {
                    "collection": "voyages",
                    "filter": {"voyageId": voyage_id},
                    "projection": self._mongo_projection_for_trace(projection),
                    "sort": None,
                    "limit": 1,
                    "pipeline": None,
                }
            )
            try:
                doc = self.mongo_agent.adapter.fetch_voyage(voyage_id, projection=projection)
            except Exception:
                doc = None
            if isinstance(doc, dict) and doc:
                records.append(doc)
                mongo_rows.append(doc)

        vnums = slots.get("voyage_numbers")
        if not isinstance(vnums, list):
            vn = slots.get("voyage_number")
            vnums = [vn] if vn not in (None, "", [], {}) else []
        unique_vnums: list[int] = []
        for v in vnums:
            try:
                iv = int(v)
                if iv not in unique_vnums:
                    unique_vnums.append(iv)
            except Exception:
                continue
        unique_vnums = unique_vnums[:3]
        if unique_vnums:
            trace_queries.append(
                {
                    "collection": "voyages",
                    "filter": {"voyageNumber": {"$in": [str(v) for v in unique_vnums]}},
                    "projection": self._mongo_projection_for_trace(projection),
                    "sort": None,
                    "limit": len(unique_vnums),
                    "pipeline": None,
                }
            )
        for vnum in unique_vnums:
            if slots.get("voyage_id") not in (None, ""):
                break
            try:
                batch = self.mongo_agent.adapter.list_voyages_by_number(
                    vnum,
                    projection=projection,
                    limit=get_mongo_limit("voyage_metadata_detail_batch", 40),
                )
            except Exception:
                batch = []
            narrowed = narrow_voyage_rows_by_entity_slots(batch, slots)
            if len(narrowed) > 1 and not (slots.get("vessel_name") or slots.get("imo") or slots.get("vessel_imo")):
                try:
                    picked = self.mongo_agent.fetch_full_voyage_context(
                        voyage_number=vnum,
                        voyage_id=slots.get("voyage_id"),
                        entity_slots=slots,
                    )
                except Exception:
                    picked = {}
                if isinstance(picked, dict) and picked.get("voyage_id"):
                    try:
                        one_doc = self.mongo_agent.adapter.fetch_voyage(
                            str(picked.get("voyage_id")),
                            projection=projection,
                        )
                    except Exception:
                        one_doc = None
                    if isinstance(one_doc, dict) and one_doc:
                        narrowed = [one_doc]
                        slots["voyage_id"] = str(picked.get("voyage_id"))
            for doc in narrowed[:1]:
                if isinstance(doc, dict) and doc:
                    records.append(doc)
                    mongo_rows.append(doc)

        # De-duplicate by voyageId if present.
        seen_vid: set[str] = set()
        deduped: list[Dict[str, Any]] = []
        for r in records:
            vid = str(r.get("voyageId") or "")
            if vid and vid in seen_vid:
                continue
            if vid:
                seen_vid.add(vid)
            deduped.append(r)
        records = deduped

        if not records:
            answer = _router_fallback("voyage_metadata_empty")
        else:
            if len(records) == 1:
                sections = select_voyage_sections(user_input, records[0], session_context)
            else:
                sections = {
                    "voyages": [select_voyage_sections(user_input, d, session_context) for d in records]
                }

            context_json = json.dumps(sections, default=str, indent=2)
            system_prompt = get_graph_router_voyage_metadata_answer_system_prompt()
            user_prompt = f"""Voyage Data:
        {context_json}

        Question: {user_input}"""
            try:
                answer = self.llm._call_with_retry(
                    system=system_prompt,
                    user=user_prompt,
                    operation="voyage_metadata_answer",
                    return_string=True,
                )
            except Exception:
                answer = ""
            if not answer:
                answer = _router_fallback("voyage_metadata_formatting_failed")

        mongo_safe = {"mode": "mongo_metadata", "ok": True, "rows": mongo_rows}
        current_trace = []
        if isinstance(state.get("artifacts"), dict) and isinstance((state.get("artifacts") or {}).get("trace"), list):
            current_trace = (state.get("artifacts") or {}).get("trace") or []

        merged = {
            "finance": {"mode": None, "rows": []},
            "ops": {"mode": None, "rows": []},
            "mongo": mongo_safe,
            "artifacts": {"intent_key": intent_key, "slots": slots, "trace": current_trace},
            "dynamic_sql_used": False,
            "dynamic_sql_agents": [],
        }
        if trace_queries:
            self._trace_single_mongo_query(
                state,
                intent_key=intent_key,
                operation="voyage_metadata_lookup",
                collection=str(trace_queries[0].get("collection") or "voyages"),
                filt=trace_queries[0].get("filter") if isinstance(trace_queries[0].get("filter"), dict) else {},
                projection=trace_queries[0].get("projection") if isinstance(trace_queries[0].get("projection"), dict) else projection,
                limit=trace_queries[0].get("limit") if isinstance(trace_queries[0].get("limit"), int) else None,
                rows=len(records),
                mongo_queries=trace_queries,
                summary="Mongo: fetched voyage metadata from the `voyages` collection using the resolved voyage anchors.",
            )
        state["merged"] = merged
        state["answer"] = answer
        state["slots"] = slots
        state["mongo"] = mongo_safe
        state["finance"] = {"mode": None, "rows": []}
        state["ops"] = {"mode": None, "rows": []}
        state["data"]["mongo"] = mongo_safe
        state["data"]["finance"] = {"mode": None, "rows": []}
        state["data"]["ops"] = {"mode": None, "rows": []}
        state["data"]["artifacts"] = {"intent_key": intent_key, "slots": slots, "trace": current_trace}
        persisted_slots = self._build_persisted_slots(base=(session_context.get("slots") or {}), updates=slots)
        focus_slots = {}
        if len(records) == 1 and isinstance(records[0], dict):
            focus_slots = self._row_focus_slots(
                {
                    "voyage_number": records[0].get("voyageNumber"),
                    "voyage_id": records[0].get("voyageId"),
                    "vessel_name": records[0].get("vesselName"),
                }
            )
        self.redis.save_session(
            state["session_id"],
            {
                **session_context,
                "last_intent": intent_key,
                "last_intent_key": intent_key,
                "memory_slots": self._extract_memory_slots(persisted_slots),
                "param_slots": self._extract_param_slots(persisted_slots),
                "slots": persisted_slots,
                "last_user_input": user_input,
                "last_focus_slots": focus_slots,
            },
        )
        state["session_ctx"] = {
            **(session_context or {}),
            "last_intent": intent_key,
            "last_intent_key": intent_key,
            "memory_slots": self._extract_memory_slots(persisted_slots),
            "param_slots": self._extract_param_slots(persisted_slots),
            "slots": persisted_slots,
            "last_user_input": user_input,
            "last_focus_slots": focus_slots,
        }
        return state

    # ---- Standard Execution for generic single intents ----

    # Mongo
    if cfg.get("needs", {}).get("mongo"):
        try:
            res = self.mongo_agent.run(
                intent_key=cfg.get("mongo_intent", "entity.auto"),
                slots=slots,
                projection=cfg.get("mongo_projection"),
                session_context=session_context,
            )
            state["mongo"] = _json_safe(res)

            # Update session with anchor
            if isinstance(state["mongo"], dict):
                anchor_type = state["mongo"].get("anchor_type")
                anchor_id = state["mongo"].get("anchor_id")
                
                if anchor_type and anchor_id:
                    persisted_slots = self._build_persisted_slots(base=(session_context.get("slots") or {}), updates=slots)
                    self.redis.save_session(
                        state["session_id"],
                        {
                            **session_context,
                            "anchor_type": anchor_type,
                            "anchor_id": anchor_id,
                            "memory_slots": self._extract_memory_slots(persisted_slots),
                            "param_slots": self._extract_param_slots(persisted_slots),
                            "slots": persisted_slots,
                            "last_user_input": state["user_input"],
                        },
                    )
                    state["session_ctx"] = self.redis.load_session(state["session_id"])

                # Extract voyage_id or imo from mongo result
                doc = (state["mongo"] or {}).get("document") or {}
                if "voyageId" in doc and "voyage_id" not in slots:
                    slots["voyage_id"] = doc["voyageId"]
                if "imo" in doc and "imo" not in slots:
                    slots["imo"] = doc["imo"]
        except Exception as e:
            _dprint(f"⚠️  Mongo failed: {e}")
            state["mongo"] = None

    # Finance
    if cfg.get("needs", {}).get("finance"):
        self._trace(
            state,
            {
                "phase": "composite_step_start",
                "step_index": 1,
                "step_count": 2 if cfg.get("needs", {}).get("ops") else 1,
                "agent": "finance",
                "operation": "registry_sql",
                "inputs": {"intent_key": intent_key},
                "goal": "Run finance registry SQL in single plan mode.",
            },
        )
        try:
            res = self.finance_agent.run(
                intent_key=intent_key,
                slots=slots,
                session_context={**session_context, "user_input": state.get("user_input") or ""},
            )
            state["finance"] = _json_safe(res)
        except Exception as e:
            _dprint(f"⚠️  Finance failed: {e}")
            state["finance"] = {
                "mode": "error",
                "rows": [],
                "fallback_reason": f"Finance agent error: {str(e)}"
            }
        try:
            fin_safe = _json_safe(state.get("finance"))
            fin_rows = fin_safe.get("rows") if isinstance(fin_safe, dict) else []
            fin_rows = fin_rows if isinstance(fin_rows, list) else []
            fin_sql = _registry_sql_from_result(fin_safe)
            self._trace(
                state,
                {
                    "phase": "composite_step_result",
                    "step_index": 1,
                    "agent": "finance",
                    "operation": "registry_sql",
                    "ok": True,
                    "mode": fin_safe.get("mode") if isinstance(fin_safe, dict) else None,
                    "rows": len(fin_rows),
                    "sql_present": bool(fin_sql),
                    "sql": fin_sql,
                    "summary": f"Finance(single): fetched {len(fin_rows)} rows using registry SQL.",
                },
            )
        except Exception:
            pass

    # Ops
    if cfg.get("needs", {}).get("ops"):
        self._trace(
            state,
            {
                "phase": "composite_step_start",
                "step_index": 2 if cfg.get("needs", {}).get("finance") else 1,
                "step_count": 2 if cfg.get("needs", {}).get("finance") else 1,
                "agent": "ops",
                "operation": "registry_sql",
                "inputs": {"intent_key": intent_key},
                "goal": "Run ops registry SQL in single plan mode.",
            },
        )
        try:
            res = self.ops_agent.run(
                intent_key=intent_key,
                slots=slots,
                session_context={**session_context, "user_input": state.get("user_input") or ""},
            )
            state["ops"] = _json_safe(res)
        except Exception as e:
            _dprint(f"⚠️  Ops failed: {e}")
            state["ops"] = {
                "mode": "error",
                "rows": [],
                "fallback_reason": f"Ops agent error: {str(e)}"
            }
        try:
            ops_safe = _json_safe(state.get("ops"))
            ops_rows = ops_safe.get("rows") if isinstance(ops_safe, dict) else []
            ops_rows = ops_rows if isinstance(ops_rows, list) else []
            ops_sql = _registry_sql_from_result(ops_safe)
            self._trace(
                state,
                {
                    "phase": "composite_step_result",
                    "step_index": 2 if cfg.get("needs", {}).get("finance") else 1,
                    "agent": "ops",
                    "operation": "registry_sql",
                    "ok": True,
                    "mode": ops_safe.get("mode") if isinstance(ops_safe, dict) else None,
                    "rows": len(ops_rows),
                    "sql_present": bool(ops_sql),
                    "sql": ops_sql,
                    "summary": f"Ops(single): fetched {len(ops_rows)} rows using registry SQL.",
                },
            )
        except Exception:
            pass

    # =========================================================
    # Optional Dynamic NoSQL enrichment for ops port queries
    # (adds remarks/grades/ports from Mongo when asked)
    # =========================================================
    if intent_key in ("ops.port_query", "ops.voyages_by_port"):
        ui_l = (user_input or "").lower()
        wants_mongo = any(k in ui_l for k in ("remark", "remarks", "grade", "grades", "cargo"))
        if wants_mongo and hasattr(self.mongo_agent, "run_llm_find"):
            try:
                voyage_ids: list[str] = []
                fin = state.get("finance")
                ops = state.get("ops")
                if isinstance(fin, dict):
                    for r in (fin.get("rows") or [])[:20]:
                        if isinstance(r, dict) and r.get("voyage_id"):
                            voyage_ids.append(str(r["voyage_id"]))
                if not voyage_ids and isinstance(ops, dict):
                    for r in (ops.get("rows") or [])[:20]:
                        if isinstance(r, dict) and r.get("voyage_id"):
                            voyage_ids.append(str(r["voyage_id"]))
                voyage_ids = list(dict.fromkeys([v for v in voyage_ids if v]))

                if voyage_ids:
                    q = (
                        "Fetch remarks + minimal context for these voyage_ids.\n"
                        "Use collection=voyages.\n"
                        "Filter MUST be: {\"voyageId\": {\"$in\": slots.voyage_ids}}.\n"
                        "Projection MUST include: {\"_id\": 0, \"voyageId\": 1, \"voyageNumber\": 1, \"remarks\": 1, "
                        "\"fixtures.grades\": 1, \"fixtures.fixtureGrades.gradeName\": 1, "
                        "\"fixtures.fixturePorts.portName\": 1, \"fixtures.fixturePorts.activityType\": 1}.\n"
                        "Return only the minimal required fields."
                    )
                    mongo_llm = self.mongo_agent.run_llm_find(question=q, slots={"voyage_ids": voyage_ids})
                    state["mongo"] = _json_safe(mongo_llm)
                    if "data" in state and isinstance(state["data"], dict):
                        state["data"]["mongo"] = state["mongo"]
                elif slots.get("port_name"):
                    # Fallback: query by port name directly if we have no IDs.
                    q = (
                        "Find voyages that called at the given port name (case-insensitive) and return minimal context.\n"
                        "Use collection=voyages.\n"
                        "Filter should use fixtures.fixturePorts.portName with $regex and $options:'i' using slots.port_name.\n"
                        "Projection MUST include: {\"_id\": 0, \"voyageId\": 1, \"voyageNumber\": 1, \"remarks\": 1, "
                        "\"fixtures.grades\": 1, \"fixtures.fixtureGrades.gradeName\": 1, "
                        "\"fixtures.fixturePorts.portName\": 1, \"fixtures.fixturePorts.activityType\": 1}.\n"
                        "Limit <= 50."
                    )
                    mongo_llm = self.mongo_agent.run_llm_find(question=q, slots={"port_name": slots.get("port_name")})
                    state["mongo"] = _json_safe(mongo_llm)
                    if "data" in state and isinstance(state["data"], dict):
                        state["data"]["mongo"] = state["mongo"]
            except Exception as e:
                _dprint(f"⚠️  Mongo enrichment failed: {e}")

    state["slots"] = slots

    # =========================================================
    # Registry-driven zero-row escalation.
    # If single path returned no rows and no entity anchor exists
    # in slots, the query was fleet-wide but mis-classified as single.
    # Rebuild as composite and signal r_after_run_single to re-route.
    # No hardcoded intent names — driven entirely by registry route.
    # =========================================================
    _has_entity_anchor = bool(
        slots.get("voyage_number") or slots.get("voyage_numbers")
        or slots.get("voyage_id") or slots.get("vessel_name") or slots.get("imo")
    )
    _fin_rows: list = []
    _ops_rows: list = []
    try:
        _f = state.get("finance") or {}
        if isinstance(_f, dict):
            _fin_rows = _f.get("rows") or []
    except Exception:
        pass
    try:
        _o = state.get("ops") or {}
        if isinstance(_o, dict):
            _ops_rows = _o.get("rows") or []
    except Exception:
        pass

    _no_results = (not _fin_rows) and (not _ops_rows)
    _intent_cfg_sr = INTENT_REGISTRY.get(intent_key, {})

    if _no_results and not _has_entity_anchor and _intent_cfg_sr.get("route") == "single":
        _dprint(f"   ⚡ ZERO-ROW ESCALATION: {intent_key} single → composite retry")
        _escalated = self.planner.build_plan(
            text=user_input,
            session_context=session_context,
            intent_key=intent_key,
            slots=slots,
            force_composite=True,
        )
        state["plan"] = {
            "plan_type": _escalated.plan_type,
            "intent_key": _escalated.intent_key,
            "required_slots": _escalated.required_slots or [],
            "confidence": _escalated.confidence,
            "steps": [asdict(s) for s in (_escalated.steps or [])],
        }
        state["plan_type"] = "composite"
        state["intent_key"] = _escalated.intent_key
        state["step_index"] = 0
        if not isinstance(state.get("data"), dict):
            state["data"] = {
                "finance": {"mode": None, "rows": []},
                "ops": {"mode": None, "rows": []},
                "mongo": {},
                "artifacts": {},
            }
        self._trace(
            state,
            {
                "phase": "zero_row_escalation",
                "original_intent": intent_key,
                "escalated_intent": _escalated.intent_key,
                "reason": "single path returned zero rows with no entity anchor",
            },
        )

    return state
    return state

        # Composite Step Execution
        # =========================================================
