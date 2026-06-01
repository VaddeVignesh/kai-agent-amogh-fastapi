from __future__ import annotations

from typing import Any, Dict

from app.orchestration.handlers.router_context import (
    bind_graph_router_globals,
    build_router_context,
)


def handle_summarize(router: Any, state: Dict[str, Any]) -> Dict[str, Any]:
    bind_graph_router_globals(globals())
    ctx = build_router_context(router)
    self = router

    """
    Generate final response with DATA SANITIZATION and TOKEN TRACKING.

    ✅ NEW: Sanitizes data before sending to LLM to prevent rate limits
    ✅ NEW: Tracks token usage for debugging
    """
    intent_key = state.get("intent_key", "out_of_scope")
    merged_full = state.get("merged") or {}
    # For ranking.*, ensure merged_rows have explicit pnl/revenue/total_expense at top level (avoid summarizer saying "not available")
    if str(intent_key).startswith("ranking."):
        artifacts = merged_full.get("artifacts")
        if isinstance(artifacts, dict) and isinstance(artifacts.get("merged_rows"), list):
            # Build lookup from raw finance rows by voyage_id (in case merge step used different key casing)
            finance_rows = (merged_full.get("finance") or {}).get("rows") or []
            fin_by_vid = {}
            for r in finance_rows:
                if isinstance(r, dict) and r.get("voyage_id"):
                    fin_by_vid[str(r["voyage_id"]).strip().upper()] = r
            def _norm_num(v):
                if v in (None, "", "Not available", "not available", "N/A", "n/a", "NA", "na"):
                    return None
                try:
                    return float(v)
                except Exception:
                    return v
            for mr in artifacts["merged_rows"]:
                if not isinstance(mr, dict):
                    continue
                vid = mr.get("voyage_id")
                fin = mr.get("finance") if isinstance(mr.get("finance"), dict) else {}
                if not fin and vid is not None:
                    fin = fin_by_vid.get(str(vid).strip().upper()) or {}
                if (_norm_num(mr.get("pnl")) is None) and fin:
                    mr["pnl"] = _norm_num(fin.get("pnl") or fin.get("PnL"))
                else:
                    mr["pnl"] = _norm_num(mr.get("pnl"))
                if (_norm_num(mr.get("revenue")) is None) and fin:
                    mr["revenue"] = _norm_num(fin.get("revenue") or fin.get("Revenue"))
                else:
                    mr["revenue"] = _norm_num(mr.get("revenue"))
                if (_norm_num(mr.get("total_expense")) is None) and fin:
                    mr["total_expense"] = _norm_num(fin.get("total_expense") or fin.get("Total_expense") or fin.get("total expense"))
                else:
                    mr["total_expense"] = _norm_num(mr.get("total_expense"))
    merged = compact_payload(merged_full)

    # Ensure merged has all required keys
    if not isinstance(merged.get("finance"), dict):
        merged["finance"] = {"mode": None, "rows": []}
    if not isinstance(merged.get("ops"), dict):
        merged["ops"] = {"mode": None, "rows": []}
    if not isinstance(merged.get("artifacts"), dict):
        merged["artifacts"] = {}

    # Track token usage before sanitization
    original_tokens = self._estimate_tokens(merged)
    self._trace(
        state,
        {
            "phase": "token_usage",
            "stage": "pre_sanitize",
            "total_tokens_est": original_tokens,
            "mongo_tokens_est": self._estimate_tokens(merged.get("mongo", {})),
            "finance_tokens_est": self._estimate_tokens(merged.get("finance", {})),
            "ops_tokens_est": self._estimate_tokens(merged.get("ops", {})),
            "artifacts_tokens_est": self._estimate_tokens(merged.get("artifacts", {})),
        },
    )
    if original_tokens > 1000:
        _dprint(f"\n📊 TOKEN USAGE ANALYSIS:")
        _dprint(f"   Original merged data: ~{original_tokens:,} tokens")
        
        # Break down by component
        mongo_tokens = self._estimate_tokens(merged.get("mongo", {}))
        finance_tokens = self._estimate_tokens(merged.get("finance", {}))
        ops_tokens = self._estimate_tokens(merged.get("ops", {}))
        artifacts_tokens = self._estimate_tokens(merged.get("artifacts", {}))
        
        _dprint(f"   ├─ Mongo: ~{mongo_tokens:,} tokens")
        _dprint(f"   ├─ Finance: ~{finance_tokens:,} tokens")
        _dprint(f"   ├─ Ops: ~{ops_tokens:,} tokens")
        _dprint(f"   └─ Artifacts: ~{artifacts_tokens:,} tokens")

    # Sanitize data to reduce token usage (after compaction)
    sanitized_merged = self._sanitize_for_llm(merged)

    # Show token savings
    sanitized_tokens = self._estimate_tokens(sanitized_merged)
    self._trace(
        state,
        {
            "phase": "token_usage",
            "stage": "post_sanitize",
            "total_tokens_est": sanitized_tokens,
            "saved_tokens_est": original_tokens - sanitized_tokens,
            "saved_pct_est": (100 * (original_tokens - sanitized_tokens) / max(original_tokens, 1)),
        },
    )
    if original_tokens > 1000:
        savings = original_tokens - sanitized_tokens
        savings_pct = 100 * savings / max(original_tokens, 1)
        _dprint(f"\n   After sanitization: ~{sanitized_tokens:,} tokens")
        _dprint(f"   💾 SAVED: ~{savings:,} tokens ({savings_pct:.1f}% reduction)")
        
        if sanitized_tokens < 5000:
            _dprint(f"   ✅ Token usage is now within safe limits!")
        elif sanitized_tokens < 10000:
            _dprint(f"   ⚠️  Token usage is moderate - should be OK")
        else:
            _dprint(f"   🔴 Token usage is still high - may hit rate limits")

    # Check if we have any data
    finance_data = sanitized_merged.get("finance", {})
    ops_data = sanitized_merged.get("ops", {})
    artifacts = sanitized_merged.get("artifacts", {})

    finance_rows = finance_data.get("rows", []) if isinstance(finance_data, dict) else []
    ops_rows = ops_data.get("rows", []) if isinstance(ops_data, dict) else []
    merged_rows = artifacts.get("merged_rows", []) if isinstance(artifacts, dict) else []

    has_data = bool(finance_rows or ops_rows or sanitized_merged.get("mongo") or merged_rows)

    try:
        # Try to generate proper response with SANITIZED data
        answer = self.llm.summarize_answer(
            question=state["user_input"],
            plan=state.get("plan") or {"plan_type": "single", "intent_key": intent_key},
            merged=sanitized_merged,
            session_context=state.get("session_ctx") or {},
        )
        
        # Validate answer quality
        if not answer or len(answer) < 20 or answer.startswith("Intent="):
            if merged_rows and (
                str(intent_key).startswith("ranking.")
                or str(intent_key).startswith("comparison.")
                or str(intent_key).startswith("aggregation.")
            ):
                answer = self._fallback_tabular_answer(
                    question=state.get("user_input") or "",
                    intent_key=str(intent_key or ""),
                    rows=merged_rows,
                )
            else:
                raise ValueError("Generated answer is too short or malformed")
            
    except Exception as e:
        # Improved fallback with data context
        error_trace = traceback.format_exc()
        _dprint(f"⚠️ WARNING: summarize_answer failed: {e}")
        _dprint(f"Error trace: {error_trace}")
        
        if has_data:
            if merged_rows and (
                str(intent_key).startswith("ranking.")
                or str(intent_key).startswith("comparison.")
                or str(intent_key).startswith("aggregation.")
            ):
                answer = self._fallback_tabular_answer(
                    question=state.get("user_input") or "",
                    intent_key=str(intent_key or ""),
                    rows=merged_rows,
                )
            else:
                answer = (
                    f"I found {len(finance_rows)} finance records and {len(ops_rows)} ops records "
                    f"for your query, but encountered an error generating the summary. "
                    f"Intent: {intent_key}. Error: {str(e)}"
                )
        else:
            answer = (
                f"No data available for this query (Intent: {intent_key}). "
                f"This could mean the requested information doesn't exist in the database."
            )

    state["answer"] = answer

    # Persist a compact result-set memory for multi-row answers (for "among these" follow-ups).
    try:
        from datetime import date, datetime
        from decimal import Decimal

        def _json_primitive(v: Any) -> Any:
            if v is None:
                return None
            if isinstance(v, (str, int, float, bool)):
                return v
            if isinstance(v, Decimal):
                try:
                    return float(v)
                except Exception:
                    return str(v)
            if isinstance(v, (datetime, date)):
                try:
                    return v.isoformat()
                except Exception:
                    return str(v)
            # Lists/dicts: keep small + stringify safely
            if isinstance(v, list):
                return [_json_primitive(x) for x in v[:8]]
            if isinstance(v, dict):
                out: Dict[str, Any] = {}
                for k, vv in list(v.items())[:25]:
                    out[str(k)] = _json_primitive(vv)
                return out
            return str(v)

        def _compact_remarks(val: Any, *, max_len: int = 600) -> str | None:
            if val in (None, "", [], {}):
                return None
            try:
                if isinstance(val, str):
                    s = val.strip()
                elif isinstance(val, list):
                    parts = []
                    for x in val[:4]:
                        if x in (None, "", [], {}):
                            continue
                        parts.append(str(x).strip())
                    s = " | ".join([p for p in parts if p])
                else:
                    s = str(val).strip()
                if not s:
                    return None
                if len(s) > max_len:
                    return s[:max_len].rstrip() + "…"
                return s
            except Exception:
                return None

        def _infer_result_set_meta(rows_in: list[dict], *, source_intent: str | None, user_query: str | None) -> Dict[str, Any]:
            metrics = (
                "expense_to_revenue_ratio",
                "avg_offhire_days",
                "offhire_days",
                "port_calls",
                "voyage_count",
                "avg_pnl",
                "pnl",
                "total_pnl",
                "avg_revenue",
                "revenue",
                "total_expense",
                "avg_total_expense",
                "avg_tce",
                "tce",
                "total_commission",
                "pnl_variance",
                "tce_variance",
                "variance_diff",
                "actual_avg_pnl",
                "when_fixed_avg_pnl",
            )
            available = []
            for m in metrics:
                ok = False
                for rr in rows_in[:50]:
                    if isinstance(rr, dict) and (
                        rr.get(m) not in (None, "", [], {})
                        or GraphRouter._result_row_metric_value(rr, m) is not None
                    ):
                        ok = True
                        break
                if ok:
                    available.append(m)
            primary = None
            source_intent = str(source_intent or "")
            ql = str(user_query or "").strip().lower()
            preferred_by_query = [
                ("expense ratio", "expense_to_revenue_ratio"),
                ("expense-to-revenue ratio", "expense_to_revenue_ratio"),
                ("expense to revenue ratio", "expense_to_revenue_ratio"),
                ("average demurrage", "avg_offhire_days"),
                ("demurrage wait", "avg_offhire_days"),
                ("wait time", "avg_offhire_days"),
                ("total pnl", "total_pnl"),
                ("average tce", "avg_tce"),
                ("avg tce", "avg_tce"),
                ("pnl variance", "pnl_variance"),
                ("tce variance", "tce_variance"),
                ("variance", "variance_diff"),
                ("average revenue", "avg_revenue"),
                ("avg revenue", "avg_revenue"),
                ("average pnl", "avg_pnl"),
                ("avg pnl", "avg_pnl"),
                ("port calls", "port_calls"),
                ("port count", "port_calls"),
                ("voyage count", "voyage_count"),
                ("commission", "total_commission"),
                ("revenue", "revenue"),
                ("expense", "total_expense"),
                ("cost", "total_expense"),
                ("tce", "tce"),
                ("pnl", "pnl"),
            ]
            for phrase, metric in preferred_by_query:
                if phrase in ql and metric in available:
                    primary = metric
                    break
            preferred_by_intent = [
                ("ranking.port_calls", "port_calls"),
                ("ops.offhire_ranking", "offhire_days"),
                ("ranking.ports", "avg_offhire_days"),
                ("analysis.by_module_type", "avg_pnl"),
                ("analysis.scenario_comparison", "pnl_variance"),
                ("analysis.high_revenue_low_pnl", "expense_to_revenue_ratio"),
            ]
            if primary is None:
                for prefix, metric in preferred_by_intent:
                    if source_intent.startswith(prefix) and metric in available:
                        primary = metric
                        break
            if primary is None:
                primary = available[0] if available else None
            return {
                "source_intent": source_intent,
                "available_metrics": available,
                "primary_metric": primary,
            }

        def _compact_result_row(raw_row: Dict[str, Any]) -> Dict[str, Any]:
            if not isinstance(raw_row, dict):
                return {}
            out: Dict[str, Any] = {}
            for key, val in list(raw_row.items())[:60]:
                if key in {"finance", "ops", "mongo", "artifacts", "commissions"}:
                    continue
                out[str(key)] = _json_primitive(val)
            for nested_key in ("finance", "ops"):
                nested = raw_row.get(nested_key)
                if isinstance(nested, dict):
                    for key, val in list(nested.items())[:60]:
                        out.setdefault(str(key), _json_primitive(val))
            if out.get("remarks") in (None, "", [], {}):
                out["remarks"] = _compact_remarks(raw_row.get("remarks"))
            else:
                out["remarks"] = _compact_remarks(out.get("remarks"))
            if out.get("delay_reason") in (None, "", [], {}):
                out["delay_reason"] = _json_primitive(raw_row.get("delay_reason") or raw_row.get("delay_reasons"))
            revenue = GraphRouter._result_row_metric_value(out, "revenue")
            total_expense = GraphRouter._result_row_metric_value(out, "total_expense")
            if out.get("expense_to_revenue_ratio") in (None, "", [], {}) and revenue not in (None, 0) and total_expense is not None:
                out["expense_to_revenue_ratio"] = float(total_expense) / float(revenue)
            return {k: v for k, v in out.items() if v not in (None, "", [], {})}

        sess = state.get("session_ctx") or {}
        art = state.get("artifacts") or {}
        if (
            str(state.get("intent_key") or "").strip().lower() == "followup.result_set"
            and isinstance(sess, dict)
            and isinstance(sess.get("last_result_set"), dict)
        ):
            latest_result_set = sess.get("last_result_set")
            raise StopIteration
        mrs = art.get("merged_rows") if isinstance(art, dict) else None
        if isinstance(mrs, list) and mrs:
            compact_rows = []
            for r in mrs[:20]:
                if not isinstance(r, dict):
                    continue
                compact = _compact_result_row(r)
                if compact:
                    compact_rows.append(compact)
            latest_result_set = {
                "source_intent": state.get("intent_key"),
                "rows": compact_rows,
                "meta": _infer_result_set_meta(compact_rows, source_intent=state.get("intent_key"), user_query=state.get("user_input")),
            }
            self.redis.save_session(
                state["session_id"],
                {
                    **(sess or {}),
                    "last_result_set": latest_result_set,
                },
            )
            if isinstance(sess, dict):
                sess["last_result_set"] = latest_result_set
        else:
            # Fallback: if we don't have merged_rows (some single intents), store from finance rows.
            merged = state.get("merged") or {}
            fin = merged.get("finance") if isinstance(merged, dict) else None
            fin_rows = (fin or {}).get("rows") if isinstance(fin, dict) else None
            if isinstance(fin_rows, list) and len(fin_rows) >= 1:
                compact_rows = []
                for r in fin_rows[:20]:
                    if not isinstance(r, dict):
                        continue
                    compact = _compact_result_row(r)
                    if compact:
                        compact_rows.append(compact)
                latest_result_set = {
                    "source_intent": state.get("intent_key"),
                    "rows": compact_rows,
                    "meta": _infer_result_set_meta(compact_rows, source_intent=state.get("intent_key"), user_query=state.get("user_input")),
                }
                self.redis.save_session(
                    state["session_id"],
                    {
                        **(sess or {}),
                        "last_result_set": latest_result_set,
                    },
                )
                if isinstance(sess, dict):
                    sess["last_result_set"] = latest_result_set
            else:
                # Fallback for ops-only/composite responses where finance rows may be empty.
                ops = merged.get("ops") if isinstance(merged, dict) else None
                ops_rows = (ops or {}).get("rows") if isinstance(ops, dict) else None
                if isinstance(ops_rows, list) and len(ops_rows) >= 1:
                    compact_rows = []
                    for r in ops_rows[:20]:
                        if not isinstance(r, dict):
                            continue
                        compact = _compact_result_row(r)
                        if compact:
                            compact_rows.append(compact)
                    latest_result_set = {
                        "source_intent": state.get("intent_key"),
                        "rows": compact_rows,
                        "meta": _infer_result_set_meta(compact_rows, source_intent=state.get("intent_key"), user_query=state.get("user_input")),
                    }
                    self.redis.save_session(
                        state["session_id"],
                        {
                            **(sess or {}),
                            "last_result_set": latest_result_set,
                        },
                    )
                    if isinstance(sess, dict):
                        sess["last_result_set"] = latest_result_set
                else:
                    derived_rows = self._extract_best_worst_rows_from_answer(
                        answer=state.get("answer") or "",
                        session_ctx=sess if isinstance(sess, dict) else {},
                    )
                    latest_result_set = {
                        "source_intent": state.get("intent_key"),
                        "rows": derived_rows,
                        "meta": _infer_result_set_meta(derived_rows, source_intent=state.get("intent_key"), user_query=state.get("user_input")) if derived_rows else {},
                    }
                    self.redis.save_session(
                        state["session_id"],
                        {
                            **(sess or {}),
                            "last_result_set": latest_result_set,
                        },
                    )
                    if isinstance(sess, dict):
                        sess["last_result_set"] = latest_result_set
    except StopIteration:
        pass
    except Exception:
        pass

    # Save to session
    sess = state.get("session_ctx") or {}
    # Never carry stale pending clarification flags across a completed turn.
    # Clarification state is set only in n_make_clarification when needed.
    if isinstance(sess, dict):
        sess.pop("pending_intent", None)
        sess.pop("missing_keys", None)
        sess.pop("clarification_options", None)
        sess.pop("pending_question", None)
        sess.pop("pending_slots", None)
        if latest_result_set is not None:
            sess["last_result_set"] = latest_result_set
    persisted_slots = self._build_persisted_slots(base=(sess.get("slots") or {}), updates=(state.get("slots") or {}))
    last_voyage_ids = None
    try:
        art = state.get("artifacts") or {}
        vids = art.get("voyage_ids") if isinstance(art, dict) else None
        if isinstance(vids, list) and vids:
            # de-dupe + cap
            last_voyage_ids = list(dict.fromkeys([str(v) for v in vids if v]))[:20]
    except Exception:
        last_voyage_ids = None
    self.redis.save_session(
        state["session_id"],
        {
            **sess,
            "last_intent": intent_key,
            "last_intent_key": intent_key,
            "memory_slots": self._extract_memory_slots(persisted_slots),
            "param_slots": self._extract_param_slots(persisted_slots),
            "slots": persisted_slots,
            "last_user_input": state.get("user_input"),
            "last_voyage_ids": last_voyage_ids,
            "last_plan_type": state.get("plan_type") or (state.get("plan") or {}).get("plan_type"),
            "_turn_marker": uuid.uuid4().hex,
            "_record_turn": self._build_turn_history_entry(
                query=state.get("user_input") or "",
                raw_user_input=state.get("raw_user_input") or state.get("user_input") or "",
                intent_key=intent_key,
                slots=persisted_slots,
                answer=state.get("answer") or "",
                plan_type=state.get("plan_type") or (state.get("plan") or {}).get("plan_type"),
            ),
        },
    )

    return state

        # =========================================================
        # Deterministic ranking/comparison fallback
        # =========================================================
