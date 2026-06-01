from __future__ import annotations

from typing import Any, Dict

from app.orchestration.handlers.router_context import (
    bind_graph_router_globals,
    build_router_context,
)


def handle_execute_step(router: Any, state: Dict[str, Any]) -> Dict[str, Any]:
    bind_graph_router_globals(globals())
    ctx = build_router_context(router)
    self = router

    """
    Execute one composite step. Caps finance/ops rows and voyage_ids, deduplicates, scenario comparison handling.
    """

    # LangGraph may populate declared state keys with None. Ensure data is always a dict.
    if not isinstance(state.get("data"), dict):
        state["data"] = {
            "finance": {"mode": None, "rows": []},
            "ops": {"mode": None, "rows": []},
            "mongo": {},
            "artifacts": {},
        }

    plan = state.get("plan") or {}
    steps = plan.get("steps") or []
    idx = int(state.get("step_index") or 0)

    artifacts = state.get("artifacts") or {}
    # Merge state slots so port_name etc. are always available for finance/ops
    slots = {**(state.get("slots") or {}), **(artifacts.get("slots") or {})}
    sess = state.get("session_ctx") or {}

    # Composite-step slot hygiene: keep only registry-declared slots plus a
    # small set of derived execution keys. This prevents stale entity slots
    # from leaking into dynamic SQL generation across steps.
    current_intent = state.get("intent_key") or (state.get("plan") or {}).get("intent_key") or ""
    current_cfg = INTENT_REGISTRY.get(current_intent, {})
    if current_cfg.get("route") == "composite":
        _allowed_step_slots = set(
            (current_cfg.get("required_slots") or [])
            + (current_cfg.get("optional_slots") or [])
        )
        _allowed_step_slots.update({
            "scenario", "limit",
            "voyage_ids", "cargo_grades", "vessel_imos",
            "voyage_number", "voyage_numbers", "voyage_id",
        })
        slots = {k: v for k, v in (slots or {}).items() if k in _allowed_step_slots}

    if idx >= len(steps):
        return state

    step = steps[idx]
    agent = (step.get("agent") or "").lower()
    op = step.get("operation") or ""

    op = re.sub(r'([a-z])([A-Z])', r'\1_\2', op).lower()

    _dprint(f"\nComposite step {idx + 1}/{len(steps)}: {agent}.{op}")

    self._trace(
        state,
        {
            "phase": "composite_step_start",
            "step_index": idx + 1,
            "step_count": len(steps),
            "agent": agent,
            "operation": op,
            "inputs": step.get("inputs") or {},
            "goal": self._step_goal_text(
                intent_key=str(state.get("intent_key") or ""),
                agent=agent,
                op=op,
                step_inputs=step.get("inputs") or {},
                slots=slots,
            ),
        },
    )

    # Ensure expected sections exist (avoid None/KeyError in error paths).
    data = state.get("data")
    if isinstance(data, dict):
        data.setdefault("finance", {"mode": None, "rows": []})
        data.setdefault("ops", {"mode": None, "rows": []})
        data.setdefault("mongo", {})
        data.setdefault("artifacts", {})
        state["data"] = data

    # =========================================================
    # FINANCE STEP
    # =========================================================
    if agent == "finance" and op in ("dynamic_sql", "registry_sql"):
        # Merge step inputs (e.g. limit from planner) into slots
        step_inputs = step.get("inputs") or {}
        for k, v in step_inputs.items():
            if k not in ("question", "intent_key") and v is not None:
                slots = {**slots, k: v}
        # Intent from state or plan (plan is authoritative after build_plan)
        intent_key = state.get("intent_key") or (state.get("plan") or {}).get("intent_key") or "composite.query"
        user_q_lower = str(state.get("user_input") or "").strip().lower()
        cargo_frequency_ops_only = (
            intent_key == "ranking.cargo"
            and any(
                phrase in user_q_lower
                for phrase in (
                    "appears most frequently across all voyages",
                    "appears most frequently",
                    "most frequent cargo grade",
                    "most common cargo grade",
                    "most commonly carried cargo grade",
                )
            )
            and not any(
                phrase in user_q_lower
                for phrase in (
                    "profitable",
                    "profitability",
                    "average pnl",
                    "avg pnl",
                    "revenue",
                    "margin",
                    "variance",
                    "when fixed",
                    "when-fixed",
                )
            )
        )
        if cargo_frequency_ops_only and op == "dynamic_sql":
            safe = {
                "mode": "skipped",
                "rows": [],
                "skipped_reason": "cargo_frequency_ops_only",
            }
            artifacts["finance_rows"] = []
            artifacts["voyage_ids"] = []
            state["finance"] = safe
            state["data"]["finance"] = safe
            self._trace(
                state,
                {
                    "phase": "composite_step_result",
                    "step_index": idx + 1,
                    "agent": agent,
                    "operation": op,
                    "ok": True,
                    "skipped": True,
                    "mode": "skipped",
                    "rows": 0,
                    "voyage_ids": 0,
                    "reason": (
                        "Cargo-frequency ranking is ops-only for this phrasing; "
                        "finance step skipped to avoid irrelevant SQL generation."
                    ),
                    "summary": "Finance step skipped - cargo frequency is sourced from ops cargo grades.",
                },
            )
            state["artifacts"] = artifacts
            state["slots"] = slots
            state["step_index"] = idx + 1
            return state
        # Composite always uses dynamic SQL for finance; registry only when plan explicitly says registry_sql (e.g. single-query path)
        use_registry = op == "registry_sql"
        try:
            if (
                intent_key == "ranking.vessels"
                and any(
                    phrase in user_q_lower
                    for phrase in (
                        "above-average pnl",
                        "above average pnl",
                    )
                )
                and any(
                    phrase in user_q_lower
                    for phrase in (
                        "most common cargo grade",
                        "most common cargo grades",
                        "common cargo grade",
                        "common cargo grades",
                    )
                )
            ):
                sql = """
                    SELECT
                      REPLACE(f.vessel_imo::TEXT, '.0', '') AS vessel_imo,
                      MAX(o.vessel_name)                   AS vessel_name,
                      COUNT(DISTINCT f.voyage_id)          AS voyage_count,
                      AVG(f.pnl)                           AS avg_pnl
                    FROM finance_voyage_kpi f
                    JOIN ops_voyage_summary o
                      ON f.voyage_number = o.voyage_number
                      AND REPLACE(f.vessel_imo::TEXT, '.0', '') = REPLACE(o.vessel_imo::TEXT, '.0', '')
                    WHERE f.scenario = COALESCE(%(scenario)s, 'ACTUAL')
                    GROUP BY REPLACE(f.vessel_imo::TEXT, '.0', '')
                    HAVING AVG(f.pnl) > (
                      SELECT AVG(pnl)
                      FROM finance_voyage_kpi
                      WHERE scenario = COALESCE(%(scenario)s, 'ACTUAL')
                    )
                    ORDER BY voyage_count DESC, avg_pnl DESC NULLS LAST
                    LIMIT %(limit)s
                """
                params = {
                    "scenario": slots.get("scenario") or "ACTUAL",
                    "limit": min(int(slots.get("limit") or 10), 50),
                }
                rows = self.finance_agent.pg.execute_dynamic_select(sql, params)
                result = {
                    "mode": "dynamic_sql",
                    "intent_key": intent_key,
                    "query_key": "narrow.ranking_vessels_above_avg_pnl",
                    "params": params,
                    "rows": rows,
                    "sql": sql,
                }
            elif use_registry:
                result = self.finance_agent.run(
                    intent_key=intent_key,
                    slots=slots,
                    session_context=sess,
                )
            else:
                finance_question = state["user_input"]
                if intent_key == "ranking.vessels" and any(
                    phrase in user_q_lower
                    for phrase in (
                        "most common cargo grade",
                        "most common cargo grades",
                        "common cargo grade",
                        "common cargo grades",
                    )
                ):
                    finance_question = re.sub(
                        r"(?i)\s*(?:,?\s*and\s+)?most common cargo grades?\b",
                        "",
                        str(finance_question or ""),
                    ).strip(" ,.")
                    if "above-average pnl" in user_q_lower or "above average pnl" in user_q_lower:
                        finance_question = (
                            finance_question.rstrip(" ?.") + "?\n\n"
                            "IMPORTANT: interpret 'above-average PnL' at the vessel aggregate level, "
                            "not at the individual voyage row level. "
                            "Use HAVING AVG(f.pnl) > (SELECT AVG(pnl) FROM finance_voyage_kpi WHERE scenario = COALESCE(%(scenario)s, 'ACTUAL')). "
                            "Do NOT filter voyages with WHERE f.pnl > average. "
                            "Do NOT join or reference cargo grades in the finance SQL."
                        )
                    if finance_question and not finance_question.endswith("?"):
                        finance_question = finance_question + "?"
                result = self.finance_agent.run_dynamic(
                    question=finance_question,
                    intent_key=intent_key,
                    slots=slots,
                    session_context=sess,
                )

            safe = _json_safe(result)
            rows = safe.get("rows", []) or []

            # Cap finance rows
            rows = rows[:20]

            safe["rows"] = rows

            state["finance"] = safe
            state["data"]["finance"] = safe

            # Extract voyage_ids safely
            voyage_ids = [
                r.get("voyage_id")
                for r in rows
                if isinstance(r, dict) and r.get("voyage_id")
            ]

            # Fallback for single-voyage composite queries:
            # If finance SQL returned KPI rows without voyage_id (common LLM omission),
            # reuse canonical voyage_id resolved by mongo.resolve_anchor so downstream
            # ops/mongo merge can still produce a coherent single-voyage snapshot.
            if not voyage_ids and rows:
                include_ids = artifacts.get("include_voyage_ids") or []
                if isinstance(include_ids, list):
                    include_ids = [x for x in include_ids if x not in (None, "", [], {})]
                else:
                    include_ids = []
                if not include_ids and slots.get("voyage_id") not in (None, "", [], {}):
                    include_ids = [slots.get("voyage_id")]
                if len(include_ids) == 1:
                    canonical_vid = include_ids[0]
                    for r in rows:
                        if isinstance(r, dict) and not r.get("voyage_id"):
                            r["voyage_id"] = canonical_vid
                            if slots.get("voyage_number") and not r.get("voyage_number"):
                                r["voyage_number"] = slots.get("voyage_number")
                    voyage_ids = [canonical_vid]

            if voyage_ids:
                # Deduplicate + hard cap
                unique_ids = list(dict.fromkeys(voyage_ids))

                # If mongo.resolve_anchor found a specific voyage_id to include,
                # ensure it is present in the list before downstream ops/mongo steps.
                include_ids = artifacts.get("include_voyage_ids") or []
                if isinstance(include_ids, list) and include_ids:
                    unique_ids = list(dict.fromkeys([*include_ids, *unique_ids]))

                unique_ids = unique_ids[:20]

                artifacts["voyage_ids"] = unique_ids
                artifacts["finance_rows"] = rows
                slots["voyage_ids"] = unique_ids

                _dprint(f"   ✅ Extracted {len(unique_ids)} voyage_ids (capped)")
                self._trace(
                    state,
                    {
                        "phase": "composite_step_result",
                        "step_index": idx + 1,
                        "agent": agent,
                        "operation": op,
                        "ok": True,
                        "mode": safe.get("mode"),
                        "rows": len(rows),
                        "voyage_ids": len(unique_ids),
                        "extracted_voyage_ids": unique_ids,
                        "sql_present": bool(safe.get("sql")),
                        "sql": safe.get("sql"),
                        "summary": f"Finance: fetched {len(rows)} rows and extracted {len(unique_ids)} voyage_ids (Postgres).",
                    },
                )
            else:
                # No voyage_ids: vessel-level or aggregate (e.g. ranking.vessels). Still store finance_rows for merge.
                artifacts["finance_rows"] = rows
                artifacts["voyage_ids"] = []
                # Some finance aggregate intents return no voyage_ids (e.g., cargo profitability by grade).
                # For those, carry forward key dimension values so ops can provide context.
                if state.get("intent_key") in ("analysis.cargo_profitability", "analysis.cargoprofitability"):
                    cargo_grades = []
                    seen_cg = set()
                    for r in rows:
                        if not isinstance(r, dict):
                            continue
                        g = r.get("cargo_grade") or r.get("grade")
                        if not isinstance(g, str):
                            continue
                        s = g.strip()
                        sn = s.lower()
                        if (not s) or sn in ("none", "null", "n/a", "na"):
                            continue
                        if sn in seen_cg:
                            continue
                        seen_cg.add(sn)
                        cargo_grades.append(s)
                    cargo_grades = cargo_grades[:50]
                    if cargo_grades:
                        artifacts["cargo_grades"] = cargo_grades
                        slots["cargo_grades"] = cargo_grades
                if state.get("intent_key") == "ranking.vessels" and rows:
                    vessel_imos = []
                    for r in rows:
                        if not isinstance(r, dict):
                            continue
                        imo = r.get("vessel_imo")
                        if imo is not None:
                            vessel_imos.append(imo)
                    if vessel_imos:
                        artifacts["vessel_imos"] = vessel_imos[:50]

                self._trace(
                    state,
                    {
                        "phase": "composite_step_result",
                        "step_index": idx + 1,
                        "agent": agent,
                        "operation": op,
                        "ok": True,
                        "mode": safe.get("mode"),
                        "rows": len(rows),
                        "voyage_ids": 0,
                        "sql_present": bool(safe.get("sql")),
                        "sql": safe.get("sql"),
                        "summary": f"Finance: fetched {len(rows)} rows (Postgres). No voyage_ids extracted.",
                    },
                )

        except Exception as e:
            err_str = str(e)
            # Retry with registry when dynamic SQL fails due to missing column (e.g. bunker_cost)
            if "bunker_cost" in err_str and (intent_key == "analysis.high_revenue_low_pnl" or (state.get("plan") or {}).get("intent_key") == "analysis.high_revenue_low_pnl"):
                try:
                    result = self.finance_agent.run(
                        intent_key=intent_key,
                        slots=slots,
                        session_context=sess,
                    )
                    safe = _json_safe(result)
                    rows = safe.get("rows", []) or []
                    rows = rows[:20]
                    safe["rows"] = rows
                    state["finance"] = safe
                    state["data"]["finance"] = safe
                    voyage_ids = [r.get("voyage_id") for r in rows if isinstance(r, dict) and r.get("voyage_id")]
                    voyage_ids = list(dict.fromkeys(voyage_ids))[:20]
                    if voyage_ids:
                        artifacts["voyage_ids"] = voyage_ids
                        artifacts["finance_rows"] = rows
                        slots["voyage_ids"] = voyage_ids
                    self._trace(state, {"phase": "composite_step_result", "step_index": idx + 1, "agent": agent, "operation": op, "ok": True, "mode": safe.get("mode"), "rows": len(rows), "voyage_ids": len(voyage_ids), "extracted_voyage_ids": voyage_ids, "sql_present": bool(safe.get("sql")), "sql": safe.get("sql"), "summary": "Finance: retried with registry SQL (Postgres)."})
                except Exception as e2:
                    _dprint(f"   ❌ finance (registry retry) failed: {e2}")
                    state["data"]["finance"] = {"mode": "error", "rows": []}
                    self._trace(state, {"phase": "composite_step_result", "step_index": idx + 1, "agent": agent, "operation": op, "ok": False, "error": err_str})
            else:
                _dprint(f"   ❌ finance.dynamic_sql failed: {e}")
                state["data"]["finance"] = {"mode": "error", "rows": []}
                self._trace(
                    state,
                    {
                        "phase": "composite_step_result",
                        "step_index": idx + 1,
                        "agent": agent,
                        "operation": op,
                        "ok": False,
                        "error": err_str,
                    },
                )

    # =========================================================
    # OPS STEP
    # =========================================================
    elif agent == "ops" and op == "voyage_ids_from_step":
        step_inputs = step.get("inputs") or {}

        # Read voyage_ids from artifacts set by the previous mongo step
        voyage_ids = artifacts.get("voyage_ids") or []

        if not voyage_ids:
            self._trace(state, {
                "phase": "composite_step_result",
                "step_index": idx + 1,
                "agent": "ops",
                "operation": op,
                "ok": False,
                "error": "No voyages found for the specified cargo grade.",
            })
            state["data"]["ops"] = {"mode": "dynamic_sql", "rows": []}

        else:
            enriched_slots = {
                **slots,
                "voyage_ids": voyage_ids,
                "limit": min(25, int(slots.get("limit") or 25)),
            }
            result = self.ops_agent.run_dynamic(
                question=state["user_input"],
                intent_key=(
                    state.get("intent_key")
                    or step_inputs.get("intent_key")
                    or "composite.query"
                ),
                slots=enriched_slots,
                session_context=sess,
            )
            safe = _json_safe(result)
            rows = (safe.get("rows") or [])[:25]
            safe["rows"] = rows

            artifacts["ops_rows"] = rows
            state["ops"] = safe
            state["data"]["ops"] = safe

            self._trace(state, {
                "phase": "composite_step_result",
                "step_index": idx + 1,
                "agent": "ops",
                "operation": op,
                "ok": True,
                "rows": len(rows),
                "voyage_ids": len(voyage_ids),
                "extracted_voyage_ids": voyage_ids,
                "sql": safe.get("sql"),
                "summary": (
                    f"ops voyage_ids_from_step: {len(rows)} rows for {len(voyage_ids)} voyage_ids"
                ),
            })

    elif agent == "ops" and op == "dynamic_sql":
        # ── Aggregate rows without voyage_ids guard ──────────────────────
        _finance_rows = (state.get("finance") or {}).get("rows") or []
        _voyage_ids_available = bool(
            artifacts.get("voyage_ids") or
            (state.get("finance") or {}).get("voyage_ids")
        )
        _intent_key_ops = state.get("intent_key") or (state.get("plan") or {}).get("intent_key") or "composite.query"
        _user_q_lower = str(state.get("user_input") or "").strip().lower()
        _needs_vessel_grade_enrichment = (
            _intent_key_ops == "ranking.vessels"
            and any(
                phrase in _user_q_lower
                for phrase in (
                    "most common cargo grade",
                    "most common cargo grades",
                    "common cargo grade",
                    "common cargo grades",
                )
            )
        )

        if (not _voyage_ids_available) and _finance_rows and not _needs_vessel_grade_enrichment:
            self._trace(state, {
                "phase": "composite_step_result",
                "step_index": idx + 1,
                "agent": "ops",
                "operation": op,
                "ok": True,
                "skipped": True,
                "rows": len(_finance_rows),
                "reason": (
                    "Finance returned aggregate rows (GROUP BY query). "
                    "No voyage_ids to pass — ops step not needed, finance result is self-contained."
                ),
                "summary": "Ops step skipped — finance aggregate query is self-contained.",
            })
            state["data"]["ops"] = {
                "mode": "skipped",
                "rows": _finance_rows,
                "skipped_reason": "aggregate_no_voyage_ids",
            }
            artifacts["slots"] = slots
            state["artifacts"] = artifacts
            state["slots"] = slots
            state["step_index"] = idx + 1
            return state

        # Resolve step inputs (e.g. voyage_ids: "$finance.voyage_ids") so we never pass literal placeholders to the agent
        step_inputs = step.get("inputs") or {}
        for k, v in step_inputs.items():
            if k in ("question", "intent_key"):
                continue
            if v == "$finance.voyage_ids" or (isinstance(v, str) and v.strip() == "$finance.voyage_ids"):
                v = artifacts.get("voyage_ids") or []
            if v is not None:
                slots = {**slots, k: v}

        voyage_ids = artifacts.get("voyage_ids") or []
        if not isinstance(voyage_ids, list):
            voyage_ids = []
        if voyage_ids:
            slots["voyage_ids"] = voyage_ids[:20]
        else:
            slots.pop("voyage_ids", None)

        intent_key_ops = state.get("intent_key") or (state.get("plan") or {}).get("intent_key") or "composite.query"
        if intent_key_ops in ("analysis.cargo_profitability", "analysis.cargoprofitability") and (artifacts.get("cargo_grades") or []):
            slots["cargo_grades"] = artifacts["cargo_grades"]
        if intent_key_ops == "ranking.vessels" and not voyage_ids:
            vessel_imos = artifacts.get("vessel_imos") or []
            if not vessel_imos and isinstance(_finance_rows, list):
                vessel_imos = []
                for r in _finance_rows:
                    if not isinstance(r, dict):
                        continue
                    imo = r.get("vessel_imo")
                    if imo in (None, "", [], {}):
                        continue
                    imo_s = str(imo)
                    if imo_s not in vessel_imos:
                        vessel_imos.append(imo_s)
                if vessel_imos:
                    artifacts["vessel_imos"] = vessel_imos[:50]
            if vessel_imos:
                slots["vessel_imos"] = vessel_imos[:50]

        # Trace resolved inputs (plan "inputs" are placeholders; these are what we actually pass to the agent)
        _resolved_vids = len(slots.get("voyage_ids") or []) if isinstance(slots.get("voyage_ids"), list) else 0
        _resolved_grades = len(slots.get("cargo_grades") or []) if isinstance(slots.get("cargo_grades"), list) else 0
        self._trace(
            state,
            {
                "phase": "composite_step_inputs_resolved",
                "step_index": idx + 1,
                "agent": agent,
                "resolved_voyage_ids_count": _resolved_vids,
                "resolved_cargo_grades_count": _resolved_grades,
                "intent_key_ops": intent_key_ops,
            },
        )

        try:
            result = self.ops_agent.run_dynamic(
                question=state["user_input"],
                intent_key=intent_key_ops,
                slots=slots,
                session_context=sess,
            )

            safe = _json_safe(result)
            rows = safe.get("rows", []) or []

            # Hard cap ops rows
            rows = rows[:20]
            safe["rows"] = rows

            state["ops"] = safe
            state["data"]["ops"] = safe

            artifacts["ops_rows"] = rows

            _dprint(f"   ✅ Got {len(rows)} ops rows (capped)")
            self._trace(
                state,
                {
                    "phase": "composite_step_result",
                    "step_index": idx + 1,
                    "agent": agent,
                    "operation": op,
                    "ok": True,
                    "mode": safe.get("mode"),
                    "rows": len(rows),
                    "voyage_ids": len(slots.get("voyage_ids") or []) if isinstance(slots.get("voyage_ids"), list) else None,
                    "extracted_voyage_ids": (slots.get("voyage_ids") or []) if isinstance(slots.get("voyage_ids"), list) else None,
                    "sql_present": bool(safe.get("sql")),
                    "sql": safe.get("sql"),
                    "summary": f"Ops: fetched {len(rows)} ops rows for voyage_ids (Postgres).",
                },
            )

        except Exception as e:
            _dprint(f"   ❌ ops.dynamic_sql failed: {e}")
            state["data"]["ops"] = {"mode": "error", "rows": []}
            self._trace(
                state,
                {
                    "phase": "composite_step_result",
                    "step_index": idx + 1,
                    "agent": agent,
                    "operation": op,
                    "ok": False,
                    "error": str(e),
                },
            )

    # =========================================================
    # MONGO STEP
    # =========================================================
    elif agent == "mongo" and op == "cargo_grade_lookup":
        step_inputs = step.get("inputs") or {}
        grades = step_inputs.get("cargo_grades") or slots.get("cargo_grades") or []
        grades = [str(g).strip().lower() for g in grades if g]

        docs = []
        voyage_ids = []

        if grades:
            mongo_resp = self.mongo_agent.run_llm_find(
                question=state["user_input"],
                slots={"cargo_grades": grades},
            )
            safe = _json_safe(mongo_resp)
            rows = safe.get("rows") if isinstance(safe, dict) else []
            rows = rows if isinstance(rows, list) else []
            docs = rows[:25]
            voyage_ids = [
                str(r.get("voyageId"))
                for r in docs
                if isinstance(r, dict) and r.get("voyageId")
            ]

        voyage_ids = list(dict.fromkeys(voyage_ids))   # deduplicate, preserve order

        # ── write to artifacts (NOT step_results) ──────────────────────
        artifacts["voyage_ids"] = voyage_ids
        artifacts["mongo_grade_docs"] = docs
        slots["voyage_ids"] = voyage_ids

        state["mongo"] = {"mode": "mongo_llm", "ok": True, "rows": docs}
        state["data"]["mongo"] = state["mongo"]

        self._trace(state, {
            "phase": "composite_step_result",
            "step_index": idx + 1,
            "agent": "mongo",
            "operation": op,
            "ok": True,
            "rows": len(docs),
            "voyage_ids": len(voyage_ids),
            "extracted_voyage_ids": voyage_ids,
            "summary": (
                f"cargo_grade_lookup: grades={grades} -> {len(voyage_ids)} voyage_ids"
            ),
        })

    elif agent == "mongo" and op == "resolve_anchor":
        q = (step.get("inputs") or {}).get("goal") or state.get("user_input") or ""
        try:
            mongo_resp = self.mongo_agent.run_llm_find(question=q, slots=slots)
            safe = _json_safe(mongo_resp)
            state["mongo"] = safe
            state["data"]["mongo"] = safe

            rows = safe.get("rows") if isinstance(safe, dict) else None
            if isinstance(rows, list) and rows:
                first = rows[0] if isinstance(rows[0], dict) else {}
                if first.get("voyageId") and not slots.get("voyage_id"):
                    slots["voyage_id"] = first["voyageId"]
                imo = first.get("imo") or first.get("vesselImo")
                if imo and not slots.get("imo"):
                    slots["imo"] = imo
                if first.get("voyageNumber") and not slots.get("voyage_number"):
                    slots["voyage_number"] = first["voyageNumber"]
                if first.get("vesselName") and not slots.get("vessel_name"):
                    slots["vessel_name"] = first["vesselName"]

            # Carry resolved voyage_id into artifacts so later steps include it
            if slots.get("voyage_id"):
                inc = artifacts.get("include_voyage_ids")
                if not isinstance(inc, list):
                    inc = []
                if slots["voyage_id"] not in inc:
                    inc.append(slots["voyage_id"])
                artifacts["include_voyage_ids"] = inc
            self._trace(
                state,
                {
                    "phase": "composite_step_result",
                    "step_index": idx + 1,
                    "agent": agent,
                    "operation": op,
                    "ok": True,
                    "mode": safe.get("mode") if isinstance(safe, dict) else None,
                    "mongo_ok": safe.get("ok") if isinstance(safe, dict) else None,
                    "collection": safe.get("collection") if isinstance(safe, dict) else None,
                    "limit": safe.get("limit") if isinstance(safe, dict) else None,
                    "mongo_query": (
                        {
                            "collection": safe.get("collection"),
                            "filter": safe.get("filter"),
                            "projection": safe.get("projection"),
                            "sort": safe.get("sort"),
                            "limit": safe.get("limit"),
                            "pipeline": safe.get("pipeline"),
                        }
                        if isinstance(safe, dict)
                        else None
                    ),
                    "rows": len(safe.get("rows") or []) if isinstance(safe, dict) else None,
                    "summary": "Mongo: resolve anchors/entities for the query (MongoDB).",
                },
            )
        except Exception as e:
            state["data"]["mongo"] = {"mode": "mongo_llm", "ok": False, "reason": str(e), "rows": []}
            self._trace(
                state,
                {
                    "phase": "composite_step_result",
                    "step_index": idx + 1,
                    "agent": agent,
                    "operation": op,
                    "ok": False,
                    "error": str(e),
                },
            )

    elif agent == "mongo" and op == "fetch_remarks":

        voyage_ids = artifacts.get("voyage_ids") or []
        remarks_by: Dict[str, Any] = {}
        cargo_by: Dict[str, Any] = {}
        ports_by: Dict[str, Any] = {}
        voyage_number_by: Dict[str, Any] = {}
        commissions_by: Dict[str, Any] = {}

        _dprint(f"   📝 Fetching remarks for {len(voyage_ids)} voyages...")

        used_llm = False
        if voyage_ids:
            try:
                slots["voyage_ids"] = voyage_ids[:20]

                # For remarks fetch, keep the LLM spec focused ONLY on voyage_ids
                # to avoid over-filtering (e.g. by port_name) and to ensure voyageId
                # is always projected for deterministic mapping.
                slots_for_mongo = {"voyage_ids": slots["voyage_ids"]}
                user_q = (state.get("user_input") or "").lower()
                include_commissions = "commission" in user_q
                q = (step.get("inputs") or {}).get("goal") or (
                    "Fetch remarks + minimal context for these voyage_ids.\n"
                    "Use collection=voyages.\n"
                    "Filter MUST be: {\"voyageId\": {\"$in\": slots.voyage_ids}}.\n"
                    "Projection MUST include: {\"_id\": 0, \"voyageId\": 1, \"voyageNumber\": 1, \"remarks\": 1, "
                    "\"fixtures.grades\": 1, \"fixtures.fixtureGrades.gradeName\": 1, "
                    "\"fixtures.fixturePorts.portName\": 1, \"fixtures.fixturePorts.activityType\": 1"
                    + (", \"fixtures.fixtureCommissions.commissionType\": 1, \"fixtures.fixtureCommissions.organizationName\": 1, \"fixtures.fixtureCommissions.rate\": 1" if include_commissions else "")
                    + "}.\n"
                    "Return only the minimal required fields."
                )

                mongo_resp = self.mongo_agent.run_llm_find(question=q, slots=slots_for_mongo)
                safe = _json_safe(mongo_resp)
                state["mongo"] = safe
                state["data"]["mongo"] = safe

                rows = safe.get("rows") if isinstance(safe, dict) else None
                if isinstance(rows, list) and rows:
                    for r in rows:
                        if not isinstance(r, dict):
                            continue
                        vid = r.get("voyageId") or r.get("voyage_id")
                        if not vid:
                            continue
                        vid_s = str(vid)
                        voyage_number_by[vid_s] = r.get("voyageNumber") or r.get("voyage_number")

                        remarks = r.get("remarks") or r.get("remarkList") or r.get("remarks_json")
                        # Normalize remarks into a list of short strings (Mongo often stores remarkList as objects).
                        remarks_norm: list[str] = []
                        if isinstance(remarks, str) and remarks.strip():
                            remarks_norm = [remarks.strip()]
                        elif isinstance(remarks, list):
                            for it in remarks:
                                if isinstance(it, str) and it.strip():
                                    remarks_norm.append(it.strip())
                                elif isinstance(it, dict):
                                    txt = it.get("remark") or it.get("text") or it.get("message")
                                    if isinstance(txt, str) and txt.strip():
                                        remarks_norm.append(txt.strip())
                        elif isinstance(remarks, dict):
                            txt = remarks.get("remark") or remarks.get("text") or remarks.get("message")
                            if isinstance(txt, str) and txt.strip():
                                remarks_norm = [txt.strip()]

                        # De-dupe preserve order; keep at most 25 to control payload size.
                        seen_rm = set()
                        remarks_dedup = []
                        for rm in remarks_norm:
                            if rm not in seen_rm:
                                seen_rm.add(rm)
                                remarks_dedup.append(rm)
                        remarks_by[vid_s] = remarks_dedup[:25] if remarks_dedup else None

                        # Cargo grade (best-effort): prefer fixtures.grades string, else fixtureGrades.gradeName list
                        grades: list[str] = []
                        comms: list[dict] = []
                        fixtures = r.get("fixtures")
                        fixtures_list = None
                        if isinstance(fixtures, list):
                            fixtures_list = fixtures
                        elif isinstance(fixtures, dict):
                            fl = fixtures.get("fixtureList") or fixtures.get("fixtures") or fixtures.get("list")
                            if isinstance(fl, list):
                                fixtures_list = fl

                        if isinstance(fixtures_list, list):
                            for fx in fixtures_list:
                                if not isinstance(fx, dict):
                                    continue
                                g = fx.get("grades")
                                if isinstance(g, str) and g.strip():
                                    grades.append(g.strip())
                                fgs = fx.get("fixtureGrades")
                                if isinstance(fgs, list):
                                    for fg in fgs:
                                        if isinstance(fg, dict):
                                            gn = fg.get("gradeName")
                                            if isinstance(gn, str) and gn.strip():
                                                grades.append(gn.strip())

                                # Bills of lading may contain grade names in some datasets
                                bols = fx.get("fixtureBillsOfLading") or fx.get("billsOfLading") or fx.get("bills")
                                if isinstance(bols, list):
                                    for bol in bols:
                                        if not isinstance(bol, dict):
                                            continue
                                        gn = bol.get("fixtureGradeName") or bol.get("gradeName") or bol.get("cargoGrade")
                                        if isinstance(gn, str) and gn.strip():
                                            grades.append(gn.strip())

                                # Commission details (best-effort)
                                fcs = fx.get("fixtureCommissions")
                                if isinstance(fcs, list):
                                    for fc in fcs:
                                        if not isinstance(fc, dict):
                                            continue
                                        ct = fc.get("commissionType")
                                        on = fc.get("organizationName")
                                        rate = fc.get("rate")
                                        if ct or on or rate is not None:
                                            comms.append({
                                                "commissionType": ct,
                                                "organizationName": on,
                                                "rate": rate,
                                            })

                        # de-dupe preserve order
                        seen_g = set()
                        grades_dedup = []
                        for g in grades:
                            if g not in seen_g:
                                seen_g.add(g)
                                grades_dedup.append(g)
                        cargo_by[vid_s] = grades_dedup

                        # de-dupe commissions by (type, org, rate)
                        if comms:
                            seen_c = set()
                            comms_dedup = []
                            for c in comms:
                                key = (c.get("commissionType"), c.get("organizationName"), c.get("rate"))
                                if key not in seen_c:
                                    seen_c.add(key)
                                    comms_dedup.append(c)
                            commissions_by[vid_s] = comms_dedup[:20]

                        # Key ports from fixturePorts
                        ports: list[dict] = []
                        if isinstance(fixtures_list, list):
                            for fx in fixtures_list:
                                if not isinstance(fx, dict):
                                    continue
                                fps = fx.get("fixturePorts")
                                if not isinstance(fps, list):
                                    continue
                                for p in fps:
                                    if not isinstance(p, dict):
                                        continue
                                    pn = p.get("portName")
                                    at = p.get("activityType")
                                    if isinstance(pn, str) and pn.strip():
                                        ports.append({"portName": pn.strip(), "activityType": at})

                        # de-dupe ports by (portName, activityType)
                        seen_p = set()
                        ports_dedup = []
                        for p in ports:
                            key = (p.get("portName"), p.get("activityType"))
                            if key not in seen_p:
                                seen_p.add(key)
                                ports_dedup.append(p)
                        ports_by[vid_s] = ports_dedup[:20]

                    # Only treat LLM path as successful if we can map at least one voyageId.
                    used_llm = bool(remarks_by)
            except Exception:
                used_llm = False

        if not used_llm:
            # Fallback: existing anchor logic (safe, but multiple small calls)
            for vid in voyage_ids[:20]:
                try:
                    resp = self.mongo_agent.run(
                        intent_key="voyage.entity",
                        slots={"voyage_id": vid},
                        projection={
                            "_id": 0,
                            "voyageId": 1,
                            "remarks": 1,
                            "remarkList": 1,
                        },
                        session_context=sess,
                    )

                    doc = resp.document or {}
                    remarks = doc.get("remarks") or doc.get("remarkList")
                    remarks_by[str(vid)] = remarks
                except Exception:
                    remarks_by[str(vid)] = None

        artifacts["remarks_by_voyage_id"] = remarks_by
        artifacts["cargo_by_voyage_id"] = cargo_by
        artifacts["ports_by_voyage_id"] = ports_by
        artifacts["voyage_number_by_voyage_id"] = voyage_number_by
        artifacts["commissions_by_voyage_id"] = commissions_by
        _dprint(f"   ✅ Fetched remarks for {len(remarks_by)} voyages")
        self._trace(
            state,
            {
                "phase": "composite_step_result",
                "step_index": idx + 1,
                "agent": agent,
                "operation": op,
                "ok": True,
                "mode": (state.get("mongo") or {}).get("mode") if isinstance(state.get("mongo"), dict) else None,
                "mongo_ok": (state.get("mongo") or {}).get("ok") if isinstance(state.get("mongo"), dict) else None,
                "mongo_query": (
                    {
                        "collection": (state.get("mongo") or {}).get("collection"),
                        "filter": (state.get("mongo") or {}).get("filter"),
                        "projection": (state.get("mongo") or {}).get("projection"),
                        "sort": (state.get("mongo") or {}).get("sort"),
                        "limit": (state.get("mongo") or {}).get("limit"),
                        "pipeline": (state.get("mongo") or {}).get("pipeline"),
                    }
                    if used_llm and isinstance(state.get("mongo"), dict)
                    else None
                ),
                "rows": len(((state.get("mongo") or {}).get("rows") or [])) if isinstance(state.get("mongo"), dict) else None,
                "voyage_ids": len(voyage_ids or []),
                "extracted_voyage_ids": (voyage_ids or []),
                "remarks_by_voyage_id": len(remarks_by or {}),
                "summary": f"Mongo: fetched remarks + minimal context for {len(voyage_ids or [])} voyages (MongoDB).",
            },
        )

    # =========================================================
    # LLM MERGE STEP
    # =========================================================
    elif agent == "llm" and op == "merge":

        merged_rows = []
        finance_rows = artifacts.get("finance_rows") or []
        ops_rows = artifacts.get("ops_rows") or []
        remarks = artifacts.get("remarks_by_voyage_id") or {}
        cargo_by = artifacts.get("cargo_by_voyage_id") or {}
        ports_by = artifacts.get("ports_by_voyage_id") or {}
        voyage_number_by = artifacts.get("voyage_number_by_voyage_id") or {}
        commissions_by = artifacts.get("commissions_by_voyage_id") or {}
        intent_key_merge = state.get("intent_key") or ""

        def _num(v):
            if v is None or v == "":
                return None
            try:
                return float(v)
            except (TypeError, ValueError):
                return v

        def _grade_key(v):
            if v is None:
                return ""
            if isinstance(v, dict):
                v = v.get("grade_name") or v.get("gradeName") or v.get("name") or v.get("grade")
            s = str(v).strip()
            if s.startswith("{") and s.endswith("}"):
                try:
                    obj = ast.literal_eval(s)
                    if isinstance(obj, dict):
                        g = obj.get("grade_name") or obj.get("gradeName") or obj.get("name") or obj.get("grade")
                        s = str(g).strip() if g not in (None, "", [], {}) else ""
                except Exception:
                    m = re.search(r"""['"](?:grade_name|gradeName|name|grade)['"]\s*:\s*['"]([^'"]+)['"]""", s)
                    if m:
                        s = m.group(1).strip()
            s = s.lower()
            if s in ("", "none", "null", "n/a", "na"):
                return ""
            return s

        def _dedup_merged_rows(rows):
            out = []
            seen_rows = set()
            for r in rows:
                if not isinstance(r, dict):
                    continue
                vid = r.get("voyage_id")
                if vid not in (None, "", [], {}):
                    key = ("voyage_id", str(vid))
                else:
                    cgs = r.get("cargo_grades") if isinstance(r.get("cargo_grades"), list) else []
                    cgs_norm = tuple(sorted({_grade_key(x) for x in cgs if _grade_key(x)}))
                    if cgs_norm:
                        key = ("cargo_grades", cgs_norm)
                    elif r.get("vessel_imo") not in (None, "", [], {}):
                        key = ("vessel_imo", str(r.get("vessel_imo")), str(r.get("voyage_number") or ""))
                    else:
                        key = (
                            "fallback",
                            str(r.get("voyage_number") or ""),
                            str(r.get("pnl") or ""),
                            str(r.get("revenue") or ""),
                            str(r.get("total_expense") or ""),
                        )
                if key in seen_rows:
                    continue
                seen_rows.add(key)
                out.append(r)
            return out

        # Index ops rows by voyage_id
        def _vid_key(v):
            if v in (None, ""):
                return ""
            return str(v).strip().upper()

        ops_by_vid = {}
        for r in ops_rows:
            if isinstance(r, dict) and r.get("voyage_id"):
                ops_by_vid.setdefault(_vid_key(r.get("voyage_id")), []).append(r)

        # ranking.vessels: finance returns vessel-level rows (vessel_imo, voyage_count, avg_pnl) without voyage_id
        if intent_key_merge == "ranking.vessels" and finance_rows:
            has_voyage_id = any(isinstance(fr, dict) and fr.get("voyage_id") for fr in finance_rows)
            if not has_voyage_id:
                def _imo_key(imo):
                    if imo is None:
                        return ""
                    try:
                        return str(int(float(imo)))
                    except (TypeError, ValueError):
                        return str(imo).strip()

                ops_by_imo = {}
                for r in ops_rows:
                    if isinstance(r, dict):
                        imo = r.get("vessel_imo")
                        if imo is not None:
                            k = _imo_key(imo)
                            if k:
                                ops_by_imo.setdefault(k, []).append(r)
                seen_imo = set()
                for fr in finance_rows:
                    if not isinstance(fr, dict):
                        continue
                    imo = fr.get("vessel_imo") or fr.get("vessel_imo")
                    if imo is None:
                        continue
                    imo_str = _imo_key(imo)
                    if not imo_str or imo_str in seen_imo:
                        continue
                    seen_imo.add(imo_str)
                    ops_for_imo = ops_by_imo.get(imo_str, [])
                    cargo_grades = []
                    for orow in ops_for_imo:
                        g = orow.get("grades_json")
                        if isinstance(g, (list, tuple)):
                            for x in g:
                                if x is not None:
                                    s = str(x).strip()
                                    if s and s not in cargo_grades:
                                        cargo_grades.append(s)
                    def _n(v):
                        if v is None or v == "": return None
                        try: return float(v)
                        except (TypeError, ValueError): return v
                    merged_rows.append({
                        "vessel_imo": imo_str,
                        "vessel_name": fr.get("vessel_name"),
                        "voyage_count": fr.get("voyage_count"),
                        "is_operating": fr.get("is_operating"),
                        "scrubber": fr.get("scrubber"),
                        "market_type": fr.get("market_type"),
                        "ballast_speed": _n(fr.get("ballast_speed")),
                        "laden_speed": _n(fr.get("laden_speed")),
                        "contract_duration_days": _n(fr.get("contract_duration_days")),
                        "avg_pnl": _n(fr.get("avg_pnl") or fr.get("total_pnl") or fr.get("pnl")),
                        "total_pnl": _n(fr.get("total_pnl") or fr.get("pnl")),
                        "pnl": _n(fr.get("avg_pnl") or fr.get("pnl") or fr.get("total_pnl")),
                        "revenue": _n(fr.get("revenue") or fr.get("total_revenue") or fr.get("avg_revenue")),
                        "total_expense": _n(fr.get("total_expense") or fr.get("avg_total_expense")),
                        "bunker_cost": _n(fr.get("bunker_cost") or fr.get("total_bunker_cost") or fr.get("avg_bunker_cost")),
                        "tce": _n(fr.get("tce") or fr.get("avg_tce")),
                        "total_commission": _n(fr.get("total_commission")),
                        "cargo_grades": cargo_grades[:15],
                        "key_ports": [],
                        "remarks": None,
                    })
                merged_rows = _dedup_merged_rows(merged_rows)
                artifacts["merged_rows"] = merged_rows
                try:
                    cov = artifacts.get("coverage") if isinstance(artifacts.get("coverage"), dict) else {}
                    if not isinstance(cov, dict):
                        cov = {}
                    cov["merged_rows_total"] = len(merged_rows)
                    cov["pnl_available"] = sum(1 for r in merged_rows if isinstance(r, dict) and r.get("pnl") not in (None, ""))
                    cov["cargo_grades_available"] = sum(1 for r in merged_rows if isinstance(r, dict) and r.get("cargo_grades"))
                    artifacts["coverage"] = cov
                except Exception:
                    pass
                _dprint(f"   ✅ Merged {len(merged_rows)} vessel-level rows (ranking.vessels)")
                self._trace(state, {"phase": "composite_step_result", "step_index": idx + 1, "agent": agent, "operation": op, "ok": True, "merged_rows": len(merged_rows), "summary": f"Merge: joined {len(merged_rows)} vessel-level rows for ranking.vessels."})
                artifacts["slots"] = slots
                state["artifacts"] = artifacts
                state["slots"] = slots
                state["step_index"] = idx + 1
                return state

        # Grade-level merge fallback for aggregate intents with no voyage_id (e.g. cargo profitability).
        # This keeps enrichment deterministic when finance rows are grouped by dimensions instead of voyage.
        has_voyage_id = any(isinstance(fr, dict) and fr.get("voyage_id") for fr in finance_rows)
        if (not has_voyage_id) and finance_rows:
            # Avoid finance/ops column collision on voyage_count for grade-level merges.
            for fr in finance_rows:
                if isinstance(fr, dict) and "voyage_count" in fr and "finance_voyage_count" not in fr:
                    fr["finance_voyage_count"] = fr.pop("voyage_count")
            for orow in ops_rows:
                if isinstance(orow, dict) and "voyage_count" in orow and "ops_voyage_count" not in orow:
                    orow["ops_voyage_count"] = orow.pop("voyage_count")

            ops_by_grade = {}
            for orow in ops_rows:
                if not isinstance(orow, dict):
                    continue
                gk = _grade_key(orow.get("cargo_grade") or orow.get("grade"))
                if not gk:
                    continue
                cur = ops_by_grade.get(gk) or {}
                if not cur.get("common_ports") and isinstance(orow.get("common_ports"), list):
                    cur["common_ports"] = orow.get("common_ports")
                if not cur.get("remarks") and isinstance(orow.get("congestion_delay_remarks"), list):
                    cur["remarks"] = orow.get("congestion_delay_remarks")
                if cur.get("ops_voyage_count") in (None, "") and orow.get("ops_voyage_count") not in (None, ""):
                    cur["ops_voyage_count"] = orow.get("ops_voyage_count")
                ops_by_grade[gk] = cur

            finance_by_grade: Dict[str, Dict[str, Any]] = {}
            for fr in finance_rows:
                if not isinstance(fr, dict):
                    continue
                grade_raw = fr.get("cargo_grade") or fr.get("grade")
                gk = _grade_key(grade_raw)
                if not gk:
                    continue
                b = finance_by_grade.get(gk)
                if not b:
                    display_grade = (
                        (grade_raw.get("grade_name") or grade_raw.get("gradeName") or grade_raw.get("name") or grade_raw.get("grade"))
                        if isinstance(grade_raw, dict)
                        else grade_raw
                    )
                    if isinstance(display_grade, str):
                        ds = display_grade.strip()
                        if ds.startswith("{") and ds.endswith("}"):
                            try:
                                obj = ast.literal_eval(ds)
                                if isinstance(obj, dict):
                                    g = obj.get("grade_name") or obj.get("gradeName") or obj.get("name") or obj.get("grade")
                                    display_grade = str(g).strip() if g not in (None, "", [], {}) else ""
                            except Exception:
                                m = re.search(r"""['"](?:grade_name|gradeName|name|grade)['"]\s*:\s*['"]([^'"]+)['"]""", ds)
                                if m:
                                    display_grade = m.group(1).strip()
                    b = {
                        "display_grade": str(display_grade).strip() if display_grade not in (None, "", [], {}) else gk,
                        "weight": 0.0,
                        "pnl_wsum": 0.0,
                        "revenue_wsum": 0.0,
                        "tce_wsum": 0.0,
                        "expense_wsum": 0.0,
                        "commission_wsum": 0.0,
                        "has_pnl": False,
                        "has_revenue": False,
                        "has_tce": False,
                        "has_expense": False,
                        "has_commission": False,
                        "voyage_count": 0.0,
                        "has_voyage_count": False,
                        "actual_avg_pnl": None,
                        "when_fixed_avg_pnl": None,
                        "variance_diff": None,
                    }
                    finance_by_grade[gk] = b

                vc = _num(fr.get("finance_voyage_count"))
                w = vc if (isinstance(vc, (int, float)) and vc and vc > 0) else 1.0
                b["weight"] += w
                if isinstance(vc, (int, float)) and vc >= 0:
                    b["voyage_count"] += float(vc)
                    b["has_voyage_count"] = True

                pnl_v = _num(fr.get("avg_pnl") if fr.get("avg_pnl") is not None else fr.get("pnl"))
                if isinstance(pnl_v, (int, float)):
                    b["pnl_wsum"] += float(pnl_v) * w
                    b["has_pnl"] = True

                rev_v = _num(fr.get("avg_revenue") if fr.get("avg_revenue") is not None else fr.get("revenue"))
                if isinstance(rev_v, (int, float)):
                    b["revenue_wsum"] += float(rev_v) * w
                    b["has_revenue"] = True

                tce_v = _num(fr.get("avg_tce") if fr.get("avg_tce") is not None else fr.get("tce"))
                if isinstance(tce_v, (int, float)):
                    b["tce_wsum"] += float(tce_v) * w
                    b["has_tce"] = True

                exp_v = _num(fr.get("total_expense"))
                if isinstance(exp_v, (int, float)):
                    b["expense_wsum"] += float(exp_v) * w
                    b["has_expense"] = True

                com_v = _num(fr.get("total_commission"))
                if isinstance(com_v, (int, float)):
                    b["commission_wsum"] += float(com_v) * w
                    b["has_commission"] = True
                if b.get("actual_avg_pnl") in (None, "") and fr.get("actual_avg_pnl") not in (None, ""):
                    b["actual_avg_pnl"] = _num(fr.get("actual_avg_pnl"))
                if b.get("when_fixed_avg_pnl") in (None, "") and fr.get("when_fixed_avg_pnl") not in (None, ""):
                    b["when_fixed_avg_pnl"] = _num(fr.get("when_fixed_avg_pnl"))
                if b.get("variance_diff") in (None, "") and fr.get("variance_diff") not in (None, ""):
                    b["variance_diff"] = _num(fr.get("variance_diff"))

            for gk, b in finance_by_grade.items():
                w = float(b.get("weight") or 0.0)
                if w <= 0:
                    continue
                octx = ops_by_grade.get(gk) or {}
                ports = []
                cp = octx.get("common_ports")
                if isinstance(cp, list):
                    for p in cp:
                        if p is None:
                            continue
                        pn = None
                        at = None
                        if isinstance(p, dict):
                            pn = p.get("portName") or p.get("port_name") or p.get("name")
                            at = p.get("activityType") or p.get("activity_type")
                        else:
                            ps = str(p).strip()
                            if ps.startswith("{") and ps.endswith("}"):
                                try:
                                    obj = ast.literal_eval(ps)
                                    if isinstance(obj, dict):
                                        pn = obj.get("port_name") or obj.get("portName") or obj.get("name")
                                        at = obj.get("activity_type") or obj.get("activityType")
                                except Exception:
                                    m_name = re.search(r"""['"](?:port_name|portName|name)['"]\s*:\s*['"]([^'"]+)['"]""", ps)
                                    m_act = re.search(r"""['"](?:activity_type|activityType)['"]\s*:\s*['"]([^'"]+)['"]""", ps)
                                    if m_name:
                                        pn = m_name.group(1).strip()
                                    if m_act:
                                        at = m_act.group(1).strip()
                            else:
                                pn = ps
                        if pn:
                            ports.append({"portName": str(pn).strip(), "activityType": at})
                if not ports:
                    mcp = fr.get("most_common_ports")
                    if isinstance(mcp, str) and mcp.strip():
                        for token in [x.strip() for x in mcp.split(",") if str(x).strip()]:
                            ports.append({"portName": token, "activityType": None})
                    elif isinstance(mcp, list):
                        for p in mcp:
                            if isinstance(p, dict):
                                pn = p.get("portName") or p.get("port_name") or p.get("name")
                                at = p.get("activityType") or p.get("activity_type")
                                if pn:
                                    ports.append({"portName": str(pn).strip(), "activityType": at})
                            elif p is not None and str(p).strip():
                                ports.append({"portName": str(p).strip(), "activityType": None})
                merged_rows.append({
                    "voyage_id": None,
                    "voyage_number": None,
                    "pnl": (b["pnl_wsum"] / w) if b.get("has_pnl") else None,
                    "revenue": (b["revenue_wsum"] / w) if b.get("has_revenue") else None,
                    "total_expense": (b["expense_wsum"] / w) if b.get("has_expense") else None,
                    "tce": (b["tce_wsum"] / w) if b.get("has_tce") else None,
                    "total_commission": (b["commission_wsum"] / w) if b.get("has_commission") else None,
                    "finance": {"cargo_grade": b.get("display_grade")},
                    "ops": [],
                    "cargo_grades": [b.get("display_grade") or gk],
                    "key_ports": ports[:20],
                    "remarks": octx.get("remarks") or None,
                    "finance_voyage_count": (int(b["voyage_count"]) if b.get("has_voyage_count") else None),
                    "ops_voyage_count": octx.get("ops_voyage_count"),
                    "voyage_count": (int(b["voyage_count"]) if b.get("has_voyage_count") else None),
                    "actual_avg_pnl": b.get("actual_avg_pnl"),
                    "when_fixed_avg_pnl": b.get("when_fixed_avg_pnl"),
                    "variance_diff": b.get("variance_diff"),
                })

        # Deduplicated merge (by voyage_id)
        seen = set()

        for fr in finance_rows:
            if not isinstance(fr, dict):
                continue

            vid = fr.get("voyage_id")
            vid_k = _vid_key(vid)

            if not vid_k or vid_k in seen:
                continue

            seen.add(vid_k)

            # Flatten core finance KPIs at the top-level of merged_rows for deterministic summarization.
            # This avoids the LLM missing KPIs when `finance.rows` is compacted away.
            # Normalize numeric keys (support alternate casing from DB) and coerce to float for JSON.
            pnl = _num(fr.get("pnl") or fr.get("PnL"))
            revenue = _num(fr.get("revenue") or fr.get("Revenue"))
            total_expense = _num(fr.get("total_expense") or fr.get("Total_expense") or fr.get("total expense"))
            tce = _num(fr.get("tce") or fr.get("TCE"))
            total_commission = _num(fr.get("total_commission") or fr.get("Total_commission") or fr.get("total commission"))
            co2 = _num(fr.get("co2") or fr.get("voyageCO2"))
            eeoi = _num(fr.get("eeoi") or fr.get("voyageEEOI"))
            aer = _num(fr.get("aer") or fr.get("voyageAER"))
            # Scenario comparison: use registry columns when present so table shows actual vs when-fixed and variance
            if intent_key_merge == "analysis.scenario_comparison":
                pnl = _num(fr.get("pnl_actual") or pnl)
                tce = _num(fr.get("tce_actual") or tce)

            ops_for_vid = ops_by_vid.get(vid_k, [])

            # Fallback extraction (ops → merged fields) when Mongo enrichment isn't present.
            # This is critical for composite intents that skip Mongo (e.g. analysis.cargo_profitability)
            # and for cases where Mongo remarks/fixtures are empty.
            cargo_grades = cargo_by.get(str(vid), [])
            key_ports = ports_by.get(str(vid), [])
            remark_val = remarks.get(str(vid)) if isinstance(remarks, dict) else None

            if not cargo_grades and isinstance(ops_for_vid, list):
                gs: list[str] = []
                for orow in ops_for_vid:
                    if not isinstance(orow, dict):
                        continue
                    g = orow.get("grades_json")
                    if isinstance(g, list):
                        for x in g:
                            if x is None:
                                continue
                            if isinstance(x, dict):
                                # Common shapes: {"gradeName": "..."} / {"grade_name": "..."} / {"name": "..."}
                                gv = x.get("gradeName") or x.get("grade_name") or x.get("name")
                                if gv is None:
                                    continue
                                s = str(gv).strip()
                            else:
                                s = str(x).strip()
                            if s and s.lower() not in ("none", "null", "n/a", "na"):
                                gs.append(s)
                # de-dupe preserve order
                seen_g = set()
                cargo_grades = [x for x in gs if not (x in seen_g or seen_g.add(x))]

            if not key_ports and isinstance(ops_for_vid, list):
                ps: list[dict] = []
                for orow in ops_for_vid:
                    if not isinstance(orow, dict):
                        continue
                    p = orow.get("ports_json")
                    if isinstance(p, list):
                        for x in p:
                            if isinstance(x, dict):
                                pn = x.get("portName") or x.get("port_name") or x.get("name")
                                at = x.get("activityType") or x.get("activity_type")
                                if pn:
                                    ps.append({"portName": str(pn).strip(), "activityType": at})
                            elif x is not None:
                                s = str(x).strip()
                                if s:
                                    ps.append({"portName": s, "activityType": None})
                # de-dupe by (portName, activityType)
                seen_p = set()
                key_ports = []
                for p in ps:
                    key = (p.get("portName"), p.get("activityType"))
                    if key in seen_p:
                        continue
                    seen_p.add(key)
                    key_ports.append(p)

            # Aggregate queries may already return a finance-computed common port list.
            # Preserve it when ops-derived key_ports are absent.
            if not key_ports:
                mcp = fr.get("most_common_ports")
                if isinstance(mcp, str) and mcp.strip():
                    parsed_ports = []
                    for token in [x.strip() for x in mcp.split(",") if str(x).strip()]:
                        parsed_ports.append({"portName": token, "activityType": None})
                    if parsed_ports:
                        key_ports = parsed_ports[:20]
                elif isinstance(mcp, list):
                    parsed_ports = []
                    for x in mcp:
                        if isinstance(x, dict):
                            pn = x.get("portName") or x.get("port_name") or x.get("name")
                            at = x.get("activityType") or x.get("activity_type")
                            if pn:
                                parsed_ports.append({"portName": str(pn).strip(), "activityType": at})
                        elif x is not None and str(x).strip():
                            parsed_ports.append({"portName": str(x).strip(), "activityType": None})
                    if parsed_ports:
                        key_ports = parsed_ports[:20]

            if remark_val in (None, "", [], {}) and isinstance(ops_for_vid, list):
                rs: list[str] = []
                for orow in ops_for_vid:
                    if not isinstance(orow, dict):
                        continue
                    rj = orow.get("remarks_json")
                    if isinstance(rj, list):
                        for x in rj:
                            if x is None:
                                continue
                            s = str(x).strip()
                            if s:
                                rs.append(s)
                # keep small; sanitize later anyway
                if rs:
                    # de-dupe preserve order
                    seen_r = set()
                    remark_val = [x for x in rs if not (x in seen_r or seen_r.add(x))]

            row = {
                "voyage_id": vid,
                "voyage_number": voyage_number_by.get(str(vid)) or fr.get("voyage_number"),
                "vessel_name": (
                    fr.get("vessel_name")
                    or (
                        (ops_for_vid[0].get("vessel_name") if isinstance(ops_for_vid, list) and ops_for_vid and isinstance(ops_for_vid[0], dict) else None)
                    )
                ),
                "vessel_imo": (
                    fr.get("vessel_imo")
                    or (
                        (ops_for_vid[0].get("vessel_imo") if isinstance(ops_for_vid, list) and ops_for_vid and isinstance(ops_for_vid[0], dict) else None)
                    )
                ),
                "pnl": pnl,
                "revenue": revenue,
                "total_expense": total_expense,
                "tce": tce,
                "total_commission": total_commission,
                "co2": co2,
                "eeoi": eeoi,
                "aer": aer,
                "finance": fr,
                "ops": ops_for_vid,
                "cargo_grades": cargo_grades,
                "key_ports": key_ports,
                "remarks": remark_val,
                "commissions": commissions_by.get(str(vid), []),
                "is_delayed": (
                    fr.get("is_delayed")
                    if fr.get("is_delayed") is not None
                    else (
                        (ops_for_vid[0].get("is_delayed") if isinstance(ops_for_vid, list) and ops_for_vid and isinstance(ops_for_vid[0], dict) else None)
                    )
                ),
            }
            if intent_key_merge == "analysis.scenario_comparison":
                row["pnl_actual"] = _num(fr.get("pnl_actual"))
                row["pnl_when_fixed"] = _num(fr.get("pnl_when_fixed"))
                row["pnl_variance"] = _num(fr.get("pnl_variance"))
                row["tce_actual"] = _num(fr.get("tce_actual"))
                row["tce_when_fixed"] = _num(fr.get("tce_when_fixed"))
                row["tce_variance"] = _num(fr.get("tce_variance"))
            # Offhire ranking: expose offhire_days and delay_reason at top level so table can show them
            if fr.get("offhire_days") is not None or intent_key_merge == "ops.offhire_ranking":
                offhire_val = fr.get("offhire_days")
                delay_val = fr.get("delay_reason")
                if (offhire_val in (None, "")) and isinstance(ops_for_vid, list) and ops_for_vid:
                    first_ops = ops_for_vid[0] if isinstance(ops_for_vid[0], dict) else {}
                    if isinstance(first_ops, dict):
                        offhire_val = first_ops.get("offhire_days")
                        delay_val = delay_val or first_ops.get("delay_reason")
                row["offhire_days"] = _num(offhire_val)
                row["delay_reason"] = delay_val
            # Port calls: preserve finance-provided count when available,
            # otherwise fall back to derived key_ports length.
            pc = fr.get("port_count")
            if pc in (None, "") and isinstance(key_ports, list):
                pc = len(key_ports)
            if pc not in (None, ""):
                try:
                    row["port_calls"] = int(pc)
                except Exception:
                    row["port_calls"] = _num(pc)
            merged_rows.append(row)

        merged_rows = _dedup_merged_rows(merged_rows)
        artifacts["merged_rows"] = merged_rows

        # Data coverage hints to prevent false "Not available" claims in summarization.
        try:
            cov = artifacts.get("coverage") if isinstance(artifacts.get("coverage"), dict) else {}
            if not isinstance(cov, dict):
                cov = {}
            cov["merged_rows_total"] = len(merged_rows)
            cov["pnl_available"] = sum(1 for r in merged_rows if isinstance(r, dict) and r.get("pnl") not in (None, ""))
            cov["cargo_grades_available"] = sum(1 for r in merged_rows if isinstance(r, dict) and r.get("cargo_grades"))
            cov["key_ports_available"] = sum(1 for r in merged_rows if isinstance(r, dict) and r.get("key_ports"))
            cov["remarks_available"] = sum(1 for r in merged_rows if isinstance(r, dict) and r.get("remarks") not in (None, "", [], {}))
            artifacts["coverage"] = cov
        except Exception:
            pass

        _dprint(f"   ✅ Merged {len(merged_rows)} unique rows")
        self._trace(
            state,
            {
                "phase": "composite_step_result",
                "step_index": idx + 1,
                "agent": agent,
                "operation": op,
                "ok": True,
                "merged_rows": len(merged_rows),
                "summary": f"Merge: joined finance + ops + mongo context into {len(merged_rows)} merged rows.",
            },
        )

    # =========================================================
    # FINAL STATE UPDATE
    # =========================================================
    artifacts["slots"] = slots
    state["artifacts"] = artifacts
    state["slots"] = slots
    state["step_index"] = idx + 1

    return state

        # =========================================================
        # Merge
        # =========================================================
