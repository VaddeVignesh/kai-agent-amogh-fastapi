from __future__ import annotations

from typing import Any, Dict

from app.orchestration.handlers.router_context import (
    bind_graph_router_globals,
    build_router_context,
)


def handle_extract_intent(router: Any, state: Dict[str, Any]) -> Dict[str, Any]:
    bind_graph_router_globals(globals())
    ctx = build_router_context(router)
    self = router

    """
    Extract intent and slots from user input.
    Clear previous slots. Pattern-based override for scenario comparisons.
    """
    # Preserve what the user actually typed this turn.
    if not isinstance(state.get("raw_user_input"), str):
        state["raw_user_input"] = state.get("user_input", "") or ""
    raw_user_input = state.get("raw_user_input", "") or ""

    # user_input is the effective query used for intent/planning/execution.
    user_input = state.get("user_input", "") or raw_user_input
    session_id = state.get("session_id", "")
    session_ctx = state.get("session_ctx") or {}
    # LangGraph may populate declared state keys with None. Ensure artifacts is always a dict.
    if not isinstance(state.get("artifacts"), dict):
        state["artifacts"] = {}

    # Fast-path exact incomplete entity questions so clarification does not wait on LLM routing.
    normalized_user_input = re.sub(r"\s+", " ", (user_input or "").strip().lower()).rstrip("?.!")
    for fastpath_intent, phrases in get_incomplete_entity_fastpath_queries().items():
        if normalized_user_input in phrases:
            state["intent_key"] = fastpath_intent
            state["slots"] = {}
            artifacts = state.get("artifacts") or {}
            artifacts["intent_key"] = fastpath_intent
            artifacts["slots"] = {}
            artifacts["user_input"] = user_input
            state["artifacts"] = artifacts
            self._trace(
                state,
                {
                    "phase": "intent_extraction",
                    "intent_key": fastpath_intent,
                    "slots": {},
                    "likely_path": "single",
                    "source": "incomplete_entity_fastpath",
                },
            )
            return state

    # Read-only guard: delete/update/modify must never fast-path to voyage.summary.
    if is_read_only_mutation_request(user_input):
        from app.config.prompt_rules_loader import get_out_of_scope_response_template

        intent_key = "out_of_scope"
        cleaned_slots: Dict[str, Any] = {}
        state["intent_key"] = intent_key
        state["slots"] = cleaned_slots
        state["answer"] = get_out_of_scope_response_template("read_only_mutation").strip()
        artifacts = state.get("artifacts") or {}
        artifacts["intent_key"] = intent_key
        artifacts["slots"] = cleaned_slots
        artifacts["user_input"] = user_input
        state["artifacts"] = artifacts
        self._trace(
            state,
            {
                "phase": "intent_extraction",
                "intent_key": intent_key,
                "slots": cleaned_slots,
                "likely_path": "single",
                "source": "read_only_mutation_fastpath",
            },
        )
        return state

    # Fast-path obvious voyage lookup requests. This avoids an LLM intent call for
    # simple inputs like "show voyage 2205" while leaving analytical/follow-up
    # questions on the normal router path.
    ul_fast = user_input.lower()
    analytical_markers = get_simple_voyage_analytical_markers()
    # Single-voyage fastpath only. Multi-voyage questions always use the LLM classifier.
    simple_voyage_match = re.search(
        r"\bvoyage\s+(?:number\s+|no\.?\s+|#\s*)?(\d{3,5})\b",
        user_input,
        flags=re.IGNORECASE,
    )
    if (
        simple_voyage_match
        and not re.search(r"\b\d{3,5}\s+(?:,|and|&)\s+\d{3,5}\b", user_input, flags=re.IGNORECASE)
        and not any(marker in ul_fast for marker in analytical_markers)
    ):
        voyage_number = int(simple_voyage_match.group(1))
        intent_key = "voyage.summary"
        cleaned_slots = {
            "voyage_number": voyage_number,
            "voyage_numbers": [voyage_number],
        }
        likely_path = "single"
        fast_source = "simple_voyage_fastpath"

        if intent_key and cleaned_slots is not None:
            state["intent_key"] = intent_key
            state["slots"] = cleaned_slots
            artifacts = state.get("artifacts") or {}
            artifacts["intent_key"] = intent_key
            artifacts["slots"] = cleaned_slots
            artifacts["user_input"] = user_input
            state["artifacts"] = artifacts
            self._trace(
                state,
                {
                    "phase": "intent_extraction",
                    "intent_key": intent_key,
                    "slots": cleaned_slots,
                    "likely_path": likely_path,
                    "source": fast_source,
                },
            )
            return state

    # Structured intent extraction feeds routing hints and diagnostic logging.
    def _shadow_llm_fn(prompt: str) -> str:
        try:
            resp = self.llm._call_with_retry(
                system=prompt,
                user=json.dumps({"query": user_input}),
                operation="shadow_intent_extraction",
            )
            if isinstance(resp, str):
                return resp
            if isinstance(resp, dict):
                return json.dumps(resp)
            return str(resp)
        except Exception:
            return ""

    _shadow_pre = None
    try:
        _shadow_pre = _shadow_extract(user_input, session_ctx, _shadow_llm_fn)
        if _shadow_pre:
            _sil_logger.info(
                f"SHADOW | query='{user_input[:80]}' | "
                f"op={_shadow_pre.get('operation')} | "
                f"confidence={_shadow_pre.get('confidence')} | "
                f"sources={_shadow_pre.get('required_sources')} | "
                f"scope={_shadow_pre.get('scope')} | "
                f"follow_up_action={_shadow_pre.get('follow_up_action')} | "
                f"det_intent={session_ctx.get('last_intent')}"
            )
        else:
            _sil_logger.warning(f"SHADOW_FAIL | query='{user_input[:80]}'")
    except Exception:
        pass
    # Persist structured intent for primary routing.
    if isinstance(_shadow_pre, dict) and _shadow_pre.get("confidence") in ("high", "medium"):
        if isinstance(session_ctx, dict):
            session_ctx["_structured_intent"] = _shadow_pre
            state["session_ctx"] = session_ctx
            # Keep aggregate-style operations on their configured primary source.
            _si_check = session_ctx.get("_structured_intent")
            if isinstance(_si_check, dict):
                _si_op_chk = str(_si_check.get("operation") or "").lower()
                _si_src = _si_check.get("required_sources") or []
                if not isinstance(_si_src, list):
                    _si_src = []
                # Operation ids whose intent_catalog sources are postgres-only (see config/intent_catalog.yaml).
                _SINGLE_SRC_OPS = {
                    "aggregate_analytics",
                    "aggregation_analytics",
                    "ranked_list",
                    "comparative_analysis",
                }
                if len(_si_src) > 1 and _si_op_chk in _SINGLE_SRC_OPS:
                    _si_check["required_sources"] = (
                        ["postgres"] if "postgres" in _si_src else [_si_src[0]]
                    )
                    session_ctx["_structured_intent"] = _si_check
            logger.info(
                "PRIMARY_STRUCTURED|phase=4a|confidence=%s|scope=%s|follow_up_action=%s",
                _shadow_pre.get("confidence"),
                _shadow_pre.get("scope"),
                _shadow_pre.get("follow_up_action"),
            )
            try:
                self.redis.save_session(
                    state["session_id"],
                    {"_structured_intent": _shadow_pre},
                )
            except Exception:
                pass

    # =========================================================
    # Pending clarification: treat the user's message as a slot value
    # =========================================================
    pending_intent = session_ctx.get("pending_intent") if isinstance(session_ctx, dict) else None
    pending_missing = session_ctx.get("missing_keys") if isinstance(session_ctx, dict) else None
    if isinstance(pending_intent, str) and isinstance(pending_missing, list) and pending_missing:
        options = {}
        if isinstance(session_ctx, dict) and isinstance(session_ctx.get("clarification_options"), dict):
            options = session_ctx.get("clarification_options") or {}
        pending_question = (session_ctx.get("pending_question") or "") if isinstance(session_ctx, dict) else ""
        pending_slots_base = (session_ctx.get("pending_slots") or {}) if isinstance(session_ctx, dict) else {}
        if not isinstance(pending_slots_base, dict):
            pending_slots_base = {}
        resolved_slots = self._try_resolve_missing_from_text(
            intent_key=pending_intent,
            missing_keys=pending_missing,
            user_input=raw_user_input,
            options=options,
        )
        if resolved_slots is not None:
            # Clear pending clarification immediately.
            try:
                self.redis.save_session(
                    state["session_id"],
                    {
                        **(session_ctx or {}),
                        "pending_intent": None,
                        "missing_keys": None,
                        "clarification_options": {},
                    },
                )
            except Exception:
                pass
            # Keep in-memory state aligned with persisted session to prevent stale
            # pending clarification data from being re-written later in this turn.
            if isinstance(session_ctx, dict):
                session_ctx["pending_intent"] = None
                session_ctx["missing_keys"] = None
                session_ctx["clarification_options"] = {}
                session_ctx["pending_question"] = None
                session_ctx["pending_slots"] = {}
                state["session_ctx"] = session_ctx

            intent_key = pending_intent
            extracted_slots = {**pending_slots_base, **resolved_slots}
            followup_used = True

            # Continue through the normal cleaning logic below, but skip the LLM call.
            # Use the ORIGINAL pending question as the effective query for planning/execution.
            if isinstance(pending_question, str) and pending_question.strip():
                user_input = pending_question.strip()
                state["user_input"] = user_input
            # ---------------------------------------------------------
            # Deterministic port/vessel/voyage shaping for incomplete queries
            # ---------------------------------------------------------
            intent_key, extracted_slots = self._maybe_override_incomplete_entity_intent(
                intent_key=intent_key,
                extracted_slots=extracted_slots,
                user_input=user_input,
            )

            # Use the same cleaning pipeline section (copied from below)
            cleaned_slots = {}
            user_input_lower = user_input.lower()
            # Registry-driven slot cleanup — no hardcoded intent list.
            _icfg = INTENT_REGISTRY.get(intent_key, {})
            _is_fleet_wide = _icfg.get("route") == "composite"
            _allowed = set(
                (_icfg.get("required_slots") or [])
                + (_icfg.get("optional_slots") or [])
            )

            if _is_fleet_wide:
                for key, val in extracted_slots.items():
                    if key in _allowed:
                        cleaned_slots[key] = val
            else:
                for key, value in extracted_slots.items():
                    cleaned_slots[key] = value

            state["intent_key"] = intent_key
            state["slots"] = cleaned_slots
            artifacts = state.get("artifacts") or {}
            artifacts["intent_key"] = intent_key
            artifacts["slots"] = cleaned_slots
            artifacts["user_input"] = user_input
            state["artifacts"] = artifacts
            self._trace(
                state,
                {
                    "phase": "intent_extraction",
                    "intent_key": intent_key,
                    "slots": cleaned_slots,
                    "source": "clarification_followup",
                    "raw_user_input": raw_user_input,
                    "effective_user_input": user_input,
                },
            )
            return state
        else:
            # If the user asked a *new question* instead of answering the clarification,
            # drop pending state to avoid polluting the new turn.
            if self._looks_like_new_question(user_input):
                try:
                    self.redis.save_session(
                        state["session_id"],
                        {**(session_ctx or {}), "pending_intent": None, "missing_keys": None},
                    )
                except Exception:
                    pass

    # Fast-path for chit-chat: avoid touching external dependencies (LLM/DB/Redis memory)
    # for greetings / identity / help messages. These should always be handled as out_of_scope.
    if self._is_chitchat(user_input):
        intent_key = "out_of_scope"
        cleaned_slots: Dict[str, Any] = {}

        state["intent_key"] = intent_key
        state["slots"] = cleaned_slots

        artifacts = state.get("artifacts") or {}
        artifacts["intent_key"] = intent_key
        artifacts["slots"] = cleaned_slots
        artifacts["user_input"] = user_input
        state["artifacts"] = artifacts
        self._trace(
            state,
            {
                "phase": "intent_extraction",
                "intent_key": intent_key,
                "slots": cleaned_slots,
                "source": "chitchat_fastpath",
            },
        )
        return state

    # =========================================================
    # Result-set follow-ups ("among these/from above/in that list")
    # If we have a previous multi-row result set stored, route TRUE follow-ups
    # to a deterministic handler. Avoid false-positives on brand-new questions
    # that merely contain words like "remarks" and "explain".
    # =========================================================
    ul = (user_input or "").strip().lower()
    words = [w for w in ul.split() if w]
    has_result_set = isinstance(session_ctx, dict) and isinstance(session_ctx.get("last_result_set"), dict)
    # Composite fleet delay + PnL + offhire queries must not be hijacked by last_result_set.
    stale_delay_guard = get_result_set_stale_delay_guard_terms()
    if (
        has_result_set
        and all(term in ul for term in stale_delay_guard.get("all", []))
        and any(term in ul for term in stale_delay_guard.get("any", []))
        and any(term in ul for term in stale_delay_guard.get("scope", []))
    ):
        has_result_set = False
    turn_type = self._classify_turn_type(session_ctx=session_ctx, user_input=user_input)
    explicit_fresh_entity = self._looks_like_explicit_fresh_entity_request(user_input)
    fresh_fleet_markers = get_fresh_fleet_markers()
    fresh_fleet_turn = any(marker in ul for marker in fresh_fleet_markers)
    explicit_rs_ref = any(
        p in ul
        for p in get_explicit_result_set_reference_phrases()
    )
    starts_like_new_request = ul.startswith(get_new_request_prefixes())
    _phase4a_fresh_ranking_utterance, _corpus_fresh_guard = GraphRouter._corpus_fresh_guard_flags(ul)

    # Use structured follow-up scope only when the operation/action is concrete.
    _si_scope = session_ctx.get("_structured_intent") if isinstance(session_ctx, dict) else None
    _structured_scope_ok = isinstance(_si_scope, dict) and _si_scope.get("confidence") in ("high", "medium")
    if _structured_scope_ok:
        _si_op = str(_si_scope.get("operation") or "").strip().lower()
        _si_fu_act = _si_scope.get("follow_up_action")
        _si_sc_raw = str(_si_scope.get("scope") or "").strip().lower()
        # Only treat as session follow-up when catalog operation is follow_up_filter
        # and the model picked a concrete in-memory action. This avoids ranking /
        # aggregate rows (e.g. T18, T22) being misclassified as followup.result_set
        # when the extractor sets scope=follow_up with a spurious follow_up_action.
        _genuine_followup = (
            _si_sc_raw in get_structured_followup_scopes()
            and _si_op in get_structured_followup_operations()
            and (_si_fu_act is not None and str(_si_fu_act).strip() != "")
        )
        if _genuine_followup and not _corpus_fresh_guard:
            turn_type = "followup"
            fresh_fleet_turn = False
            starts_like_new_request = False

    thread_override = None
    if not explicit_fresh_entity and turn_type == "followup" and not _corpus_fresh_guard:
        thread_override = self._direct_session_thread_override(
            session_ctx=session_ctx,
            user_input=user_input,
        )

    if (
        thread_override is not None
        and "context" in ul
        and any(p in ul for p in get_session_thread_context_markers())
    ):
        override_intent, override_slots = thread_override
        state["intent_key"] = override_intent
        state["slots"] = override_slots
        artifacts = state.get("artifacts") or {}
        artifacts["intent_key"] = override_intent
        artifacts["slots"] = override_slots
        artifacts["user_input"] = user_input
        state["artifacts"] = artifacts
        self._trace(
            state,
            {
                "phase": "intent_extraction",
                "intent_key": override_intent,
                "slots": override_slots,
                "source": "session_thread_context_override",
            },
        )
        return state

    if has_result_set and explicit_rs_ref and turn_type == "followup" and not explicit_fresh_entity:
        # Explicitly referencing the prior list/result set.
        state["intent_key"] = "followup.result_set"
        state["slots"] = {"action": "compare_extremes"}
        artifacts = state.get("artifacts") or {}
        artifacts["intent_key"] = "followup.result_set"
        artifacts["slots"] = state["slots"]
        artifacts["user_input"] = user_input
        state["artifacts"] = artifacts
        self._trace(
            state,
            {"phase": "intent_extraction", "intent_key": "followup.result_set", "slots": state["slots"], "source": "result_set_followup"},
        )
        return state

    # Explain remarks / what went wrong: only treat as follow-up if it's a short follow-up prompt,
    # NOT a full new request like "Show me the top 5 ... and include remarks that explain why ...".
    is_short_followup = len(words) <= 12
    starts_followup_verb = ul.startswith(get_result_set_explain_verbs())
    mentions_remarks = ("remark" in ul) or ("remarks" in ul)
    if has_result_set and mentions_remarks and is_short_followup and starts_followup_verb and not starts_like_new_request and turn_type == "followup" and not explicit_fresh_entity:
        # Removed brand-specific routing — scope handled by structured intent when available.
        _looks_fresh = any(
            ul.startswith(p) for p in get_result_set_fresh_start_prefixes()
        )
        if _looks_fresh:
            pass  # fall through to normal intent extraction
        else:
            state["intent_key"] = "followup.result_set"
            state["slots"] = {"action": "explain_remarks"}
            artifacts = state.get("artifacts") or {}
            artifacts["intent_key"] = "followup.result_set"
            artifacts["slots"] = state["slots"]
            artifacts["user_input"] = user_input
            state["artifacts"] = artifacts
            self._trace(
                state,
                {"phase": "intent_extraction", "intent_key": "followup.result_set", "slots": state["slots"], "source": "result_set_followup"},
            )
            return state

    # Extremes (highest/lowest) as a short follow-up prompt.
    if (
        has_result_set
        and any(k in ul for k in get_result_set_extreme_keywords())
        and is_short_followup
        and not explicit_fresh_entity
        and (
            not starts_like_new_request
            or any(k in ul for k in get_result_set_scope_phrases())
            or ul.startswith(get_result_set_question_prefixes())
        )
        and turn_type == "followup"
        and not any(ul.startswith(p) for p in get_result_set_fresh_start_prefixes())
    ):
        state["intent_key"] = "followup.result_set"
        state["slots"] = {"action": "compare_extremes"}
        artifacts = state.get("artifacts") or {}
        artifacts["intent_key"] = "followup.result_set"
        artifacts["slots"] = state["slots"]
        artifacts["user_input"] = user_input
        state["artifacts"] = artifacts
        self._trace(
            state,
            {"phase": "intent_extraction", "intent_key": "followup.result_set", "slots": state["slots"], "source": "result_set_followup"},
        )
        return state

    # Generic result-set operations (top/bottom/project/compare/filter) across any prior list result.
    # Allow polite command phrasing ("can you list ...") even if turn classifier is conservative.
    if has_result_set:
        op_slots = self._parse_result_set_followup_action(user_input=user_input, session_ctx=session_ctx)
        polite_followup = ul.startswith(get_polite_followup_prefixes())
        concise_followup_override = (not fresh_fleet_turn) and len(words) <= 12 and any(
            k in ul for k in get_concise_followup_keywords()
        )
        # Never let the metric parser hijack fleet-wide or corpus questions; the classifier
        # may mark them new_question but metric_followup_override used to bypass that.
        # Do not let stale result-set memory hijack fresh ranking or trend asks.
        metric_followup_override = (
            (not fresh_fleet_turn)
            and (not _corpus_fresh_guard)
            and isinstance(op_slots, dict)
            and op_slots.get("action") in get_metric_followup_override_actions()
            and len(words) <= 12
        )
        result_set_refinement = concise_followup_override and any(
            k in ul
            for k in get_result_set_refinement_phrases()
        )
        if (
            has_result_set
            and any(k in ul for k in get_operational_followup_keywords())
            and any(k in ul for k in get_same_entity_reference_words())
        ):
            lfs = session_ctx.get("last_focus_slots") if isinstance(session_ctx, dict) else None
            if isinstance(lfs, dict):
                scoped_slots = {}
                for key in ("vessel_name", "imo", "vessel_imo", "voyage_number"):
                    if lfs.get(key) not in (None, "", [], {}):
                        scoped_slots[key] = lfs.get(key)
                if scoped_slots:
                    state["intent_key"] = "vessel.metadata"
                    state["slots"] = scoped_slots
                    artifacts = state.get("artifacts") or {}
                    artifacts["intent_key"] = "vessel.metadata"
                    artifacts["slots"] = scoped_slots
                    artifacts["user_input"] = user_input
                    state["artifacts"] = artifacts
                    self._trace(
                        state,
                        {"phase": "intent_extraction", "intent_key": "vessel.metadata", "slots": scoped_slots, "source": "result_set_operational_followup"},
                    )
                    return state
        if (
            (not _corpus_fresh_guard)
            and isinstance(op_slots, dict)
            and op_slots.get("action")
            and ((not explicit_fresh_entity) or result_set_refinement)
            and (
                turn_type == "followup"
                or polite_followup
                or concise_followup_override
                or metric_followup_override
            )
        ):
            state["intent_key"] = "followup.result_set"
            state["slots"] = op_slots
            artifacts = state.get("artifacts") or {}
            artifacts["intent_key"] = "followup.result_set"
            artifacts["slots"] = state["slots"]
            artifacts["user_input"] = user_input
            state["artifacts"] = artifacts
            self._trace(
                state,
                {"phase": "intent_extraction", "intent_key": "followup.result_set", "slots": state["slots"], "source": "result_set_followup_generic"},
            )
            return state

    if thread_override is not None:
        override_intent, override_slots = thread_override
        state["intent_key"] = override_intent
        state["slots"] = override_slots
        artifacts = state.get("artifacts") or {}
        artifacts["intent_key"] = override_intent
        artifacts["slots"] = override_slots
        artifacts["user_input"] = user_input
        state["artifacts"] = artifacts
        self._trace(
            state,
            {
                "phase": "intent_extraction",
                "intent_key": override_intent,
                "slots": override_slots,
                "source": "session_thread_override",
            },
        )
        return state

    ex = self.llm.extract_intent_slots(
        text=user_input,
        supported_intents=list(SUPPORTED_INTENTS),
        schema_hint={
            "slots": [
                "vessel_name", "imo",
                "voyage_number", "voyage_id", "voyage_numbers",
                "date_from", "date_to",
                "limit", "port_name", "scenario",
                "cargo_type", "cargo_grade",
                "metric", "group_by", "threshold"
            ]
        },
        session_context=session_ctx,
    )

    # Log structured intent output for diagnostics.
    _shadow = _shadow_extract(user_input, session_ctx, _shadow_llm_fn)
    if _shadow:
        _sil_logger.info(
            f"SHADOW | query='{user_input[:80]}' | "
            f"op={_shadow.get('operation')} | "
            f"confidence={_shadow.get('confidence')} | "
            f"sources={_shadow.get('required_sources')} | "
            f"scope={_shadow.get('scope')} | "
            f"follow_up_action={_shadow.get('follow_up_action')} | "
            f"det_intent={ex.get('intent_key')}"
        )
    else:
        _sil_logger.warning(f"SHADOW_FAIL | query='{user_input[:80]}'")

    intent_key = ex.get("intent_key", "out_of_scope")
    extracted_slots = ex.get("slots", {}) or {}
    # session_ctx already loaded above

    # The LLM may return followup.result_set for fresh ranking/trend asks when
    # session still holds an unrelated last_result_set — remap to primary fleet intents.
    if _phase4a_fresh_ranking_utterance and intent_key == "followup.result_set":
        if any(term in ul for term in get_fresh_ranking_pnl_terms()):
            intent_key = "ranking.voyages_by_pnl"
        elif any(term in ul for term in get_fresh_ranking_segment_terms()):
            intent_key = "analysis.segment_performance"
        elif any(term in ul for term in get_fresh_ranking_trend_terms()):
            intent_key = "aggregation.trends"
        if isinstance(ex, dict):
            ex["intent_key"] = intent_key

    # Fix common LLM glitch: "tell me about voyage 1901" → vessel_name="voyage 1901"
    try:
        vn = extracted_slots.get("voyage_number")
        vname = extracted_slots.get("vessel_name")
        if isinstance(vname, str):
            vname_norm = vname.strip().lower()
            if vname_norm.startswith("voyage ") and any(ch.isdigit() for ch in vname_norm):
                # If it looks like a voyage reference, drop it as a vessel name.
                extracted_slots.pop("vessel_name", None)
            elif vn is not None and vname_norm == f"voyage {int(float(vn))}":
                extracted_slots.pop("vessel_name", None)
    except Exception:
        pass

    # Pattern-based override for scenario comparisons
    is_scenario_comp, updated_slots = self._detect_scenario_comparison(
        user_input, extracted_slots, session_ctx
    )
    if is_scenario_comp:
        # Override intent if LLM classified incorrectly
        if intent_key in ["out_of_scope", "voyage.summary", "composite.query"]:
            _dprint(f"   🔄 PATTERN OVERRIDE: {intent_key} → comparison.scenario")
            intent_key = "comparison.scenario"
        extracted_slots = updated_slots
    # Fallback: when-fixed + compare/variance but pattern missed → force scenario comparison
    ul = user_input.lower()
    if intent_key == "out_of_scope" and ("when-fixed" in ul or "when fixed" in ul):
        if "compare" in ul or "variance" in ul or "versus" in ul or "vs" in ul:
            intent_key = "analysis.scenario_comparison"
            _dprint(f"   🔄 FALLBACK: out_of_scope → analysis.scenario_comparison")

    # Convert voyage_number(s) float to int
    if "voyage_number" in extracted_slots:
        try:
            vn = extracted_slots["voyage_number"]
            if isinstance(vn, list):
                extracted_slots["voyage_number"] = [int(float(v)) for v in vn]
            else:
                extracted_slots["voyage_number"] = int(float(vn))
        except (ValueError, TypeError):
            pass

    if "voyage_numbers" in extracted_slots:
        try:
            vns = extracted_slots["voyage_numbers"]
            if isinstance(vns, list):
                extracted_slots["voyage_numbers"] = [int(float(v)) for v in vns]
        except (ValueError, TypeError):
            pass

    # Convert limit float to int
    if "limit" in extracted_slots:
        try:
            extracted_slots["limit"] = int(float(extracted_slots["limit"]))
        except (ValueError, TypeError):
            pass

    # Convert threshold float to int
    if "threshold" in extracted_slots:
        try:
            extracted_slots["threshold"] = int(float(extracted_slots["threshold"]))
        except (ValueError, TypeError):
            pass

    # Explicit voyage identifiers in the current query must take precedence over
    # previously inferred vessel anchors from session memory.
    explicit_voyage_request = bool(
        re.search(r"\bvoyage\s+\d{3,5}\b", ul)
        or (
            (
                extracted_slots.get("voyage_number") not in (None, "", [], {})
                or (isinstance(extracted_slots.get("voyage_numbers"), list) and bool(extracted_slots.get("voyage_numbers")))
                or extracted_slots.get("voyage_id") not in (None, "", [], {})
            )
            and any(k in ul for k in ("voyage", "voyages", "belongs to", "assigned"))
        )
    )
    explicit_vessel_in_query = len(GraphRouter._explicit_vessel_mentions(user_input)) > 0 or bool(re.search(r"\b\d{7}\b", user_input or ""))
    if explicit_voyage_request and not explicit_vessel_in_query:
        extracted_slots.pop("vessel_name", None)
        extracted_slots.pop("imo", None)
        extracted_slots.pop("vessel_imo", None)

    # =========================================================
    # Deterministic intent for cargo-grade voyage filters
    # "what voyages have NHC as cargo grade" should not go out_of_scope.
    # =========================================================
    if any(k in ul for k in get_cargo_grade_terms()) and any(k in ul for k in get_cargo_grade_voyage_terms()):
        if extracted_slots.get("cargo_grade") or extracted_slots.get("cargo_type"):
            extracted_slots["cargo_grade"] = extracted_slots.get("cargo_grade") or extracted_slots.get("cargo_type")
            intent_key = "ops.voyages_by_cargo_grade"

    # =========================================================
    # Session memory (follow-up resolution)
    # If the user asks a follow-up without repeating the entity,
    # reuse the last known voyage/vessel from Redis session slots.
    # =========================================================
    llm_followup = bool(ex.get("is_followup"))
    backward_reference = bool(ex.get("backward_reference"))
    followup_confidence = str(ex.get("followup_confidence") or "").strip().lower()
    inherit_slot_keys = ex.get("inherit_slots_from_session")
    inherit_slot_keys = inherit_slot_keys if isinstance(inherit_slot_keys, list) else []
    inherit_slot_keys = [str(k).strip() for k in inherit_slot_keys if str(k).strip()]

    extracted_slots = self._validate_entity_slots(extracted_slots)

    sess_mem = session_ctx.get("memory_slots") or session_ctx.get("slots") or {}
    followup_used = False
    if llm_followup or (followup_confidence == "high" and inherit_slot_keys):
        intent_key, extracted_slots, followup_used = self._apply_session_followup(
            intent_key=intent_key,
            extracted_slots=extracted_slots,
            session_ctx=session_ctx,
            user_input=user_input,
            inherit_slot_keys=inherit_slot_keys,
            backward_reference=backward_reference,
        )
        extracted_slots = self._validate_entity_slots(extracted_slots)
        if explicit_voyage_request and not explicit_vessel_in_query:
            extracted_slots.pop("vessel_name", None)
            extracted_slots.pop("imo", None)
            extracted_slots.pop("vessel_imo", None)

    # Session follow-up logic can re-map to
    # followup.result_set for fresh ranking/trend utterances — restore primary intents.
    if _phase4a_fresh_ranking_utterance and intent_key == "followup.result_set":
        if any(term in ul for term in get_fresh_ranking_pnl_terms()):
            intent_key = "ranking.voyages_by_pnl"
        elif any(term in ul for term in get_fresh_ranking_segment_terms()):
            intent_key = "analysis.segment_performance"
        elif any(term in ul for term in get_fresh_ranking_trend_terms()):
            intent_key = "aggregation.trends"

    if intent_key == "cargo.details" and not (
        extracted_slots.get("cargo_type")
        or extracted_slots.get("cargo_grade")
        or extracted_slots.get("cargo_grades")
    ):
        sess_mem_local = session_ctx.get("memory_slots") or session_ctx.get("slots") or {}
        if any((sess_mem_local or {}).get(k) not in (None, "", [], {}) for k in ("voyage_number", "voyage_id", "voyage_numbers")):
            intent_key = "voyage.metadata"

    if intent_key == "cargo.details" and (
        extracted_slots.get("voyage_number")
        or extracted_slots.get("voyage_id")
        or extracted_slots.get("voyage_numbers")
    ):
        intent_key = "voyage.metadata"

    # =========================================================
    # Direct entity-thread rescue
    # Let the LLM see recent turn history first, then rescue weak/new-query
    # classifications back onto the active voyage/vessel thread when the
    # user is clearly continuing that thread.
    # =========================================================
    if llm_followup and not self._has_entity_anchor(extracted_slots):
        skip_v_hist = len(GraphRouter._explicit_vessel_mentions(user_input)) >= 2
        for key in self._entity_slot_keys():
            if skip_v_hist and key in ("vessel_name", "imo"):
                continue
            inherited_value = self._find_slot_in_turn_history(
                session_ctx=session_ctx,
                slot_key=key,
                prefer_older=backward_reference,
            )
            if inherited_value not in (None, "", [], {}):
                extracted_slots[key] = inherited_value
        extracted_slots = self._validate_entity_slots(extracted_slots)
        followup_used = self._has_entity_anchor(extracted_slots)

    extracted_slots = self._normalize_vessel_anchor_from_query(
        extracted_slots, user_input, session_ctx
    )
    extracted_slots = self._validate_entity_slots(extracted_slots)

    comparison_override = self._comparison_followup_override(
        session_ctx=session_ctx,
        user_input=user_input,
        intent_key=intent_key,
        extracted_slots=extracted_slots,
        backward_reference=backward_reference,
    )
    if comparison_override is not None:
        intent_key, extracted_slots, effective_user_input = comparison_override
        state["user_input"] = effective_user_input
        user_input = effective_user_input
        followup_used = True

    # =========================================================
    # Lightweight parameter memory (limit/date/scenario/etc.)
    # Only applies on follow-ups and only for safe preference-like keys.
    # =========================================================
    extracted_slots = self._apply_session_param_memory(
        extracted_slots=extracted_slots,
        session_ctx=session_ctx,
        user_input=user_input,
        followup_used=followup_used,
    )

    # =========================================================
    # Deterministic port extraction + intent override for ops port queries
    # Example: "Find voyages with Rotterdam in the route ..."
    # =========================================================
    intent_key, extracted_slots = self._maybe_override_to_port_query(
        intent_key=intent_key,
        extracted_slots=extracted_slots,
        user_input=user_input,
    )

    # Delayed + negative PnL: avoid ops.port_query with port_name="negative PnL" → force finance.loss_due_to_delay
    if intent_key == "ops.port_query" and extracted_slots.get("port_name"):
        pn = str(extracted_slots.get("port_name", "")).lower()
        if "pnl" in pn or ("negative" in pn and "port" not in pn):
            _dprint(f"   🔄 OVERRIDE: ops.port_query (port_name={extracted_slots.get('port_name')}) → finance.loss_due_to_delay")
            intent_key = "finance.loss_due_to_delay"
            extracted_slots.pop("port_name", None)

    # Handle incomplete entity questions ("tell me about port/vessel/voyage") deterministically.
    intent_key, extracted_slots = self._maybe_override_incomplete_entity_intent(
        intent_key=intent_key,
        extracted_slots=extracted_slots,
        user_input=user_input,
    )

    # Clean slots based on what's in the query
    cleaned_slots = {}
    user_input_lower = user_input.lower()

    # Registry-driven slot cleanup — no hardcoded intent list.
    # Reads route + allowed slots directly from INTENT_REGISTRY.
    _icfg = INTENT_REGISTRY.get(intent_key, {})
    _is_fleet_wide = _icfg.get("route") == "composite"
    _allowed = set(
        (_icfg.get("required_slots") or [])
        + (_icfg.get("optional_slots") or [])
    )

    if _is_fleet_wide:
        # Fleet-wide intents: keep only registry-declared slots.
        # Prevents entity anchors (voyage_number, vessel_name, imo, etc.)
        # from polluting fleet-wide queries when the LLM hallucinates them.
        for key, val in extracted_slots.items():
            if key in _allowed:
                cleaned_slots[key] = val

        # Preserve explicit voyage anchors for scoped aggregate prompts such as
        # "Which ports were visited on voyage 2306?" while still dropping
        # hallucinated entity text for generic fleet-wide asks.
        if "voyage_number" in extracted_slots:
            cleaned_slots["voyage_number"] = extracted_slots["voyage_number"]
        if "voyage_numbers" in extracted_slots:
            cleaned_slots["voyage_numbers"] = extracted_slots["voyage_numbers"]
        if "voyage_id" in extracted_slots:
            cleaned_slots["voyage_id"] = extracted_slots["voyage_id"]

        # Keep explicit vessel anchors only when the query text truly contains
        # the candidate value (prevents false anchors like "is fastest on ballast").
        vname = extracted_slots.get("vessel_name")
        if isinstance(vname, str) and vname.strip() and vname.lower() in user_input_lower:
            cleaned_slots["vessel_name"] = vname
        if "imo" in extracted_slots and str(extracted_slots.get("imo") or "").strip():
            cleaned_slots["imo"] = extracted_slots["imo"]
    else:
        # For specific entity queries, validate slots are mentioned in query
        for key, value in extracted_slots.items():
            if key == "voyage_number":
                # Keep only if voyage number is mentioned in query
                if followup_used or str(value) in user_input:
                    cleaned_slots[key] = value
            elif key == "voyage_numbers":
                # Always keep voyage_numbers for comparisons
                cleaned_slots[key] = value
            elif key == "vessel_name":
                # Keep only if vessel name is mentioned in query
                if followup_used or (isinstance(value, str) and value.lower() in user_input_lower):
                    cleaned_slots[key] = value
            elif key == "scenario":
                # Always keep scenario for comparisons
                cleaned_slots[key] = value
            else:
                # Keep other slots
                cleaned_slots[key] = value

    # =========================================================
    # Normalize voyage_numbers to voyage_number (single)
    # =========================================================
    if "voyage_numbers" in cleaned_slots and isinstance(cleaned_slots["voyage_numbers"], list) and len(cleaned_slots["voyage_numbers"]) == 1:
        cleaned_slots["voyage_number"] = cleaned_slots["voyage_numbers"][0]

    # Final guard: explicit voyage references should not carry stale vessel filters.
    if explicit_voyage_request and not explicit_vessel_in_query:
        cleaned_slots.pop("vessel_name", None)
        cleaned_slots.pop("imo", None)
        cleaned_slots.pop("vessel_imo", None)

    _dprint("\nIntent extraction:")
    _dprint(f"   Intent: {intent_key}")
    _dprint(f"   Slots (cleaned): {cleaned_slots}")

    # Final guard before persisting intent: avoids stale-session
    # followup.result_set on fresh fleet ranking / trend asks regardless of upstream overrides.
    _ul_fin = (user_input or "").strip().lower()
    _, _cf_fin = GraphRouter._corpus_fresh_guard_flags(_ul_fin)
    if intent_key == "followup.result_set" and _cf_fin:
        if any(term in _ul_fin for term in get_fresh_ranking_pnl_terms()):
            intent_key = "ranking.voyages_by_pnl"
        elif any(term in _ul_fin for term in get_fresh_ranking_segment_terms()):
            intent_key = "analysis.segment_performance"
        elif "trend" in _ul_fin and ("month" in _ul_fin or "average" in _ul_fin):
            intent_key = "aggregation.trends"
        elif len(_ul_fin.split()) > 12:
            intent_key = "composite.query"
        elif re.search(r"\b(top|bottom)\s+\d{1,2}\b", _ul_fin):
            intent_key = "composite.query"
    elif intent_key == "followup.result_set" and _ul_fin.startswith(
        ("show ", "show me", "list ", "rank ", "find ", "tell ", "what ", "which ")
    ):
        if "most profitable" in _ul_fin:
            intent_key = "ranking.voyages_by_pnl"
        elif "loss-making" in _ul_fin or "loss making" in _ul_fin:
            intent_key = "analysis.segment_performance"
        elif "trend" in _ul_fin and ("month" in _ul_fin or "average" in _ul_fin):
            intent_key = "aggregation.trends"

    if intent_key == "voyage.metadata" and (
        is_voyage_composite_profile_query(user_input, cleaned_slots)
        or is_voyage_operational_profile_query(user_input, cleaned_slots)
    ):
        intent_key = "voyage.summary"
        _dprint("   🔄 VOYAGE PROFILE: voyage.metadata → voyage.summary")

    vnums_multi = cleaned_slots.get("voyage_numbers")
    _ul_mv = (user_input or "").strip().lower()
    _compare_explicit = any(
        k in _ul_mv for k in ("compare", " versus", " vs ", "vs ", "difference between")
    )
    _scenario_compare_explicit = (
        ("actual" in _ul_mv)
        and (
            "when-fixed" in _ul_mv
            or "when fixed" in _ul_mv
            or "whenfixed" in _ul_mv
        )
    )
    if isinstance(vnums_multi, list) and len(vnums_multi) >= 2:
        cleaned_slots.pop("voyage_number", None)
        if re.search(r"\bvoyag", _ul_mv):
            if _scenario_compare_explicit:
                intent_key = "analysis.scenario_comparison"
                _dprint("   🎯 MULTI-VOYAGE: actual vs when-fixed → analysis.scenario_comparison")
            elif _compare_explicit:
                intent_key = "comparison.voyages"
                _dprint("   🎯 MULTI-VOYAGE: explicit compare → comparison.voyages")
            else:
                intent_key = "voyage.summary"
                cleaned_slots.pop("vessel_name", None)
                cleaned_slots.pop("imo", None)
                cleaned_slots.pop("vessel_imo", None)
                _dprint("   🎯 MULTI-VOYAGE: voyage.summary single path (per-voyage sections)")

    if is_read_only_mutation_request(user_input):
        intent_key = "out_of_scope"
        cleaned_slots = {}
        _dprint("   🔄 READ-ONLY: mutation request → out_of_scope")

    cleaned_slots = merge_voyage_slot_fields(cleaned_slots, user_input)

    # Pre-classify single vs composite
    is_single = self._is_single_entity_query(intent_key, cleaned_slots, user_input)
    if is_single:
        _dprint(f"   🎯 PRE-CLASSIFIED AS SINGLE (specific entity)")
    else:
        _dprint(f"   🎯 LIKELY COMPOSITE (ranking + multi-agent)")

    # Update state with cleaned slots
    state["intent_key"] = intent_key
    state["slots"] = cleaned_slots

    # Store in artifacts
    artifacts = state.get("artifacts") or {}
    artifacts["intent_key"] = intent_key
    artifacts["slots"] = cleaned_slots
    artifacts["user_input"] = user_input
    state["artifacts"] = artifacts

    self._trace(
        state,
        {
            "phase": "intent_extraction",
            "intent_key": intent_key,
            "slots": cleaned_slots,
            "likely_path": "single" if is_single else "composite",
            "source": "llm_extract_intent_slots",
        },
    )

    return state
