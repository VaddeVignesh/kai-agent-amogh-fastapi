from __future__ import annotations

from types import SimpleNamespace

from app.adapters.redis_store import RedisConfig, RedisStore
from app.orchestration.graph_router import GraphRouter
from app.orchestration.planner import ExecutionPlan


def _dummy_agent(rows: list[dict] | None = None):
    payload_rows = rows or [{"voyage_number": 2205, "pnl": -1000, "revenue": 5000, "total_expense": 6000}]
    return SimpleNamespace(
        pg=None,
        run=lambda **kwargs: {"mode": "registry_sql", "rows": payload_rows},
        fetch_full_voyage_context=lambda **kwargs: {},
        run_llm_find=lambda **kwargs: {"mode": "mongo_llm", "ok": False, "rows": []},
        adapter=SimpleNamespace(
            get_vessel_imo_by_name=lambda name: "9705902" if str(name).strip().lower() == "elka delphi" else None,
            fetch_vessel=lambda imo, projection=None: {"imo": str(imo)} if str(imo).strip() in ("9705902", "9766322") else None,
            fetch_vessel_by_name=lambda name, projection=None: {"name": str(name), "imo": "9705902"} if str(name).strip().lower() == "elka delphi" else None,
            get_voyage_by_number=lambda voyage_number, projection=None: {"voyageId": f"VID{int(voyage_number)}"} if int(voyage_number) in (2205, 2306) else None,
            fetch_voyage=lambda voyage_id, projection=None: {"voyageId": str(voyage_id)} if str(voyage_id).strip() in ("VID2205", "VID2306") else None,
        ),
    )


def test_redis_store_records_turn_history_only_for_recorded_turns(monkeypatch) -> None:
    monkeypatch.setenv("REDIS_DISABLED", "1")
    store = RedisStore(RedisConfig())
    sid = "s-memory-turns"

    store.save_session(
        sid,
        {
            "slots": {"voyage_number": 2205},
            "last_result_set": {"rows": [{"voyage_number": 2205}]},
        },
    )
    sess = store.load_session(sid)
    assert sess.get("turn") == 0
    assert sess.get("turn_history") == []

    turn_record = {
        "query": "show voyage 2205",
        "intent_key": "voyage.summary",
        "slots": {"voyage_number": 2205},
        "answer_headline": "Voyage 2205 had a loss.",
    }
    store.save_session(
        sid,
        {
            "last_intent": "voyage.summary",
            "last_user_input": "show voyage 2205",
            "slots": {"voyage_number": 2205},
            "_turn_marker": "turn-1",
            "_record_turn": turn_record,
        },
    )
    # Re-saving within the same request should not append the same turn again.
    store.save_session(
        sid,
        {
            "_turn_marker": "turn-1",
            "_record_turn": turn_record,
        },
    )

    sess = store.load_session(sid)
    assert sess.get("turn") == 1
    assert len(sess.get("turn_history") or []) == 1
    assert (sess.get("turn_history") or [])[0]["query"] == "show voyage 2205"


def test_redis_store_keeps_entity_context_within_same_family(monkeypatch) -> None:
    monkeypatch.setenv("REDIS_DISABLED", "1")
    store = RedisStore(RedisConfig())
    sid = "s-memory-family"

    store.save_session(
        sid,
        {
            "last_intent": "voyage.summary",
            "slots": {"voyage_number": 2205},
            "last_result_set": {"rows": [{"voyage_number": 2205}]},
            "_turn_marker": "turn-1",
            "_record_turn": {
                "query": "show voyage 2205",
                "intent_key": "voyage.summary",
                "slots": {"voyage_number": 2205},
                "answer_headline": "Voyage 2205 had a loss.",
            },
        },
    )
    store.save_session(
        sid,
        {
            "last_intent": "voyage.metadata",
            "slots": {"voyage_number": 2205},
        },
    )

    sess = store.load_session(sid)
    assert (sess.get("slots") or {}).get("voyage_number") == 2205
    assert isinstance(sess.get("last_result_set"), dict)

    store.save_session(
        sid,
        {
            "last_intent": "vessel.summary",
            "slots": {"vessel_name": "Stena Superior"},
        },
    )
    sess = store.load_session(sid)
    assert (sess.get("slots") or {}).get("vessel_name") == "Stena Superior"
    assert "last_result_set" not in sess


def test_graph_router_passes_recent_session_context_to_llm(monkeypatch) -> None:
    monkeypatch.setenv("REDIS_DISABLED", "1")
    redis = RedisStore(RedisConfig())
    sid = "s-router-history"
    redis.save_session(
        sid,
        {
            "last_intent": "voyage.summary",
            "memory_slots": {"voyage_number": 2205},
            "slots": {"voyage_number": 2205},
            "turn_history": [
                {
                    "turn": 1,
                    "query": "show voyage 2205",
                    "intent_key": "voyage.summary",
                    "slots": {"voyage_number": 2205},
                    "answer_headline": "Voyage 2205 had a loss.",
                }
            ],
        },
    )

    captured: dict[str, dict] = {}

    def _extract_intent_slots(**kwargs):
        captured["extract"] = kwargs
        return {
            "intent_key": "voyage.summary",
            "slots": {},
            "is_followup": True,
            "inherit_slots_from_session": ["voyage_number"],
            "backward_reference": False,
            "followup_confidence": "high",
        }

    def _summarize_answer(**kwargs):
        captured["summarize"] = kwargs
        return "The voyage remained loss-making because expenses were higher than revenue."

    llm = SimpleNamespace(
        extract_intent_slots=_extract_intent_slots,
        summarize_answer=_summarize_answer,
    )

    agent = _dummy_agent()
    router = GraphRouter(
        llm=llm,
        redis_store=redis,
        mongo_agent=agent,
        finance_agent=agent,
        ops_agent=agent,
    )
    router.planner = SimpleNamespace(
        build_plan=lambda **kwargs: ExecutionPlan(
            plan_type="single",
            intent_key=kwargs.get("intent_key"),
            required_slots=[],
            confidence=0.9,
            steps=[],
        ),
    )

    out = router.handle(session_id=sid, user_input="what about expenses?")
    assert out.get("answer")
    if captured.get("extract"):
        assert (captured.get("extract") or {}).get("session_context", {}).get("turn_history")
    assert (captured.get("summarize") or {}).get("session_context", {}).get("turn_history")


def test_direct_session_thread_override_keeps_clear_voyage_followup() -> None:
    llm_calls = {"extract": 0}

    def _extract_intent_slots(**kwargs):
        llm_calls["extract"] += 1
        return {
            "intent_key": "voyage.metadata",
            "slots": {},
            "is_followup": True,
            "inherit_slots_from_session": ["voyage_number"],
            "backward_reference": False,
            "followup_confidence": "high",
        }

    llm = SimpleNamespace(
        extract_intent_slots=_extract_intent_slots,
        summarize_answer=lambda **kwargs: "ok",
    )

    agent = _dummy_agent()
    redis = RedisStore(RedisConfig())
    sid = "s-direct-thread-override"
    redis.save_session(
        sid,
        {
            "last_intent": "voyage.summary",
            "memory_slots": {"voyage_number": 2306},
            "slots": {"voyage_number": 2306},
        },
    )

    router = GraphRouter(
        llm=llm,
        redis_store=redis,
        mongo_agent=agent,
        finance_agent=agent,
        ops_agent=agent,
    )
    router.planner = SimpleNamespace(
        build_plan=lambda **kwargs: ExecutionPlan(
            plan_type="single",
            intent_key=kwargs.get("intent_key"),
            required_slots=[],
            confidence=0.9,
            steps=[],
        ),
    )

    out = router.handle(session_id=sid, user_input="what cargo was carried?")
    assert out.get("intent_key") == "voyage.metadata"
    assert out.get("slots", {}).get("voyage_number") == 2306
    assert llm_calls["extract"] == 0


def test_followup_keeps_vessel_metadata_intent_for_same_vessel(monkeypatch) -> None:
    monkeypatch.setenv("REDIS_DISABLED", "1")
    redis = RedisStore(RedisConfig())
    sid = "s-vessel-metadata-followup"
    redis.save_session(
        sid,
        {
            "last_intent": "vessel.metadata",
            "memory_slots": {"vessel_name": "Elka Delphi"},
            "slots": {"vessel_name": "Elka Delphi"},
            "turn_history": [
                {
                    "turn": 1,
                    "query": "Show vessel id, IMO, and account code for vessel Elka Delphi",
                    "intent_key": "vessel.metadata",
                    "slots": {"vessel_name": "Elka Delphi"},
                    "answer_headline": "Elka Delphi IMO 9705902 account code PC2644.",
                }
            ],
        },
    )

    captured: dict[str, dict] = {}

    def _extract_intent_slots(**kwargs):
        captured["extract"] = kwargs
        return {
            "intent_key": "vessel.metadata",
            "slots": {},
            "is_followup": True,
            "inherit_slots_from_session": ["vessel_name"],
            "backward_reference": False,
            "followup_confidence": "high",
        }

    llm = SimpleNamespace(
        extract_intent_slots=_extract_intent_slots,
        summarize_answer=lambda **kwargs: "metadata ok",
    )
    agent = _dummy_agent(rows=[{"vessel_name": "Elka Delphi"}])
    router = GraphRouter(llm=llm, redis_store=redis, mongo_agent=agent, finance_agent=agent, ops_agent=agent)
    router.planner = SimpleNamespace(
        build_plan=lambda **kwargs: ExecutionPlan(plan_type="single", intent_key=kwargs.get("intent_key"), required_slots=[], confidence=0.9, steps=[]),
    )

    out = router.handle(session_id=sid, user_input="What is the hire rate, scrubber status, and market type?")
    assert out.get("intent_key") == "vessel.metadata"
    assert (out.get("slots") or {}).get("vessel_name") == "Elka Delphi"


def test_best_worst_followup_projects_field_without_losing_result_set(monkeypatch) -> None:
    monkeypatch.setenv("REDIS_DISABLED", "1")
    redis = RedisStore(RedisConfig())
    sid = "s-best-worst-field"
    redis.save_session(
        sid,
        {
            "last_result_set": {
                "source_intent": "vessel.summary",
                "rows": [
                    {"voyage_number": 2302, "vessel_name": "Stena Superior", "pnl": 100.0, "cargo_grades": ["cpp"]},
                    {"voyage_number": 2204, "vessel_name": "Stena Superior", "pnl": 10.0, "cargo_grades": ["nhc"]},
                ],
                "meta": {"primary_metric": "pnl", "available_metrics": ["pnl"]},
            },
            "last_focus_slots": {"vessel_name": "Stena Superior"},
        },
    )

    llm = SimpleNamespace(
        extract_intent_slots=lambda **kwargs: {"intent_key": "out_of_scope", "slots": {}},
        summarize_answer=lambda **kwargs: "ok",
    )
    agent = _dummy_agent()
    router = GraphRouter(llm=llm, redis_store=redis, mongo_agent=agent, finance_agent=agent, ops_agent=agent)
    router.planner = SimpleNamespace(
        build_plan=lambda **kwargs: ExecutionPlan(plan_type="single", intent_key=kwargs.get("intent_key"), required_slots=[], confidence=0.9, steps=[]),
    )

    out = router.handle(session_id=sid, user_input="What was the cargo grade on the worst voyage?")
    assert "nhc" in (out.get("answer") or "").lower()

    sess = redis.load_session(sid)
    rows = ((sess.get("last_result_set") or {}).get("rows") or [])
    assert len(rows) == 2


def test_session_followup_inherits_vessel_anchor_without_repeated_anchor() -> None:
    session_ctx = {
        "last_intent": "vessel.metadata",
        "memory_slots": {"vessel_name": "Elka Delphi"},
        "slots": {"vessel_name": "Elka Delphi"},
    }
    router = GraphRouter(
        llm=SimpleNamespace(),
        redis_store=RedisStore(RedisConfig()),
        mongo_agent=_dummy_agent(rows=[{"vessel_name": "Elka Delphi"}]),
        finance_agent=_dummy_agent(),
        ops_agent=_dummy_agent(),
    )
    intent_key, slots, used = router._apply_session_followup(
        intent_key="vessel.metadata",
        extracted_slots={},
        session_ctx=session_ctx,
        user_input="What is the hire rate, scrubber status, and market type?",
        inherit_slot_keys=["vessel_name"],
        backward_reference=False,
    )
    assert used is True
    assert intent_key == "vessel.metadata"
    assert slots.get("vessel_name") == "Elka Delphi"


def test_session_followup_inherits_voyage_anchor_for_cargo_without_repeated_anchor() -> None:
    session_ctx = {
        "last_intent": "voyage.summary",
        "memory_slots": {"voyage_number": 2306},
        "slots": {"voyage_number": 2306},
    }
    router = GraphRouter(
        llm=SimpleNamespace(),
        redis_store=RedisStore(RedisConfig()),
        mongo_agent=_dummy_agent(rows=[{"voyage_number": 2306}]),
        finance_agent=_dummy_agent(),
        ops_agent=_dummy_agent(),
    )
    intent_key, slots, used = router._apply_session_followup(
        intent_key="cargo.details",
        extracted_slots={},
        session_ctx=session_ctx,
        user_input="What cargo was carried? Show grade, BL quantity, shipper, load port, and discharge port.",
        inherit_slot_keys=["voyage_number"],
        backward_reference=False,
    )
    assert used is True
    assert intent_key == "cargo.details"
    assert slots.get("voyage_number") == 2306


def test_router_keeps_saved_vessel_anchor_when_llm_extracts_currently(monkeypatch) -> None:
    monkeypatch.setenv("REDIS_DISABLED", "1")
    redis = RedisStore(RedisConfig())
    sid = "s-currently-anchor"
    redis.save_session(
        sid,
        {
            "last_intent": "vessel.metadata",
            "memory_slots": {"vessel_name": "Elka Delphi", "imo": "9705902"},
            "slots": {"vessel_name": "Elka Delphi", "imo": "9705902"},
            "turn_history": [
                {
                    "turn": 1,
                    "query": "Show vessel id, IMO, and account code for vessel Elka Delphi",
                    "intent_key": "vessel.metadata",
                    "slots": {"vessel_name": "Elka Delphi", "imo": "9705902"},
                    "answer_headline": "Elka Delphi IMO 9705902 account code PC2644.",
                }
            ],
        },
    )

    llm = SimpleNamespace(
        extract_intent_slots=lambda **kwargs: {
            "intent_key": "vessel.metadata",
            "slots": {"vessel_name": "currently"},
            "is_followup": True,
            "inherit_slots_from_session": ["vessel_name", "imo"],
            "backward_reference": False,
            "followup_confidence": "high",
        },
        summarize_answer=lambda **kwargs: "ok",
    )
    agent = _dummy_agent(rows=[{"vessel_name": "Elka Delphi", "is_operating": True}])
    router = GraphRouter(llm=llm, redis_store=redis, mongo_agent=agent, finance_agent=agent, ops_agent=agent)
    router.planner = SimpleNamespace(
        build_plan=lambda **kwargs: ExecutionPlan(plan_type="single", intent_key=kwargs.get("intent_key"), required_slots=[], confidence=0.9, steps=[]),
    )

    out = router.handle(session_id=sid, user_input="Is this vessel currently operational?")
    assert out.get("intent_key") == "vessel.metadata"
    assert (out.get("slots") or {}).get("vessel_name") == "Elka Delphi"
    assert (out.get("slots") or {}).get("imo") == "9705902"


def test_router_rescues_cargo_followup_from_generic_ops_path(monkeypatch) -> None:
    monkeypatch.setenv("REDIS_DISABLED", "1")
    redis = RedisStore(RedisConfig())
    sid = "s-cargo-thread-followup"
    redis.save_session(
        sid,
        {
            "last_intent": "voyage.metadata",
            "memory_slots": {"voyage_number": 2306},
            "slots": {"voyage_number": 2306},
            "turn_history": [
                {
                    "turn": 1,
                    "query": "What is the charterer, broker, and commission rates for voyage 2306?",
                    "intent_key": "voyage.metadata",
                    "slots": {"voyage_number": 2306},
                    "answer_headline": "Voyage 2306 charterer and broker details.",
                }
            ],
        },
    )

    llm = SimpleNamespace(
        extract_intent_slots=lambda **kwargs: {
            "intent_key": "voyage.metadata",
            "slots": {},
            "is_followup": True,
            "inherit_slots_from_session": ["voyage_number"],
            "backward_reference": False,
            "followup_confidence": "high",
        },
        summarize_answer=lambda **kwargs: "ok",
    )
    agent = _dummy_agent(rows=[{"voyage_number": 2306, "cargo_grades": ["NHC"]}])
    router = GraphRouter(llm=llm, redis_store=redis, mongo_agent=agent, finance_agent=agent, ops_agent=agent)
    router.planner = SimpleNamespace(
        build_plan=lambda **kwargs: ExecutionPlan(plan_type="single", intent_key=kwargs.get("intent_key"), required_slots=[], confidence=0.9, steps=[]),
    )

    out = router.handle(session_id=sid, user_input="What cargo was carried?")
    assert out.get("intent_key") == "voyage.metadata"
    assert (out.get("slots") or {}).get("voyage_number") == 2306


def test_multi_voyage_summary_branch_persists_session_memory(monkeypatch) -> None:
    monkeypatch.setenv("REDIS_DISABLED", "1")
    redis = RedisStore(RedisConfig())
    sid = "s-multi-voyage-summary-save"

    llm = SimpleNamespace(
        extract_intent_slots=lambda **kwargs: {
            "intent_key": "voyage.metadata",
            "slots": {"voyage_number": 2306},
            "is_followup": False,
            "inherit_slots_from_session": [],
            "backward_reference": False,
            "followup_confidence": "low",
        },
        summarize_answer=lambda **kwargs: "unused",
        _call_with_retry=lambda **kwargs: "Voyage 2306 answer",
    )

    finance_agent = SimpleNamespace(
        pg=None,
        run=lambda **kwargs: {
            "mode": "registry_sql",
            "rows": [
                {
                    "voyage_id": "VID2306",
                    "voyage_number": 2306,
                    "vessel_imo": "9667485",
                    "vessel_name": "Stena Imperial",
                    "pnl": 100.0,
                    "revenue": 200.0,
                    "total_expense": 100.0,
                    "tce": 10.0,
                    "total_commission": 5.0,
                }
            ],
        },
    )
    mongo_agent = SimpleNamespace(
        adapter=SimpleNamespace(
            fetch_voyage=lambda voyage_id, projection=None: {
                "voyageId": "VID2306",
                "voyageNumber": 2306,
                "vesselName": "Stena Imperial",
                "remarks": ["remark 1"],
            },
            get_voyage_by_number=lambda voyage_number, projection=None: {
                "voyageId": "VID2306",
                "voyageNumber": 2306,
                "vesselName": "Stena Imperial",
                "remarks": ["remark 1"],
            },
        ),
        fetch_full_voyage_context=lambda **kwargs: {
            "voyage_id": "VID2306",
            "voyage_number": 2306,
            "vessel_imo": "9667485",
            "vessel_name": "Stena Imperial",
        },
        run_llm_find=lambda **kwargs: {"mode": "mongo_llm", "ok": False, "rows": []},
    )
    ops_agent = _dummy_agent(rows=[])

    router = GraphRouter(
        llm=llm,
        redis_store=redis,
        mongo_agent=mongo_agent,
        finance_agent=finance_agent,
        ops_agent=ops_agent,
    )

    out = router.handle(session_id=sid, user_input="What is the charterer, broker, and commission rates for voyage 2306?")
    assert out.get("answer")

    sess = redis.load_session(sid)
    assert (sess.get("memory_slots") or {}).get("voyage_number") == 2306
    assert (sess.get("memory_slots") or {}).get("voyage_id") == "VID2306"
    assert (sess.get("last_intent") or "") == "voyage.summary"


def test_result_set_followup_top_ports_refines_rows_not_field_projection() -> None:
    session_ctx = {
        "last_result_set": {
            "rows": [
                {"port_name": "rotterdam", "finance_voyage_count": 175},
                {"port_name": "houston", "finance_voyage_count": 153},
                {"port_name": "singapore", "finance_voyage_count": 88},
            ],
            "meta": {"primary_metric": "voyage_count", "available_metrics": ["voyage_count"]},
        }
    }
    action = GraphRouter._parse_result_set_followup_action(
        user_input="What about the top 5 ports?",
        session_ctx=session_ctx,
    )
    assert action == {"action": "top_n", "n": 5, "metric": "voyage_count"}


def test_result_set_followup_avg_revenue_uses_avg_metric() -> None:
    session_ctx = {
        "last_result_set": {
            "rows": [
                {"module_type": "Spot", "avg_pnl": 723800.0, "avg_revenue": 2198048.0, "voyage_count": 1321},
                {"module_type": "TC Voyage", "avg_pnl": 575965.0, "avg_revenue": 1944477.0, "voyage_count": 32},
            ],
            "meta": {"primary_metric": "avg_pnl", "available_metrics": ["avg_pnl", "avg_revenue", "voyage_count"]},
        }
    }
    action = GraphRouter._parse_result_set_followup_action(
        user_input="What about average revenue only?",
        session_ctx=session_ctx,
    )
    assert action == {"action": "project_selected_metric", "metric": "avg_revenue", "selector": "last_focus"}


def test_result_set_followup_top_cargo_variance_refines_rows() -> None:
    session_ctx = {
        "last_result_set": {
            "rows": [
                {"cargo_grade": "no heat crude", "variance_diff": 854339.14},
                {"cargo_grade": "fo", "variance_diff": 582007.75},
                {"cargo_grade": "nhc", "variance_diff": 472235.71},
            ],
            "meta": {"primary_metric": "variance_diff", "available_metrics": ["variance_diff"]},
        }
    }
    action = GraphRouter._parse_result_set_followup_action(
        user_input="Show the top 5 cargo grades by variance.",
        session_ctx=session_ctx,
    )
    assert action == {"action": "top_n", "n": 5, "metric": "variance_diff"}


def test_result_set_followup_tce_variance_only_compare_metrics() -> None:
    session_ctx = {
        "last_result_set": {
            "rows": [
                {"voyage_number": 1901, "tce_variance": 2.17},
                {"voyage_number": 1902, "tce_variance": 8.51},
                {"voyage_number": 2301, "tce_variance": -54.29},
            ],
            "meta": {"primary_metric": "pnl_variance", "available_metrics": ["pnl_variance", "tce_variance"]},
        }
    }
    action = GraphRouter._parse_result_set_followup_action(
        user_input="What about TCE variance only?",
        session_ctx=session_ctx,
    )
    assert action == {"action": "compare_metrics", "metrics": ["tce_variance"]}


def test_parse_result_set_returns_none_for_fleet_cargo_frequency_question() -> None:
    session_ctx = {
        "last_result_set": {
            "rows": [
                {"vessel_name": "A", "avg_tce": 100.0},
                {"vessel_name": "B", "avg_tce": 90.0},
            ],
            "meta": {"primary_metric": "avg_tce", "available_metrics": ["avg_tce"]},
        }
    }
    action = GraphRouter._parse_result_set_followup_action(
        user_input="Which cargo grade appears most frequently across all voyages?",
        session_ctx=session_ctx,
    )
    assert action is None


def test_parse_result_set_returns_none_for_all_voyages_without_list_scope() -> None:
    session_ctx = {
        "last_result_set": {
            "rows": [{"voyage_number": 1901, "pnl": 1.0}],
            "meta": {"primary_metric": "pnl", "available_metrics": ["pnl"]},
        }
    }
    action = GraphRouter._parse_result_set_followup_action(
        user_input="Show average delay for all voyages last year",
        session_ctx=session_ctx,
    )
    assert action is None


def test_fleet_scoped_avg_tce_bypasses_result_set_metric_hijack(monkeypatch) -> None:
    monkeypatch.setenv("REDIS_DISABLED", "1")
    redis = RedisStore(RedisConfig())
    sid = "s-fleet-avg-tce-not-rs"
    redis.save_session(
        sid,
        {
            "last_result_set": {
                "source_intent": "ranking.vessels",
                "rows": [
                    {"vessel_name": "Dallas", "avg_tce": 16748.18, "voyage_count": 5},
                ],
                "meta": {"primary_metric": "avg_tce", "available_metrics": ["avg_tce", "voyage_count"]},
            }
        },
    )

    llm = SimpleNamespace(
        extract_intent_slots=lambda **kwargs: {
            "intent_key": "aggregation.vessel_metrics",
            "slots": {"metric": "avg_tce", "group_by": "vessel"},
            "is_followup": False,
            "inherit_slots_from_session": [],
            "backward_reference": False,
            "followup_confidence": "low",
        },
        summarize_answer=lambda **kwargs: "ok",
    )
    agent = _dummy_agent()
    router = GraphRouter(llm=llm, redis_store=redis, mongo_agent=agent, finance_agent=agent, ops_agent=agent)
    router.planner = SimpleNamespace(
        build_plan=lambda **kwargs: ExecutionPlan(plan_type="single", intent_key=kwargs.get("intent_key"), required_slots=[], confidence=0.9, steps=[]),
    )

    out = router.handle(session_id=sid, user_input="What is the average TCE per vessel?")
    assert out.get("intent_key") == "aggregation.vessel_metrics"


def test_result_set_followup_best_one_cargo_grade_works_for_aggregate_grade_field() -> None:
    session_ctx = {
        "last_result_set": {
            "rows": [
                {"module_type": "Spot", "avg_pnl": 723800.03, "most_common_grade": "cpp"},
                {"module_type": "TC Voyage", "avg_pnl": 575965.48, "most_common_grade": "vegoil"},
            ],
            "meta": {"primary_metric": "avg_pnl", "available_metrics": ["avg_pnl"]},
        }
    }
    action = GraphRouter._parse_result_set_followup_action(
        user_input="Show the cargo grade for the best one.",
        session_ctx=session_ctx,
    )
    assert action == {"action": "project_extreme_field", "field": "cargo_grades", "metric": "avg_pnl", "extreme": "high"}


def test_worst_expense_ratio_followup_selects_highest_ratio(monkeypatch) -> None:
    monkeypatch.setenv("REDIS_DISABLED", "1")
    redis = RedisStore(RedisConfig())
    sid = "s-worst-expense-ratio"
    redis.save_session(
        sid,
        {
            "last_result_set": {
                "source_intent": "analysis.high_revenue_low_pnl",
                "rows": [
                    {"voyage_number": 2103, "expense_to_revenue_ratio": 0.023},
                    {"voyage_number": 2204, "expense_to_revenue_ratio": 0.355},
                ],
                "meta": {"primary_metric": "expense_to_revenue_ratio", "available_metrics": ["expense_to_revenue_ratio"]},
            }
        },
    )

    llm = SimpleNamespace(
        extract_intent_slots=lambda **kwargs: {"intent_key": "out_of_scope", "slots": {}},
        summarize_answer=lambda **kwargs: "ok",
    )
    agent = _dummy_agent()
    router = GraphRouter(llm=llm, redis_store=redis, mongo_agent=agent, finance_agent=agent, ops_agent=agent)
    router.planner = SimpleNamespace(
        build_plan=lambda **kwargs: ExecutionPlan(plan_type="single", intent_key=kwargs.get("intent_key"), required_slots=[], confidence=0.9, steps=[]),
    )

    out = router.handle(session_id=sid, user_input="Which one has the worst expense ratio?")
    answer = (out.get("answer") or "").lower()
    assert "2204" in answer
    assert "0.36" in answer or "0.35" in answer


def test_monthly_avg_pnl_followup_can_rank_time_buckets(monkeypatch) -> None:
    monkeypatch.setenv("REDIS_DISABLED", "1")
    redis = RedisStore(RedisConfig())
    sid = "s-monthly-avg-pnl"
    redis.save_session(
        sid,
        {
            "last_result_set": {
                "source_intent": "aggregation.trends",
                "rows": [
                    {"time_bucket": "2019-08-01T00:00:00+00:00", "avg_pnl": 80105.14, "voyage_count": 2},
                    {"time_bucket": "2019-11-01T00:00:00+00:00", "avg_pnl": 1683566.47, "voyage_count": 7},
                    {"time_bucket": "2020-02-01T00:00:00+00:00", "avg_pnl": 1450088.19, "voyage_count": 4},
                ],
                "meta": {"primary_metric": "avg_pnl", "available_metrics": ["avg_pnl", "voyage_count"]},
            }
        },
    )

    llm = SimpleNamespace(
        extract_intent_slots=lambda **kwargs: {"intent_key": "out_of_scope", "slots": {}},
        summarize_answer=lambda **kwargs: "ok",
    )
    agent = _dummy_agent()
    router = GraphRouter(llm=llm, redis_store=redis, mongo_agent=agent, finance_agent=agent, ops_agent=agent)
    router.planner = SimpleNamespace(
        build_plan=lambda **kwargs: ExecutionPlan(plan_type="single", intent_key=kwargs.get("intent_key"), required_slots=[], confidence=0.9, steps=[]),
    )

    out = router.handle(session_id=sid, user_input="Which month has the highest average PnL?")
    answer = out.get("answer") or ""
    assert "2019-11-01" in answer
    assert "avg_pnl" in answer.lower()
