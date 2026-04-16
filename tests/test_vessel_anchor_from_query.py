from __future__ import annotations

from types import SimpleNamespace

from app.adapters.redis_store import RedisConfig, RedisStore
from app.orchestration.graph_router import GraphRouter
from app.orchestration.planner import ExecutionPlan


def test_explicit_vessel_mentions_profile_for_vessel() -> None:
    q = "Can you show the full profile details for vessel Stena Imperial including its technical and commercial tags?"
    assert GraphRouter._explicit_vessel_mentions(q) == ["Stena Imperial"]


def test_explicit_vessel_mentions_does_have() -> None:
    q = "Does Stena Imperial have a scrubber installed, and how does that impact compliance?"
    assert GraphRouter._explicit_vessel_mentions(q) == ["Stena Imperial"]


def test_explicit_vessel_mentions_for_at_end() -> None:
    q = "What is the IMO number and operator entity for Stena Primorsk?"
    assert GraphRouter._explicit_vessel_mentions(q) == ["Stena Primorsk"]


def test_explicit_vessel_mentions_does_belong() -> None:
    q = "Which pool and segment does Stenaweco Energy belong to?"
    assert GraphRouter._explicit_vessel_mentions(q) == ["Stenaweco Energy"]


def test_normalize_overrides_session_vessel(monkeypatch) -> None:
    monkeypatch.setenv("REDIS_DISABLED", "1")
    redis = RedisStore(RedisConfig())
    sid = "s-vessel-anchor-override"
    redis.save_session(
        sid,
        {
            "memory_slots": {"vessel_name": "Stena Primorsk", "imo": "9299147"},
            "last_intent": "vessel.metadata",
        },
    )
    llm = SimpleNamespace(
        extract_intent_slots=lambda **kwargs: {
            "intent_key": "vessel.metadata",
            "slots": {},
            "is_followup": True,
            "inherit_slots_from_session": ["vessel_name", "imo"],
            "backward_reference": False,
            "followup_confidence": "high",
        },
        summarize_answer=lambda **kwargs: "ok",
    )
    agent = SimpleNamespace(
        pg=None,
        run=lambda **kwargs: {"mode": "registry_sql", "rows": []},
        fetch_full_voyage_context=lambda **kwargs: {},
        run_llm_find=lambda **kwargs: {"mode": "mongo_llm", "ok": False, "rows": []},
        adapter=SimpleNamespace(
            get_vessel_imo_by_name=lambda name: "9667485" if "imperial" in str(name).lower() else "9299147",
            fetch_vessel=lambda imo, projection=None: {"imo": str(imo)} if str(imo).strip() in ("9667485", "9299147") else None,
            fetch_vessel_by_name=lambda name, projection=None: {"name": str(name), "imo": "9667485"},
            get_voyage_by_number=lambda voyage_number, projection=None: None,
            fetch_voyage=lambda voyage_id, projection=None: None,
        ),
    )
    router = GraphRouter(llm=llm, redis_store=redis, mongo_agent=agent, finance_agent=agent, ops_agent=agent)
    router.planner = SimpleNamespace(
        build_plan=lambda **kwargs: ExecutionPlan(plan_type="single", intent_key=kwargs.get("intent_key"), required_slots=[], confidence=0.9, steps=[]),
    )
    out = router.handle(
        session_id=sid,
        user_input="Does Stena Imperial have a scrubber installed, and how does that impact compliance?",
    )
    assert (out.get("slots") or {}).get("vessel_name") == "Stena Imperial"
    assert (out.get("slots") or {}).get("imo") in (None, "")


def test_multi_vessel_question_skips_session_vessel_injection(monkeypatch) -> None:
    monkeypatch.setenv("REDIS_DISABLED", "1")
    redis = RedisStore(RedisConfig())
    sid = "s-multi-vessel-no-inject"
    redis.save_session(
        sid,
        {
            "memory_slots": {"vessel_name": "Stena Primorsk"},
            "last_intent": "vessel.metadata",
        },
    )
    llm = SimpleNamespace(
        extract_intent_slots=lambda **kwargs: {
            "intent_key": "vessel.metadata",
            "slots": {},
            "is_followup": True,
            "inherit_slots_from_session": ["vessel_name"],
            "backward_reference": False,
            "followup_confidence": "high",
        },
        summarize_answer=lambda **kwargs: "ok",
    )
    agent = SimpleNamespace(
        pg=None,
        run=lambda **kwargs: {"mode": "registry_sql", "rows": []},
        fetch_full_voyage_context=lambda **kwargs: {},
        run_llm_find=lambda **kwargs: {"mode": "mongo_llm", "ok": False, "rows": []},
        adapter=SimpleNamespace(
            get_vessel_imo_by_name=lambda name: "9667485",
            fetch_vessel=lambda imo, projection=None: {"imo": str(imo)},
            fetch_vessel_by_name=lambda name, projection=None: {"name": str(name), "imo": "9667485"},
            get_voyage_by_number=lambda voyage_number, projection=None: None,
            fetch_voyage=lambda voyage_id, projection=None: None,
        ),
    )
    router = GraphRouter(llm=llm, redis_store=redis, mongo_agent=agent, finance_agent=agent, ops_agent=agent)
    router.planner = SimpleNamespace(
        build_plan=lambda **kwargs: ExecutionPlan(plan_type="single", intent_key=kwargs.get("intent_key"), required_slots=[], confidence=0.9, steps=[]),
    )
    out = router.handle(
        session_id=sid,
        user_input="How do the vessel profiles differ between Stena Imperial and Stena Primorsk in terms of segment?",
    )
    assert (out.get("slots") or {}).get("vessel_name") in (None, "")
