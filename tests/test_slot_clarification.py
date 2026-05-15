from __future__ import annotations

from types import SimpleNamespace

from app.orchestration.graph_router import GraphRouter
from app.orchestration.planner import ExecutionPlan


class _MemRedis:
    def __init__(self) -> None:
        self._store: dict[str, dict] = {}

    def load_session(self, session_id: str) -> dict:
        return dict(self._store.get(session_id) or {"slots": {}, "turn": 0})

    def save_session(self, session_id: str, session_patch: dict) -> None:
        cur = self.load_session(session_id)
        cur.update(session_patch or {})
        self._store[session_id] = cur


def _router_for_clarification_tests() -> GraphRouter:
    llm = SimpleNamespace(
        extract_intent_slots=lambda **kwargs: {"intent_key": "out_of_scope", "slots": {}},
        summarize_answer=lambda **kwargs: "dummy",
    )
    dummy_agent = SimpleNamespace(
        pg=None,
        run=lambda **kwargs: {"mode": "error", "rows": []},
        fetch_full_voyage_context=lambda **kwargs: {},
        run_llm_find=lambda **kwargs: {"mode": "mongo_llm", "ok": False, "rows": []},
    )
    return GraphRouter(
        llm=llm,
        redis_store=_MemRedis(),
        mongo_agent=dummy_agent,
        finance_agent=dummy_agent,
        ops_agent=dummy_agent,
    )


def test_incomplete_vessel_and_voyage_variants_trigger_clarification() -> None:
    router = _router_for_clarification_tests()

    cases = [
        ("tell me about vessel", "vessel.summary", "vessel"),
        ("tell me about vesssl", "vessel.summary", "vessel"),
        ("tell me about vessels", "vessel.summary", "vessel"),
        ("tell me about voyage", "voyage.summary", "voyage"),
        ("tell me about voyages", "voyage.summary", "voyage"),
        ("tell me about voyge", "voyage.summary", "voyage"),
    ]

    for query, expected_intent, expected_word in cases:
        out = router.handle(session_id=f"s-{query.replace(' ', '-')}", user_input=query)
        assert out.get("intent_key") == expected_intent
        assert out.get("clarification"), f"Expected clarification for {query!r}"
        assert expected_word in str(out.get("clarification")).lower()


def test_clarification_numeric_choice_fills_port_name() -> None:
    """
    "what is port" should trigger a clarification and store suggestions.
    A numeric follow-up (e.g. "3") should be resolved into slots.port_name.
    """

    # Dummy LLM: force out_of_scope so router's deterministic incomplete-intent logic runs.
    llm = SimpleNamespace(
        extract_intent_slots=lambda **kwargs: {"intent_key": "out_of_scope", "slots": {}},
        summarize_answer=lambda **kwargs: "dummy",
    )

    # Dummy agents: only need pg for suggestions, but we'll supply suggestions via session directly.
    dummy_agent = SimpleNamespace(
        pg=None,
        run=lambda **kwargs: {"mode": "error", "rows": []},
        fetch_full_voyage_context=lambda **kwargs: {},
        run_llm_find=lambda **kwargs: {"mode": "mongo_llm", "ok": False, "rows": []},
    )

    redis = _MemRedis()
    router = GraphRouter(
        llm=llm,
        redis_store=redis,
        mongo_agent=dummy_agent,
        finance_agent=dummy_agent,
        ops_agent=dummy_agent,
    )

    sid = "s-test"

    # Trigger clarification
    out1 = router.handle(session_id=sid, user_input="what is port")
    assert out1.get("clarification"), "Expected a clarification question"

    # Inject deterministic suggestions to emulate Postgres-backed suggestion list
    sess = redis.load_session(sid)
    redis.save_session(
        sid,
        {
            **sess,
            "clarification_options": {"port_name": ["Rotterdam", "Houston", "Singapore"]},
        },
    )

    # Answer clarification with numeric choice
    out2 = router.handle(session_id=sid, user_input="3")
    slots = out2.get("slots") or {}
    assert slots.get("port_name") == "Singapore"


def test_clarification_followup_uses_pending_question_as_effective_query() -> None:
    """
    When the user answers a clarification with just a value (e.g. "2301"),
    the router must continue execution using the ORIGINAL pending question as the effective query.
    """
    captured: dict = {}

    llm = SimpleNamespace(
        extract_intent_slots=lambda **kwargs: {"intent_key": "out_of_scope", "slots": {}},
        summarize_answer=lambda **kwargs: captured.setdefault("question", kwargs.get("question")) or "ok",
    )

    dummy_agent = SimpleNamespace(
        pg=None,
        run=lambda **kwargs: {"mode": "registry_sql", "rows": [{"voyage_number": kwargs.get("slots", {}).get("voyage_number")}]},
        fetch_full_voyage_context=lambda **kwargs: {},
        run_llm_find=lambda **kwargs: {"mode": "mongo_llm", "ok": False, "rows": []},
    )

    redis = _MemRedis()
    router = GraphRouter(llm=llm, redis_store=redis, mongo_agent=dummy_agent, finance_agent=dummy_agent, ops_agent=dummy_agent)

    # Force planner to single so the test doesn't require dynamic SQL generators.
    router.planner = SimpleNamespace(
        build_plan=lambda **kwargs: ExecutionPlan(plan_type="single", intent_key=kwargs.get("intent_key"), required_slots=[], confidence=0.9, steps=[]),
    )

    sid = "s-test-clarify-effective"
    original_q = "tell me about voyage"

    out1 = router.handle(session_id=sid, user_input=original_q)
    assert out1.get("clarification")

    out2 = router.handle(session_id=sid, user_input="2301")
    assert out2.get("answer") or out2.get("data") is not None

    # The LLM should see the original question, not the raw "2301".
    assert captured.get("question") == original_q


def test_rankingish_followup_is_salvaged_and_does_not_inherit_entity_anchor() -> None:
    """
    A short ranking-ish follow-up like "which is highest pnl and lowest pnl" should not be coerced into
    voyage.summary by inheriting the last voyage anchor.
    """
    llm = SimpleNamespace(
        extract_intent_slots=lambda **kwargs: {
            "intent_key": "ranking.voyages_by_pnl",
            "slots": {},
            "is_followup": False,
            "inherit_slots_from_session": [],
            "backward_reference": False,
            "followup_confidence": "low",
        },
        summarize_answer=lambda **kwargs: "ok",
    )

    dummy_agent = SimpleNamespace(
        pg=None,
        run=lambda **kwargs: {"mode": "registry_sql", "rows": [{"ok": True}]},
        fetch_full_voyage_context=lambda **kwargs: {},
        run_llm_find=lambda **kwargs: {"mode": "mongo_llm", "ok": False, "rows": []},
    )

    redis = _MemRedis()
    sid = "s-test-ranking-followup"
    # Seed session with a prior voyage anchor (simulates having discussed a voyage earlier).
    redis.save_session(sid, {"memory_slots": {"voyage_number": 2301}, "slots": {"voyage_number": 2301}, "last_intent": "voyage.summary"})

    router = GraphRouter(llm=llm, redis_store=redis, mongo_agent=dummy_agent, finance_agent=dummy_agent, ops_agent=dummy_agent)
    router.planner = SimpleNamespace(
        build_plan=lambda **kwargs: ExecutionPlan(plan_type="single", intent_key=kwargs.get("intent_key"), required_slots=[], confidence=0.9, steps=[]),
    )

    out = router.handle(session_id=sid, user_input="which is highest pnl and lowest pnl")
    assert (out.get("intent_key") or "").startswith("ranking.")
    assert (out.get("slots") or {}).get("voyage_number") in (None, ""), "Ranking follow-up must not inherit voyage_number anchor"


def test_result_set_followup_explain_remarks_triggers_clarification() -> None:
    """
    After a list/composite answer, a follow-up like "explain me remarks" should bind to the last
    result set and ask which voyage number (slot clarification with suggestions).
    """
    llm = SimpleNamespace(
        extract_intent_slots=lambda **kwargs: {"intent_key": "out_of_scope", "slots": {}},
        summarize_answer=lambda **kwargs: "ok",
    )
    dummy_agent = SimpleNamespace(
        pg=None,
        run=lambda **kwargs: {"mode": "registry_sql", "rows": [{"ok": True}]},
        fetch_full_voyage_context=lambda **kwargs: {},
        run_llm_find=lambda **kwargs: {"mode": "mongo_llm", "ok": False, "rows": []},
    )

    redis = _MemRedis()
    sid = "s-test-rs-followup"
    redis.save_session(
        sid,
        {
            "last_result_set": {
                "source_intent": "ranking.voyages_by_pnl",
                "rows": [
                    {"voyage_number": 2401, "pnl": 1.0, "remarks": ["a"]},
                    {"voyage_number": 2301, "pnl": 2.0, "remarks": ["b"]},
                ],
            }
        },
    )

    router = GraphRouter(llm=llm, redis_store=redis, mongo_agent=dummy_agent, finance_agent=dummy_agent, ops_agent=dummy_agent)
    router.planner = SimpleNamespace(
        build_plan=lambda **kwargs: ExecutionPlan(plan_type="single", intent_key=kwargs.get("intent_key"), required_slots=[], confidence=0.9, steps=[]),
    )

    out = router.handle(session_id=sid, user_input="explain me remarks")
    assert out.get("clarification"), "Expected clarification asking which voyage from last list"


def test_result_set_memory_fallback_from_finance_rows() -> None:
    """
    If a response has multiple finance rows but no artifacts.merged_rows (single path),
    we should still persist last_result_set so "among these" follow-ups work.
    """
    llm = SimpleNamespace(
        extract_intent_slots=lambda **kwargs: {"intent_key": "ranking.voyages_by_pnl", "slots": {"limit": 5}},
        summarize_answer=lambda **kwargs: "ok",
    )
    redis = _MemRedis()

    # Stub router so summarize runs and sees merged.finance.rows
    dummy_agent = SimpleNamespace(
        pg=None,
        run=lambda **kwargs: {"mode": "registry_sql", "rows": [{"voyage_number": 1, "pnl": 10}, {"voyage_number": 2, "pnl": 5}]},
        fetch_full_voyage_context=lambda **kwargs: {},
        run_llm_find=lambda **kwargs: {"mode": "mongo_llm", "ok": False, "rows": []},
    )
    router = GraphRouter(llm=llm, redis_store=redis, mongo_agent=dummy_agent, finance_agent=dummy_agent, ops_agent=dummy_agent)
    # Force plan to single and skip composite
    router.planner = SimpleNamespace(
        build_plan=lambda **kwargs: ExecutionPlan(plan_type="single", intent_key=kwargs.get("intent_key"), required_slots=[], confidence=0.9, steps=[]),
    )

    sid = "s-test-rs-fallback"
    router.handle(session_id=sid, user_input="top 2 voyages by pnl")
    sess = redis.load_session(sid)
    assert isinstance(sess.get("last_result_set"), dict)


def test_result_set_detector_does_not_hijack_new_ranking_question() -> None:
    """
    Regression: a NEW question containing words like "remarks" and "explain why" must not be
    hijacked into followup.result_set.
    """
    llm = SimpleNamespace(
        extract_intent_slots=lambda **kwargs: {"intent_key": "ranking.voyages_by_pnl", "slots": {"limit": 5}},
        summarize_answer=lambda **kwargs: "ok",
    )
    dummy_agent = SimpleNamespace(
        pg=None,
        run=lambda **kwargs: {"mode": "registry_sql", "rows": [{"voyage_number": 1, "pnl": 10}]},
        fetch_full_voyage_context=lambda **kwargs: {},
        run_llm_find=lambda **kwargs: {"mode": "mongo_llm", "ok": False, "rows": []},
    )
    redis = _MemRedis()
    sid = "s-test-rs-no-hijack"
    # Seed a last_result_set so the detector is active.
    redis.save_session(sid, {"last_result_set": {"rows": [{"voyage_number": 999, "pnl": 1.0}]}})

    router = GraphRouter(llm=llm, redis_store=redis, mongo_agent=dummy_agent, finance_agent=dummy_agent, ops_agent=dummy_agent)
    router.planner = SimpleNamespace(
        build_plan=lambda **kwargs: ExecutionPlan(plan_type="single", intent_key=kwargs.get("intent_key"), required_slots=[], confidence=0.9, steps=[]),
    )

    q = "Show me the top 5 most profitable voyages and for each one list the cargo grade, key ports visited, and any remarks that explain why performance was high."
    out = router.handle(session_id=sid, user_input=q)
    assert out.get("intent_key") != "followup.result_set"
