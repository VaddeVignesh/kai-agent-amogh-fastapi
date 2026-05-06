from __future__ import annotations

from app.adapters.redis_store import RedisConfig, RedisStore
from app.main import QueryRequest, query_agent


class _CountingRouter:
    def __init__(self) -> None:
        self.calls = 0

    def handle(self, *, session_id: str, user_input: str) -> dict:
        self.calls += 1
        return {
            "answer": f"answer-{self.calls}",
            "intent_key": "test.intent",
            "slots": {"query": user_input, "call": self.calls},
        }


def test_query_idempotency_reuses_cached_response_for_same_session_and_request(monkeypatch) -> None:
    monkeypatch.setenv("REDIS_DISABLED", "1")

    import app.main as main

    router = _CountingRouter()
    monkeypatch.setattr(main, "router", router)
    monkeypatch.setattr(main, "redis_store", RedisStore(RedisConfig(idem_ttl_sec=60)))

    req = QueryRequest(query="show voyage 2205", session_id="customer:customer:s1", request_id="req-1")

    first = query_agent(req)
    second = query_agent(req)

    assert router.calls == 1
    assert first.answer == "answer-1"
    assert second.answer == first.answer
    assert second.slots == first.slots


def test_query_idempotency_different_request_id_runs_again(monkeypatch) -> None:
    monkeypatch.setenv("REDIS_DISABLED", "1")

    import app.main as main

    router = _CountingRouter()
    monkeypatch.setattr(main, "router", router)
    monkeypatch.setattr(main, "redis_store", RedisStore(RedisConfig(idem_ttl_sec=60)))

    first = query_agent(QueryRequest(query="show voyage 2205", session_id="customer:customer:s1", request_id="req-1"))
    second = query_agent(QueryRequest(query="show voyage 2205", session_id="customer:customer:s1", request_id="req-2"))

    assert router.calls == 2
    assert first.answer == "answer-1"
    assert second.answer == "answer-2"


def test_query_without_request_id_is_not_cached(monkeypatch) -> None:
    monkeypatch.setenv("REDIS_DISABLED", "1")

    import app.main as main

    router = _CountingRouter()
    monkeypatch.setattr(main, "router", router)
    monkeypatch.setattr(main, "redis_store", RedisStore(RedisConfig(idem_ttl_sec=60)))

    req = QueryRequest(query="show voyage 2205", session_id="customer:customer:s1")

    first = query_agent(req)
    second = query_agent(req)

    assert router.calls == 2
    assert first.answer == "answer-1"
    assert second.answer == "answer-2"
