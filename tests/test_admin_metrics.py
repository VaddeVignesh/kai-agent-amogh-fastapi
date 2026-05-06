from __future__ import annotations

from app.adapters.redis_store import RedisConfig, RedisStore


def test_admin_metrics_counts_unique_users_sessions_and_query_activity(monkeypatch) -> None:
    monkeypatch.setenv("REDIS_DISABLED", "1")
    store = RedisStore(RedisConfig())

    store.save_session("customer:customer1:abc123", {"username": "customer1", "role": "customer"})
    store.save_session("customer:customer2:def456", {"username": "customer2", "role": "customer"})
    store.save_session("admin:admin1:ghi789", {"username": "admin1", "role": "admin"})

    store.record_query_metrics(1.25)
    store.record_query_metrics(2.75)

    metrics = store.get_admin_metrics(total_users=12)

    assert metrics["total_users"] == 12
    assert metrics["active_sessions"] == 3
    assert metrics["queries_today"] == 2
    assert metrics["avg_response_time"] == 2.0


def test_admin_metrics_counts_same_user_multiple_sessions_as_one_user(monkeypatch) -> None:
    monkeypatch.setenv("REDIS_DISABLED", "1")
    store = RedisStore(RedisConfig())

    store.save_session("customer:customer1:first", {"username": "customer1", "role": "customer"})
    store.save_session("customer:customer1:second", {"username": "customer1", "role": "customer"})

    metrics = store.get_admin_metrics(total_users=12)

    assert metrics["total_users"] == 12
    assert metrics["active_sessions"] == 2


def test_admin_metrics_can_fallback_to_unique_session_users(monkeypatch) -> None:
    monkeypatch.setenv("REDIS_DISABLED", "1")
    store = RedisStore(RedisConfig())

    store.save_session("customer:customer1:first", {"username": "customer1", "role": "customer"})
    store.save_session("admin:admin1:first", {"username": "admin1", "role": "admin"})

    metrics = store.get_admin_metrics()

    assert metrics["total_users"] == 2
    assert metrics["active_sessions"] == 2


def test_admin_users_combines_configured_users_with_live_sessions_and_queries(monkeypatch) -> None:
    monkeypatch.setenv("REDIS_DISABLED", "1")
    store = RedisStore(RedisConfig())
    configured = {
        "customer1": {"role": "customer"},
        "admin1": {"role": "admin"},
        "customer2": {"role": "customer"},
    }

    store.save_session("customer:customer1:first", {"username": "customer1", "role": "customer", "login_ts": 100})
    store.save_session("customer:customer1:second", {"username": "customer1", "role": "customer", "last_updated_ts": 200})
    store.save_session("admin:admin1:first", {"username": "admin1", "role": "admin", "login_ts": 150})
    store.record_user_query("customer:customer1:first")
    store.record_user_query("customer:customer1:first")
    store.record_user_query("admin:admin1:first")

    rows = store.get_admin_users(configured)
    by_user = {row["username"]: row for row in rows}

    assert by_user["customer1"]["status"] == "Active"
    assert by_user["customer1"]["active_sessions"] == 2
    assert by_user["customer1"]["queries_today"] == 2
    assert by_user["admin1"]["queries_today"] == 1
    assert by_user["customer2"]["status"] == "Offline"
    assert by_user["customer2"]["active_sessions"] == 0


def test_admin_audit_log_records_compact_activity(monkeypatch) -> None:
    monkeypatch.setenv("REDIS_DISABLED", "1")
    store = RedisStore(RedisConfig())
    long_query = "Which voyages had high revenue but low or negative PnL? Show revenue, total expense, PnL, and expense-to-revenue ratio."

    store.record_login_audit("customer1", "customer", "customer:customer1:first")
    store.record_query_audit(
        session_id="customer:customer1:first",
        query=long_query,
        intent_key="analysis.high_revenue_low_pnl",
        duration_seconds=2.345,
    )
    store.record_logout_audit("customer:customer1:first")

    events = store.get_admin_audit_log(limit=10)
    actions = [event["action"] for event in events]

    assert actions == ["logout", "query", "login"]
    query_event = events[1]
    assert query_event["actor"] == "customer1"
    assert query_event["role"] == "customer"
    assert query_event["intent_key"] == "analysis.high_revenue_low_pnl"
    assert query_event["query_length"] == len(long_query)
    assert len(query_event["query_preview"]) <= 90
    assert query_event["duration_seconds"] == 2.35


def test_execution_history_keeps_latest_100_records(monkeypatch) -> None:
    monkeypatch.setenv("REDIS_DISABLED", "1")
    store = RedisStore(RedisConfig())

    for i in range(105):
        store.record_execution_history({
            "timestamp": f"2026-04-29T00:00:{i:02d}",
            "session_id": f"customer:customer1:{i}",
            "role": "customer",
            "query_preview": f"query {i}",
            "query_length": 7,
            "intent": "test.intent",
            "response_time": 0.1,
            "status": "success",
        })

    history = store.get_execution_history()

    assert len(history) == 100
    assert history[0]["query_preview"] == "query 104"
    assert history[-1]["query_preview"] == "query 5"
