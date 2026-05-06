from __future__ import annotations

from app.adapters.redis_store import RedisConfig, RedisStore
from app.auth import generate_session_id, login


CUSTOMER_USERS = {
    "customer1": "cust123",
    "customer2": "cust223",
    "customer3": "cust323",
    "customer4": "cust423",
    "customer5": "cust523",
}

ADMIN_USERS = {
    "admin1": "admin123",
    "admin2": "admin223",
    "admin3": "admin323",
    "admin4": "admin423",
    "admin5": "admin523",
}


def test_multiple_customer_logins_get_unique_customer_sessions() -> None:
    sessions = []

    for username, password in CUSTOMER_USERS.items():
        role = login(username, password)
        assert role == "customer"
        session_id = generate_session_id(username, role)
        assert session_id.startswith(f"customer:{username}:")
        sessions.append(session_id)

    assert len(set(sessions)) == len(CUSTOMER_USERS)


def test_multiple_admin_logins_get_unique_admin_sessions() -> None:
    sessions = []

    for username, password in ADMIN_USERS.items():
        role = login(username, password)
        assert role == "admin"
        session_id = generate_session_id(username, role)
        assert session_id.startswith(f"admin:{username}:")
        sessions.append(session_id)

    assert len(set(sessions)) == len(ADMIN_USERS)


def test_same_user_relogin_gets_new_session_id() -> None:
    role = login("customer1", "cust123")
    assert role == "customer"

    session_one = generate_session_id("customer1", role)
    session_two = generate_session_id("customer1", role)

    assert session_one != session_two
    assert session_one.startswith("customer:customer1:")
    assert session_two.startswith("customer:customer1:")


def test_session_memory_is_isolated_per_logged_in_user(monkeypatch) -> None:
    monkeypatch.setenv("REDIS_DISABLED", "1")
    store = RedisStore(RedisConfig())

    customer_session = generate_session_id("customer1", "customer")
    admin_session = generate_session_id("admin1", "admin")

    store.save_session(customer_session, {"slots": {"voyage_number": 2205}, "last_intent": "voyage.summary"})
    store.save_session(admin_session, {"slots": {"voyage_number": 3301}, "last_intent": "voyage.summary"})

    assert store.load_session(customer_session)["slots"]["voyage_number"] == 2205
    assert store.load_session(admin_session)["slots"]["voyage_number"] == 3301
