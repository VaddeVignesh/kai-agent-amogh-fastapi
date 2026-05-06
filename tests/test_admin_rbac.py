from __future__ import annotations

from fastapi.testclient import TestClient

from app.adapters.redis_store import RedisConfig, RedisStore
from app.auth import ROLE_ACCESS, get_role_access


def test_role_access_config_matches_expected_admin_and_customer_scope() -> None:
    assert ROLE_ACCESS["customer"]["admin_apis"] == []
    assert ROLE_ACCESS["customer"]["redis"] == []
    assert "/admin/users" in ROLE_ACCESS["admin"]["admin_apis"]
    assert "admin_users" in ROLE_ACCESS["admin"]["redis"]
    assert "finance_voyage_kpi" in ROLE_ACCESS["admin"]["postgres_tables"]
    assert "voyages" in ROLE_ACCESS["customer"]["mongo_collections"]


def test_admin_metrics_requires_admin_session(monkeypatch) -> None:
    monkeypatch.setenv("REDIS_DISABLED", "1")

    import app.main as main

    store = RedisStore(RedisConfig())
    customer_session = "customer:customer1:test"
    admin_session = "admin:admin1:test"
    store.save_session(customer_session, {
        "username": "customer1",
        "role": "customer",
        "role_access": get_role_access("customer"),
    })
    store.save_session(admin_session, {
        "username": "admin1",
        "role": "admin",
        "role_access": get_role_access("admin"),
    })
    monkeypatch.setattr(main, "redis_store", store)

    client = TestClient(main.app)

    missing = client.get("/admin/metrics")
    customer = client.get("/admin/metrics", headers={"X-Session-Id": customer_session})
    admin = client.get("/admin/metrics", headers={"X-Session-Id": admin_session})

    assert missing.status_code == 401
    assert customer.status_code == 403
    assert admin.status_code == 200
