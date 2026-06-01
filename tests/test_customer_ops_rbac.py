"""RBAC: customer_ops_only (e.g. customer5) — Postgres ops KPI only, no finance KPI."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from app.config.response_rules_loader import get_finance_kpi_scope_restricted_user_message
from app.agents.finance_agent import FinanceAgent
from app.agents.ops_agent import OpsAgent
from app.auth import ROLE_ACCESS, USERS, get_role_access, get_session_postgres_table_allowlist
from app.llm.llm_client import LLMClient, LLMConfig
from app.orchestration.graph_router import GraphRouter
from app.sql.registry_role_access import is_registry_query_allowed_for_session
from app.utils.mongo_ops_slice import (
    build_mongo_llm_row_for_ops_tenant,
    build_voyage_summary_merged_row,
    operational_fixtures_for_summary,
)


def test_ops_only_voyage_snapshot_markdown_uses_shrunk_ports() -> None:
    ops_safe = {
        "rows": [
            {
                "voyage_number": 1901,
                "vessel_name": "Test Vessel",
                "module_type": "Spot",
                "offhire_days": 2,
                "ports_json": [{"portName": "Singapore"}, {"portName": "Rotterdam"}],
                "grades_json": [{"gradeName": "crude"}],
            }
        ],
    }
    md = GraphRouter._ops_only_voyage_snapshot_markdown(ops_safe)
    assert "Operations snapshot" in md
    assert "Singapore" in md
    assert "crude" in md


def test_customer5_uses_ops_only_role() -> None:
    assert USERS["customer5"]["role"] == "customer_ops_only"
    assert "finance_voyage_kpi" not in ROLE_ACCESS["customer_ops_only"]["postgres_tables"]
    assert "ops_voyage_summary" in ROLE_ACCESS["customer_ops_only"]["postgres_tables"]


def test_ops_only_session_allowlist_is_ops_table_only() -> None:
    ctx = {"role_access": get_role_access("customer_ops_only")}
    allowed = get_session_postgres_table_allowlist(ctx)
    assert allowed == frozenset({"ops_voyage_summary"})


def test_registry_finance_query_denied_for_ops_only() -> None:
    ctx = {"role_access": get_role_access("customer_ops_only")}
    assert is_registry_query_allowed_for_session("kpi.voyage_by_reference", ctx) is False


def test_registry_ops_only_query_allowed() -> None:
    ctx = {"role_access": get_role_access("customer_ops_only")}
    assert is_registry_query_allowed_for_session("kpi.vessel_most_common_grades", ctx) is True


def test_registry_ops_voyage_by_reference_allowed_for_ops_only() -> None:
    ctx = {"role_access": get_role_access("customer_ops_only")}
    assert is_registry_query_allowed_for_session("ops.voyage_by_reference", ctx) is True
    assert is_registry_query_allowed_for_session("ops.vessel_voyages_by_reference", ctx) is True


def test_ops_map_intent_voyage_summary_sql_key_by_role() -> None:
    mock_pg = MagicMock()
    agent = OpsAgent(mock_pg)
    slots = {"voyage_number": 2302, "vessel_name": "Emma Grace"}
    qk_ops, params = agent.map_intent("voyage.summary", slots, {"role_access": get_role_access("customer_ops_only")})
    assert qk_ops == "ops.voyage_by_reference"
    assert params.get("vessel_name") == "Emma Grace"
    qk_full, params = agent.map_intent("voyage.summary", slots, {"role_access": get_role_access("customer")})
    assert qk_full == "kpi.voyage_by_reference"
    assert "scenario" in params


def test_ops_map_intent_vessel_summary_sql_key_by_role() -> None:
    mock_pg = MagicMock()
    agent = OpsAgent(mock_pg)
    slots = {"vessel_name": "Test Vessel"}
    qk_ops, p1 = agent.map_intent("vessel.summary", slots, {"role_access": get_role_access("customer_ops_only")})
    assert qk_ops == "ops.vessel_voyages_by_reference"
    assert p1.get("limit") is not None
    qk_fin, p2 = agent.map_intent("vessel.summary", slots, {"role_access": get_role_access("customer")})
    assert qk_fin == "kpi.vessel_voyages_by_reference"
    assert p2.get("limit") is not None


def test_finance_agent_run_denies_ops_only_session() -> None:
    """Finance registry path must not hit Postgres when role lacks finance_voyage_kpi."""
    ctx = {"role_access": get_role_access("customer_ops_only")}
    mock_pg = MagicMock()
    agent = FinanceAgent(mock_pg)
    out = agent.run(intent_key="ranking.voyages", slots={"limit": 5}, session_context=ctx, user_input="top voyages by pnl")
    assert out.get("query_key") == "access_denied"
    fr = out.get("fallback_reason") or ""
    assert fr == get_finance_kpi_scope_restricted_user_message()
    assert "Data scope" in fr or "operational" in fr.lower()
    assert "intentional" in fr.lower() or "access boundary" in fr.lower()
    mock_pg.fetch_all.assert_not_called()


def test_ops_agent_run_allows_pure_ops_registry_query() -> None:
    """Ops registry SQL that only references ops_voyage_summary is allowed for customer5."""
    ctx = {"role_access": get_role_access("customer_ops_only")}
    mock_pg = MagicMock()
    mock_pg.fetch_all.return_value = [{"vessel_imo": "123", "grades_json": []}]
    agent = OpsAgent(mock_pg)
    # map_intent has no stable key for kpi.vessel_most_common_grades; patch mapping for this unit check.
    with patch.object(OpsAgent, "map_intent", return_value=("kpi.vessel_most_common_grades", {"vessel_imos": ["123"]})):
        out = agent.run(intent_key="__synthetic_test__", slots={}, session_context=ctx, user_input="grades")
    assert out.get("query_key") == "kpi.vessel_most_common_grades"
    assert out.get("rows") == [{"vessel_imo": "123", "grades_json": []}]
    assert "fallback_reason" not in out or out.get("fallback_reason") is None
    mock_pg.fetch_all.assert_called_once_with(
        "kpi.vessel_most_common_grades", {"vessel_imos": ["123"]},
    )


def test_operational_fixtures_keep_ports_not_hire_rates() -> None:
    fixtures = [
        {
            "hireRate": 50000,
            "fixturePorts": [{"portName": "Rotterdam", "activityType": "D"}],
            "fixtureGrades": [{"gradeName": "NHC"}],
        }
    ]
    slim = operational_fixtures_for_summary(fixtures)
    assert len(slim) == 1
    assert slim[0]["fixturePorts"][0]["portName"] == "Rotterdam"
    assert slim[0]["fixtureGrades"][0]["gradeName"] == "NHC"
    assert "hireRate" not in slim[0]


def test_build_voyage_summary_merged_row_combines_ops_and_mongo() -> None:
    ops_row = {
        "voyage_number": 2305,
        "vessel_name": "Emma Grace",
        "offhire_days": 0,
        "is_delayed": False,
        "ports": ["Singapore"],
        "cargo_grade_names": ["NHC"],
    }
    mongo_safe = {
        "voyage_number": 2305,
        "vessel_name": "Emma Grace",
        "legs": [{"portName": "Rotterdam", "activityType": "D"}],
        "fixtures": [{"fixturePorts": [{"portName": "Apapa", "activityType": "L"}], "fixtureGrades": []}],
        "remarks": [{"remark": "Waiting at berth", "text": "Waiting at berth"}],
    }
    row = build_voyage_summary_merged_row(ops_row=ops_row, mongo_safe=mongo_safe, slots={})
    assert "Singapore" in row.get("key_ports", [])
    assert "Rotterdam" in row.get("key_ports", [])
    assert "Apapa" in row.get("key_ports", [])
    assert "NHC" in row.get("cargo_grades", [])


def test_ops_port_query_maps_to_ops_port_details_for_ops_only() -> None:
    mock_pg = MagicMock()
    agent = OpsAgent(mock_pg)
    ctx = {"role_access": get_role_access("customer_ops_only")}
    qk, params = agent.map_intent("ops.port_query", {"port_name": "Singapore", "limit": 5}, ctx)
    assert qk == "ops.port_details"
    assert params.get("port_name") == "Singapore"
    assert is_registry_query_allowed_for_session(qk, ctx) is True
