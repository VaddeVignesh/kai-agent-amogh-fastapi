from app.config.sql_rules_loader import (
    get_allowed_columns,
    get_allowed_tables,
    get_column_fixes,
    get_finance_only_columns,
    get_forbidden_patterns,
    get_invalid_columns,
    get_jsonb_functions,
    get_ops_only_columns,
    get_sql_generator_agent_table_scopes,
    get_sql_generator_default_intent_key,
    get_sql_generator_default_limit,
    get_sql_generator_forbidden_columns,
    get_sql_generator_optional_placeholder_slots,
    get_sql_generator_retryable_pg_errors,
    get_sql_generator_validation_messages,
    get_sql_guard_rewrite_patterns,
    get_sql_guard_table_domains,
    get_sql_guard_default_limit,
    get_table_fixes,
)
from app.sql.sql_allowlist import DEFAULT_ALLOWLIST


def test_sql_allowlist_loads_from_yaml() -> None:
    assert "finance_voyage_kpi" in DEFAULT_ALLOWLIST.allowed_tables
    assert "ops_voyage_summary" in DEFAULT_ALLOWLIST.allowed_tables
    assert "pnl" in DEFAULT_ALLOWLIST.allowed_columns["finance_voyage_kpi"]
    assert "ports_json" in DEFAULT_ALLOWLIST.allowed_columns["ops_voyage_summary"]
    assert r"\bDROP\b" in DEFAULT_ALLOWLIST.forbidden_patterns


def test_sql_guard_rule_groups_load_from_yaml() -> None:
    assert get_sql_guard_default_limit() == 50
    assert get_column_fixes()["profit"] == "pnl"
    assert get_table_fixes()["voyage_kpi"] == "finance_voyage_kpi"
    assert "cargo_grade" in get_invalid_columns()
    assert "revenue" in get_finance_only_columns()
    assert "ports_json" in get_ops_only_columns()
    assert "jsonb_array_elements" in get_jsonb_functions()
    assert get_sql_guard_table_domains()["finance_voyage_kpi"] == "finance"
    assert get_sql_guard_table_domains()["ops_voyage_summary"] == "ops"
    assert any("pattern" in rewrite for rewrite in get_sql_guard_rewrite_patterns())


def test_sql_generator_rules_load_from_yaml() -> None:
    assert get_sql_generator_default_limit() == 25
    assert get_sql_generator_default_intent_key() == "composite.query"
    assert "column" in get_sql_generator_retryable_pg_errors()
    assert get_sql_generator_agent_table_scopes()["ops"] == ["ops_voyage_summary"]
    assert get_sql_generator_optional_placeholder_slots()["%(voyage_id)s"] == "voyage_id"
    assert get_sql_generator_validation_messages()["non_select"] == "SQL does not start with SELECT"
    assert get_sql_generator_forbidden_columns()[0]["column"] == "o.voyage_days"


def test_sql_loader_shapes_are_normalized() -> None:
    assert get_allowed_tables() == DEFAULT_ALLOWLIST.allowed_tables
    assert get_allowed_columns() == DEFAULT_ALLOWLIST.allowed_columns
    assert get_forbidden_patterns() == DEFAULT_ALLOWLIST.forbidden_patterns
