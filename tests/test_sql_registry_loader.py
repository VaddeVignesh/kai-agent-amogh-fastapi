from app.config.sql_registry_loader import get_sql_registry_entries, get_supported_query_keys
from app.registries.sql_registry import SQL_REGISTRY, SUPPORTED_QUERY_KEYS, QuerySpec


def test_sql_registry_loads_named_queries_from_yaml() -> None:
    entries = get_sql_registry_entries()

    assert "kpi.voyage_by_reference" in entries
    assert "finance.compare_scenarios" in entries
    assert entries["finance.rank_voyages_safe"]["required_params"] == ["limit"]
    assert "FROM finance_voyage_kpi" in entries["finance.rank_voyages_safe"]["sql"]


def test_sql_registry_facade_preserves_python_shape() -> None:
    assert get_supported_query_keys() == SUPPORTED_QUERY_KEYS
    assert isinstance(SQL_REGISTRY["kpi.voyage_by_reference"], QuerySpec)
    assert SQL_REGISTRY["kpi.voyage_by_reference"].required_params == []
    assert "LEFT JOIN ops_voyage_summary" in SQL_REGISTRY["kpi.voyage_by_reference"].sql
