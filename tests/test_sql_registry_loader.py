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


def test_sql_registry_uses_voyage_id_for_finance_ops_joins() -> None:
    joined_queries = [
        "kpi.voyage_by_reference",
        "kpi.voyages_by_flexible_filters",
        "kpi.voyages_by_cargo_grade",
        "kpi.vessel_voyages_by_reference",
        "kpi.vessel_performance_summary",
        "kpi.cargo_profitability_analysis",
        "kpi.module_type_performance",
        "kpi.port_performance_analysis",
        "kpi.delayed_voyages_analysis",
        "kpi.offhire_ranking",
        "finance.compare_voyages",
    ]

    for query_key in joined_queries:
        sql = SQL_REGISTRY[query_key].sql
        assert "ON f.voyage_id = o.voyage_id" in sql
        assert "f.voyage_number = o.voyage_number" not in sql


def test_voyage_by_reference_prefers_canonical_voyage_id_over_number() -> None:
    sql = SQL_REGISTRY["kpi.voyage_by_reference"].sql

    assert "%(voyage_id)s IS NOT NULL AND f.voyage_id::TEXT = %(voyage_id)s::TEXT" in sql
    assert "%(voyage_id)s IS NULL" in sql
    assert "AND f.voyage_number::TEXT = %(voyage_number)s::TEXT" in sql


def test_scenario_comparison_rolls_up_to_one_row_per_voyage_number() -> None:
    sql = SQL_REGISTRY["finance.compare_scenarios"].sql

    assert "GROUP BY f.voyage_id, f.voyage_number" in sql
    assert "GROUP BY p.voyage_number" in sql
    assert "SUM(COALESCE(p.pnl_actual_by_pair" in sql
    assert "AVG(p.tce_actual_by_pair" in sql
    assert "FROM rolled r" in sql
