from __future__ import annotations

from app.sql.sql_guard import validate_and_prepare_sql


def test_sql_guard_rewrites_profit_to_pnl() -> None:
    sql = "SELECT voyage_number, profit FROM finance_voyage_kpi ORDER BY profit DESC LIMIT %(limit)s"
    res = validate_and_prepare_sql(sql=sql, params={"limit": 5})
    assert res.ok, res.reason
    assert "profit" not in res.sql.lower()
    assert "pnl" in res.sql.lower()

