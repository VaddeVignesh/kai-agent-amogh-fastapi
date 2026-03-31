from __future__ import annotations

from types import SimpleNamespace

from app.agents.finance_agent import FinanceAgent
from app.agents.ops_agent import OpsAgent
from app.services.response_merger import compact_payload
from app.sql.sql_generator import SQLGenerator


class _DummyLLM:
    def __init__(self, result: dict):
        self._result = result

    def generate_sql(self, **kwargs):
        return dict(self._result)


def test_sql_generator_always_returns_output() -> None:
    """
    Regression test: SQLGenerator.generate() previously only returned when it injected limit.
    If the model already provided params['limit'], it could fall through and return None.
    """
    llm = _DummyLLM(
        {
            "sql": "SELECT 1 AS x LIMIT %(limit)s",
            "params": {"limit": 5},
            "tables": [],
            "confidence": 0.9,
        }
    )
    gen = SQLGenerator(llm)  # type: ignore[arg-type]
    out = gen.generate(question="q", agent="finance", slots={"limit": 5}, intent_key="ranking.voyages")
    assert out is not None
    assert out.sql.strip().lower().startswith("select 1")
    assert out.params.get("limit") == 5


def test_finance_agent_run_dynamic_uses_llm_for_composite_ranking() -> None:
    """
    Composite queries must use LLM-generated dynamic SQL (allowlist + guard), not registry.
    When run_dynamic is called with sql_generator, result must be mode=dynamic_sql.
    """
    executed = []

    class _DummyPG:
        def execute_dynamic_select(self, sql, params):
            executed.append(("execute_dynamic_select", sql, dict(params)))
            return [
                {"voyage_id": "V1", "voyage_number": 2401, "pnl": 100.0, "revenue": 200.0, "total_expense": 100.0},
            ]

    llm = _DummyLLM({
        "sql": "SELECT voyage_id, voyage_number, revenue - total_expense AS pnl, revenue, total_expense FROM finance_voyage_kpi WHERE scenario = COALESCE(%(scenario)s, 'ACTUAL') ORDER BY pnl DESC LIMIT %(limit)s",
        "params": {"limit": 5, "scenario": "ACTUAL"},
        "tables": ["finance_voyage_kpi"],
        "confidence": 0.9,
    })
    gen = SQLGenerator(llm)  # type: ignore[arg-type]
    agent = FinanceAgent(pg=_DummyPG(), llm_client=llm, sql_generator=gen)
    res = agent.run_dynamic(question="top voyages", intent_key="ranking.voyages", slots={"limit": 5, "scenario": "ACTUAL"})
    assert res.mode == "dynamic_sql"
    assert res.query_key == "dynamic.sql"
    assert len(executed) == 1 and executed[0][0] == "execute_dynamic_select"
    assert res.rows and res.rows[0]["pnl"] == 100.0


def test_ops_agent_voyage_ids_lookup_does_not_truncate_to_display_limit() -> None:
    captured = {}

    class _DummyPG:
        def execute_dynamic_select(self, sql, params):
            captured["params"] = dict(params)
            # Return one row per voyage_id to emulate full retrieval.
            vids = params["voyage_ids"]
            # sql_guard may wrap list params into a 1-tuple for psycopg2 compatibility.
            if isinstance(vids, tuple) and len(vids) == 1 and isinstance(vids[0], list):
                vids = vids[0]
            return [{"voyage_id": vid, "voyage_number": i} for i, vid in enumerate(vids, 1)]

    ops = OpsAgent(pg=_DummyPG(), llm_client=None, sql_generator=SimpleNamespace())
    voyage_ids = ["a", "b", "c", "d", "e", "f"]
    res = ops.run_dynamic(
        question="q",
        intent_key="ranking.voyages",
        slots={"voyage_ids": voyage_ids, "limit": 5},
    )

    assert res.mode == "dynamic_sql"
    assert len(res.rows) == len(voyage_ids)
    assert captured["params"]["limit"] == len(voyage_ids)


def test_finance_agent_ranking_voyages_repairs_sql_that_joins_ops() -> None:
    """
    Regression test: the LLM sometimes generates ranking.voyages SQL that JOINs ops_voyage_summary.
    For ranking.voyages, finance MUST query finance_voyage_kpi only (no joins) to avoid duplicates
    and keep voyage_ids stable for downstream enrichment.
    """
    executed = []

    class _DummyPG:
        def execute_dynamic_select(self, sql, params):
            executed.append(sql.lower())
            return [
                {"voyage_id": "V1", "voyage_number": 2401, "pnl": 100.0, "revenue": 200.0, "total_expense": 100.0},
            ]

    class _LLM:
        def __init__(self):
            self.calls = 0

        def generate_sql(self, **kwargs):
            self.calls += 1
            # First call incorrectly joins ops; second call is repaired finance-only query.
            if self.calls == 1:
                return {
                    "sql": "WITH ranked AS (SELECT voyage_id, voyage_number, revenue-total_expense AS pnl, revenue, total_expense FROM finance_voyage_kpi WHERE scenario = COALESCE(%(scenario)s,'ACTUAL') ORDER BY pnl DESC LIMIT %(limit)s) SELECT r.*, o.ports_json FROM ranked r JOIN ops_voyage_summary o ON r.voyage_id=o.voyage_id LIMIT %(limit)s",
                    "params": {"limit": 5, "scenario": "ACTUAL"},
                    "tables": ["finance_voyage_kpi", "ops_voyage_summary"],
                    "confidence": 0.9,
                }
            return {
                "sql": "SELECT voyage_id, voyage_number, revenue - total_expense AS pnl, revenue, total_expense FROM finance_voyage_kpi WHERE scenario = COALESCE(%(scenario)s,'ACTUAL') ORDER BY pnl DESC LIMIT %(limit)s",
                "params": {"limit": 5, "scenario": "ACTUAL"},
                "tables": ["finance_voyage_kpi"],
                "confidence": 0.9,
            }

    llm = _LLM()
    gen = SQLGenerator(llm)  # type: ignore[arg-type]
    agent = FinanceAgent(pg=_DummyPG(), llm_client=llm, sql_generator=gen)
    res = agent.run_dynamic(question="q", intent_key="ranking.voyages", slots={"limit": 5, "scenario": "ACTUAL"})
    assert res.mode == "dynamic_sql"
    assert res.rows and res.rows[0]["voyage_id"] == "V1"
    assert any("ops_voyage_summary" not in s for s in executed)


def test_compact_payload_parses_dict_shaped_grade_and_keeps_avg_metrics() -> None:
    merged = {
        "artifacts": {
            "merged_rows": [
                {
                    "voyage_id": None,
                    "voyage_number": None,
                    "pnl": None,
                    "revenue": None,
                    "finance": {
                        "cargo_grade": {"grade_name": "pmf", "display_order": 0},
                        "avg_pnl": 4590747.52,
                        "avg_revenue": 7628230.58,
                        "voyage_count": 7,
                    },
                    "cargo_grades": [{"grade_name": "pmf", "display_order": 0}],
                    "key_ports": [{"port_name": "Pasir Gudang", "activity_type": "L"}],
                    "remarks": [],
                }
            ]
        }
    }
    out = compact_payload(merged)
    rows = (((out.get("artifacts") or {}).get("merged_rows")) or [])
    assert len(rows) == 1
    r0 = rows[0]
    assert r0.get("cargo_grades") == ["pmf"]
    assert r0.get("pnl") == 4590747.52
    assert r0.get("revenue") == 7628230.58
    assert r0.get("voyage_count") == 7

