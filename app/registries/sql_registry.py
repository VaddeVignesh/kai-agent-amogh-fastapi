from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List


@dataclass(frozen=True)
class QuerySpec:
    description: str
    required_params: List[str]
    sql: str


SQL_REGISTRY: Dict[str, QuerySpec] = {

    # =====================================================
    # FLEXIBLE VOYAGE QUERIES (Finance + Ops joined)
    # =====================================================

    "kpi.voyage_by_reference": QuerySpec(
        description="Get voyage by ANY reference (voyage_id or voyage_number)",
        required_params=[],
        sql="""
            SELECT DISTINCT
              f.voyage_id,
              f.voyage_number,
              f.vessel_imo,
              f.scenario,
              f.revenue,
              f.total_expense,
              f.pnl,
              f.tce,
              f.total_commission,
              f.voyage_start_date,
              f.voyage_end_date,
              o.vessel_name,
              o.module_type,
              o.fixture_count,
              o.is_delayed,
              o.delay_reason,
              o.offhire_days,
              o.ports_json,
              o.grades_json,
              o.activities_json
            FROM finance_voyage_kpi f
            LEFT JOIN ops_voyage_summary o
              ON f.voyage_number = o.voyage_number
              AND REPLACE(f.vessel_imo::TEXT, '.0', '') = REPLACE(o.vessel_imo::TEXT, '.0', '')
            WHERE f.scenario = COALESCE(%(scenario)s, 'ACTUAL')
              AND (
                (%(voyage_number)s IS NOT NULL AND f.voyage_number::TEXT = %(voyage_number)s::TEXT)
                OR (%(voyage_id)s IS NOT NULL AND f.voyage_id::TEXT = %(voyage_id)s::TEXT)
              )
              AND (
                %(vessel_imo)s IS NULL
                OR REPLACE(f.vessel_imo::TEXT, '.0', '') = %(vessel_imo)s::TEXT
              )
            LIMIT COALESCE(%(limit)s, 10);
        """,
    ),

    "kpi.voyages_by_flexible_filters": QuerySpec(
        description="Get voyages with flexible optional filters (port, cargo, vessel, dates, PnL thresholds)",
        required_params=["limit"],
        sql="""
            SELECT
              f.voyage_id,
              f.voyage_number,
              f.vessel_imo,
              f.revenue,
              f.total_expense,
              f.pnl,
              f.tce,
              f.total_commission,
              f.voyage_start_date,
              f.voyage_end_date,
              o.vessel_name,
              o.module_type,
              o.is_delayed,
              o.offhire_days,
              o.delay_reason,
              o.ports_json,
              o.grades_json
            FROM finance_voyage_kpi f
            LEFT JOIN ops_voyage_summary o
              ON f.voyage_number = o.voyage_number
              AND REPLACE(f.vessel_imo::TEXT, '.0', '') = REPLACE(o.vessel_imo::TEXT, '.0', '')
            WHERE f.scenario = COALESCE(%(scenario)s, 'ACTUAL')
              AND (%(voyage_number)s IS NULL OR f.voyage_number = %(voyage_number)s)
              AND (%(port_name)s IS NULL OR o.ports_json::text ILIKE '%%' || %(port_name)s || '%%')
              AND (%(vessel_name)s IS NULL OR o.vessel_name ILIKE '%%' || %(vessel_name)s || '%%')
              AND (%(vessel_imo)s IS NULL OR REPLACE(f.vessel_imo::TEXT, '.0', '') = %(vessel_imo)s::TEXT)
              AND (%(date_from)s IS NULL OR f.voyage_end_date >= %(date_from)s::DATE)
              AND (%(date_to)s IS NULL OR f.voyage_end_date <= %(date_to)s::DATE)
              AND (%(min_revenue)s IS NULL OR f.revenue >= %(min_revenue)s::NUMERIC)
              AND (%(max_revenue)s IS NULL OR f.revenue <= %(max_revenue)s::NUMERIC)
              AND (%(min_pnl)s IS NULL OR f.pnl >= %(min_pnl)s::NUMERIC)
              AND (%(max_pnl)s IS NULL OR f.pnl <= %(max_pnl)s::NUMERIC)
              AND (%(is_delayed)s IS NULL OR o.is_delayed = %(is_delayed)s::BOOLEAN)
              AND (%(module_type)s IS NULL OR o.module_type ILIKE '%%' || %(module_type)s || '%%')
            ORDER BY f.pnl DESC NULLS LAST
            LIMIT %(limit)s;
        """,
    ),

    "finance.high_revenue_low_pnl": QuerySpec(
        description="Voyages with high revenue but low or negative PnL (uses only core columns: no bunker_cost/port_cost)",
        required_params=["limit"],
        sql="""
            SELECT
              voyage_id,
              voyage_number,
              vessel_imo,
              scenario,
              revenue,
              total_expense,
              pnl,
              tce,
              total_commission,
              voyage_start_date,
              voyage_end_date
            FROM finance_voyage_kpi
            WHERE scenario = COALESCE(%(scenario)s, 'ACTUAL')
              AND revenue IS NOT NULL
              AND pnl IS NOT NULL
              AND revenue > 0
              AND pnl < 0
            ORDER BY revenue DESC NULLS LAST, pnl ASC NULLS LAST
            LIMIT %(limit)s;
        """,
    ),

    "finance.rank_voyages_safe": QuerySpec(
        description="Rank voyages by a safe metric (pnl/revenue/expense/commission/tce) with safe direction",
        required_params=["limit"],
        sql="""
            SELECT
              voyage_id,
              voyage_number,
              vessel_imo,
              scenario,
              revenue,
              total_expense,
              pnl,
              tce,
              total_commission,
              voyage_start_date,
              voyage_end_date
            FROM finance_voyage_kpi
            WHERE scenario = COALESCE(%(scenario)s, 'ACTUAL')
              AND pnl IS NOT NULL
            ORDER BY
              CASE WHEN COALESCE(%(direction)s, 'desc') = 'desc' THEN
                CASE COALESCE(%(metric)s, 'pnl')
                  WHEN 'pnl'              THEN pnl
                  WHEN 'revenue'           THEN revenue
                  WHEN 'total_expense'     THEN total_expense
                  WHEN 'total_commission'  THEN total_commission
                  WHEN 'tce'               THEN tce
                  ELSE pnl
                END
              END DESC NULLS LAST,
              CASE WHEN COALESCE(%(direction)s, 'desc') = 'asc' THEN
                CASE COALESCE(%(metric)s, 'pnl')
                  WHEN 'pnl'              THEN pnl
                  WHEN 'revenue'           THEN revenue
                  WHEN 'total_expense'     THEN total_expense
                  WHEN 'total_commission'  THEN total_commission
                  WHEN 'tce'               THEN tce
                  ELSE pnl
                END
              END ASC NULLS LAST,
              voyage_number DESC NULLS LAST
            LIMIT %(limit)s;
        """,
    ),

    "kpi.voyages_by_cargo_grade": QuerySpec(
        description="Find voyages that carried a given cargo grade (via ops grades_json) with finance KPIs",
        required_params=["cargo_grade", "limit"],
        sql="""
            SELECT
              f.voyage_id,
              f.voyage_number,
              f.vessel_imo,
              f.scenario,
              f.revenue,
              f.total_expense,
              f.pnl,
              f.tce,
              f.total_commission,
              f.voyage_start_date,
              f.voyage_end_date,
              o.vessel_name,
              o.module_type,
              o.is_delayed,
              o.offhire_days,
              o.delay_reason,
              o.ports_json,
              o.grades_json,
              o.activities_json,
              o.remarks_json
            FROM finance_voyage_kpi f
            LEFT JOIN ops_voyage_summary o
              ON f.voyage_number = o.voyage_number
              AND REPLACE(f.vessel_imo::TEXT, '.0', '') = REPLACE(o.vessel_imo::TEXT, '.0', '')
            WHERE f.scenario = COALESCE(%(scenario)s, 'ACTUAL')
              AND o.grades_json IS NOT NULL
              AND o.grades_json::text ILIKE '%%' || %(cargo_grade)s || '%%'
            ORDER BY f.pnl DESC NULLS LAST
            LIMIT %(limit)s;
        """,
    ),

    # =====================================================
    # VESSEL QUERIES
    # =====================================================

    "kpi.vessel_voyages_by_reference": QuerySpec(
        description="Get all voyages for a vessel (by name or IMO)",
        required_params=["vessel_ref", "limit"],
        sql="""
            SELECT
              f.voyage_id,
              f.voyage_number,
              f.vessel_imo,
              f.revenue,
              f.total_expense,
              f.pnl,
              f.tce,
              f.total_commission,
              f.voyage_start_date,
              f.voyage_end_date,
              o.vessel_name,
              o.module_type,
              o.is_delayed,
              o.offhire_days,
              o.ports_json,
              o.grades_json
            FROM finance_voyage_kpi f
            LEFT JOIN ops_voyage_summary o
              ON f.voyage_number = o.voyage_number
              AND REPLACE(f.vessel_imo::TEXT, '.0', '') = REPLACE(o.vessel_imo::TEXT, '.0', '')
            WHERE f.scenario = COALESCE(%(scenario)s, 'ACTUAL')
              AND (
                o.vessel_name ILIKE '%%' || %(vessel_ref)s || '%%'
                OR REPLACE(f.vessel_imo::TEXT, '.0', '') = %(vessel_ref)s::TEXT
                OR f.vessel_imo::TEXT = %(vessel_ref)s::TEXT
              )
            ORDER BY f.voyage_end_date DESC
            LIMIT %(limit)s;
        """,
    ),

    "kpi.vessel_performance_summary": QuerySpec(
        description="Aggregate vessel performance (voyage count, avg PnL, total revenue)",
        required_params=["limit"],
        sql="""
            SELECT
              f.vessel_imo,
              MAX(o.vessel_name) AS vessel_name,
              COUNT(*) AS voyage_count,
              AVG(f.pnl) AS avg_pnl,
              SUM(f.pnl) AS total_pnl,
              AVG(f.tce) AS avg_tce,
              SUM(f.revenue) AS total_revenue,
              AVG(o.offhire_days) AS avg_offhire_days
            FROM finance_voyage_kpi f
            LEFT JOIN ops_voyage_summary o
              ON f.voyage_number = o.voyage_number
              AND REPLACE(f.vessel_imo::TEXT, '.0', '') = REPLACE(o.vessel_imo::TEXT, '.0', '')
            WHERE f.scenario = COALESCE(%(scenario)s, 'ACTUAL')
              AND f.pnl IS NOT NULL
            GROUP BY f.vessel_imo
            HAVING COUNT(*) >= COALESCE(%(min_voyage_count)s, 1)
            ORDER BY AVG(f.pnl) DESC NULLS LAST
            LIMIT %(limit)s;
        """,
    ),

    "kpi.vessel_most_common_grades": QuerySpec(
        description="Most common cargo grades per vessel (for ranking.vessels)",
        required_params=["vessel_imos"],
        sql="""
            WITH expanded AS (
              SELECT
                REPLACE(o.vessel_imo::TEXT, '.0', '') AS vessel_imo,
                o.vessel_name,
                jsonb_array_elements_text(o.grades_json) AS grade
              FROM ops_voyage_summary o
              WHERE REPLACE(o.vessel_imo::TEXT, '.0', '') = ANY(%(vessel_imos)s)
                AND o.grades_json IS NOT NULL AND o.grades_json::text != '[]'
            ),
            counted AS (
              SELECT vessel_imo, vessel_name, grade, COUNT(*) AS cnt
              FROM expanded
              WHERE grade IS NOT NULL AND TRIM(grade) != ''
              GROUP BY vessel_imo, vessel_name, grade
            ),
            ranked AS (
              SELECT vessel_imo, vessel_name, grade,
                     ROW_NUMBER() OVER (PARTITION BY vessel_imo ORDER BY cnt DESC) AS rn
              FROM counted
            )
            SELECT vessel_imo, vessel_name,
                   array_agg(grade ORDER BY rn) AS grades_json
            FROM ranked
            WHERE rn <= 10
            GROUP BY vessel_imo, vessel_name;
        """,
    ),

    # =====================================================
    # ANALYSIS QUERIES (Cargo, Module, Port)
    # =====================================================

    "kpi.cargo_profitability_analysis": QuerySpec(
        description="Analyze profitability by cargo grade",
        required_params=["limit"],
        sql="""
            WITH cargo_grades AS (
              SELECT
                f.voyage_id,
                f.voyage_number,
                f.pnl,
                f.revenue,
                f.tce,
                lower(trim(jsonb_array_elements_text(o.grades_json))) AS cargo_grade
              FROM finance_voyage_kpi f
              JOIN ops_voyage_summary o
                ON (
                  (f.voyage_id IS NOT NULL AND o.voyage_id IS NOT NULL AND f.voyage_id = o.voyage_id)
                  OR (
                    f.voyage_number = o.voyage_number
                    AND REPLACE(f.vessel_imo::TEXT, '.0', '') = REPLACE(o.vessel_imo::TEXT, '.0', '')
                  )
                )
              WHERE f.scenario = COALESCE(%(scenario)s, 'ACTUAL')
                AND o.grades_json IS NOT NULL
                AND o.grades_json::text != '[]'
            )
            SELECT
              cargo_grade,
              COUNT(*) AS voyage_count,
              AVG(pnl) AS avg_pnl,
              SUM(pnl) AS total_pnl,
              AVG(tce) AS avg_tce,
              AVG(revenue) AS avg_revenue
            FROM cargo_grades
            WHERE cargo_grade IS NOT NULL
              AND cargo_grade != ''
              AND cargo_grade != 'none'
              AND cargo_grade != 'null'
              AND cargo_grade != 'n/a'
              AND cargo_grade != 'na'
            GROUP BY cargo_grade
            HAVING COUNT(*) >= 3
            ORDER BY AVG(pnl) DESC NULLS LAST
            LIMIT %(limit)s;
        """,
    ),

    "kpi.module_type_performance": QuerySpec(
        description="Analyze performance by module type (TC Voyage, Spot, etc.)",
        required_params=["limit"],
        sql="""
            SELECT
              o.module_type,
              COUNT(*) AS voyage_count,
              AVG(f.pnl) AS avg_pnl,
              SUM(f.pnl) AS total_pnl,
              AVG(f.tce) AS avg_tce,
              AVG(f.revenue) AS avg_revenue,
              AVG(o.offhire_days) AS avg_offhire_days
            FROM finance_voyage_kpi f
            JOIN ops_voyage_summary o
              ON f.voyage_number = o.voyage_number
              AND REPLACE(f.vessel_imo::TEXT, '.0', '') = REPLACE(o.vessel_imo::TEXT, '.0', '')
            WHERE f.scenario = COALESCE(%(scenario)s, 'ACTUAL')
              AND o.module_type IS NOT NULL
            GROUP BY o.module_type
            HAVING COUNT(*) >= 3
            ORDER BY AVG(f.pnl) DESC NULLS LAST
            LIMIT %(limit)s;
        """,
    ),

    "kpi.port_performance_analysis": QuerySpec(
        description="Analyze voyages by port (frequency, profitability)",
        required_params=["port_name", "limit"],
        sql="""
            SELECT
              f.voyage_id,
              f.voyage_number,
              f.vessel_imo,
              f.revenue,
              f.pnl,
              f.tce,
              o.vessel_name,
              o.ports_json,
              o.grades_json,
              o.is_delayed,
              o.offhire_days,
              o.voyage_end_date
            FROM finance_voyage_kpi f
            JOIN ops_voyage_summary o
              ON f.voyage_number = o.voyage_number
              AND REPLACE(f.vessel_imo::TEXT, '.0', '') = REPLACE(o.vessel_imo::TEXT, '.0', '')
            WHERE f.scenario = COALESCE(%(scenario)s, 'ACTUAL')
              AND o.ports_json::text ILIKE '%%' || %(port_name)s || '%%'
            ORDER BY f.pnl DESC NULLS LAST
            LIMIT %(limit)s;
        """,
    ),

    "ops.port_details": QuerySpec(
        description="Port details from ops only (voyages, grades, remarks)",
        required_params=["port_name", "limit"],
        sql="""
            SELECT
              o.voyage_id,
              o.voyage_number,
              o.vessel_imo,
              o.vessel_name,
              o.module_type,
              o.fixture_count,
              o.offhire_days,
              o.is_delayed,
              o.delay_reason,
              o.ports_json,
              o.grades_json,
              o.remarks_json,
              o.voyage_start_date,
              o.voyage_end_date
            FROM ops_voyage_summary o
            WHERE o.ports_json::text ILIKE '%%' || %(port_name)s || '%%'
            ORDER BY o.voyage_end_date DESC NULLS LAST
            LIMIT %(limit)s;
        """,
    ),

    # =====================================================
    # DELAYED / OFFHIRE
    # =====================================================

    "kpi.delayed_voyages_analysis": QuerySpec(
        description="Get delayed voyages with finance impact",
        required_params=["limit"],
        sql="""
            SELECT
              f.voyage_id,
              f.voyage_number,
              f.vessel_imo,
              f.revenue,
              f.total_expense,
              f.pnl,
              f.tce,
              o.vessel_name,
              o.is_delayed,
              o.offhire_days,
              o.delay_reason,
              o.ports_json,
              o.grades_json,
              o.voyage_end_date
            FROM finance_voyage_kpi f
            JOIN ops_voyage_summary o
              ON f.voyage_number = o.voyage_number
              AND REPLACE(f.vessel_imo::TEXT, '.0', '') = REPLACE(o.vessel_imo::TEXT, '.0', '')
            WHERE f.scenario = COALESCE(%(scenario)s, 'ACTUAL')
              AND o.is_delayed = TRUE
              AND (%(min_offhire)s IS NULL OR o.offhire_days >= %(min_offhire)s::NUMERIC)
            ORDER BY o.offhire_days DESC NULLS LAST
            LIMIT %(limit)s;
        """,
    ),

    "kpi.offhire_ranking": QuerySpec(
        description="Rank voyages by offhire days with PnL, TCE and remarks",
        required_params=["limit"],
        sql="""
            SELECT
              f.voyage_id,
              f.voyage_number,
              f.vessel_imo,
              f.revenue,
              f.total_expense,
              f.pnl,
              f.tce,
              o.vessel_name,
              o.offhire_days,
              o.delay_reason,
              o.remarks_json,
              o.ports_json,
              o.grades_json,
              o.voyage_end_date
            FROM finance_voyage_kpi f
            JOIN ops_voyage_summary o
              ON f.voyage_number = o.voyage_number
              AND REPLACE(f.vessel_imo::TEXT, '.0', '') = REPLACE(o.vessel_imo::TEXT, '.0', '')
            WHERE f.scenario = COALESCE(%(scenario)s, 'ACTUAL')
            ORDER BY o.offhire_days DESC NULLS LAST, f.voyage_number DESC
            LIMIT %(limit)s;
        """,
    ),

    # =====================================================
    # SCENARIO COMPARISON (kept generic)
    # =====================================================

    "finance.compare_scenarios": QuerySpec(
        description="Compare ACTUAL vs WHEN_FIXED for specific voyages",
        required_params=["voyage_numbers"],
        sql="""
            WITH voyage_nums AS (
                SELECT unnest(%(voyage_numbers)s::integer[]) AS vnum
            ),
            per_pair AS (
                SELECT
                  f.voyage_number,
                  REPLACE(f.vessel_imo::TEXT, '.0', '') AS vessel_imo_key,
                  MAX(f.voyage_id) AS voyage_id,
                  MAX(CASE WHEN f.scenario = COALESCE(%(scenario_actual)s, 'ACTUAL') THEN f.pnl END) AS pnl_actual_by_pair,
                  MAX(CASE WHEN f.scenario = 'WHEN_FIXED' THEN f.pnl END) AS pnl_when_fixed_by_pair,
                  MAX(CASE WHEN f.scenario = COALESCE(%(scenario_actual)s, 'ACTUAL') THEN f.tce END) AS tce_actual_by_pair,
                  MAX(CASE WHEN f.scenario = 'WHEN_FIXED' THEN f.tce END) AS tce_when_fixed_by_pair,
                  MAX(CASE WHEN f.scenario = COALESCE(%(scenario_actual)s, 'ACTUAL') THEN f.revenue END) AS revenue_actual_by_pair,
                  MAX(f.voyage_end_date) AS voyage_end_date
                FROM finance_voyage_kpi f
                INNER JOIN voyage_nums vn ON f.voyage_number = vn.vnum
                WHERE f.scenario IN (
                  COALESCE(%(scenario_actual)s, 'ACTUAL'),
                  COALESCE(%(scenario_when_fixed)s, 'WHEN_FIXED')
                )
                GROUP BY f.voyage_number, REPLACE(f.vessel_imo::TEXT, '.0', '')
            )
            SELECT
              MAX(voyage_id) AS voyage_id,
              voyage_number,
              SUM(COALESCE(pnl_actual_by_pair, 0)) AS pnl_actual,
              SUM(COALESCE(pnl_when_fixed_by_pair, 0)) AS pnl_when_fixed,
              (SUM(COALESCE(pnl_when_fixed_by_pair, 0)) - SUM(COALESCE(pnl_actual_by_pair, 0))) AS pnl_variance,
              AVG(tce_actual_by_pair) AS tce_actual,
              AVG(tce_when_fixed_by_pair) AS tce_when_fixed,
              (AVG(tce_when_fixed_by_pair) - AVG(tce_actual_by_pair)) AS tce_variance,
              SUM(COALESCE(revenue_actual_by_pair, 0)) AS revenue_actual,
              MAX(voyage_end_date) AS voyage_end_date
            FROM per_pair
            GROUP BY voyage_number
            ORDER BY voyage_number;
        """,
    ),

    "finance.compare_voyages": QuerySpec(
        description="Compare specific voyages side-by-side by voyage numbers",
        required_params=["voyage_numbers", "limit"],
        sql="""
            SELECT
              f.voyage_id,
              f.voyage_number,
              f.vessel_imo,
              f.revenue,
              f.total_expense,
              f.pnl,
              f.tce,
              f.total_commission,
              f.voyage_start_date,
              f.voyage_end_date,
              o.vessel_name,
              o.module_type,
              o.is_delayed,
              o.offhire_days,
              o.delay_reason,
              o.ports_json,
              o.grades_json,
              o.remarks_json
            FROM finance_voyage_kpi f
            LEFT JOIN ops_voyage_summary o
              ON f.voyage_number = o.voyage_number
              AND REPLACE(f.vessel_imo::TEXT, '.0', '') = REPLACE(o.vessel_imo::TEXT, '.0', '')
            WHERE f.scenario = COALESCE(%(scenario)s, 'ACTUAL')
              AND f.voyage_number = ANY(%(voyage_numbers)s::INT[])
            ORDER BY f.voyage_number ASC
            LIMIT %(limit)s;
        """,
    ),
}


SUPPORTED_QUERY_KEYS = set(SQL_REGISTRY.keys())
