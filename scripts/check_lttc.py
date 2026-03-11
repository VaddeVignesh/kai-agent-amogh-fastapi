from __future__ import annotations

import os
import sys

from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.adapters.postgres_adapter import PostgresAdapter, PostgresConfig
import json


def main() -> None:
    pg = PostgresAdapter(PostgresConfig.from_env())

    queries = [
        (
            "distinct_module_types",
            """
            SELECT module_type, COUNT(*) AS n
            FROM ops_voyage_summary
            GROUP BY module_type
            ORDER BY n DESC
            """,
        ),
        (
            "lttc_counts",
            """
            SELECT COUNT(*) AS n,
                   SUM(CASE WHEN ports_json IS NOT NULL THEN 1 ELSE 0 END) AS ports_nonnull,
                   SUM(CASE WHEN grades_json IS NOT NULL THEN 1 ELSE 0 END) AS grades_nonnull
            FROM ops_voyage_summary
            WHERE module_type = 'LTTC'
            """,
        ),
        (
            "lttc_grade_rows",
            """
            SELECT COUNT(*) AS voyages_with_grades
            FROM ops_voyage_summary
            WHERE module_type = 'LTTC'
              AND grades_json IS NOT NULL
              AND jsonb_typeof(grades_json) = 'array'
              AND jsonb_array_length(grades_json) <> 0
            """,
        ),
        (
            "lttc_port_rows",
            """
            SELECT COUNT(*) AS voyages_with_ports
            FROM ops_voyage_summary
            WHERE module_type = 'LTTC'
              AND ports_json IS NOT NULL
              AND jsonb_typeof(ports_json) = 'array'
              AND jsonb_array_length(ports_json) <> 0
            """,
        ),
        (
            "lttc_avg_pnl",
            """
            SELECT AVG(f.pnl) AS avg_pnl_raw,
                   AVG(COALESCE(f.pnl, 0)) AS avg_pnl_coalesced,
                   COUNT(*) AS joined_rows
            FROM finance_voyage_kpi f
            JOIN ops_voyage_summary o
              ON f.voyage_id = o.voyage_id
            WHERE f.scenario = 'ACTUAL'
              AND o.module_type = 'LTTC'
            """,
        ),
        (
            "lttc_pnl_nulls",
            """
            SELECT COUNT(*) AS n,
                   SUM(CASE WHEN f.pnl IS NULL THEN 1 ELSE 0 END) AS pnl_nulls,
                   SUM(CASE WHEN f.pnl = 0 THEN 1 ELSE 0 END) AS pnl_zero
            FROM finance_voyage_kpi f
            JOIN ops_voyage_summary o
              ON f.voyage_id = o.voyage_id
            WHERE f.scenario = 'ACTUAL'
              AND o.module_type = 'LTTC'
            """,
        ),
    ]

    for name, sql in queries:
        rows = pg.execute_dynamic_select(sql, {})
        print("\n==", name)
        if name == "distinct_module_types":
            for r in rows:
                print(r)
        else:
            print(rows[0] if rows else rows)

    sample = pg.execute_dynamic_select(
        """
        SELECT voyage_id, voyage_number, module_type, grades_json, ports_json
        FROM ops_voyage_summary
        WHERE module_type = 'LTTC'
        LIMIT 1
        """,
        {},
    )
    print("\n== lttc_sample_ops_row")
    print(json.dumps(sample[0] if sample else {}, default=str, indent=2))

    grade_name_stats = pg.execute_dynamic_select(
        """
        SELECT
            COUNT(*) AS voyage_count,
            SUM(
                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements(COALESCE(grades_json, '[]'::jsonb)) g
                        WHERE COALESCE(g->>'grade_name', g->>'gradeName') IS NOT NULL
                          AND COALESCE(g->>'grade_name', g->>'gradeName') <> ''
                          AND COALESCE(g->>'grade_name', g->>'gradeName') <> 'null'
                    )
                    THEN 1 ELSE 0
                END
            ) AS voyages_with_any_grade_name
        FROM ops_voyage_summary
        WHERE module_type = 'LTTC'
        """,
        {},
    )
    print("\n== lttc_grade_name_stats")
    print(grade_name_stats[0] if grade_name_stats else grade_name_stats)


if __name__ == "__main__":
    main()

