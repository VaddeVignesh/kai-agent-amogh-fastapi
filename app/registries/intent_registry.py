# app/registries/intent_registry.py
"""
Intent Registry - Maps user intents to data requirements and agent actions.

Design principles:
- `description`: Rich natural-language description used directly in the LLM
  classification prompt. Must clearly distinguish entity-anchored intents
  (specific vessel/voyage known) from fleet-wide aggregate intents (no entity).
- `route`: "composite" forces composite + dynamic SQL regardless of slots.
  "single" allows the planner to decide. Driven by registry, not hardcoded logic.
- `required_slots`: Slots that MUST be present for the intent to execute.
- `optional_slots`: Slots the intent CAN use if present — only these survive
  registry-driven slot cleanup in the router.
- `needs`: Which agents are involved.
- `sql_hints`: Per-agent SQL generation instructions injected into the LLM
  prompt by sql_generator.py. Keyed by agent name ("finance", "ops").
  Adding a new intent only requires updating this registry — sql_generator.py
  never needs a hardcoded elif block.
"""


# =========================================================
# Supported Intents - COMPLETE LIST
# =========================================================

SUPPORTED_INTENTS = [
    # Entity queries
    "voyage.summary",
    "voyage.entity",
    "voyage.metadata",
    "vessel.summary",
    "vessel.entity",
    "vessel.metadata",
    "cargo.details",
    "port.details",

    # Analysis queries
    "analysis.revenue_vs_pnl",
    "analysis.high_revenue_low_pnl",
    "analysis.cargo_profitability",
    "analysis.by_module_type",
    "analysis.segment_performance",
    "analysis.scenario_comparison",
    "analysis.cost_breakdown",
    "analysis.variance",
    "analysis.profitability",

    # Ranking queries
    "ranking.voyages",
    "ranking.pnl",
    "ranking.revenue",
    "ranking.port_calls",
    "ranking.voyages_by_pnl",
    "ranking.voyages_by_revenue",
    "ranking.voyages_by_commission",
    "ranking.vessels",
    "ranking.cargo",
    "ranking.ports",
    "ranking.routes",

    # Operational queries
    "ops.offhire_ranking",
    "finance.loss_due_to_delay",
    "ops.voyages_by_port",
    "ops.port_query",
    "ops.cargo_movements",
    "ops.route_analysis",
    "ops.vessel_utilization",
    "ops.demurrage",
    "ops.voyages_by_cargo_grade",

    # Comparison queries
    "comparison.scenario",
    "comparison.voyages",
    "comparison.vessels",
    "comparison.periods",

    # Aggregation queries
    "aggregation.count",
    "aggregation.average",
    "aggregation.total",
    "aggregation.trends",

    # Temporal queries
    "temporal.period",
    "temporal.trend",

    # Composite and fallback
    "composite.query",
    "followup.result_set",
    "out_of_scope",
]


# =========================================================
# Intent Configuration Registry
# =========================================================

INTENT_REGISTRY = {

    # ---------------------------------------------------------
    # ENTITY QUERIES
    # ---------------------------------------------------------

    "voyage.summary": {
        "description": (
            "Full summary of a SPECIFIC voyage already identified by voyage number. "
            "Use ONLY when the user refers to a voyage by its number (e.g. 'voyage 1901', '1901'). "
            "Fetches finance KPIs, ops snapshot, and Mongo remarks for that one voyage. "
            "Do NOT use for fleet-wide questions or when no voyage number is present."
        ),
        "route": "single",
        "required_slots": ["voyage_number"],
        "optional_slots": ["voyage_id", "scenario"],
        "needs": {"mongo": True, "finance": True, "ops": True},
        "mongo_intent": "entity.voyage",
        "mongo_projection": None,
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance voyage.summary):
- Query `finance_voyage_kpi`. Join `ops_voyage_summary` only if needed to resolve vessel name.
- MUST filter scenario: `scenario = COALESCE(%(scenario)s, 'ACTUAL')`.
- Filter by voyage_number = %(voyage_number)s.
- Return voyage-level rows with exact aliases:
  voyage_id, voyage_number, revenue, total_expense, pnl, tce, total_commission,
  voyage_start_date, voyage_end_date.
- Keep LIMIT %(limit)s.
""",
            "ops": "",
        },
    },

    "voyage.entity": {
        "description": (
            "Find voyages matching a filter — by port, vessel, date range, or cargo grade — "
            "when no specific voyage number is given. Broader than voyage.summary. "
            "Use when the user says 'find voyages that...' or 'which voyages went to X'."
        ),
        "route": "single",
        "required_slots": [],
        "optional_slots": ["voyage_number", "voyage_id", "port_name", "vessel_name"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": "",
            "ops": "",
        },
    },

    "voyage.metadata": {
        "description": (
            "Document-level metadata for a SPECIFIC voyage from MongoDB only — "
            "fixture/commercial terms, cargo details, route legs, bunkers, emissions, "
            "projected results, tags, and source URL. Use when the question is about "
            "voyage document facts, not KPI performance trends."
        ),
        "route": "single",
        "required_slots": [],
        "optional_slots": ["voyage_number", "voyage_numbers", "voyage_id"],
        "needs": {"mongo": True, "finance": False, "ops": False},
        "mongo_intent": "entity.voyage",
        "mongo_projection": {
            "_id": 0,
            "voyageId": 1,
            "voyageNumber": 1,
            "vesselName": 1,
            "vesselImo": 1,
            "url": 1,
            "extracted_at": 1,
            "tags": 1,
            "fixtures": 1,
            "revenues": 1,
            "expenses": 1,
            "legs.port_name": 1,
            "legs.activity_type": 1,
            "legs.display_order": 1,
            "cargoes.grade_name": 1,
            "cargoes.quantity": 1,
            "cargoes.unit": 1,
            "remarks": 1,
            "offhire_events": 1,
            "bunkers": 1,
            "emissions": 1,
            "projected_results": 1,
            "startDateUtc": 1,
            "endDateUtc": 1,
        },
        "sql_hints": {
            "finance": "",
            "ops": "",
        },
    },

    "vessel.summary": {
        "description": (
            "Full performance summary of a SPECIFIC vessel already identified by name or IMO. "
            "Use ONLY when the user names a specific vessel (e.g. 'Vessel-001', IMO 9667485). "
            "Fetches all voyages for that vessel with finance + ops + Mongo context. "
            "Do NOT use when the user is asking which vessel ranks best/worst across the fleet — "
            "use ranking.vessels for that."
        ),
        "route": "single",
        "required_slots": [],
        "optional_slots": ["vessel_name", "imo"],
        "needs": {"mongo": True, "finance": True, "ops": True},
        "mongo_intent": "entity.vessel",
        "mongo_projection": None,
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance vessel.summary):
- Query `finance_voyage_kpi`. Join `ops_voyage_summary` only when needed to resolve vessel name.
- MUST filter scenario: `scenario = COALESCE(%(scenario)s, 'ACTUAL')`.
- If imo/vessel_imo is available, filter by normalized IMO key:
  REPLACE(f.vessel_imo::TEXT, '.0', '') = REPLACE(%(imo)s::TEXT, '.0', '')
- Return voyage-level rows with exact aliases:
  voyage_id, voyage_number, revenue, total_expense, pnl, tce, total_commission,
  voyage_start_date.
- Keep LIMIT %(limit)s.
""",
            "ops": "",
        },
    },

    "vessel.entity": {
        "description": (
            "Fetch Mongo entity document for a SPECIFIC named vessel. "
            "Use when only vessel metadata from Mongo is needed (no finance/ops). "
            "Requires vessel_name or imo."
        ),
        "route": "single",
        "required_slots": [],
        "optional_slots": ["vessel_name", "imo"],
        "needs": {"mongo": True, "finance": False, "ops": False},
        "mongo_intent": "entity.vessel",
        "mongo_projection": None,
        "sql_hints": {
            "finance": "",
            "ops": "",
        },
    },

    "vessel.metadata": {
        "description": (
            "Technical and commercial metadata for a SPECIFIC vessel from MongoDB only — "
            "consumption profiles, hire rate, scrubber, passage types, market type, contract history, tags. "
            "Use when the question is about vessel configuration or spec, not voyage performance. "
            "Requires vessel_name or imo (or voyage_number to resolve the vessel)."
        ),
        "route": "single",
        "required_slots": [],
        "optional_slots": ["vessel_name", "imo", "voyage_number", "voyage_numbers"],
        "needs": {"mongo": True, "finance": False, "ops": False},
        "mongo_intent": "entity.vessel",
        "mongo_projection": {
            "_id": 0,
            "vesselId": 1,
            "name": 1,
            "imo": 1,
            "accountCode": 1,
            "hireRate": 1,
            "scrubber": 1,
            "marketType": 1,
            "consumption_profiles.profileName": 1,
            "consumption_profiles.passageProfile.passageType": 1,
            "consumption_profiles.passageProfile.consumption.speed": 1,
            "consumption_profiles.passageProfile.consumption.ifo": 1,
            "consumption_profiles.passageProfile.consumption.mgo": 1,
            "consumption_profiles.passageProfile.consumption.isDefault": 1,
            "consumption_profiles.nonPassageProfile.consumption": 1,
            "tags.category": 1,
            "tags.value": 1,
            "contract_history.list": 1,
            "isVesselOperating": 1,
            "extracted_at": 1,
        },
        "sql_hints": {
            "finance": "",
            "ops": "",
        },
    },

    "ranking.vessel_metadata": {
        "description": (
            "Fleet-wide vessel metadata listing/ranking from MongoDB `vessels` only. "
            "Use when the user asks about metadata-style vessel attributes across the fleet such as "
            "operating status, scrubber, hire rate, pool tags, market type, default ballast/laden speed, "
            "or current contract duration. "
            "Do NOT answer these using finance aggregates as a proxy."
        ),
        "route": "single",
        "required_slots": [],
        "optional_slots": ["limit"],
        "needs": {"mongo": True, "finance": False, "ops": False},
        "mongo_intent": "vessel.list_all",
        "mongo_projection": {
            "_id": 0,
            "vesselId": 1,
            "imo": 1,
            "name": 1,
            "accountCode": 1,
            "hireRate": 1,
            "scrubber": 1,
            "marketType": 1,
            "consumption_profiles.profileName": 1,
            "consumption_profiles.passageProfile.passageType": 1,
            "consumption_profiles.passageProfile.consumption.speed": 1,
            "consumption_profiles.passageProfile.consumption.isDefault": 1,
            "tags.category": 1,
            "tags.value": 1,
            "contract_history.list": 1,
            "isVesselOperating": 1,
            "extracted_at": 1,
        },
        "sql_hints": {
            "finance": "",
            "ops": "",
        },
    },

    "cargo.details": {
        "description": (
            "Details and specifications for a specific cargo type on a specific voyage. "
            "Requires cargo_type to be identified in the query."
        ),
        "route": "single",
        "required_slots": ["cargo_type"],
        "optional_slots": ["voyage_number", "voyage_id"],
        "needs": {"mongo": False, "finance": False, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": "",
            "ops": "",
        },
    },

    "port.details": {
        "description": (
            "Statistics and information about ONE SPECIFIC named port that the user has explicitly named. "
            "Requires port_name to be present in the query. "
            "Use ONLY for: 'tell me about Rotterdam', 'what happened at Houston', 'Singapore port activity'. "
            "Do NOT use for fleet-wide questions like 'most visited port', 'busiest port', "
            "'which port is visited most', 'most common port', 'most commonly visited port' — "
            "use ranking.ports for ALL fleet-wide port questions."
        ),
        "route": "single",
        "required_slots": ["port_name"],
        "optional_slots": ["date_from", "date_to"],
        "needs": {"mongo": False, "finance": False, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": "",
            "ops": "",
        },
    },

    # ---------------------------------------------------------
    # ANALYSIS QUERIES
    # ---------------------------------------------------------

    "analysis.revenue_vs_pnl": {
        "description": (
            "Fleet-wide analysis of revenue vs PnL correlation across all voyages. "
            "Identifies voyages with high revenue but poor PnL or vice versa. "
            "No specific voyage or vessel required. Needs GROUP BY or full-scan dynamic SQL."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["voyage_number", "voyage_id", "vessel_name", "date_from", "date_to"],
        "needs": {"mongo": True, "finance": True, "ops": True},
        "mongo_intent": "entity.auto",
        "mongo_projection": None,
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance analysis.revenue_vs_pnl):
- Query `finance_voyage_kpi`.
- MUST filter scenario: `scenario = COALESCE(%(scenario)s, 'ACTUAL')`.
- Return voyage-level rows: voyage_id, voyage_number, revenue, total_expense, pnl, tce.
- ORDER BY revenue DESC.
- Keep LIMIT %(limit)s.
""",
            "ops": "",
        },
    },

    "analysis.high_revenue_low_pnl": {
        "description": (
            "Find voyages across the ENTIRE fleet that have high revenue but low or negative PnL. "
            "No specific voyage or vessel known. Needs dynamic SQL with threshold filters. "
            "Use when user asks 'which voyages made money but lost PnL' or similar anomaly detection."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["min_revenue", "max_pnl", "limit", "date_from", "date_to"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance analysis.high_revenue_low_pnl):
- Query `finance_voyage_kpi`.
- MUST filter scenario: `scenario = COALESCE(%(scenario)s, 'ACTUAL')`.
- Filter for high revenue AND low/negative pnl using appropriate thresholds derived from the question.
- Return exact aliases: voyage_id, voyage_number, revenue, total_expense, pnl, tce.
- ORDER BY revenue DESC, pnl ASC.
- Keep LIMIT %(limit)s.
""",
            "ops": "",
        },
    },

    "analysis.cargo_profitability": {
        "description": (
            "Fleet-wide profitability breakdown by cargo grade or cargo type. "
            "Aggregates PnL, revenue, and expenses grouped by cargo across all voyages. "
            "No specific voyage or vessel required. Needs GROUP BY cargo + AVG/SUM dynamic SQL."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["cargo_grade", "cargo_type", "date_from", "date_to", "limit"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance analysis.cargo_profitability):
- Join `finance_voyage_kpi` with `ops_voyage_summary` and unnest grades_json.
- MUST filter scenario: `f.scenario = COALESCE(%(scenario)s, 'ACTUAL')`.
- GROUP BY cargo grade with AVG(pnl), AVG(revenue), COUNT(*) as voyage_count.
- Return exact aliases: cargo_grade, avg_pnl, avg_revenue, voyage_count.
- ORDER BY avg_pnl DESC NULLS LAST.
- Keep LIMIT %(limit)s.
- If the question also asks for common ports or congestion/delay remarks for profitable cargo grades:
  - still GROUP ONLY BY normalized cargo grade,
  - add `string_agg(DISTINCT lower(trim(p->>'port_name')), ', ') AS most_common_ports`,
  - aggregate only congestion/delay-like remarks instead of grouping by raw remarks_json,
  - return one row per cargo grade, never one row per remark payload.
- If the question compares ACTUAL vs WHEN_FIXED or asks for variance/difference between scenarios by cargo grade:
  - include BOTH scenarios in the filtered set instead of ACTUAL-only,
  - return exact aliases: cargo_grade, actual_avg_pnl, when_fixed_avg_pnl, variance_diff,
  - compute `variance_diff` as `ABS(AVG(CASE WHEN f.scenario = 'ACTUAL' THEN f.pnl END) - AVG(CASE WHEN f.scenario = 'WHEN_FIXED' THEN f.pnl END))`,
  - do NOT use statistical `VARIANCE()` / `STDDEV()` functions for this question,
  - ORDER BY variance_diff DESC NULLS LAST.
""",
            "ops": "",
        },
    },

    "analysis.by_module_type": {
        "description": (
            "Fleet-wide breakdown of average PnL, most common cargo grades and ports "
            "grouped by module type (TC Voyage, Spot, etc.). "
            "No specific entity required. Needs GROUP BY module_type dynamic SQL. "
            "Use this when module type is the grouped subject even if cargo grades or ports are requested as output columns."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["limit"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance analysis.by_module_type):
- Join `finance_voyage_kpi` f with `ops_voyage_summary` o on voyage_number and normalized vessel_imo.
- MUST filter scenario: `f.scenario = COALESCE(%(scenario)s, 'ACTUAL')`.
- GROUP BY o.module_type.
- Return exact aliases: module_type, avg_pnl, avg_revenue, voyage_count.
- ORDER BY avg_pnl DESC.
- Keep LIMIT %(limit)s.
""",
            "ops": (
                "For module_type queries, ops should fetch ports_json and grades_json "
                "grouped by module_type using = ANY(%(voyage_ids)s) filter when voyage_ids "
                "are available, otherwise return aggregate port/grade data per module_type."
            ),
        },
    },

    "analysis.segment_performance": {
        "description": (
            "Fleet-wide performance analysis by vessel segment, module type, or route. "
            "Identifies which segments are loss-making or high-performing. "
            "No specific entity required. Needs GROUP BY + aggregate dynamic SQL."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["vessel_name", "imo", "module_type", "segment", "date_from", "date_to", "limit"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "guardrails": {"segment_performance_fallback": True},
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance analysis.segment_performance):
- Join `finance_voyage_kpi` f with `ops_voyage_summary` o on voyage_number and normalized vessel_imo.
- MUST filter scenario: `f.scenario = COALESCE(%(scenario)s, 'ACTUAL')`.
- GROUP BY o.module_type (or segment if available).
- Return exact aliases: module_type, avg_pnl, total_pnl, avg_revenue, voyage_count.
- Include loss-making flag: CASE WHEN avg_pnl < 0 THEN true ELSE false END AS is_loss_making.
- ORDER BY avg_pnl ASC (worst first).
- Keep LIMIT %(limit)s.
""",
            "ops": "",
        },
    },

    "analysis.scenario_comparison": {
        "description": (
            "Compare ACTUAL vs WHEN_FIXED vs BUDGET scenario values for one or more voyages. "
            "Use when the user explicitly mentions scenario names like ACTUAL, WHEN_FIXED, BUDGET, "
            "or uses words like 'compare scenarios', 'variance between scenarios'. "
            "voyage_numbers are optional — if absent, compares scenarios across all voyages."
        ),
        "route": "single",
        "required_slots": [],
        "optional_slots": ["voyage_numbers", "scenario"],
        "needs": {"mongo": False, "finance": True, "ops": False},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "guardrails": {"inject_voyage_numbers_param": True, "verify_scenario_variance_columns": True},
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (analysis.scenario_comparison):
- Query `finance_voyage_kpi` comparing scenario='ACTUAL' vs scenario='WHEN_FIXED'.
- MUST filter: `voyage_number = ANY(%(voyage_numbers)s)`.
- Pre-aggregate each scenario to one row per (voyage_number, vessel_imo_key), then join,
  then aggregate to FINAL one row per voyage_number.
- Avoid many-to-many joins:
  1) CTE actual: one row per (voyage_number, vessel_imo_key) for ACTUAL
  2) CTE when_fixed: one row per (voyage_number, vessel_imo_key) for WHEN_FIXED
  3) JOIN on both keys, then GROUP BY voyage_number
- Return exact aliases:
  voyage_number, pnl_actual, pnl_when_fixed, pnl_variance,
  tce_actual, tce_when_fixed, tce_variance.
- ORDER BY voyage_number.
- Keep LIMIT %(limit)s.
""",
            "ops": "",
        },
    },

    "analysis.cost_breakdown": {
        "description": (
            "Detailed cost structure and expense breakdown analysis. "
            "Can be for a specific voyage/vessel or fleet-wide. "
            "Use when user asks about cost components, expense categories, or cost drivers."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["voyage_number", "vessel_name", "date_from", "date_to"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance analysis.cost_breakdown):
- Query `finance_voyage_kpi`.
- MUST filter scenario: `scenario = COALESCE(%(scenario)s, 'ACTUAL')`.
- Return expense breakdown aliases: voyage_id, voyage_number, revenue, total_expense, pnl,
  and any available cost sub-components.
- Keep LIMIT %(limit)s.
""",
            "ops": "",
        },
    },

    "analysis.variance": {
        "description": (
            "Variance analysis between scenarios (ACTUAL vs WHEN_FIXED) or time periods. "
            "Quantifies the gap between planned and actual values. "
            "Similar to analysis.scenario_comparison but focuses on delta/variance metric."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["voyage_numbers", "scenario", "metric"],
        "needs": {"mongo": False, "finance": True, "ops": False},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance analysis.variance):
- Query `finance_voyage_kpi` comparing ACTUAL vs WHEN_FIXED.
- Return variance metrics: pnl_variance, tce_variance, revenue_variance.
- Keep LIMIT %(limit)s.
""",
            "ops": "",
        },
    },

    "analysis.profitability": {
        "description": (
            "General fleet-wide profitability analysis — which voyages/vessels/routes are most profitable. "
            "No specific entity required. Can be grouped by any dimension. "
            "Needs GROUP BY + aggregate dynamic SQL."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["group_by", "date_from", "date_to", "limit"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance analysis.profitability):
- Query `finance_voyage_kpi`.
- MUST filter scenario: `scenario = COALESCE(%(scenario)s, 'ACTUAL')`.
- Return voyage-level rows: voyage_id, voyage_number, pnl, revenue, total_expense, tce.
- ORDER BY pnl DESC.
- Keep LIMIT %(limit)s.
""",
            "ops": "",
        },
    },

    # ---------------------------------------------------------
    # RANKING QUERIES
    # ---------------------------------------------------------

    "ranking.voyages": {
        "description": (
            "Fleet-wide ranking of ALL voyages by any metric — PnL, revenue, TCE, commission, offhire. "
            "No specific voyage number or vessel name is known or needed. "
            "Use as fallback when a more specific ranking intent does not apply. "
            "Needs ORDER BY + LIMIT dynamic SQL across the full dataset."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["limit", "metric", "date_from", "date_to", "cargo_grade", "port_name"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "guardrails": {"finance_no_ops_join": True, "require_kpi_columns": True},
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance ranking.voyages):
- Query ONLY `finance_voyage_kpi`. Do NOT join ops tables here.
- MUST filter scenario: `scenario = COALESCE(%(scenario)s, 'ACTUAL')`.
- Return exact aliases: voyage_id, voyage_number, pnl, revenue, total_expense, tce, total_commission.
- Rank by pnl DESC unless the question asks otherwise.
- Keep LIMIT %(limit)s.
""",
            "ops": "",
        },
    },

    "ranking.pnl": {
        "description": (
            "Rank voyages or grouped entities by PnL. "
            "Use for requests like 'top 10 voyages by PnL' or 'highest profit voyages'. "
            "For cargo-grade phrasing, allow aggregate grouping by cargo grade."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["limit", "group_by", "metric", "date_from", "date_to"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "guardrails": {"require_kpi_columns": True},
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance ranking.pnl):
- Rank by PnL metric.
- If the question asks by cargo grade, GROUP BY normalized cargo grade and include avg_pnl/avg_revenue/voyage_count.
- For cargo-grade ranking, include most_common_ports using string_agg(DISTINCT port_text, ', ') AS most_common_ports.
- If the question asks by voyage, return voyage-level rows with pnl/revenue/total_expense.
- When ordering by pnl or another numeric KPI, add `NULLS LAST`.
- Keep LIMIT %(limit)s and scenario = COALESCE(%(scenario)s, 'ACTUAL') when relevant.
""",
            "ops": "",
        },
    },

    "ranking.revenue": {
        "description": (
            "Rank voyages or grouped entities by revenue. "
            "Use for requests like 'top voyages by revenue' or 'highest revenue voyages'."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["limit", "group_by", "metric", "date_from", "date_to"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "guardrails": {"require_kpi_columns": True},
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance ranking.revenue):
- Rank by revenue metric.
- When ordering by revenue or another numeric KPI, add `NULLS LAST`.
- Keep LIMIT %(limit)s and scenario = COALESCE(%(scenario)s, 'ACTUAL') when relevant.
""",
            "ops": "",
        },
    },

    "ranking.port_calls": {
        "description": (
            "Rank voyages by number of port calls/port visits/port stops. "
            "Use for requests like 'Show 10 voyages with most port calls'."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["limit", "date_from", "date_to"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "guardrails": {"require_kpi_columns": True},
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance ranking.port_calls):
- Use one row per voyage.
- To count ports use jsonb_array_length(o.ports_json) AS port_count.
- Do NOT explode ports_json with jsonb_array_elements for this intent.
- Join finance_voyage_kpi f with ops_voyage_summary o on voyage_number + normalized vessel_imo.
- Include voyage_id, voyage_number, pnl, revenue, total_expense, tce, total_commission and port_count.
- ORDER BY port_count DESC.
- Keep LIMIT %(limit)s and scenario = COALESCE(%(scenario)s, 'ACTUAL').
""",
            "ops": "",
        },
    },

    "ranking.voyages_by_pnl": {
        "description": (
            "Fleet-wide ranking of ALL voyages ordered by PnL (profit and loss) — highest to lowest. "
            "No specific voyage or vessel needed. "
            "Use for 'top N most profitable voyages', 'worst performing voyages by PnL'. "
            "Needs ORDER BY pnl DESC LIMIT dynamic SQL. "
            "Extra requested columns like vessel name, cargo grades, ports, remarks, margin %, or expense ratio do NOT change the grouped subject away from voyages."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["limit"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "guardrails": {"finance_no_ops_join": True, "require_kpi_columns": True},
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance ranking.voyages_by_pnl):
- Query ONLY `finance_voyage_kpi`. Do NOT join ops tables here.
- MUST filter scenario: `scenario = COALESCE(%(scenario)s, 'ACTUAL')`.
- Return exact aliases: voyage_id, voyage_number, pnl, revenue, total_expense, tce, total_commission.
- ORDER BY pnl DESC.
- Keep LIMIT %(limit)s.
""",
            "ops": "",
        },
    },

    "ranking.voyages_by_revenue": {
        "description": (
            "Fleet-wide ranking of ALL voyages ordered by total revenue — highest to lowest. "
            "No specific voyage or vessel needed. "
            "Use for 'top N voyages by revenue', 'highest earning voyages'. "
            "Needs ORDER BY revenue DESC LIMIT dynamic SQL."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["limit"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "guardrails": {"finance_no_ops_join": True, "require_kpi_columns": True},
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance ranking.voyages_by_revenue):
- Query ONLY `finance_voyage_kpi`. Do NOT join ops tables here.
- MUST filter scenario: `scenario = COALESCE(%(scenario)s, 'ACTUAL')`.
- Return exact aliases: voyage_id, voyage_number, pnl, revenue, total_expense, tce, total_commission.
- ORDER BY revenue DESC.
- Keep LIMIT %(limit)s.
""",
            "ops": "",
        },
    },

    "ranking.voyages_by_commission": {
        "description": (
            "Fleet-wide ranking of ALL voyages ordered by total commission — highest to lowest. "
            "No specific voyage or vessel needed. "
            "Use for 'top voyages by commission', 'which voyages had the highest commission'. "
            "Needs ORDER BY total_commission DESC LIMIT dynamic SQL."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["limit"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "guardrails": {"finance_no_ops_join": True, "require_kpi_columns": True},
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance ranking.voyages_by_commission):
- Query ONLY `finance_voyage_kpi`. Do NOT join ops tables here.
- MUST filter scenario: `scenario = COALESCE(%(scenario)s, 'ACTUAL')`.
- Return exact aliases: voyage_id, voyage_number, pnl, revenue, total_expense, tce, total_commission.
- ORDER BY total_commission DESC.
- Keep LIMIT %(limit)s.
""",
            "ops": "",
        },
    },

    "ranking.vessels": {
        "description": (
            "Fleet-wide aggregate ranking of ALL vessels by any metric — "
            "voyage count, total PnL, average TCE, total revenue, most offhire days. "
            "No specific vessel name or IMO is known or needed. "
            "Use when user asks 'which vessel has the most voyages', "
            "'which vessel earned the highest PnL', 'rank vessels by performance'. "
            "Needs GROUP BY vessel + aggregate dynamic SQL. "
            "Do NOT use when user names a specific vessel — use vessel.summary for that."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["limit", "metric", "date_from", "date_to"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance ranking.vessels):
- MANDATORY: Keep this vessel-level GROUP BY structure and do NOT use a CTE that omits the ops join:
  SELECT
    REPLACE(f.vessel_imo::TEXT, '.0', '') AS vessel_imo,
    MAX(o.vessel_name)                   AS vessel_name,
    COUNT(DISTINCT f.voyage_id)          AS voyage_count,
    AVG(f.pnl)                           AS avg_pnl,
    SUM(f.pnl)                           AS total_pnl
  FROM finance_voyage_kpi f
  JOIN ops_voyage_summary o
    ON REPLACE(f.vessel_imo::TEXT, '.0', '') = REPLACE(o.vessel_imo::TEXT, '.0', '')
  WHERE f.scenario = COALESCE(%(scenario)s, 'ACTUAL')
  GROUP BY REPLACE(f.vessel_imo::TEXT, '.0', '')
  ORDER BY <chosen_metric> DESC NULLS LAST, voyage_count DESC
  LIMIT %(limit)s
- CRITICAL: vessel_name MUST come from MAX(o.vessel_name) — it does NOT exist in finance_voyage_kpi.
- NEVER use a CTE that queries finance_voyage_kpi alone without joining ops_voyage_summary.
- Pick aggregates and aliases based on the question:
  - total profit / total pnl / earned most -> SUM(f.pnl) AS total_pnl
  - average pnl / profitability -> AVG(f.pnl) AS avg_pnl
  - total revenue / earnings -> SUM(f.revenue) AS total_revenue
  - average revenue -> AVG(f.revenue) AS avg_revenue
  - total expense / most expensive -> SUM(f.total_expense) AS total_expense
  - average expense / cost -> AVG(f.total_expense) AS avg_total_expense
  - TCE -> AVG(f.tce) AS avg_tce
- Always include voyage_count. Order by the metric implied by the question, not always by voyage_count.
- When ordering by avg/total KPI columns, add `NULLS LAST`.
""",
            "ops": """
INTENT-SPECIFIC RULES (ops ranking.vessels):
- Query ONLY `ops_voyage_summary`.
- CRITICAL: Use vessel_imo NOT vessel_id. The column vessel_id does NOT exist.
- Available columns include: voyage_id, voyage_number, vessel_imo, vessel_name,
  module_type, offhire_days, is_delayed, ports_json, grades_json.
- GROUP BY vessel_imo, vessel_name.
- Return exact aliases: vessel_imo, vessel_name, COUNT(*) AS voyage_count.
- ORDER BY voyage_count DESC.
- Keep LIMIT %(limit)s.
""",
        },
    },

    "ranking.cargo": {
        "description": (
            "Fleet-wide ranking of ALL cargo grades by profitability, volume, frequency, or margin. "
            "No specific cargo grade known in advance. "
            "Use when user asks 'which cargo grade is most profitable', "
            "'most frequently carried cargo', 'best performing cargo'. "
            "Do NOT use when voyages, vessels, or module types are the main rows and cargo grade is only a requested output column. "
            "Needs GROUP BY cargo_grade + aggregate dynamic SQL with JSONB unnest."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["limit", "metric", "date_from", "date_to"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance ranking.cargo):
- Finance has NO cargo_grade column. DO NOT attempt to query cargo_grade.
- DO NOT generate any finance SQL for this intent — return nothing.
- All cargo grade data lives exclusively in ops_voyage_summary.grades_json (JSONB).
""",
            "ops": """
INTENT-SPECIFIC RULES (ops ranking.cargo):
- CRITICAL: There is NO column named 'cargo_grade'. Cargo grades live in grades_json (JSONB array).
- Use this EXACT query — do not change alias names or add any WHERE filter:
  SELECT
    grade_text        AS cargo_grade,
    COUNT(*)          AS voyage_count
  FROM ops_voyage_summary,
  jsonb_array_elements_text(grades_json) AS grade_text
  GROUP BY grade_text
  ORDER BY voyage_count DESC
  LIMIT %(limit)s
- CRITICAL: The lateral alias is `grade_text` (NOT cargo_grade) to avoid column name conflicts.
- DO NOT add WHERE voyage_id = ANY(...) — this is a fleet-wide aggregate with NO voyage_id filter.
- Return exact output aliases: cargo_grade, voyage_count.
""",
        },
    },

    "ranking.ports": {
        "description": (
            "Fleet-wide ranking of ALL ports across the entire dataset — no specific port name needed. "
            "Use for ANY question asking about ports in aggregate: "
            "'most visited port', 'most commonly visited port', 'busiest port', "
            "'which port appears most', 'most frequent port', 'top ports by visit count', "
            "'which port is visited most across all voyages'. "
            "Needs GROUP BY port + aggregate dynamic SQL using JSONB unnest on ports_json. "
            "Do NOT use port.details — that requires a specific named port."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["limit", "metric", "date_from", "date_to"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance ranking.ports):
- Use `ops_voyage_summary` with `jsonb_array_elements(ports_json)` to rank ports fleet-wide.
- Return exact alias `port_name`.
- If the question asks for most visited / most common / busiest ports:
  - return `COUNT(*) AS finance_voyage_count`
  - ORDER BY finance_voyage_count DESC.
- If the question asks for demurrage / wait time / delay by port:
  - use `AVG(offhire_days) AS avg_offhire_days` as the port-level waiting proxy,
  - ignore rows where port_name is null/blank,
  - ORDER BY avg_offhire_days DESC.
- Do NOT return `avg_voyage_days` or other unrelated aliases for wait-time questions.
- Keep LIMIT %(limit)s.
""",
            "ops": """
INTENT-SPECIFIC RULES (ops ranking.ports):
- CRITICAL: There is NO column named 'port_name' at the row level. Ports live in ports_json (JSONB array).
- Use this EXACT query — do not change alias names or add any WHERE filter:
  SELECT
    port_text         AS port_name,
    COUNT(*)          AS visit_count
  FROM ops_voyage_summary,
  jsonb_array_elements_text(ports_json) AS port_text
  GROUP BY port_text
  ORDER BY visit_count DESC
  LIMIT %(limit)s
- CRITICAL: The lateral alias is `port_text` (NOT port_name) to avoid column name conflicts.
- DO NOT add WHERE voyage_id = ANY(...) — this is a fleet-wide aggregate with NO voyage_id filter.
- Return exact output aliases: port_name, visit_count.
- If the user asks for demurrage / wait time / delay by port, aggregate `AVG(offhire_days) AS avg_offhire_days`
  instead of visit_count and ORDER BY avg_offhire_days DESC.
""",
        },
    },

    "ranking.routes": {
        "description": (
            "Fleet-wide ranking of ALL routes (port pairs / origin-destination combinations) "
            "by profitability, frequency, efficiency, or voyage count. "
            "No specific route known in advance. "
            "Use for 'most profitable route', 'most common trade route', 'busiest route'. "
            "Needs GROUP BY route + aggregate dynamic SQL."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["limit", "metric", "date_from", "date_to"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": "",
            "ops": """
INTENT-SPECIFIC RULES (ops ranking.routes):
- Query `ops_voyage_summary`.
- GROUP BY load_port, discharge_port (or equivalent route columns).
- Return route, voyage_count, avg_pnl when joined with finance.
- ORDER BY voyage_count DESC.
- Keep LIMIT %(limit)s.
""",
        },
    },

    # ---------------------------------------------------------
    # OPERATIONAL QUERIES
    # ---------------------------------------------------------

    "ops.offhire_ranking": {
        "description": (
            "Fleet-wide ranking of voyages by offhire days — most to least. "
            "No specific voyage known. "
            "Use for 'which voyages had most offhire', 'top delayed voyages by offhire duration'. "
            "Needs ORDER BY offhire_days DESC dynamic SQL."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["date_from", "date_to", "limit"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance ops.offhire_ranking):
- Join `ops_voyage_summary` o with `finance_voyage_kpi` f ON o.voyage_id = f.voyage_id.
- MUST filter finance scenario: `f.scenario = COALESCE(%(scenario)s, 'ACTUAL')`.
- DETECT question level from the question text:
  A) If the question asks WHICH VESSEL has most offhire (contains 'vessel', 'ship'):
     GROUP BY vessel, return:
       REPLACE(f.vessel_imo::TEXT, '.0', '') AS vessel_imo,
       MAX(o.vessel_name) AS vessel_name,
       SUM(o.offhire_days) AS total_offhire_days,
       COUNT(DISTINCT f.voyage_id) AS voyage_count,
       AVG(f.pnl) AS avg_pnl
     ORDER BY total_offhire_days DESC
  B) If the question asks about VOYAGES with most offhire (contains 'voyage'):
     Return voyage-level: voyage_id, voyage_number, offhire_days, pnl, tce, revenue, total_expense
     ORDER BY offhire_days DESC
- Keep LIMIT %(limit)s.
""",
            "ops": "",
        },
    },

    "finance.loss_due_to_delay": {
        "description": (
            "Fleet-wide analysis of delayed voyages with negative PnL — financial impact of delays. "
            "Use when user asks about delayed voyages that lost money, root cause of losses due to delays, "
            "or financial impact of operational delays. "
            "Needs dynamic SQL joining ops delay flags with finance PnL."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["date_from", "date_to", "limit"],
        "needs": {"mongo": True, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance finance.loss_due_to_delay):
- Join `finance_voyage_kpi` f with `ops_voyage_summary` o on voyage_number and normalized vessel_imo.
- MUST filter scenario: `f.scenario = COALESCE(%(scenario)s, 'ACTUAL')`.
- Filter for delayed voyages: o.offhire_days > 0 AND f.pnl < 0.
- Return exact aliases: voyage_id, voyage_number, pnl, revenue, total_expense, offhire_days.
- ORDER BY pnl ASC (worst losses first).
- Keep LIMIT %(limit)s.
""",
            "ops": "",
        },
    },

    "ops.voyages_by_port": {
        "description": (
            "Find all voyages that called at a SPECIFIC named port. "
            "Requires a port name to be present in the query. "
            "Use for 'which voyages went to Rotterdam', 'voyages that called at Singapore'. "
            "Do NOT use for ranking ports fleet-wide — use ranking.ports for that."
        ),
        "route": "single",
        "required_slots": ["port_name"],
        "optional_slots": ["date_from", "date_to", "limit"],
        "needs": {"mongo": False, "finance": False, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": "",
            "ops": """
INTENT-SPECIFIC RULES (ops ops.voyages_by_port):
- Query `ops_voyage_summary`.
- Filter using JSONB: ports_json @> to_jsonb(%(port_name)s::text) or jsonb_array_elements_text.
- Return: voyage_id, voyage_number, vessel_name, vessel_imo.
- Keep LIMIT %(limit)s.
""",
        },
    },

    "ops.port_query": {
        "description": (
            "Voyages that visited a SPECIFIC named port with associated finance data. "
            "Requires port_name. Combines ops port visit data with finance KPIs. "
            "Use for 'voyages that visited X with their PnL', 'financial performance of port X calls'. "
            "Do NOT use for fleet-wide port ranking — use ranking.ports for that."
        ),
        "route": "single",
        "required_slots": ["port_name"],
        "optional_slots": ["activity_type", "date_from", "date_to", "limit"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "guardrails": {"inject_filter_port_param": True},
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance ops.port_query):
- Join `ops_voyage_summary` o with `finance_voyage_kpi` f on voyage_number and normalized vessel_imo.
- Filter by port: o.ports_json @> to_jsonb(%(port_name)s::text).
- MUST filter scenario: `f.scenario = COALESCE(%(scenario)s, 'ACTUAL')`.
- Return: voyage_id, voyage_number, vessel_name, pnl, revenue, total_expense.
- Keep LIMIT %(limit)s.
""",
            "ops": "",
        },
    },

    "ops.cargo_movements": {
        "description": (
            "Track cargo loading and discharge activities across voyages. "
            "Can be filtered by cargo type or port. "
            "Use for 'where was cargo X loaded/discharged', 'cargo movement activity'."
        ),
        "route": "single",
        "required_slots": [],
        "optional_slots": ["cargo_type", "port_name", "date_from", "date_to"],
        "needs": {"mongo": False, "finance": False, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": "",
            "ops": "",
        },
    },

    "ops.route_analysis": {
        "description": (
            "Analyze performance and efficiency of vessel routes. "
            "Can include origin/destination filtering. "
            "Use for 'how efficient is the Singapore to Rotterdam route', 'route performance analysis'."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["origin", "destination", "date_from", "date_to"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": "",
            "ops": "",
        },
    },

    "ops.vessel_utilization": {
        "description": (
            "Analyze vessel usage patterns and utilization rates across voyages. "
            "Can be for a specific vessel or fleet-wide. "
            "Use for 'how well utilized is vessel X', 'vessel utilization analysis'."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["vessel_name", "imo", "date_from", "date_to"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": "",
            "ops": "",
        },
    },

    "ops.demurrage": {
        "description": (
            "Analyze demurrage incidents and associated costs across voyages. "
            "Can be filtered by port or threshold. "
            "Use for 'demurrage analysis', 'which voyages had high demurrage costs'."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["port_name", "threshold", "date_from", "date_to", "limit"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": "",
            "ops": "",
        },
    },

    "ops.voyages_by_cargo_grade": {
        "description": (
            "Find voyages that carried a SPECIFIC named cargo grade and rank by PnL. "
            "Requires cargo_grade to be present in the query (e.g. 'NHC', 'VLSFO'). "
            "Use for 'which voyages carried NHC cargo', 'voyages with crude oil cargo grade'. "
            "Do NOT use for fleet-wide cargo ranking — use ranking.cargo for that."
        ),
        "route": "single",
        "required_slots": ["cargo_grade"],
        "optional_slots": ["limit", "scenario"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": "",
            "ops": """
INTENT-SPECIFIC RULES (ops ops.voyages_by_cargo_grade):
- Query `ops_voyage_summary`.
- Filter using JSONB: grades_json @> to_jsonb(%(cargo_grade)s::text).
- Return: voyage_id, voyage_number, vessel_name, vessel_imo.
- Keep LIMIT %(limit)s.
""",
        },
    },

    # ---------------------------------------------------------
    # COMPARISON QUERIES
    # ---------------------------------------------------------

    "comparison.scenario": {
        "description": (
            "Compare ACTUAL vs WHEN_FIXED vs BUDGET scenario values for specific voyages. "
            "Use when the user explicitly mentions scenario names and comparison words. "
            "voyage_numbers optional — without them, compares across all voyages."
        ),
        "route": "single",
        "required_slots": [],
        "optional_slots": ["voyage_number", "voyage_numbers", "scenario"],
        "needs": {"mongo": False, "finance": True, "ops": False},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance comparison.scenario):
- Query `finance_voyage_kpi` comparing ACTUAL vs WHEN_FIXED.
- Return pnl_actual, pnl_when_fixed, pnl_variance, tce_actual, tce_when_fixed, tce_variance.
- Keep LIMIT %(limit)s.
""",
            "ops": "",
        },
    },

    "comparison.voyages": {
        "description": (
            "Side-by-side comparison of MULTIPLE specific voyages by their numbers. "
            "Requires at least two voyage numbers to compare. "
            "Use for 'compare voyage 1901 and 1902', 'how do voyages 1901, 1902, 1903 differ'."
        ),
        "route": "single",
        "required_slots": ["voyage_numbers"],
        "optional_slots": ["metric"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance comparison.voyages):
- Query `finance_voyage_kpi`.
- Filter: voyage_number = ANY(%(voyage_numbers)s).
- MUST filter scenario: `scenario = COALESCE(%(scenario)s, 'ACTUAL')`.
- Return: voyage_id, voyage_number, pnl, revenue, total_expense, tce, total_commission.
- Keep LIMIT %(limit)s.
""",
            "ops": "",
        },
    },

    "comparison.vessels": {
        "description": (
            "Fleet-wide comparison of ALL vessels by performance metrics. "
            "No specific vessel names required — compares the entire fleet. "
            "Use for 'how do vessels compare by PnL', 'vessel performance benchmarking'. "
            "Needs GROUP BY vessel dynamic SQL."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["vessel_names", "metric", "date_from", "date_to"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance comparison.vessels):
- Query `finance_voyage_kpi`.
- MUST filter scenario: `scenario = COALESCE(%(scenario)s, 'ACTUAL')`.
- GROUP BY vessel_imo.
- Return: vessel_imo, avg_pnl, total_pnl, voyage_count.
- Keep LIMIT %(limit)s.
""",
            "ops": "",
        },
    },

    "comparison.periods": {
        "description": (
            "Compare performance across two different time periods fleet-wide. "
            "No specific entity required. "
            "Use for 'compare Q1 vs Q2', 'how did performance change from last year to this year'."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["period1_from", "period1_to", "period2_from", "period2_to"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance comparison.periods):
- Query `finance_voyage_kpi`.
- MUST filter scenario: `scenario = COALESCE(%(scenario)s, 'ACTUAL')`.
- Use CASE WHEN to bucket rows into period1 and period2, then compare avg_pnl, avg_revenue.
- Keep LIMIT %(limit)s.
""",
            "ops": "",
        },
    },

    # ---------------------------------------------------------
    # AGGREGATION QUERIES
    # ---------------------------------------------------------

    "aggregation.count": {
        "description": (
            "Count queries across the ENTIRE fleet — no specific entity needed. "
            "Use for 'how many voyages total', 'how many voyages per vessel', "
            "'how many delayed voyages', 'count of voyages by port'. "
            "Needs COUNT(*) with optional GROUP BY dynamic SQL."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["group_by", "threshold", "date_from", "date_to", "filter"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance aggregation.count):
- MUST filter scenario: `f.scenario = COALESCE(%(scenario)s, 'ACTUAL')`.
- When counting voyages per vessel, use this EXACT structure:
  SELECT
    REPLACE(f.vessel_imo::TEXT, '.0', '') AS vessel_imo,
    MAX(o.vessel_name)                   AS vessel_name,
    COUNT(DISTINCT f.voyage_id)          AS voyage_count
  FROM finance_voyage_kpi f
  JOIN ops_voyage_summary o
    ON REPLACE(f.vessel_imo::TEXT, '.0', '') = REPLACE(o.vessel_imo::TEXT, '.0', '')
  WHERE f.scenario = COALESCE(%(scenario)s, 'ACTUAL')
  GROUP BY REPLACE(f.vessel_imo::TEXT, '.0', '')
  ORDER BY voyage_count DESC
  LIMIT %(limit)s
- CRITICAL: Use exact descriptive aliases (vessel_imo, vessel_name, voyage_count) — NEVER 'group_key'.
- vessel_name MUST come from MAX(o.vessel_name) — it does NOT exist in finance_voyage_kpi.
- Keep LIMIT %(limit)s.
""",
            "ops": "",
        },
    },

    "aggregation.average": {
        "description": (
            "Average/mean calculations across the ENTIRE fleet — no specific entity needed. "
            "Use for 'average PnL per vessel', 'average TCE across all voyages', "
            "'mean revenue by module type'. "
            "Needs AVG() with optional GROUP BY dynamic SQL."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["metric", "group_by", "date_from", "date_to"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance aggregation.average):
- MUST filter scenario: `f.scenario = COALESCE(%(scenario)s, 'ACTUAL')`.
- When grouping by vessel, use this EXACT structure:
  SELECT
    REPLACE(f.vessel_imo::TEXT, '.0', '') AS vessel_imo,
    MAX(o.vessel_name)                   AS vessel_name,
    AVG(f.<metric>)                      AS avg_<metric>
  FROM finance_voyage_kpi f
  JOIN ops_voyage_summary o
    ON REPLACE(f.vessel_imo::TEXT, '.0', '') = REPLACE(o.vessel_imo::TEXT, '.0', '')
  WHERE f.scenario = COALESCE(%(scenario)s, 'ACTUAL')
  GROUP BY REPLACE(f.vessel_imo::TEXT, '.0', '')
  ORDER BY avg_<metric> DESC NULLS LAST
  LIMIT %(limit)s
- CRITICAL: Replace <metric> with the actual column (tce, pnl, revenue, total_expense).
- CRITICAL: Use exact descriptive aliases (avg_tce, avg_pnl etc.) — NEVER use 'group_key' or 'avg_value'.
- vessel_name MUST come from MAX(o.vessel_name) — it does NOT exist in finance_voyage_kpi.
- When ordering by avg_<metric>, add `NULLS LAST`.
- Keep LIMIT %(limit)s.
""",
            "ops": "",
        },
    },

    "aggregation.total": {
        "description": (
            "Sum/total calculations across the ENTIRE fleet — no specific entity needed. "
            "Use for 'total revenue across all voyages', 'total offhire days per vessel', "
            "'sum of expenses by cargo type'. "
            "Needs SUM() with optional GROUP BY dynamic SQL."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["metric", "group_by", "date_from", "date_to"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance aggregation.total):
- MUST filter scenario: `scenario = COALESCE(%(scenario)s, 'ACTUAL')`.
- Use SUM() on the metric implied by the question.
- GROUP BY the dimension implied by the question if any.
- Return: group_key (if grouped), total_value.
- Keep LIMIT %(limit)s.
""",
            "ops": "",
        },
    },

    "aggregation.trends": {
        "description": (
            "Trend analysis over time across the ENTIRE fleet — no specific entity needed. "
            "Use for 'how has PnL trended over the last 6 months', "
            "'revenue trend by quarter', 'are delays increasing over time'. "
            "Needs time-bucketed GROUP BY dynamic SQL."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["metric", "period", "date_from", "date_to"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance aggregation.trends):
- MUST filter scenario: `scenario = COALESCE(%(scenario)s, 'ACTUAL')`.
- Use DATE_TRUNC('month', voyage_start_date) or DATE_TRUNC('quarter', ...) to bucket by time.
- GROUP BY time bucket.
- Return: time_bucket, avg_pnl (or relevant metric), voyage_count.
- ORDER BY time_bucket ASC.
- Keep LIMIT %(limit)s.
""",
            "ops": "",
        },
    },

    # ---------------------------------------------------------
    # TEMPORAL QUERIES
    # ---------------------------------------------------------

    "temporal.period": {
        "description": (
            "Performance analysis scoped to a specific time period — monthly, quarterly, yearly. "
            "No specific entity required. "
            "Use for 'Q1 performance', 'voyages in January 2024', 'last quarter results'."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["period", "date_from", "date_to", "metric"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance temporal.period):
- MUST filter scenario: `scenario = COALESCE(%(scenario)s, 'ACTUAL')`.
- Filter by date range: voyage_start_date BETWEEN %(date_from)s AND %(date_to)s.
- Return: voyage_id, voyage_number, pnl, revenue, total_expense, voyage_start_date.
- Keep LIMIT %(limit)s.
""",
            "ops": "",
        },
    },

    "temporal.trend": {
        "description": (
            "Trend detection over time — increasing or decreasing patterns across the fleet. "
            "No specific entity required. "
            "Use for 'is TCE improving over time', 'PnL trend analysis', 'are delays worsening'."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": ["metric", "date_from", "date_to"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance temporal.trend):
- MUST filter scenario: `scenario = COALESCE(%(scenario)s, 'ACTUAL')`.
- Use DATE_TRUNC to bucket by month or quarter.
- GROUP BY time bucket, return avg_metric and voyage_count.
- ORDER BY time bucket ASC.
- Keep LIMIT %(limit)s.
""",
            "ops": "",
        },
    },

    # ---------------------------------------------------------
    # COMPOSITE & FALLBACK
    # ---------------------------------------------------------

    "composite.query": {
        "description": (
            "Multi-step complex query requiring multiple agents and data synthesis. "
            "Use ONLY as a last resort when no other intent fits. "
            "Prefer specific ranking.*, aggregation.*, or analysis.* intents over this."
        ),
        "route": "composite",
        "required_slots": [],
        "optional_slots": [],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": """
INTENT-SPECIFIC RULES (finance composite.query):
- Return voyage-level KPI rows with stable aliases:
  voyage_id, voyage_number, revenue, total_expense, pnl, tce, total_commission.
- Keep LIMIT %(limit)s.
""",
            "ops": "",
        },
    },

    "followup.result_set": {
        "description": (
            "Answer a follow-up question about the PREVIOUS multi-row result set in this session. "
            "Use ONLY when the user explicitly refers back to prior results — "
            "'among these', 'from the above list', 'those voyages', 'in that list'. "
            "Do NOT use for new questions that happen to contain the word 'these' or 'those'."
        ),
        "route": "single",
        "required_slots": [],
        "optional_slots": ["action", "metric", "direction", "voyage_number"],
        "needs": {"mongo": False, "finance": False, "ops": False},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": "",
            "ops": "",
        },
    },

    "out_of_scope": {
        "description": (
            "Query is outside system capabilities — weather, news, general knowledge, "
            "or anything unrelated to maritime voyage finance and operations. "
            "Use ONLY when no other intent applies at all."
        ),
        "route": "single",
        "required_slots": [],
        "optional_slots": [],
        "needs": {"mongo": False, "finance": False, "ops": False},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
        "sql_hints": {
            "finance": "",
            "ops": "",
        },
    },
}


# =========================================================
# Intent aliases
# =========================================================

# INTENT_ALIASES — maps legacy, shorthand, or natural language variants
# to canonical intent keys. These exist for backward compatibility and
# to handle common user phrasings without requiring exact intent key matches.
# Long-form English aliases (e.g. full sentence keys) should be kept minimal
# and are candidates for replacement by LLM normalisation in future.
INTENT_ALIASES = {
    # Analysis aliases
    "analysis.highrevenuelowpnl":               "analysis.high_revenue_low_pnl",
    "analysis.high_revenue_low_pnl":             "analysis.high_revenue_low_pnl",
    "analysis.cargoprofitability":               "analysis.cargo_profitability",
    "analysis.bymoduletype":                     "analysis.by_module_type",
    "analysis.segmentperformance":               "analysis.segment_performance",
    "analysis.revenueVsPnl":                     "analysis.revenue_vs_pnl",
    "analysis.revenuevspnl":                     "analysis.revenue_vs_pnl",
    "analysis.scenariocomparison":               "analysis.scenario_comparison",
    "analysis.costbreakdown":                    "analysis.cost_breakdown",

    # Ranking aliases
    "ranking.cargogrades":                       "ranking.cargo",
    "ranking.cargograde":                        "ranking.cargo",
    "ranking.voyageprofitabilityandports":       "ranking.voyages_by_pnl",
    "vessel.profitability":                      "ranking.vessels",
    "voyage.profitability":                      "ranking.voyages_by_pnl",

    # Ops aliases
    "ops.portquery":                             "ops.port_query",
    "ops.delayedvoyages":                        "ops.offhire_ranking",
    "ops.delayed_voyages":                       "ops.offhire_ranking",
    "delayed voyages with negative pnl":         "finance.loss_due_to_delay",
    "ops.voyagesbyport":                         "ops.voyages_by_port",

    # Aggregation aliases
    "aggregation.moduletype":                    "aggregation.average",
    "modulepnlcargogradesports":                 "aggregation.average",
}


# =========================================================
# resolve_intent() - Safe lookup with alias fallback
# =========================================================

def resolve_intent(intent_key: str) -> str:
    """
    Resolve an intent key to its canonical form.

    1. Direct match in INTENT_REGISTRY → return as-is
    2. Check INTENT_ALIASES → return canonical key
    3. Try lowercase variants
    4. Return original (caller handles unknown as out_of_scope)
    """
    if not intent_key:
        return "out_of_scope"

    # 1. Direct match
    if intent_key in INTENT_REGISTRY:
        return intent_key

    # 2. Alias match (exact)
    resolved = INTENT_ALIASES.get(intent_key)
    if resolved and resolved in INTENT_REGISTRY:
        return resolved

    # 3. Lowercase match
    lower = intent_key.lower()
    if lower in INTENT_REGISTRY:
        return lower

    # 4. Lowercase alias match
    resolved_lower = INTENT_ALIASES.get(lower)
    if resolved_lower and resolved_lower in INTENT_REGISTRY:
        return resolved_lower

    # 5. Fallback
    return intent_key