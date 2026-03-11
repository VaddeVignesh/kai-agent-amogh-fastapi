# app/registries/intent_registry.py
"""
Intent Registry - Maps user intents to data requirements and agent actions.
Includes INTENT_ALIASES for LLM variants and resolve_intent() for safe lookup.
Ranking intents split by metric (pnl, revenue, commission).
"""


# =========================================================
# Supported Intents - COMPLETE LIST
# =========================================================

SUPPORTED_INTENTS = [
    # Entity queries
    "voyage.summary",
    "voyage.entity",
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
    # Session follow-ups over previous result sets
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
        "description": "Get complete voyage summary including finance, ops, and metadata",
        "required_slots": ["voyage_number"],
        "optional_slots": ["voyage_id", "scenario"],
        "needs": {"mongo": True, "finance": True, "ops": True},
        "mongo_intent": "entity.voyage",
        "mongo_projection": None,
    },

    "voyage.entity": {
        "description": "Find voyages by port, vessel, date, or other filter (no specific voyage number)",
        "required_slots": [],
        "optional_slots": ["voyage_number", "voyage_id", "port_name", "vessel_name"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "vessel.summary": {
        "description": "Get complete vessel summary including finance, ops, and metadata",
        "required_slots": [],
        "optional_slots": ["vessel_name", "imo"],
        "needs": {"mongo": True, "finance": True, "ops": True},
        "mongo_intent": "entity.vessel",
        "mongo_projection": None,
    },

    "vessel.entity": {
        "description": "Get vessel entity metadata only",
        "required_slots": [],
        "optional_slots": ["vessel_name", "imo"],
        "needs": {"mongo": True, "finance": False, "ops": False},
        "mongo_intent": "entity.vessel",
        "mongo_projection": None,
    },

    "vessel.metadata": {
        "description": "Get vessel technical/commercial metadata from Mongo only",
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
    },

    "cargo.details": {
        "description": "Get cargo details and specifications",
        "required_slots": ["cargo_type"],
        "optional_slots": ["voyage_number", "voyage_id"],
        "needs": {"mongo": False, "finance": False, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "port.details": {
        "description": "Get port information and statistics",
        "required_slots": ["port_name"],
        "optional_slots": ["date_from", "date_to"],
        "needs": {"mongo": False, "finance": False, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    # ---------------------------------------------------------
    # ANALYSIS QUERIES
    # ---------------------------------------------------------

    "analysis.revenue_vs_pnl": {
        "description": "Analyze revenue vs PnL correlation for voyages",
        "required_slots": [],
        "optional_slots": ["voyage_number", "voyage_id", "vessel_name", "date_from", "date_to"],
        "needs": {"mongo": True, "finance": True, "ops": True},
        "mongo_intent": "entity.auto",
        "mongo_projection": None,
    },

    "analysis.high_revenue_low_pnl": {
        "description": "Find voyages with high revenue but low or negative PnL",
        "required_slots": [],
        "optional_slots": ["min_revenue", "max_pnl", "limit", "date_from", "date_to"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "analysis.cargo_profitability": {
        "description": "Analyze profitability by cargo grade or type",
        "required_slots": [],
        "optional_slots": ["cargo_grade", "cargo_type", "date_from", "date_to", "limit"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "analysis.by_module_type": {
        "description": "Average PnL and most common cargo grades/ports per module type (TC Voyage, Spot, etc.)",
        "required_slots": [],
        "optional_slots": ["limit"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "analysis.segment_performance": {
        "description": "Analyze performance by vessel segment, module type, or route",
        "required_slots": [],
        "optional_slots": ["vessel_name", "imo", "module_type", "segment", "date_from", "date_to", "limit"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "analysis.scenario_comparison": {
        "description": "Compare ACTUAL vs WHEN_FIXED vs BUDGET scenarios",
        "required_slots": [],
        "optional_slots": ["voyage_numbers", "scenario"],
        "needs": {"mongo": False, "finance": True, "ops": False},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "analysis.cost_breakdown": {
        "description": "Analyze cost structure and expense breakdown",
        "required_slots": [],
        "optional_slots": ["voyage_number", "vessel_name", "date_from", "date_to"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "analysis.variance": {
        "description": "Variance analysis between scenarios or periods",
        "required_slots": [],
        "optional_slots": ["voyage_numbers", "scenario", "metric"],
        "needs": {"mongo": False, "finance": True, "ops": False},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "analysis.profitability": {
        "description": "General profitability analysis",
        "required_slots": [],
        "optional_slots": ["group_by", "date_from", "date_to", "limit"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    # ---------------------------------------------------------
    # RANKING QUERIES
    # ---------------------------------------------------------

    "ranking.voyages": {
        "description": "General fallback for ranking voyages",
        "required_slots": [],
        "optional_slots": ["limit", "metric", "date_from", "date_to", "cargo_grade", "port_name"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "ranking.voyages_by_pnl": {
        "description": "Rank voyages by profit and loss (PNL)",
        "required_slots": [],
        "optional_slots": ["limit"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "ranking.voyages_by_revenue": {
        "description": "Rank voyages by total revenue",
        "required_slots": [],
        "optional_slots": ["limit"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "ranking.voyages_by_commission": {
        "description": "Rank voyages by total commission",
        "required_slots": [],
        "optional_slots": ["limit"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "ranking.vessels": {
        "description": "Rank/compare vessels by profitability, utilization, voyage count, or performance",
        "required_slots": [],
        "optional_slots": ["limit", "metric", "date_from", "date_to"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "ranking.cargo": {
        "description": "Rank/compare cargo grades by profitability, volume, frequency, or margin",
        "required_slots": [],
        "optional_slots": ["limit", "metric", "date_from", "date_to"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "ranking.ports": {
        "description": "Rank/compare ports by visit frequency, delays, costs, or profitability",
        "required_slots": [],
        "optional_slots": ["limit", "metric", "date_from", "date_to"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "ranking.routes": {
        "description": "Rank/compare routes by profitability, frequency, efficiency, or distance",
        "required_slots": [],
        "optional_slots": ["limit", "metric", "date_from", "date_to"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    # ---------------------------------------------------------
    # OPERATIONAL QUERIES
    # ---------------------------------------------------------

    "ops.offhire_ranking": {
        "description": "Rank voyages by offhire days",
        "required_slots": [],
        "optional_slots": ["date_from", "date_to", "limit"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },
    
    "finance.loss_due_to_delay": {
        "description": "Analyze delayed voyages with financial impact and root cause",
        "required_slots": [],
        "optional_slots": ["date_from", "date_to", "limit"],
        "needs": {"mongo": True, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "ops.voyages_by_port": {
        "description": "Get voyages that called at a specific port",
        "required_slots": ["port_name"],
        "optional_slots": ["date_from", "date_to", "limit"],
        "needs": {"mongo": False, "finance": False, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "ops.port_query": {
        "description": "Which voyages visited/called at specific port(s), with finance data",
        "required_slots": ["port_name"],
        "optional_slots": ["activity_type", "date_from", "date_to", "limit"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "ops.cargo_movements": {
        "description": "Track cargo loading/discharge activities",
        "required_slots": [],
        "optional_slots": ["cargo_type", "port_name", "date_from", "date_to"],
        "needs": {"mongo": False, "finance": False, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "ops.route_analysis": {
        "description": "Analyze route performance and efficiency",
        "required_slots": [],
        "optional_slots": ["origin", "destination", "date_from", "date_to"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "ops.vessel_utilization": {
        "description": "Analyze vessel usage patterns and utilization rates",
        "required_slots": [],
        "optional_slots": ["vessel_name", "imo", "date_from", "date_to"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "ops.demurrage": {
        "description": "Analyze demurrage incidents and costs",
        "required_slots": [],
        "optional_slots": ["port_name", "threshold", "date_from", "date_to", "limit"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "ops.voyages_by_cargo_grade": {
        "description": "Find voyages that carried a specific cargo grade (and rank by PnL)",
        "required_slots": ["cargo_grade"],
        "optional_slots": ["limit", "scenario"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    # ---------------------------------------------------------
    # COMPARISON QUERIES
    # ---------------------------------------------------------

    "comparison.scenario": {
        "description": "Compare ACTUAL vs WHEN_FIXED vs BUDGET scenarios for specific voyages",
        "required_slots": [],
        "optional_slots": ["voyage_number", "voyage_numbers", "scenario"],
        "needs": {"mongo": False, "finance": True, "ops": False},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "comparison.voyages": {
        "description": "Compare multiple voyages side-by-side",
        "required_slots": ["voyage_numbers"],
        "optional_slots": ["metric"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "comparison.vessels": {
        "description": "Compare vessel performance",
        "required_slots": [],
        "optional_slots": ["vessel_names", "metric", "date_from", "date_to"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "comparison.periods": {
        "description": "Compare performance across time periods",
        "required_slots": [],
        "optional_slots": ["period1_from", "period1_to", "period2_from", "period2_to"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    # ---------------------------------------------------------
    # AGGREGATION QUERIES
    # ---------------------------------------------------------

    "aggregation.count": {
        "description": "Count queries - how many voyages, vessels, ports, etc.",
        "required_slots": [],
        "optional_slots": ["group_by", "threshold", "date_from", "date_to", "filter"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "aggregation.average": {
        "description": "Average/sum/aggregation queries - average PNL, total revenue, etc.",
        "required_slots": [],
        "optional_slots": ["metric", "group_by", "date_from", "date_to"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "aggregation.total": {
        "description": "Total/sum calculations across entities",
        "required_slots": [],
        "optional_slots": ["metric", "group_by", "date_from", "date_to"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "aggregation.trends": {
        "description": "Trend analysis over time periods",
        "required_slots": [],
        "optional_slots": ["metric", "period", "date_from", "date_to"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    # ---------------------------------------------------------
    # TEMPORAL QUERIES
    # ---------------------------------------------------------

    "temporal.period": {
        "description": "Specific time period analysis (monthly, quarterly, yearly)",
        "required_slots": [],
        "optional_slots": ["period", "date_from", "date_to", "metric"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "temporal.trend": {
        "description": "Trends over time (increasing, decreasing patterns)",
        "required_slots": [],
        "optional_slots": ["metric", "date_from", "date_to"],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    # ---------------------------------------------------------
    # COMPOSITE & FALLBACK
    # ---------------------------------------------------------

    "composite.query": {
        "description": "Multi-step complex queries requiring multiple agents and data synthesis",
        "required_slots": [],
        "optional_slots": [],
        "needs": {"mongo": False, "finance": True, "ops": True},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "followup.result_set": {
        "description": "Answer follow-up questions about the previous multi-row result set (e.g., 'among these', 'from above')",
        "required_slots": [],
        "optional_slots": ["action", "metric", "direction", "voyage_number"],
        "needs": {"mongo": False, "finance": False, "ops": False},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },

    "out_of_scope": {
        "description": "Query is outside system capabilities",
        "required_slots": [],
        "optional_slots": [],
        "needs": {"mongo": False, "finance": False, "ops": False},
        "mongo_intent": "entity.skip",
        "mongo_projection": None,
    },
}


# =========================================================
# Intent aliases
# Handles camelCase / no-underscore variants the LLM returns
# =========================================================

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
    "ranking.voyageprofitabilityandports":       "ranking.voyages_by_pnl", # Default mapped to PNL
    "vessel.profitability":                      "ranking.vessels",
    "voyage.profitability":                      "ranking.voyages_by_pnl",
    "ranking.voyages":                           "ranking.voyages_by_pnl", # Fallback mapping

    # Ops aliases
    "ops.portquery":                             "ops.port_query",
    "ops.delayedvoyages":                        "ops.offhire_ranking",
    "ops.delayed_voyages":                       "ops.offhire_ranking",
    "delayed voyages with negative pnl":         "finance.loss_due_to_delay",
    "ops.voyagesbyport":                         "ops.voyages_by_port",

    # Aggregation aliases
    "aggregation.moduletype":                    "aggregation.average",
    "modulepnlcargogradesports":                 "aggregation.average",

    # Vessel / voyage aliases
    "vessel.entity":                             "vessel.summary",
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
    4. Return original (caller handles out_of_scope)

    Usage:
        canonical = resolve_intent(raw_intent_from_llm)
        cfg = INTENT_REGISTRY.get(canonical, INTENT_REGISTRY["out_of_scope"])
    """
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

    # 5. Fallback - return original (will hit out_of_scope)
    return intent_key