"""Guards against treating fresh corpus queries as last_result_set follow-ups."""

from __future__ import annotations

import pytest

from app.orchestration.graph_router import GraphRouter

# Representative “full question” corpus (admin/customer benchmarks); must not be treated
# as refinements over an unrelated prior table just because last_result_set exists.
BENCHMARK_QUERIES = [
    "For voyage 1901, give me the financial summary with revenue, expenses, and PnL, the main ports involved, and any voyage remarks explaining delays or issues.",
    "Show me the top 5 most profitable voyages and for each one list the cargo grade, key ports visited, and any remarks that explain why performance was high.",
    "Which voyages had high revenue but low or negative PnL, and what were the main cost drivers and operational issues mentioned in remarks?",
    "For vessel Stena Conquest, show me its voyage profitability over time and the most frequent ports it operates in.",
    "Compare the actual results versus when-fixed results for voyages 1901, 1902, and 2301, and show me the variance in PnL and TCE.",
    "Which cargo grades are most profitable overall, and for those grades what are the most common ports and any recurring remarks about congestion or delays?",
    "Show me the top 3 voyages with the highest total commission, including commission type, and the route with remarks.",
    "For each module type like TC Voyage or Spot, what is the average PnL and what are the most common cargo grades and ports?",
    "Find vessels that have high voyage count and above-average profitability, then show their most common cargo grades.",
    "For voyage 2306, generate an executive summary covering what happened operationally with ports and cargo, what it earned financially, and what remarks were recorded.",
    "Show me the 10 voyages with the most port calls, compare their profitability, and extract any remarks indicating execution issues.",
    "For voyages that visited Singapore, rank them by PnL, list the cargo grades moved, and include any remarks about congestion or waiting.",
    "Which voyages had the most offhire days, what was the financial impact on their PnL and TCE, and what were the reasons mentioned in remarks?",
    "Show me the top 5 loss-making voyages with their revenue and expense breakdown, the operational route and activities, and remarks explaining what went wrong.",
    "For delayed voyages with negative PnL, show me the offhire days, delay reasons, expense breakdown, and any remarks that explain the root cause.",
    "For voyage 2305, what went wrong (if anything)? Use remarks to explain, and include a short financial snapshot.",
    "Give me a concise incident-style summary for voyage 2204: delays/offhire, cost drivers, and remarks.",
    "Is Stena Superior doing well or poorly overall? Explain briefly, then show best vs worst voyage and why (from data/remarks)",
]


def test_corpus_fresh_guard_flags_long_prefixed_query() -> None:
    ul = (
        "show me the top 3 voyages with the highest total commission, including commission type, "
        "and the route with remarks."
    )
    _, corpus = GraphRouter._corpus_fresh_guard_flags(ul)
    assert corpus is True


def test_corpus_fresh_guard_top_n_short_query() -> None:
    _, corpus = GraphRouter._corpus_fresh_guard_flags("show me top 3 voyages")
    assert corpus is True


def test_classify_turn_type_long_show_me_is_new_question_with_stale_result_set() -> None:
    ctx: dict = {"last_result_set": {"rows": [{"module_type": "Spot", "avg_pnl": 1.0}]}}
    q = (
        "Show me the top 3 voyages with the highest total commission, including commission type, "
        "and the route with remarks."
    )
    assert GraphRouter._classify_turn_type(session_ctx=ctx, user_input=q) == "new_question"


def test_parse_result_set_followup_returns_none_for_long_prefixed_query() -> None:
    ctx: dict = {"last_result_set": {"rows": [{"pnl": 1.0}]}}
    q = (
        "Show me the top 3 voyages with the highest total commission, including commission type, "
        "and the route with remarks."
    )
    assert GraphRouter._parse_result_set_followup_action(user_input=q, session_ctx=ctx) is None


def test_generic_followup_marker_including_does_not_hit_include() -> None:
    assert GraphRouter._generic_followup_markers_hit("including remarks about delays") is False


def test_generic_followup_marker_standalone_include_hits() -> None:
    assert GraphRouter._generic_followup_markers_hit("include delays in the table") is True


@pytest.mark.parametrize("query", BENCHMARK_QUERIES, ids=[f"bench{i + 1}" for i in range(len(BENCHMARK_QUERIES))])
def test_benchmark_queries_stay_new_question_with_stale_result_set(query: str) -> None:
    ctx: dict = {
        "last_result_set": {
            "rows": [{"module_type": "TC Voyage", "avg_pnl": 1.0, "pnl": 100.0, "voyage_count": 10}],
        },
    }
    assert GraphRouter._classify_turn_type(session_ctx=ctx, user_input=query) == "new_question"
