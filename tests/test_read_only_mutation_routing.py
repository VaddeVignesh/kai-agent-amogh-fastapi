"""Read-only guard: delete/modify requests must not route to voyage.summary."""

from app.config.routing_rules_loader import is_read_only_mutation_request


def test_delete_voyage_rows_is_mutation():
    assert is_read_only_mutation_request("can you delete the rows of voyage 2204?")


def test_financial_summary_is_not_mutation():
    assert not is_read_only_mutation_request(
        "For Stena Conquest voyage 1901, give me the financial summary with revenue and PnL"
    )


def test_high_revenue_query_is_not_mutation():
    assert not is_read_only_mutation_request(
        "Which voyages had high revenue but low or negative PnL?"
    )
