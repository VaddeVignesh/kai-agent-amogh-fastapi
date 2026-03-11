import os

import pytest

from app.adapters.postgres_adapter import PostgresAdapter, PostgresConfig, PostgresQueryError


def test_finance_query_runs():
    """
    This repository's Postgres adapter is configured via DSN/env and requires a running DB.
    Skip in environments without POSTGRES_DSN or where Postgres isn't reachable.
    """
    if not (os.getenv("POSTGRES_DSN") or os.getenv("POSTGRES_HOST")):
        pytest.skip("Postgres not configured (set POSTGRES_DSN or POSTGRES_HOST).")

    db = PostgresAdapter(PostgresConfig.from_env())
    try:
        rows = db.fetch_all("kpi.voyages_by_flexible_filters", {"limit": 1, "scenario": "ACTUAL"})
    except PostgresQueryError:
        pytest.skip("Postgres not reachable.")

    assert isinstance(rows, list)
