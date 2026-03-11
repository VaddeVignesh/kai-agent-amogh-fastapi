from app.adapters.postgres_adapter import PostgresAdapter
from app.registries.sql_registry import SQL_REGISTRY


class KPIAgent:
    def __init__(self):
        self.db = PostgresAdapter()

    def top_voyages_by_pnl(self, limit=10):
        query = """
        SELECT
            voyage_id,
            pnl,
            tce,
            days
        FROM finance.finance_voyage_results
        WHERE pnl IS NOT NULL
        ORDER BY pnl DESC
        LIMIT %s
        """
        return self.db.fetch_all(query, (limit,))

    def top_vessels_by_avg_tce(self, limit=10):
        query = """
        SELECT
            v.vessel_name,
            AVG(r.tce::NUMERIC) AS avg_tce,
            COUNT(*) AS voyage_count
        FROM finance.finance_voyage_results r
        JOIN ops.ops_voyages v
            ON r.voyage_id = v.voyage_id
        WHERE r.tce IS NOT NULL
        GROUP BY v.vessel_name
        ORDER BY avg_tce DESC
        LIMIT %s
        """
        return self.db.fetch_all(query, (limit,))

    def worst_voyages_by_pnl(self, limit=10):
        query = """
        SELECT
            voyage_id,
            pnl,
            tce,
            days
        FROM finance.finance_voyage_results
        WHERE pnl IS NOT NULL
        ORDER BY pnl ASC
        LIMIT %s
        """
        return self.db.fetch_all(query, (limit,))
