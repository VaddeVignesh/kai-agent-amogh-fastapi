from app.adapters.postgres_adapter import PostgresAdapter
from app.registries.sql_registry import SQL_REGISTRY


db = PostgresAdapter()

finance_count = db.fetch_one(
    "SELECT COUNT(*) AS cnt FROM finance.finance_voyage_results"
)
ops_count = db.fetch_one(
    "SELECT COUNT(*) AS cnt FROM ops.ops_voyages"
)

print("Finance voyage rows:", finance_count["cnt"])
print("Ops voyage rows:", ops_count["cnt"])

db.close()
