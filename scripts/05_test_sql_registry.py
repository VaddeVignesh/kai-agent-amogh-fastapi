from app.adapters.postgres_adapter import PostgresAdapter
from app.registries.sql_registry import SQL_REGISTRY
from app.adapters.postgres_registry_adapter import PostgresRegistryAdapter

db = PostgresRegistryAdapter()

# Finance query
finance = db.run(
    "finance.top_voyages_by_pnl",
    {"limit": 3}
)

print("Top finance voyages:")
for row in finance:
    print(row)

# Ops query
ops = db.run(
    "ops.voyage_summary",
    {"voyage_id": finance[0]["voyage_id"]}
)

print("\nOps summary:")
print(ops[0])

db.close()
