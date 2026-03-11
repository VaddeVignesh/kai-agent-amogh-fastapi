from app.adapters.postgres_adapter import PostgresAdapter, PostgresConfig
from app.agents.finance_agent import FinanceAgent
from app.agents.ops_agent import OpsAgent

# ---- CHANGE ONLY IF YOUR PASSWORD/DB IS DIFFERENT
cfg = PostgresConfig(
    dsn="postgresql://admin:admin123@localhost:5432/pocdb"
)

db = PostgresAdapter(cfg)

finance = FinanceAgent(db)
ops = OpsAgent(db)

print("\n--- Finance: Top voyages by PnL ---")
rows = finance.top_voyages_by_pnl(
    date_from="2021-01-01",
    date_to="2026-01-01",
    limit=5
)
for r in rows:
    print(r)

print("\n--- Ops: Delayed voyages ---")
rows = ops.delayed_voyages(
    date_from="2021-01-01",
    date_to="2026-01-01",
    limit=5
)
for r in rows:
    print(r)

print("\n--- Finance: High revenue but low/negative PnL ---")
rows = db.fetch_all(
    "finance.high_revenue_low_pnl",
    {
        "date_from": "2022-01-01",
        "date_to": "2025-12-31",
        "limit": 5,
    }
)
for r in rows:
    print(r)


db.close()
