from app.agents.finance_agent import FinanceAgent
from app.adapters.postgres_adapter import PostgresAdapter
from app.registries.sql_registry import SQL_REGISTRY

agent = FinanceAgent()

# pick any voyage_id from your table
sample_voyage = agent.db.fetch_one(
    "SELECT voyage_id FROM finance.finance_voyage_results LIMIT 1"
)["voyage_id"]

print("Voyage ID:", sample_voyage)

print("\nPNL:")
print(agent.get_voyage_pnl(sample_voyage))

print("\nFixture revenue:")
print(agent.get_fixture_revenue(sample_voyage))

agent.close()
