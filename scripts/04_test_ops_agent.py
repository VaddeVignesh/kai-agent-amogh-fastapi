from app.agents.ops_agent import OpsAgent
from app.adapters.postgres_adapter import PostgresAdapter
from app.registries.sql_registry import SQL_REGISTRY


agent = OpsAgent()

sample_voyage = agent.db.fetch_one(
    "SELECT voyage_id FROM ops.ops_voyages LIMIT 1"
)["voyage_id"]

print("Voyage ID:", sample_voyage)

print("\nVoyage summary:")
print(agent.get_voyage_summary(sample_voyage))

print("\nFixtures:")
print(agent.get_fixtures(sample_voyage)[:2])  # print first 2

print("\nPorts:")
print(agent.get_ports(sample_voyage)[:3])

print("\nGrades:")
print(agent.get_grades(sample_voyage))

agent.close()
