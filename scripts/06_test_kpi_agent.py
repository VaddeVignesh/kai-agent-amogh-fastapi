from app.agents.kpi_agent import KPIAgent

agent = KPIAgent()

print("Top 5 voyages by PnL:")
for row in agent.top_voyages_by_pnl(limit=5):
    print(row)

print("\nTop 5 vessels by avg TCE:")
for row in agent.top_vessels_by_avg_tce(limit=5):
    print(row)

print("\nWorst 5 voyages by PnL:")
for row in agent.worst_voyages_by_pnl(limit=5):
    print(row)
