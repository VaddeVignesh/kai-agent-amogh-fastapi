import psycopg2
from app.adapters.postgres_adapter import PostgresAdapter
from app.registries.sql_registry import SQL_REGISTRY

# Create connection to Postgres
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="pocdb",
    user="admin",
    password="admin123"
)

cur = conn.cursor()

# Finance check
cur.execute("SELECT COUNT(*) FROM finance.finance_voyage_results;")
print("Finance voyages:", cur.fetchone()[0])

# Ops check
cur.execute("SELECT COUNT(*) FROM ops.ops_voyages;")
print("Ops voyages:", cur.fetchone()[0])

conn.close()
