from pymongo import MongoClient
import os

from app.orchestration.router import Router
from app.adapters.redis_store import RedisStore, RedisConfig
from app.adapters.mongo_adapter import MongoAdapter
from app.adapters.postgres_adapter import PostgresAdapter, PostgresConfig
from app.agents.mongo_agent import MongoAgent
from app.agents.finance_agent import FinanceAgent
from app.agents.ops_agent import OpsAgent

from app.llm.llm_client import LLMClient, LLMConfig


# -------------------
# Redis
# -------------------
redis_store = RedisStore(RedisConfig())

# -------------------
# Mongo
# -------------------
mongo_client = MongoClient("mongodb://localhost:27017")
mongo_adapter = MongoAdapter(mongo_client)
mongo_agent = MongoAgent(mongo_adapter)

# -------------------
# Postgres
# -------------------
pg_cfg = PostgresConfig(
    dsn="postgresql://admin:admin123@localhost:5432/pocdb"
)
pg_adapter = PostgresAdapter(pg_cfg)

finance_agent = FinanceAgent(pg_adapter)
ops_agent = OpsAgent(pg_adapter)

# -------------------
# LLM (Groq)
# -------------------
api_key = os.getenv("GROQ_API_KEY")
if not api_key:
    raise RuntimeError("GROQ_API_KEY not set in environment")

llm = LLMClient(
    LLMConfig(api_key=api_key)
)

# -------------------
# Router
# -------------------
router = Router(
    redis_store=redis_store,
    llm_client=llm,
    mongo_agent=mongo_agent,
    finance_agent=finance_agent,
    ops_agent=ops_agent,
)

# -------------------
# Test
# -------------------
session_id = "integration_test_1"
result = router.handle_message(session_id, "Show voyage 2401 summary")

print("\n=== ROUTER RESULT ===")
print(result)
