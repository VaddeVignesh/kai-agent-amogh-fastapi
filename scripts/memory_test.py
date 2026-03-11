from __future__ import annotations

import os
import sys
import uuid

from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.adapters.mongo_adapter import MongoAdapter
from app.adapters.postgres_adapter import PostgresAdapter, PostgresConfig
from app.adapters.redis_store import RedisConfig, RedisStore
from app.agents.finance_agent import FinanceAgent
from app.agents.mongo_agent import MongoAgent
from app.agents.ops_agent import OpsAgent
from app.config.database import get_mongo_db
from app.llm.llm_client import LLMClient, LLMConfig
from app.orchestration.graph_router import GraphRouter


def build_router() -> GraphRouter:
    groq_api_key = os.getenv("GROQ_API_KEY")
    if not groq_api_key:
        raise RuntimeError("GROQ_API_KEY is not set")

    llm = LLMClient(
        LLMConfig(
            api_key=groq_api_key,
            model=os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile"),
            temperature=float(os.getenv("GROQ_TEMPERATURE", "0.0")),
        )
    )

    db = get_mongo_db()
    mongo_adapter = MongoAdapter(db.client, db_name=db.name)
    mongo_agent = MongoAgent(mongo_adapter, llm_client=llm)

    pg = PostgresAdapter(PostgresConfig.from_env())
    finance_agent = FinanceAgent(pg, llm_client=llm)
    ops_agent = OpsAgent(pg, llm_client=llm)

    redis_store = RedisStore(RedisConfig(host="localhost", port=6379, db=0))

    return GraphRouter(
        llm=llm,
        redis_store=redis_store,
        mongo_agent=mongo_agent,
        finance_agent=finance_agent,
        ops_agent=ops_agent,
    )


def main() -> None:
    os.environ["DYNAMIC_SQL_ENABLED"] = "true"

    router = build_router()
    session_id = os.getenv("SESSION_ID") or f"mem-{uuid.uuid4().hex[:8]}"

    scenarios = [
        (
            "Seed voyage context",
            "For voyage 1901, summarize financials, key ports, and remarks.",
        ),
        (
            "Follow-up (no voyage mentioned)",
            "What about expenses and any remarks?",
        ),
        (
            "Seed vessel context",
            "Tell me about vessel Stena Superior",
        ),
        (
            "Follow-up (no vessel mentioned)",
            "Give me a captain's-brief style summary of the last 3 voyages: route pattern, cargo pattern, and anything notable in remarks.",
        ),
    ]

    print(f"Session: {session_id}")
    for label, q in scenarios:
        print(f"\n=== {label} ===")
        print("Q:", q)
        r = router.handle(session_id=session_id, user_input=q)
        print("Intent:", r.get("intent_key"))
        print("Slots:", r.get("slots") or {})
        ans = (r.get("answer") or "").strip()
        print("Answer (preview):", (ans[:350] + ("..." if len(ans) > 350 else "")))


if __name__ == "__main__":
    main()

