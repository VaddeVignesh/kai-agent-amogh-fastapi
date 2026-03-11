from fastapi import FastAPI
from pydantic import BaseModel
from typing import Any, Dict, List, Optional
import uuid
import os
 
from dotenv import load_dotenv
load_dotenv()
 
# =========================================================
# IMPORTS — EXACT SAME STACK AS c.py
# =========================================================
 
from app.orchestration.graph_router import GraphRouter
 
from app.llm.llm_client import LLMClient, LLMConfig
 
from app.config.database import get_mongo_db
from app.adapters.mongo_adapter import MongoAdapter
 
from app.adapters.postgres_adapter import (
    PostgresAdapter,
    PostgresConfig,
)
 
from app.adapters.redis_store import (
    RedisStore,
    RedisConfig,
)
 
from app.agents.mongo_agent import MongoAgent
from app.agents.finance_agent import FinanceAgent
from app.agents.ops_agent import OpsAgent
 
 
# =========================================================
# CREATE FASTAPI APP
# =========================================================
 
app = FastAPI(
    title="KAI Agent API",
    description="Maritime Analytics AI Assistant",
    version="1.0",
)
 
 
# =========================================================
# INITIALIZE DEPENDENCIES (MATCHES c.py EXACTLY)
# =========================================================
 
# ---------- LLM ----------
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
 
 
# ---------- Mongo ----------
db = get_mongo_db()
 
mongo_adapter = MongoAdapter(
    db.client,
    db_name=db.name
)
 
mongo_agent = MongoAgent(
    mongo_adapter,
    llm_client=llm
)
 
 
# ---------- Postgres ----------
pg = PostgresAdapter(PostgresConfig.from_env())
 
finance_agent = FinanceAgent(
    pg,
    llm_client=llm
)
 
ops_agent = OpsAgent(
    pg,
    llm_client=llm
)
 
 
# ---------- Redis ----------
redis_store = RedisStore(
    RedisConfig(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_DB", "0")),
    )
)
 
 
# ---------- Graph Router ----------
router = GraphRouter(
    llm=llm,
    redis_store=redis_store,
    mongo_agent=mongo_agent,
    finance_agent=finance_agent,
    ops_agent=ops_agent,
)
 
 
# =========================================================
# REQUEST / RESPONSE MODELS
# =========================================================
 
class QueryRequest(BaseModel):
    query: str
    session_id: str | None = None
 
 
class QueryResponse(BaseModel):
    session_id: str
    answer: str
    clarification: Optional[str] = None
    trace: Optional[List[Dict[str, Any]]] = None
    intent_key: Optional[str] = None
    slots: Optional[Dict[str, Any]] = None
    dynamic_sql_used: Optional[bool] = None
    dynamic_sql_agents: Optional[List[str]] = None
 
 
# =========================================================
# HEALTH CHECK
# =========================================================
 
@app.get("/")
def root():
    return {"status": "KAI Agent API running 🚀"}
 
 
# =========================================================
# MAIN QUERY ENDPOINT
# =========================================================
 
@app.post("/query", response_model=QueryResponse)
def query_agent(req: QueryRequest):

    if os.getenv("KAI_DEBUG", "").strip().lower() in ("1", "true", "yes", "y", "on"):
        print("Received query:", req.query)
    session_id = req.session_id or f"api-{uuid.uuid4().hex[:8]}"
 
    result = router.handle(
        session_id=session_id,
        user_input=req.query
    )
 
    return QueryResponse(
        session_id=session_id,
        answer=result.get("answer", ""),
        clarification=result.get("clarification"),
        trace=result.get("trace"),
        intent_key=result.get("intent_key"),
        slots=result.get("slots"),
        dynamic_sql_used=result.get("dynamic_sql_used"),
        dynamic_sql_agents=result.get("dynamic_sql_agents"),
    )
 