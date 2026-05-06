from fastapi import BackgroundTasks, Depends, FastAPI, Header, HTTPException, Request
from pydantic import BaseModel
from typing import Any, Dict, List, Optional
from datetime import datetime
import uuid
import os
import time

from dotenv import load_dotenv
load_dotenv()

os.environ["LANGCHAIN_TRACING_V2"] = os.getenv("LANGCHAIN_TRACING_V2", "false")
os.environ["LANGCHAIN_API_KEY"] = os.getenv("LANGCHAIN_API_KEY", "")
os.environ["LANGCHAIN_PROJECT"] = os.getenv("LANGCHAIN_PROJECT", "kai-agent-poc")

from app.auth import USERS, ROLE_ACCESS, get_role_access, login, generate_session_id
from app.core.logger import get_logger

# =========================================================
# IMPORTS
# =========================================================

from app.orchestration.graph_router import GraphRouter
from app.llm.llm_client import LLMClient, LLMConfig
from app.config.database import get_mongo_db
from app.adapters.mongo_adapter import MongoAdapter
from app.adapters.postgres_adapter import PostgresAdapter, PostgresConfig
from app.adapters.redis_store import RedisStore, RedisConfig
from app.agents.mongo_agent import MongoAgent
from app.agents.finance_agent import FinanceAgent
from app.agents.ops_agent import OpsAgent

logger = get_logger("api")
hist_logger = get_logger("execution_history")


# =========================================================
# CREATE FASTAPI APP
# =========================================================

app = FastAPI(
    title="KAI Agent API",
    description="Maritime Analytics AI Assistant",
    version="1.0",
)


# =========================================================
# CORS MIDDLEWARE
# =========================================================

from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8080",
        "http://127.0.0.1:8080",
        "http://localhost:5173",
        "http://localhost:5174",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================================================
# INIT LLM + AGENTS
# =========================================================

llm = LLMClient(
    LLMConfig(
        api_key=os.getenv("GROQ_API_KEY"),
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

redis_store = RedisStore(
    RedisConfig(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_DB", "0")),
    )
)

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
    request_id: str | None = None
    chat_history: Optional[List[Dict[str, Any]]] = []


class QueryResponse(BaseModel):
    session_id: str
    answer: str
    clarification: Optional[str] = None
    trace: Optional[List[Dict[str, Any]]] = None
    intent_key: Optional[str] = None
    slots: Optional[Dict[str, Any]] = None
    dynamic_sql_used: Optional[bool] = None
    dynamic_sql_agents: Optional[List[str]] = None


class SessionClearRequest(BaseModel):
    session_id: str
    include_idem: bool = True
    include_lock: bool = True


class SessionClearResponse(BaseModel):
    session_id: str
    ok: bool
    deleted: int = 0
    deleted_keys: Optional[List[str]] = None
    reason: Optional[str] = None


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    success: bool
    role: str = ""
    session_id: str = ""
    message: str = ""


class AdminMetricsResponse(BaseModel):
    total_users: int = 0
    active_sessions: int = 0
    queries_today: int = 0
    avg_response_time: float = 0.0


class AdminUserResponse(BaseModel):
    username: str
    role: str
    status: str
    active_sessions: int = 0
    last_active: str = "Never"
    queries_today: int = 0


class AdminAuditEventResponse(BaseModel):
    timestamp: float
    actor: str
    role: str
    action: str
    status: str = "completed"
    session_id: str = ""
    query_preview: Optional[str] = None
    query_length: Optional[int] = None
    intent_key: Optional[str] = None
    duration_seconds: Optional[float] = None


class AdminSystemHealthResponse(BaseModel):
    name: str
    status: str
    latency_ms: Optional[float] = None
    detail: str = ""


def _session_role_from_id(session_id: str) -> Optional[str]:
    parts = str(session_id or "").split(":")
    if len(parts) >= 3 and parts[0] in ROLE_ACCESS:
        return parts[0]
    return None


def _role_access_for_session(session_id: str) -> tuple[Optional[str], Dict[str, Any]]:
    session = redis_store.load_session(session_id)
    role = str(session.get("role") or session.get("auth", {}).get("role") or _session_role_from_id(session_id) or "")
    access = session.get("role_access")
    if not isinstance(access, dict):
        access = get_role_access(role)
    return role or None, access


def require_admin_access(
    request: Request,
    x_session_id: Optional[str] = Header(default=None, alias="X-Session-Id"),
) -> Dict[str, Any]:
    session_id = str(x_session_id or "").strip()
    if not session_id:
        raise HTTPException(status_code=401, detail="Missing admin session")

    session = redis_store.load_session(session_id)
    if not session:
        raise HTTPException(status_code=401, detail="Invalid or expired session")

    role = str(session.get("role") or session.get("auth", {}).get("role") or "")
    access = session.get("role_access")
    if not isinstance(access, dict):
        access = get_role_access(role)

    allowed_admin_apis = set(access.get("admin_apis") or [])
    if role != "admin" or request.url.path not in allowed_admin_apis:
        raise HTTPException(status_code=403, detail="Admin access required")

    return session


# =========================================================
# ENDPOINTS
# =========================================================

@app.get("/")
def root():
    return {"status": "KAI Agent API running 🚀"}


@app.post("/login", response_model=LoginResponse)
async def login_endpoint(req: LoginRequest, background_tasks: BackgroundTasks):
    role = login(req.username, req.password)
    if not role:
        return LoginResponse(success=False, message="Invalid username or password")
    session_id = generate_session_id(req.username, role)
    redis_store.save_session(
        session_id,
        {
            "auth": {"username": req.username, "role": role},
            "username": req.username,
            "role": role,
            "role_access": get_role_access(role),
            "login_ts": time.time(),
        },
    )
    background_tasks.add_task(redis_store.record_login_audit, req.username, role, session_id)
    return LoginResponse(success=True, role=role, session_id=session_id)


def _record_query_side_effects(
    *,
    session_id: str,
    query: str,
    role: Optional[str],
    intent_key: Optional[str],
    elapsed: float,
) -> None:
    redis_store.record_query_metrics(elapsed)
    redis_store.record_user_query(session_id)
    redis_store.record_query_audit(
        session_id=session_id,
        query=query,
        intent_key=intent_key,
        duration_seconds=elapsed,
    )
    execution_record = {
        "timestamp": datetime.now().isoformat(),
        "session_id": session_id,
        "role": role or _session_role_from_id(session_id) or "unknown",
        "query_preview": redis_store._query_preview(query),
        "query_length": len(query or ""),
        "intent": intent_key,
        "response_time": round(elapsed, 3),
        "status": "success",
    }
    redis_store.record_execution_history(execution_record)
    hist_logger.info(
        "EXECUTION_HISTORY | session=%s | role=%s | intent=%s | latency=%ss | query_chars=%s",
        session_id,
        execution_record["role"],
        intent_key,
        execution_record["response_time"],
        execution_record["query_length"],
    )


@app.post("/query", response_model=QueryResponse)
def query_agent(req: QueryRequest, background_tasks: BackgroundTasks = None):
    if os.getenv("KAI_DEBUG", "").strip().lower() in ("1", "true", "yes", "y", "on"):
        print("Received query:", req.query)

    session_id = req.session_id or f"api-{uuid.uuid4().hex[:8]}"
    request_id = (req.request_id or "").strip()
    role, role_access = _role_access_for_session(session_id)
    if role and role_access:
        redis_store.save_session(session_id, {"role": role, "role_access": role_access})

    if request_id:
        cached = redis_store.idem_get(session_id, request_id)
        if isinstance(cached, dict):
            return QueryResponse(**cached)

    start_time = time.time()
    result = router.handle(
        session_id=session_id,
        user_input=req.query
    )
    elapsed = time.time() - start_time

    response = QueryResponse(
        session_id=session_id,
        answer=result.get("answer", ""),
        clarification=result.get("clarification"),
        trace=result.get("trace"),
        intent_key=result.get("intent_key"),
        slots=result.get("slots"),
        dynamic_sql_used=result.get("dynamic_sql_used"),
        dynamic_sql_agents=result.get("dynamic_sql_agents"),
    )

    if request_id:
        payload = response.model_dump() if hasattr(response, "model_dump") else response.dict()
        redis_store.idem_set(session_id, request_id, payload)

    side_effect_kwargs = {
        "session_id": session_id,
        "query": req.query,
        "role": role,
        "intent_key": response.intent_key,
        "elapsed": elapsed,
    }
    if background_tasks is not None:
        background_tasks.add_task(_record_query_side_effects, **side_effect_kwargs)
    else:
        _record_query_side_effects(**side_effect_kwargs)
    return response


@app.get("/admin/metrics", response_model=AdminMetricsResponse)
def admin_metrics(_session: Dict[str, Any] = Depends(require_admin_access)):
    return AdminMetricsResponse(**redis_store.get_admin_metrics(total_users=len(USERS)))


@app.get("/admin/users", response_model=List[AdminUserResponse])
def admin_users(_session: Dict[str, Any] = Depends(require_admin_access)):
    return [AdminUserResponse(**row) for row in redis_store.get_admin_users(USERS)]


@app.get("/admin/audit-log", response_model=List[AdminAuditEventResponse])
def admin_audit_log(_session: Dict[str, Any] = Depends(require_admin_access)):
    return [AdminAuditEventResponse(**row) for row in redis_store.get_admin_audit_log(limit=10)]


@app.get("/admin/system-health", response_model=List[AdminSystemHealthResponse])
def admin_system_health(_session: Dict[str, Any] = Depends(require_admin_access)):
    checks: List[AdminSystemHealthResponse] = []

    checks.append(AdminSystemHealthResponse(
        name="Backend API",
        status="Operational",
        latency_ms=0.0,
        detail="FastAPI process is running",
    ))

    start = time.time()
    try:
        db.command("ping")
        checks.append(AdminSystemHealthResponse(
            name="MongoDB",
            status="Operational",
            latency_ms=round((time.time() - start) * 1000, 1),
            detail=f"Database `{db.name}` reachable",
        ))
    except Exception as exc:
        checks.append(AdminSystemHealthResponse(
            name="MongoDB",
            status="Down",
            latency_ms=round((time.time() - start) * 1000, 1),
            detail=str(exc).splitlines()[0][:120],
        ))

    start = time.time()
    conn = None
    try:
        pool = pg._ensure_pool()
        conn = pool.getconn()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        checks.append(AdminSystemHealthResponse(
            name="PostgreSQL",
            status="Operational",
            latency_ms=round((time.time() - start) * 1000, 1),
            detail="Finance database reachable",
        ))
    except Exception as exc:
        checks.append(AdminSystemHealthResponse(
            name="PostgreSQL",
            status="Down",
            latency_ms=round((time.time() - start) * 1000, 1),
            detail=str(exc).splitlines()[0][:120],
        ))
    finally:
        if conn is not None and pg.pool is not None:
            pg.pool.putconn(conn)

    start = time.time()
    if redis_store._redis_disabled():
        checks.append(AdminSystemHealthResponse(
            name="Redis",
            status="Degraded",
            latency_ms=None,
            detail="Redis disabled; using in-memory fallback",
        ))
    else:
        try:
            redis_store.client.ping()
            redis_store._redis_available = True
            checks.append(AdminSystemHealthResponse(
                name="Redis",
                status="Operational",
                latency_ms=round((time.time() - start) * 1000, 1),
                detail="Session, metrics, audit cache reachable",
            ))
        except Exception as exc:
            redis_store._redis_available = False
            checks.append(AdminSystemHealthResponse(
                name="Redis",
                status="Down",
                latency_ms=round((time.time() - start) * 1000, 1),
                detail=str(exc).splitlines()[0][:120],
            ))

    return checks


@app.post("/session/clear", response_model=SessionClearResponse)
def clear_session_memory(req: SessionClearRequest):
    result = redis_store.clear_session(
        req.session_id,
        include_idem=bool(req.include_idem),
        include_lock=bool(req.include_lock),
    )
    if result.get("ok"):
        redis_store.record_logout_audit(req.session_id)
    return SessionClearResponse(
        session_id=req.session_id,
        ok=bool(result.get("ok")),
        deleted=int(result.get("deleted") or 0),
        deleted_keys=result.get("deleted_keys") if isinstance(result.get("deleted_keys"), list) else [],
        reason=result.get("reason"),
    )