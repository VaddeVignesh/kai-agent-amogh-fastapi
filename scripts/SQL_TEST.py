"""
Interactive query testing with full diagnostics
"""
import os
import sys
import uuid
from dotenv import load_dotenv

# Force reload .env
load_dotenv(override=True)

# Print loaded env vars
print("="*80)
print("🔍 ENVIRONMENT CHECK")
print("="*80)
print(f"POSTGRES_DSN: {os.getenv('POSTGRES_DSN', 'NOT SET')}")
print(f"POSTGRES_DB: {os.getenv('POSTGRES_DB', 'NOT SET')}")
print(f"MONGO_URI: {os.getenv('MONGO_URI', 'NOT SET')}")
print(f"MONGO_DB_NAME: {os.getenv('MONGO_DB_NAME', 'NOT SET')}")
print(f"GROQ_API_KEY: {'SET' if os.getenv('GROQ_API_KEY') else 'NOT SET'}")
print("="*80 + "\n")

if sys.stdout.encoding and sys.stdout.encoding.lower().startswith("cp"):
    sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.adapters.postgres_adapter import PostgresAdapter, PostgresConfig

# Test DB connection first
print("="*80)
print("🔌 DATABASE CONNECTION TEST")
print("="*80)

pg_cfg = PostgresConfig.from_env()
print(f"Connecting to: {pg_cfg.dsn}\n")

pg = PostgresAdapter(pg_cfg)

try:
    # Test finance table
    result = pg.execute_dynamic_select("SELECT COUNT(*) as count FROM finance_voyage_kpi")
    print(f"✅ finance_voyage_kpi: {result[0]['count']} rows")
    
    # Test ops table
    result = pg.execute_dynamic_select("SELECT COUNT(*) as count FROM ops_voyage_summary")
    print(f"✅ ops_voyage_summary: {result[0]['count']} rows")
    
except Exception as e:
    print(f"❌ DATABASE ERROR: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

pg.close()

print("\n" + "="*80)
print("🎯 INITIALIZING ROUTER")
print("="*80 + "\n")

# Now test the full router
from app.adapters.mongo_adapter import MongoAdapter
from app.config.database import get_mongo_db
from app.adapters.redis_store import RedisStore, RedisConfig
from app.orchestration.graph_router import GraphRouter
from app.llm.llm_client import LLMClient, LLMConfig
from app.agents.mongo_agent import MongoAgent
from app.agents.finance_agent import FinanceAgent
from app.agents.ops_agent import OpsAgent
from app.sql.sql_generator import SQLGenerator
from app.sql.sql_allowlist import DEFAULT_ALLOWLIST

groq_api_key = os.getenv("GROQ_API_KEY")
if not groq_api_key:
    raise RuntimeError("GROQ_API_KEY is not set")

llm = LLMClient(
    LLMConfig(
        api_key=groq_api_key,
        model=os.getenv("GROQ_MODEL", "openai/gpt-oss-120b"),
        temperature=float(os.getenv("GROQ_TEMPERATURE", "0.0")),
         reasoning_effort=os.getenv("GROQ_REASONING_EFFORT", "medium")
    )
)

db = get_mongo_db()
mongo_adapter = MongoAdapter(db.client, db_name=db.name)
mongo_agent = MongoAgent(mongo_adapter, llm_client=llm)

pg = PostgresAdapter(PostgresConfig.from_env())
sql_generator = SQLGenerator(llm=llm, allowlist=DEFAULT_ALLOWLIST)
finance_agent = FinanceAgent(pg, sql_generator=sql_generator, allowlist=DEFAULT_ALLOWLIST)
ops_agent = OpsAgent(pg, sql_generator=sql_generator, allowlist=DEFAULT_ALLOWLIST)

redis_store = RedisStore(RedisConfig(host="localhost", port=6379, db=0))

router = GraphRouter(
    llm=llm,
    redis_store=redis_store,
    mongo_agent=mongo_agent,
    finance_agent=finance_agent,
    ops_agent=ops_agent,
)

session_id = str(uuid.uuid4())

print("✅ Router initialized successfully!\n")
print("="*80)
print("💬 INTERACTIVE MODE")
print("="*80)
print("Type your query (or 'quit' to exit)\n")

# Interactive loop
while True:
    try:
        user_input = input("You: ").strip()
    except (EOFError, KeyboardInterrupt):
        print("\n\nGoodbye!")
        break
    
    if not user_input:
        continue
    
    if user_input.lower() in ('quit', 'exit', 'q'):
        print("Goodbye!")
        break
    
    print("\n" + "="*80)
    print("🔄 PROCESSING...")
    print("="*80 + "\n")
    
    try:
        response = router.handle(session_id=session_id, user_input=user_input)
        
        print("="*80)
        print("📊 RESPONSE")
        print("="*80 + "\n")
        
        if response.get('clarification'):
            print(f"🤔 Assistant: {response['clarification']}\n")
        else:
            print(f"✅ Assistant:\n{response.get('answer', 'No answer')}\n")
        
        # Show detailed diagnostics
        print("="*80)
        print("🔍 DIAGNOSTICS")
        print("="*80)
        
        print(f"Intent: {response.get('intent_key', 'unknown')}")
        print(f"Slots: {response.get('slots', {})}")
        
        data = response.get('data', {})
        
        mongo_data = data.get('mongo')
        if mongo_data:
            print(f"\n📦 Mongo:")
            print(f"   Type: {type(mongo_data).__name__}")
            if isinstance(mongo_data, dict):
                print(f"   Keys: {list(mongo_data.keys())}")
        
        finance_data = data.get('finance')
        if finance_data and isinstance(finance_data, dict):
            mode = finance_data.get('mode', 'unknown')
            rows = len(finance_data.get('rows', []))
            print(f"\n💰 Finance:")
            print(f"   Mode: {mode}")
            print(f"   Rows: {rows}")
            if finance_data.get('query_key'):
                print(f"   Query: {finance_data['query_key']}")
            if finance_data.get('sql'):
                print(f"   SQL: {finance_data['sql'][:100]}...")
            if finance_data.get('fallback_reason'):
                print(f"   ⚠️ Fallback: {finance_data['fallback_reason']}")
        
        ops_data = data.get('ops')
        if ops_data and isinstance(ops_data, dict):
            mode = ops_data.get('mode', 'unknown')
            rows = len(ops_data.get('rows', []))
            print(f"\n⚙️ Ops:")
            print(f"   Mode: {mode}")
            print(f"   Rows: {rows}")
            if ops_data.get('query_key'):
                print(f"   Query: {ops_data['query_key']}")
            if ops_data.get('sql'):
                print(f"   SQL: {ops_data['sql'][:100]}...")
            if ops_data.get('fallback_reason'):
                print(f"   ⚠️ Fallback: {ops_data['fallback_reason']}")
        
        if data.get('dynamic_sql_used'):
            print(f"\n🔧 Dynamic SQL used by: {', '.join(data.get('dynamic_sql_agents', []))}")
        
        print("\n" + "="*80 + "\n")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}\n")
        import traceback
        traceback.print_exc()
        print("\n" + "="*80 + "\n")
