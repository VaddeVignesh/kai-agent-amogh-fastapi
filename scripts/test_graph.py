import os
from pymongo import MongoClient
import sys
from dotenv import load_dotenv
import uuid

# Avoid UnicodeEncodeError when printing to Windows console (cp1252)
if sys.stdout.encoding and sys.stdout.encoding.lower().startswith("cp"):
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config.database import get_mongo_db
from app.adapters.mongo_adapter import MongoAdapter
from app.adapters.postgres_adapter import PostgresAdapter, PostgresConfig
from app.adapters.redis_store import RedisStore, RedisConfig

from app.agents.mongo_agent import MongoAgent
from app.agents.finance_agent import FinanceAgent
from app.agents.ops_agent import OpsAgent

from app.llm.llm_client import LLMClient, LLMConfig
from app.orchestration.graph_router import GraphRouter


def build_graph_router():
    groq_api_key = os.getenv("GROQ_API_KEY")
    if not groq_api_key:
        raise RuntimeError("GROQ_API_KEY is not set")

    # LLM
    llm = LLMClient(
        LLMConfig(
            api_key=groq_api_key,
            model=os.getenv("GROQ_MODEL", "llama-3.1-8b-instant"),
            temperature=float(os.getenv("GROQ_TEMPERATURE", "0.0")),
            reasoning_effort=os.getenv("GROQ_REASONING_EFFORT", "medium"),
        )
    )
    
    db = get_mongo_db()
    mongo_client = db.client
    mongo_db = db.name
    mongo_adapter = MongoAdapter(mongo_client, db_name=mongo_db)
    mongo_agent = MongoAgent(mongo_adapter, llm_client=llm)

    pg = PostgresAdapter(PostgresConfig.from_env())
    finance_agent = FinanceAgent(pg)
    ops_agent = OpsAgent(pg)
   
    redis_store = RedisStore(RedisConfig(host="localhost", port=6379, db=0))

    return GraphRouter(
        llm=llm,
        redis_store=redis_store,
        mongo_agent=mongo_agent,
        finance_agent=finance_agent,
        ops_agent=ops_agent,
    )


if __name__ == "__main__":
    print("="*80)
    print("KAI AGENT - INTERACTIVE QUERY TESTER")
    print("="*80)
    print("\nInitializing Graph Router...")
    
    gr = build_graph_router()
    
    print("✅ Router initialized successfully!\n")
    print("="*80)
    print("INSTRUCTIONS:")
    print("- Each query gets a FRESH session (no follow-up context)")
    print("- Type your query and press Enter")
    print("- Type 'exit' or 'quit' to stop")
    print("="*80)
    
    query_count = 0
    
    while True:
        print("\n" + "-"*80)
        user_input = input("\n📝 Enter your query (or 'exit' to quit): ").strip()
        
        if user_input.lower() in ['exit', 'quit', 'q', '']:
            print("\n👋 Exiting tester. Goodbye!")
            break
        
        query_count += 1
        
        # ✅ NEW SESSION for EACH query (no follow-ups)
        session_id = f"test-q{query_count}-{uuid.uuid4()}"
        
        print(f"\n🔄 Processing Query #{query_count}...")
        print(f"   Session ID: {session_id}")
        
        try:
            result = gr.handle(
                session_id=session_id,
                user_input=user_input
            )
            
            print("\n" + "="*80)
            print(f"✅ RESULT for Query #{query_count}")
            print("="*80)
            
            # Pretty print result
            print(f"\n📌 Intent: {result.get('intent_key', 'unknown')}")
            print(f"📌 Slots: {result.get('slots', {})}")
            
            if result.get('clarification'):
                print(f"\n❓ CLARIFICATION NEEDED:")
                print(f"   {result['clarification']}")
            elif result.get('answer'):
                print(f"\n💬 ANSWER:")
                print(f"{result['answer']}")
            else:
                print("\n⚠️ No answer generated")
            
            # Show data sources used
            data = result.get('data', {})
            sources_used = []
            if data.get('mongo'):
                sources_used.append('MongoDB')
            if data.get('finance'):
                sources_used.append('PostgreSQL-Finance')
            if data.get('ops'):
                sources_used.append('PostgreSQL-Ops')
            
            if sources_used:
                print(f"\n📊 Data sources used: {', '.join(sources_used)}")
            
            print("\n" + "="*80)
            
        except Exception as e:
            print("\n" + "="*80)
            print(f"❌ ERROR in Query #{query_count}")
            print("="*80)
            print(f"Error type: {type(e).__name__}")
            print(f"Error message: {str(e)}")
            print("\n⚠️ This query failed. Try another query or type 'exit' to quit.")
    
    print(f"\n📊 Total queries tested: {query_count}")
    print("="*80)
