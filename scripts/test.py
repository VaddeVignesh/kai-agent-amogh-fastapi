"""
Live test for critical data fabrication fix
"""
import os
import sys

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.adapters.postgresadapter import PostgresAdapter, PostgresConfig
from app.agents.llmclient import LLMClient, LLMConfig
from app.agents.financeagent import FinanceAgent
from app.agents.opsagent import OpsAgent
from app.agents.mongoagent import MongoAgent
from app.stores.redisstore import RedisStore
from app.routers.router import GraphRouter

def setup():
    """Initialize all components"""
    # Postgres
    pg_cfg = PostgresConfig.from_env()
    pg = PostgresAdapter(pg_cfg)
    
    # LLM
    llm_cfg = LLMConfig(
        apikey=os.getenv("GROQ_API_KEY"),
        model="openai/gpt-oss-120b",
        temperature=0.0
    )
    llm = LLMClient(llm_cfg)
    
    # Redis
    redis = RedisStore.from_env()
    
    # Agents
    finance = FinanceAgent(pg=pg, sql_generator=llm)
    ops = OpsAgent(pg=pg, sql_generator=llm)
    mongo = MongoAgent(...)  # Configure as needed
    
    # Router
    router = GraphRouter(
        llm=llm,
        redisstore=redis,
        mongoagent=mongo,
        financeagent=finance,
        opsagent=ops
    )
    
    return router

def test_query(router, query_num, query):
    """Execute a single test query"""
    print(f"\n{'='*80}")
    print(f"🧪 TEST QUERY #{query_num}")
    print(f"{'='*80}")
    print(f"Query: {query}")
    print(f"{'='*80}\n")
    
    result = router.handle(
        sessionid=f"test-session-{query_num}",
        userinput=query
    )
    
    print(f"Intent: {result.get('intentkey')}")
    print(f"Slots: {result.get('slots')}")
    print(f"\n📝 ANSWER:\n{result.get('answer')}\n")
    
    # Check for fabrication indicators
    answer = result.get('answer', '').lower()
    fabricated = False
    indicators = []
    
    fabrication_checks = {
        "mv example": "Fabricated vessel name",
        "mv sample": "Fabricated vessel name",
        "mv test": "Fabricated vessel name",
        "hypothetical": "Hypothetical data",
        "assuming the data": "Hypothetical assumption",
        "here is what the answer could look like": "Sample data",
        "vessel a": "Fabricated vessel",
        "cargo grade a": "Fabricated cargo grade",
    }
    
    for indicator, description in fabrication_checks.items():
        if indicator in answer:
            fabricated = True
            indicators.append(description)
    
    if fabricated:
        print(f"❌ FABRICATION DETECTED: {', '.join(indicators)}")
        return False
    else:
        print(f"✅ NO FABRICATION DETECTED")
        return True

def main():
    print("🔴 CRITICAL FIX - LIVE TEST SUITE")
    print("="*80)
    
    try:
        router = setup()
    except Exception as e:
        print(f"❌ Failed to initialize: {e}")
        return
    
    results = {}
    
    # Test Query #14
    results[14] = test_query(
        router,
        14,
        "Show me voyages where revenue exceeds 2M but PnL is negative, and include the vessel and cargo details."
    )
    
    # Test Query #15
    results[15] = test_query(
        router,
        15,
        "What is the total PnL for all voyages combined, broken down by cargo grade?"
    )
    
    # Regression Test Query #6
    results[6] = test_query(
        router,
        6,
        "Which cargo grades are most profitable overall?"
    )
    
    # Summary
    print(f"\n{'='*80}")
    print("📊 TEST SUMMARY")
    print(f"{'='*80}")
    for query_num, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"Query #{query_num}: {status}")
    
    all_passed = all(results.values())
    if all_passed:
        print(f"\n🎉 ALL TESTS PASSED - Critical fix verified!")
    else:
        print(f"\n⚠️  SOME TESTS FAILED - Review results above")

if __name__ == "__main__":
    main()
