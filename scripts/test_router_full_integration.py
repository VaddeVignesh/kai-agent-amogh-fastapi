"""
test_with_swasti_files.py - Complete End-to-End Test with Remarks Visible
"""

import os
import sys
from pymongo import MongoClient
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from app.adapters.mongo_adapter import MongoAdapter
from app.adapters.postgres_adapter import PostgresAdapter, PostgresConfig
from app.adapters.redis_store import RedisStore, RedisConfig
from app.llm.llm_client import LLMClient, LLMConfig
from app.agents.mongo_agent import MongoAgent
from app.agents.finance_agent import FinanceAgent
from app.agents.ops_agent import OpsAgent
from app.orchestration.router import Router
from app.registries.intent_registry import INTENT_REGISTRY


def banner(text, char="="):
    print(f"\n{char*80}")
    print(f"  {text}")
    print(f"{char*80}")


def test_query(num, query_text, router, show_details=True):
    """Test a single query and show results"""
    
    print(f"\n{'─'*80}")
    print(f"TEST {num}: {query_text[:70]}...")
    print('─'*80)
    
    try:
        result = router.handle(f"test_{num}", query_text)
        
        intent = result.get("intent", "unknown")
        slots = result.get("slots", {})
        answer = result.get("answer", "")
        clarification = result.get("clarification", "")
        
        if clarification:
            print(f"⚠️  STATUS: NEEDS CLARIFICATION")
            print(f"   Intent: {intent}")
            print(f"   Question: {clarification}")
            return "CLARIFICATION"
        
        elif intent == "out_of_scope":
            print(f"❌ STATUS: OUT OF SCOPE")
            print(f"   Intent: {intent}")
            return "OUT_OF_SCOPE"
        
        else:
            has_mongo = bool(result.get("mongo"))
            has_finance = bool(result.get("finance"))
            has_ops = bool(result.get("ops"))
            
            data_count = sum([has_mongo, has_finance, has_ops])
            
            if data_count >= 2:
                print(f"✅ STATUS: PASS")
            elif data_count == 1:
                print(f"⚠️  STATUS: PARTIAL")
            else:
                print(f"❌ STATUS: NO DATA")
            
            print(f"   Intent: {intent}")
            print(f"   Slots: {slots}")
            print(f"   Data Sources: Mongo={has_mongo}, Finance={has_finance}, Ops={has_ops}")
            
            # Show natural language answer
            if answer and show_details:
                print(f"\n   💬 ANSWER:")
                for line in answer.split('\n'):
                    print(f"      {line}")
            
            # Show data details
            if show_details:
                if has_mongo and isinstance(result.get("mongo"), dict):
                    doc = result["mongo"]
                    print(f"\n   📦 MONGO DATA:")
                    if "voyageNumber" in doc:
                        print(f"      Voyage: {doc.get('voyageNumber')} - {doc.get('vesselName')}")
                        
                        # ✅ SHOW REMARKS IN DETAIL
                        if "remarkList" in doc and doc["remarkList"]:
                            print(f"      Remarks: {len(doc['remarkList'])} found")
                            print(f"\n      📝 DETAILED REMARKS:")
                            for i, remark in enumerate(doc["remarkList"][:5], 1):
                                title = remark.get('title', 'Unknown')
                                text = remark.get('text', '')
                                date = remark.get('date', '')[:10]
                                print(f"         {i}. [{date}] {title}")
                                print(f"            {text}")
                        else:
                            print(f"      Remarks: None")
                    elif "imo" in doc:
                        print(f"      Vessel: {doc.get('name')} (IMO: {doc.get('imo')})")
                
                if has_finance and isinstance(result.get("finance"), list):
                    finance_rows = result["finance"]
                    print(f"\n   💰 FINANCE DATA:")
                    print(f"      Rows: {len(finance_rows)}")
                    if finance_rows:
                        row = finance_rows[0]
                        if 'pnl' in row:
                            print(f"      Sample P&L: ${row.get('pnl', 0):,.2f}")
                        if 'tce' in row:
                            print(f"      Sample TCE: ${row.get('tce', 0):,.2f}")
                
                if has_ops and isinstance(result.get("ops"), list):
                    ops_rows = result["ops"]
                    print(f"\n   🚢 OPS DATA:")
                    print(f"      Rows: {len(ops_rows)}")
            
            if data_count >= 2:
                return "PASS"
            elif data_count == 1:
                return "PARTIAL"
            else:
                return "NO_DATA"
    
    except Exception as e:
        print(f"❌ STATUS: ERROR")
        print(f"   Error: {e}")
        import traceback
        if show_details:
            traceback.print_exc()
        return "ERROR"


def check_remarks_in_mongo(mongo_adapter):
    """Check if MongoDB has remarks data"""
    banner("MONGODB REMARKS CHECK", "─")
    
    print("\n🔍 Checking for voyage remarks...")
    
    # ✅ FIX: Look for 'remarks' field
    voyages_with_remarks = list(mongo_adapter.voyages.find(
        {"remarks": {"$exists": True, "$ne": []}},
        {"voyageNumber": 1, "vesselName": 1, "remarks": 1}
    ).limit(5))
    
    if voyages_with_remarks:
        print(f"✅ Found {len(voyages_with_remarks)} sample voyages with remarks:")
        for v in voyages_with_remarks:
            voyage_num = v.get("voyageNumber")
            remarks = v.get("remarks", [])
            print(f"\n   Voyage {voyage_num} ({v.get('vesselName')}): {len(remarks)} remarks")
            for i, remark in enumerate(remarks[:3], 1):
                if isinstance(remark, dict):
                    text = remark.get("remark", "")[:70]
                    author = remark.get("modifiedByFull", "Unknown")
                    date = remark.get("modifiedDate", "")[:10]
                    print(f"      {i}. [{date}] {author}")
                    print(f"         → {text}...")
    else:
        print("⚠️  No voyages with remarks found")
    
    # Statistics
    total_with_remarks = mongo_adapter.voyages.count_documents({
        "remarks": {"$exists": True, "$ne": []}
    })
    total_voyages = mongo_adapter.voyages.count_documents({})
    
    print(f"\n📊 Statistics:")
    print(f"   Total voyages: {total_voyages:,}")
    print(f"   Voyages with remarks: {total_with_remarks:,}")
    if total_voyages > 0:
        print(f"   Coverage: {total_with_remarks/total_voyages*100:.1f}%")


def main():
    banner("🧪 COMPLETE END-TO-END TEST WITH REMARKS")
    print("Testing with Swasti's updated Router & LLM Client")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Setup
    banner("SETUP")
    print("\n🔧 Initializing system...")
    
    try:
        mongo_client = MongoClient("mongodb://localhost:27017/")
        mongo_adapter = MongoAdapter(mongo_client, db_name="kai_agent")
        
        dsn = "postgresql://admin:admin123@localhost:5432/stena_finance_ops"
        postgres_adapter = PostgresAdapter(cfg=PostgresConfig(dsn=dsn))
        
        redis_store = RedisStore(cfg=RedisConfig(host="localhost", port=6379))
        
        groq_api_key = os.getenv("GROQ_API_KEY")
        if not groq_api_key:
            print("   ❌ GROQ_API_KEY not set!")
            return
        
        llm_client = LLMClient(cfg=LLMConfig(
            api_key=groq_api_key,
            model="openai/gpt-oss-120b",
            temperature=0.0
        ))
        
        router = Router(
            llm=llm_client,
            mongo_agent=MongoAgent(mongo_adapter),
            finance_agent=FinanceAgent(postgres_adapter),
            ops_agent=OpsAgent(postgres_adapter),
            redis_store=redis_store
        )
        
        print("   ✅ All components initialized")
        
    except Exception as e:
        print(f"   ❌ Setup failed: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Check MongoDB remarks
    check_remarks_in_mongo(mongo_adapter)
    
    # Test first 3 queries in detail
    banner("TESTING QUERIES (DETAILED)")
    
    queries = [
        "For voyage 2306, give me the financial summary (revenue/expenses/PnL), the main ports involved, and any voyage remarks explaining delays or issues.",
        "Show the top 5 profit-making voyages, and for each: list the cargo grade, key ports visited, and any remarks that justify why performance was high.",
        "Pick voyage 1901 and generate an executive summary: what happened operationally (ports and grades), what it earned financially, and what remarks were recorded.",
    ]
    
    results = []
    for i, query in enumerate(queries, 1):
        result = test_query(i, query, router, show_details=True)
        results.append(result)
    
    banner("✅ TESTING COMPLETE")
    print(f"\n🎯 Tested {len(results)} queries")
    print(f"   ✅ PASS: {results.count('PASS')}/{len(results)}")
    
    # Cleanup
    mongo_client.close()
    postgres_adapter.close()


if __name__ == "__main__":
    main()
