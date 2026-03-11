# tests/test_interactive_validation.py
from __future__ import annotations
import os
import sys
import argparse
from dotenv import load_dotenv
load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import uuid
from datetime import datetime
from typing import Dict, Any

from app.orchestration.graph_router import GraphRouter
from app.llm.llm_client import LLMClient, LLMConfig
from app.config.database import get_mongo_db
from app.adapters.redis_store import RedisStore, RedisConfig
from app.adapters.mongo_adapter import MongoAdapter
from app.adapters.postgres_adapter import PostgresAdapter, PostgresConfig
from app.agents.mongo_agent import MongoAgent
from app.agents.finance_agent import FinanceAgent
from app.agents.ops_agent import OpsAgent


class TestValidator:
    """Validation for KAI Agent"""

    def __init__(self):
        self.test_results = []
        self.current_test = {}

    # =========================================================
    # INTENT VALIDATION
    # =========================================================

    def validate_intent(self, result: Dict[str, Any], query: str) -> Dict[str, Any]:
        """Validate intent extraction"""
        intent_key = result.get("intent_key", "unknown")
        slots = result.get("slots", {}) or {}

        validation = {"section": "Intent Extraction", "checks": []}

        # Check 1: Intent identified
        if intent_key and intent_key != "unknown":
            validation["checks"].append(("✅", "Intent identified", intent_key))
        else:
            validation["checks"].append(("❌", "Intent NOT identified", "unknown"))

        # Check 2: Slots extracted
        if slots:
            validation["checks"].append(("✅", f"Slots extracted: {len(slots)}", str(slots)))
        else:
            validation["checks"].append(("ℹ️", "No slots extracted", "May be expected"))

        # Check 3: Intent makes sense for query
        query_lower = query.lower()
        intent_match = False
        
        if 'top' in query_lower or 'most' in query_lower or 'highest' in query_lower:
            intent_match = 'ranking' in intent_key or 'composite' in intent_key
        elif 'voyage' in query_lower and any(str(i) in query for i in range(1000, 9999)):
            intent_match = 'voyage.summary' in intent_key
        elif 'compare' in query_lower:
            intent_match = 'analysis' in intent_key or 'composite' in intent_key
        else:
            intent_match = True  # Can't validate
        
        if intent_match:
            validation['checks'].append(('✅', 'Intent matches query', 'Logical'))
        else:
            validation['checks'].append(('⚠️', 'Intent may not match query', 'Review needed'))

        validation["status"] = "PASS" if validation["checks"][0][0] == "✅" else "FAIL"
        return validation

    # =========================================================
    # SQL VALIDATION
    # =========================================================

    def validate_sql(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Validate SQL generation"""
        validation = {"section": "SQL Generation", "checks": []}

        dynamic_used = result.get("dynamic_sql_used", False)
        agents = result.get("dynamic_sql_agents", [])

        if dynamic_used:
            validation["checks"].append(("✅", "Dynamic SQL used", str(agents)))
            validation["status"] = "PASS"
        else:
            validation["checks"].append(("ℹ️", "Dynamic SQL not used", "May be template or single flow"))
            validation["status"] = "PARTIAL"

        return validation

    # =========================================================
    # NoSQL (Mongo LLM find) VALIDATION
    # =========================================================

    def validate_nosql(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Validate dynamic NoSQL (Mongo LLM find) usage"""
        validation = {"section": "NoSQL (Mongo) Generation", "checks": []}

        data = result.get("data", {}) or {}
        mongo = data.get("mongo")
        intent = result.get("intent_key") or ""

        if isinstance(mongo, dict) and mongo.get("mode") == "mongo_llm":
            ok = mongo.get("ok")
            rows = mongo.get("rows") or []
            reason = mongo.get("reason")

            if ok is True:
                validation["checks"].append(("✅", "Dynamic NoSQL used", "mongo_llm"))
                validation["checks"].append(("✅", "Mongo LLM ok", "True"))
                validation["checks"].append(("ℹ️", f"Mongo rows returned: {len(rows)}", "0 means no match"))
                validation["status"] = "PASS" if len(rows) > 0 else "PARTIAL"
            else:
                validation["checks"].append(("❌", "Dynamic NoSQL failed", reason or "unknown"))
                validation["status"] = "FAIL"
        else:
            # Many aggregate intents do not require Mongo; treat as PASS to avoid false negatives.
            mongo_not_required = {
                "analysis.by_module_type",
                "analysis.cargo_profitability",
                "ranking.vessels",
                "analysis.scenario_comparison",
                "vessel.summary",
            }
            if intent in mongo_not_required:
                validation["checks"].append(("✅", "Dynamic NoSQL not required", intent))
                validation["status"] = "PASS"
            else:
                validation["checks"].append(("ℹ️", "Dynamic NoSQL not used", "mongo anchor/fallback path"))
                validation["status"] = "PARTIAL"

        return validation

    # =========================================================
    # EXECUTION VALIDATION (SMART)
    # =========================================================

    def validate_execution(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Validate query execution"""
        validation = {"section": "Agent Execution", "checks": []}

        data = result.get("data", {}) or {}
        plan = result.get("plan", {}) or {}
        intent = result.get("intent_key")

        finance_rows = data.get("finance", {}).get("rows", []) or []
        ops_rows = data.get("ops", {}).get("rows", []) or []
        mongo_data = data.get("mongo")

        plan_type = plan.get("plan_type", "single")

        # SINGLE out_of_scope → do not fail
        if intent == "out_of_scope":
            validation["checks"].append(("ℹ️", "Out of scope intent", "No agents expected"))
            validation["status"] = "PASS"
            return validation

        # COMPOSITE
        if plan_type == "composite":
            if finance_rows:
                validation["checks"].append(("✅", f"Finance rows: {len(finance_rows)}", "OK"))
            else:
                validation["checks"].append(("⚠️", "Finance returned 0 rows", "Check SQL"))

            if ops_rows:
                validation["checks"].append(("✅", f"Ops rows: {len(ops_rows)}", "OK"))
            else:
                validation["checks"].append(("ℹ️", "Ops returned 0 rows", "May be acceptable"))

            validation["status"] = "PASS" if finance_rows else "FAIL"
            return validation

        # SINGLE (entity summary)
        if plan_type == "single":
            if finance_rows or ops_rows or mongo_data:
                validation["checks"].append(("✅", "At least one agent returned data", "OK"))
                validation["status"] = "PASS"
            else:
                validation["checks"].append(("⚠️", "No agent returned data", "Check anchor resolution"))
                validation["status"] = "PARTIAL"

            return validation

        validation["status"] = "PARTIAL"
        return validation

    # =========================================================
    # ANSWER VALIDATION (ANTI-HALLUCINATION SAFE)
    # =========================================================

    def validate_answer(self, result: Dict[str, Any], query: str) -> Dict[str, Any]:
        """Validate final answer quality"""
        validation = {"section": "Answer Logic", "checks": []}

        answer = result.get("answer", "") or ""
        data = result.get("data", {}) or {}

        finance_rows = data.get("finance", {}).get("rows", []) or []
        ops_rows = data.get("ops", {}).get("rows", []) or []

        if not answer:
            validation["checks"].append(("❌", "No answer generated", "FAIL"))
            validation["status"] = "FAIL"
            return validation

        if len(answer) < 40:
            validation["checks"].append(("⚠️", "Very short answer", "Check summarization"))

        # Detect contradiction
        if (finance_rows or ops_rows) and "no data" in answer.lower():
            validation["checks"].append(("❌", "Contradiction: data exists but answer says none", "FAIL"))
            validation["status"] = "FAIL"
            return validation

        if not (finance_rows or ops_rows) and any(k in answer.lower() for k in ["revenue", "pnl", "expense"]):
            validation["checks"].append(("⚠️", "Answer references finance but no rows returned", "Possible hallucination"))

        # Check 2: No error messages in answer
        error_keywords = ['error', 'failed', 'exception', 'rate limit', 'timeout']
        has_errors = any(keyword in answer.lower() for keyword in error_keywords)
        
        if not has_errors:
            validation['checks'].append(('✅', 'No error messages', 'Clean'))
        else:
            validation['checks'].append(('❌', 'Contains error messages', 'FAIL'))
        
        # Check 3: Contains tables or structured data
        has_table = '|' in answer and '---' in answer
        if has_table:
            validation['checks'].append(('✅', 'Contains markdown table', 'Well-structured'))
        else:
            validation['checks'].append(('ℹ️', 'No tables', 'May be narrative'))
        
        # Check 4: Contains numbers/data (not just text)
        has_numbers = any(char.isdigit() for char in answer)
        if has_numbers:
            validation['checks'].append(('✅', 'Contains numeric data', 'Factual'))
        else:
            validation['checks'].append(('⚠️', 'No numbers found', 'May be issue'))
        
        # Check 5: Professional formatting
        has_headers = any(line.startswith('#') for line in answer.split('\n'))
        if has_headers:
            validation['checks'].append(('✅', 'Uses markdown headers', 'Professional'))
        else:
            validation['checks'].append(('ℹ️', 'No headers', 'Simple format'))

        validation["checks"].append(("✅", "Answer generated", f"{len(answer)} chars"))
        
        # Overall status for Answer validation
        fail_count = sum(1 for c in validation['checks'] if c[0] == '❌')
        warn_count = sum(1 for c in validation['checks'] if c[0] == '⚠️')
        
        if fail_count > 0:
            validation['status'] = 'FAIL'
        elif warn_count > 1:
            validation['status'] = 'PARTIAL'
        else:
            validation['status'] = 'PASS'
            
        return validation
    
    def print_validation(self, validations: list, query: str):
        """Print validation results in a nice format"""
        print("\n" + "="*100)
        print("📋 VALIDATION REPORT")
        print("="*100)
        print(f"Query: {query}")
        print("-"*100)
        
        overall_status = 'PASS'
        
        for validation in validations:
            section = validation['section']
            status = validation['status']
            checks = validation['checks']
            
            # Update overall status
            if status == 'FAIL':
                overall_status = 'FAIL'
            elif status == 'PARTIAL' and overall_status != 'FAIL':
                overall_status = 'PARTIAL'
            
            # Print section header
            status_emoji = '✅' if status == 'PASS' else '❌' if status == 'FAIL' else '⚠️'
            print(f"\n{status_emoji} {section}: {status}")
            print("-"*100)
            
            # Print checks
            for emoji, check_name, detail in checks:
                print(f"  {emoji} {check_name}: {detail}")
        
        # Print overall result
        print("\n" + "="*100)
        if overall_status == 'PASS':
            print("🎉 OVERALL: ✅ PASS - Query performed excellently!")
        elif overall_status == 'PARTIAL':
            print("⚠️  OVERALL: ⚠️  PARTIAL - Query worked but has minor issues")
        else:
            print("❌ OVERALL: ❌ FAIL - Query has critical issues")
        print("="*100)
        
        return overall_status
    
    def log_test(self, query: str, result: Dict[str, Any], overall_status: str):
        """Log test results to file"""
        mongo = (result.get("data") or {}).get("mongo") or {}
        is_mongo_llm = isinstance(mongo, dict) and mongo.get("mode") == "mongo_llm"
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'query': query,
            'status': overall_status,
            'intent': result.get('data', {}).get('artifacts', {}).get('intent_key'),
            'dynamic_sql': result.get('dynamic_sql_used'),
            'agents': result.get('dynamic_sql_agents'),
            'dynamic_nosql': bool(is_mongo_llm),
            'mongo_llm_ok': (mongo.get("ok") if isinstance(mongo, dict) else None),
            'mongo_llm_rows': (len(mongo.get("rows") or []) if isinstance(mongo, dict) else None),
            'mongo_llm_reason': (mongo.get("reason") if isinstance(mongo, dict) else None),
        }
        
        self.test_results.append(log_entry)
        
        # Write to log file
        with open('test_validation_log.json', 'w') as f:
            json.dump(self.test_results, f, indent=2)


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

    router = GraphRouter(
        llm=llm,
        redis_store=redis_store,
        mongo_agent=mongo_agent,
        finance_agent=finance_agent,
        ops_agent=ops_agent,
    )

    return router


def run_chat(*, router: GraphRouter) -> None:
    session_id = f"chat-{uuid.uuid4().hex[:8]}"

    print("\n" + "=" * 96)
    print("KAI-Agent — Maritime Analytics Assistant (Chat)")
    print("=" * 96)
    print(f"Session: {session_id}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    print("Commands:")
    print("- Type your message and press Enter")
    print("- 'new' to start a new session")
    print("- 'debug on' / 'debug off' to toggle verbose logs")
    print("- 'exit' to quit")
    print("=" * 96 + "\n")

    while True:
        try:
            try:
                msg = input("You: ").strip()
            except EOFError:
                print("\nGoodbye.\n")
                break

            if not msg:
                continue

            ml = msg.lower()
            if ml in ("exit", "quit", "q"):
                print("\nGoodbye.\n")
                break

            if ml == "new":
                session_id = f"chat-{uuid.uuid4().hex[:8]}"
                print(f"\nSession: {session_id}\n")
                continue

            if ml in ("debug on", "debug true", "debug 1"):
                os.environ["KAI_DEBUG"] = "true"
                print("\nDebug: ON\n")
                continue

            if ml in ("debug off", "debug false", "debug 0"):
                os.environ["KAI_DEBUG"] = "false"
                print("\nDebug: OFF\n")
                continue

            result = router.handle(session_id=session_id, user_input=msg)
            clarification = (result.get("clarification") or "").strip()
            if clarification:
                print("\nAssistant:\n" + clarification + "\n")
                print("-" * 96 + "\n")
                continue

            answer = (result.get("answer") or "").strip() or "Not available in dataset."
            print("\nAssistant:\n" + answer + "\n")
            print("-" * 96 + "\n")
            
        except KeyboardInterrupt:
            print("\n\n(Interrupted) Type 'exit' to quit.\n")
            continue
        except Exception as e:
            print(f"\nError: {e}\n")
            continue


def run_validate(*, router: GraphRouter) -> None:
    # Windows consoles often default to cp1252, which can't print many unicode symbols.
    # Make output robust by encoding non-encodable characters with backslash escapes.
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")  # type: ignore[attr-defined]
    except Exception:
        pass

    validator = TestValidator()
    session_id = f"test-{uuid.uuid4().hex[:8]}"

    print("\n" + "=" * 100)
    print("🧪 KAI AGENT - COMPREHENSIVE TESTING & VALIDATION TOOL")
    print("=" * 100)
    print(f"📝 Session ID: {session_id}")
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\n📋 Commands:")
    print("  - Type/paste your query and press Enter to test")
    print("  - Type 'exit' or 'quit' to stop")
    print("  - Type 'new' to start a new session")
    print("  - Type 'summary' to see test statistics")
    print("=" * 100 + "\n")

    test_count = 0
    pass_count = 0
    fail_count = 0
    partial_count = 0

    while True:
        try:
            try:
                query = input("\n🔍 Enter Query (or command): ").strip()
            except EOFError:
                print("\n\n👋 Exiting... Goodbye!\n")
                break

            if query.lower() in ['exit', 'quit', 'q']:
                print(f"\n📊 Test Summary:")
                print(f"   Total Tests: {test_count}")
                print(f"   ✅ Passed: {pass_count}")
                print(f"   ⚠️  Partial: {partial_count}")
                print(f"   ❌ Failed: {fail_count}")
                print(f"\n📁 Results saved to: test_validation_log.json")
                print("\n👋 Exiting... Goodbye!\n")
                break

            if query.lower() == 'new':
                session_id = f"test-{uuid.uuid4().hex[:8]}"
                print(f"✅ New session started: {session_id}")
                continue

            if query.lower() == 'summary':
                print(f"\n📊 Current Test Statistics:")
                print(f"   Total Tests: {test_count}")
                print(f"   ✅ Passed: {pass_count} ({pass_count/test_count*100:.1f}%)" if test_count > 0 else "   No tests yet")
                print(f"   ⚠️  Partial: {partial_count} ({partial_count/test_count*100:.1f}%)" if test_count > 0 else "")
                print(f"   ❌ Failed: {fail_count} ({fail_count/test_count*100:.1f}%)" if test_count > 0 else "")
                continue

            if not query:
                print("⚠️  Empty query, please try again.")
                continue

            test_count += 1
            print(f"\n⏳ Processing query #{test_count}...")
            print(f"📝 Query: {query}")

            result = router.handle(session_id=session_id, user_input=query)

            mongo = (result.get("data") or {}).get("mongo") or {}
            print("\nMONGO DEBUG:", json.dumps(
                {k: mongo.get(k) for k in ["mode", "ok", "collection", "filter", "projection", "limit"]},
                indent=2,
                default=str
            ))
            if isinstance(mongo, dict) and mongo.get("rows"):
                first = mongo["rows"][0] if isinstance(mongo["rows"][0], dict) else {}
                print("\nMONGO FIRST ROW KEYS:", list(first.keys()))

            validations = [
                validator.validate_intent(result, query),
                validator.validate_sql(result),
                validator.validate_nosql(result),
                validator.validate_execution(result),
                validator.validate_answer(result, query),
            ]

            overall_status = validator.print_validation(validations, query)

            if overall_status == 'PASS':
                pass_count += 1
            elif overall_status == 'PARTIAL':
                partial_count += 1
            else:
                fail_count += 1

            validator.log_test(query, result, overall_status)

            print("\n" + "=" * 100)
            print("📄 FULL ANSWER:")
            print("=" * 100)
            print(result.get('answer', 'No answer generated'))
            print("=" * 100)

            print(f"\n📊 Progress: {test_count} tests | ✅ {pass_count} | ⚠️  {partial_count} | ❌ {fail_count}")

        except KeyboardInterrupt:
            print("\n\n⚠️  Interrupted. Type 'exit' to quit or continue testing.")
            continue
        except Exception as e:
            print(f"\n❌ Error occurred: {e}")
            print("💡 Try another query or type 'exit' to quit.")
            fail_count += 1
            continue

    print("\n✅ Testing session completed.\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="KAI CLI")
    parser.add_argument("--validate", action="store_true", help="Run the validation/test harness UI")
    parser.add_argument("--debug", action="store_true", help="Enable verbose internal logs (KAI_DEBUG=true)")
    args = parser.parse_args()

    if args.debug:
        os.environ["KAI_DEBUG"] = "true"
    else:
        os.environ.setdefault("KAI_DEBUG", "false")

    router = build_router()

    if args.validate:
        run_validate(router=router)
    else:
        run_chat(router=router)


if __name__ == "__main__":
    os.environ["DYNAMIC_SQL_ENABLED"] = "true" 
    main()