"""
Comprehensive Test Suite for Finance & Ops Agents
Author: VGangadhar
Date: 2026-02-05
"""

from app.adapters.postgres_adapter import PostgresAdapter, PostgresConfig
from app.agents.finance_agent import FinanceAgent
from app.agents.ops_agent import OpsAgent

print("="*100)
print("🧪 FINANCE & OPS AGENTS TEST SUITE")
print("="*100)

# Setup
DSN = "postgresql://admin:admin123@localhost:5432/stena_finance_ops"
config = PostgresConfig(dsn=DSN)
pg_adapter = PostgresAdapter(config)

finance_agent = FinanceAgent(pg_adapter)
ops_agent = OpsAgent(pg_adapter)

print("\n✅ Agents initialized successfully!\n")

# Test counters
total_tests = 0
passed_tests = 0
failed_tests = 0

def test_case(name, test_func):
    """Run a single test case"""
    global total_tests, passed_tests, failed_tests
    total_tests += 1
    
    print(f"\n{'='*100}")
    print(f"TEST {total_tests}: {name}")
    print('='*100)
    
    try:
        test_func()
        passed_tests += 1
        print("✅ PASS")
    except AssertionError as e:
        failed_tests += 1
        print(f"❌ FAIL: {e}")
    except Exception as e:
        failed_tests += 1
        print(f"❌ ERROR: {e}")

# ============================================
# SECTION 1: FINANCE AGENT TESTS
# ============================================

print("\n" + "="*100)
print("💰 SECTION 1: FINANCE AGENT")
print("="*100)

def test_top_voyages_by_pnl():
    """Test: Top voyages by P&L"""
    print("\n   Testing: finance.summary (top voyages by P&L)")
    
    result = finance_agent.run(
        intent_key="finance.summary",
        slots={"date_from": "2021-01-01", "date_to": "2024-12-31", "limit": 5}
    )
    
    assert result.intent_key == "finance.summary"
    assert result.query_key == "finance.top_voyages_by_pnl"
    assert len(result.rows) > 0
    assert len(result.rows) <= 5
    
    print(f"\n   ✅ Retrieved {len(result.rows)} voyages")
    for i, row in enumerate(result.rows[:3], 1):
        print(f"      {i}. Voyage {row['voyage_number']}: P&L=${row['pnl']:,.0f}")

test_case("Finance: Top Voyages by P&L", test_top_voyages_by_pnl)


def test_voyage_summary_finance():
    """Test: Voyage summary (finance part)"""
    print("\n   Testing: voyage.summary with specific voyage_id")
    
    # Get a sample voyage_id first
    sample = pg_adapter.fetch_one("finance.top_voyages_by_pnl", {
        "date_from": "2021-01-01",
        "date_to": "2024-12-31",
        "limit": 1
    })
    
    voyage_id = sample['voyage_id']
    print(f"   Using voyage_id: {voyage_id[:30]}...")
    
    result = finance_agent.run(
        intent_key="voyage.summary",
        slots={"voyage_id": voyage_id}
    )
    
    assert result.intent_key == "voyage.summary"
    assert len(result.rows) > 0
    
    row = result.rows[0]
    print(f"\n   ✅ Voyage {row['voyage_number']} Summary:")
    print(f"      Revenue: ${row['revenue']:,.0f}")
    print(f"      Expenses: ${row['total_expense']:,.0f}")
    print(f"      P&L: ${row['pnl']:,.0f}")
    print(f"      TCE: ${row['tce']:,.2f}/day")

test_case("Finance: Voyage Summary", test_voyage_summary_finance)


def test_actual_vs_when_fixed():
    """Test: Compare ACTUAL vs WHEN_FIXED"""
    print("\n   Testing: Scenario comparison (ACTUAL vs WHEN_FIXED)")
    
    # Get voyage with both scenarios
    result_actual = finance_agent.run(
        intent_key="finance.summary",
        slots={"date_from": "2021-01-01", "date_to": "2024-12-31", "limit": 1, "scenario": "ACTUAL"}
    )
    
    voyage_id = result_actual.rows[0]['voyage_id']
    
    result_fixed = finance_agent.run(
        intent_key="voyage.summary",
        slots={"voyage_id": voyage_id, "scenario": "WHEN_FIXED"}
    )
    
    actual_pnl = result_actual.rows[0]['pnl']
    fixed_pnl = result_fixed.rows[0]['pnl'] if result_fixed.rows else None
    
    print(f"\n   ✅ Voyage {result_actual.rows[0]['voyage_number']} Comparison:")
    print(f"      Projected P&L: ${fixed_pnl:,.0f}" if fixed_pnl else "      Projected P&L: N/A")
    print(f"      Actual P&L: ${actual_pnl:,.0f}")
    if fixed_pnl:
        variance = actual_pnl - fixed_pnl
        print(f"      Variance: ${variance:,.0f} ({'better' if variance > 0 else 'worse'} than expected)")

test_case("Finance: ACTUAL vs WHEN_FIXED", test_actual_vs_when_fixed)


# ============================================
# SECTION 2: OPERATIONS AGENT TESTS
# ============================================

print("\n" + "="*100)
print("⚙️ SECTION 2: OPERATIONS AGENT")
print("="*100)

def test_delayed_voyages():
    """Test: Find delayed voyages"""
    print("\n   Testing: ops.delayed_voyages")
    
    result = ops_agent.run(
        intent_key="ops.delayed_voyages",
        slots={"date_from": "2021-01-01", "date_to": "2024-12-31", "limit": 5}
    )
    
    assert result.intent_key == "ops.delayed_voyages"
    assert result.query_key == "ops.delayed_voyages_in_range"
    
    print(f"\n   ✅ Found {len(result.rows)} delayed voyages")
    for i, row in enumerate(result.rows[:3], 1):
        print(f"      {i}. Voyage {row['voyage_number']} ({row['vessel_imo']})")

test_case("Operations: Delayed Voyages", test_delayed_voyages)


def test_voyages_by_port():
    """Test: Find voyages by port"""
    print("\n   Testing: ops.voyages_by_port")
    
    result = ops_agent.run(
        intent_key="ops.voyages_by_port",
        slots={"port_name": "Singapore", "date_from": "2021-01-01", "date_to": "2024-12-31", "limit": 5}
    )
    
    assert result.intent_key == "ops.voyages_by_port"
    
    print(f"\n   ✅ Found {len(result.rows)} voyages calling at Singapore")
    for i, row in enumerate(result.rows[:3], 1):
        print(f"      {i}. Voyage {row['voyage_number']} ({row['vessel_imo']})")

test_case("Operations: Voyages by Port", test_voyages_by_port)


def test_voyage_summary_ops():
    """Test: Voyage ops summary"""
    print("\n   Testing: voyage.summary (ops part)")
    
    # Get a sample voyage_id
    sample = pg_adapter.fetch_one("ops.delayed_voyages_in_range", {
        "date_from": "2021-01-01",
        "date_to": "2024-12-31",
        "limit": 1
    })
    
    if sample:
        voyage_id = sample['voyage_id']
        print(f"   Using voyage_id: {voyage_id[:30]}...")
        
        result = ops_agent.run(
            intent_key="voyage.summary",
            slots={"voyage_id": voyage_id}
        )
        
        assert len(result.rows) > 0
        
        row = result.rows[0]
        print(f"\n   ✅ Voyage {row['voyage_number']} Ops Summary:")
        print(f"      Vessel: {row['vessel_imo']}")
        print(f"      Module: {row['module_type']}")
        print(f"      Fixtures: {row['fixture_count']}")
        print(f"      Delayed: {row['is_delayed']}")
        
        # Parse ports JSON - FIX: Check if it's already a string
        import json
        ports_json = row['ports_json']
        
        # If it's already a list (parsed by JSONB), use it directly
        if isinstance(ports_json, list):
            ports = ports_json
        # If it's a string, parse it
        elif isinstance(ports_json, str):
            ports = json.loads(ports_json)
        else:
            ports = []
        
        print(f"      Ports: {len(ports)} port calls")
        if ports:
            for port in ports[:3]:
                print(f"         - {port.get('port_name', 'Unknown')} ({port.get('activity_type', 'N/A')})")
    else:
        print("   ⚠️ No delayed voyages found for testing")

test_case("Operations: Voyage Summary", test_voyage_summary_ops)


# ============================================
# SECTION 3: INTEGRATION TESTS
# ============================================

print("\n" + "="*100)
print("🔗 SECTION 3: INTEGRATION (FINANCE + OPS)")
print("="*100)

def test_full_voyage_summary():
    """Test: Complete voyage summary (finance + ops)"""
    print("\n   Testing: Complete voyage summary using SQL join")
    
    # Use the joined query directly
    sample = pg_adapter.fetch_one("kpi.voyage_full_summary_by_id", {
        "voyage_id": "F07C63363AA52CE4DA88F51E243FDF49"  # Known voyage from earlier
    })
    
    if sample:
        print(f"\n   ✅ Full Voyage Summary:")
        print(f"      Voyage: {sample['voyage_number']}")
        print(f"      Vessel IMO: {sample['vessel_imo']}")
        print(f"      Revenue: ${sample['revenue']:,.0f}" if sample['revenue'] else "      Revenue: N/A")
        print(f"      P&L: ${sample['pnl']:,.0f}" if sample['pnl'] else "      P&L: N/A")
        print(f"      TCE: ${sample['tce']:,.2f}/day" if sample['tce'] else "      TCE: N/A")
        print(f"      Fixtures: {sample['fixture_count']}")
        print(f"      Delayed: {sample['is_delayed']}")
    else:
        print("   ⚠️ Voyage not found")

test_case("Integration: Full Voyage Summary", test_full_voyage_summary)


# ============================================
# FINAL REPORT
# ============================================

print("\n" + "="*100)
print("📊 FINAL TEST SUMMARY")
print("="*100)

print(f"\n{'Total Tests:':<20} {total_tests}")
print(f"{'Passed:':<20} ✅ {passed_tests}")
print(f"{'Failed:':<20} ❌ {failed_tests}")
print(f"{'Success Rate:':<20} 📈 {(passed_tests/total_tests*100):.1f}%")

if failed_tests == 0:
    print("\n" + "="*100)
    print("🎉 ALL TESTS PASSED!")
    print("Finance & Ops agents tests passed.")
    print("="*100)
else:
    print(f"\n⚠️  {failed_tests} test(s) failed. Review failures above.")

# Cleanup
pg_adapter.close()

print("\n✅ Test suite completed!")
