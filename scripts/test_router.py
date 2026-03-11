"""
COMPLETE DATA PROFILE - MongoDB + Postgres
Full schema, samples, mappings for query planning
"""

import psycopg2
from pymongo import MongoClient
import os
import json
from dotenv import load_dotenv
from collections import Counter

load_dotenv()

print("="*80)
print("🔍 COMPLETE DATA PROFILE - FULL SYSTEM AUDIT")
print("="*80)

# Connect
mongo_client = MongoClient(os.getenv("MONGODB_URI", "mongodb://localhost:27017"))
mongo_db = mongo_client["kai_agent"]

pg_dsn = f"postgresql://{os.getenv('POSTGRES_USER', 'admin')}:{os.getenv('POSTGRES_PASSWORD', 'admin123')}@localhost:5432/{os.getenv('POSTGRES_DB', 'stena_finance_ops')}"
pg_conn = psycopg2.connect(pg_dsn)
pg_cursor = pg_conn.cursor()

print("\n✅ Connected to both systems\n")


print("="*80)
print("1️⃣ MONGODB VOYAGE SCHEMA")
print("="*80)

# Get one complete voyage document
sample_voyage = mongo_db.voyages.find_one({"voyageNumber": "1901"})

if sample_voyage:
    print("\n📋 COMPLETE VOYAGE DOCUMENT STRUCTURE:\n")

    def analyze_field(key, value, indent=0):
        prefix = "  " * indent
        if isinstance(value, dict):
            print(f"{prefix}{key}: <dict> {len(value)} keys")
            if len(value) <= 5:
                for k, v in value.items():
                    analyze_field(k, v, indent+1)
        elif isinstance(value, list):
            print(f"{prefix}{key}: <list> {len(value)} items")
            if len(value) > 0 and isinstance(value[0], dict):
                print(f"{prefix}  First item keys: {list(value[0].keys())}")
        else:
            val_str = str(value)[:60] if value else "null"
            print(f"{prefix}{key}: {type(value).__name__} = {val_str}")

    for key, value in sample_voyage.items():
        if key != '_id':
            analyze_field(key, value)

# Analyze remarks structure
print("\n📌 REMARKS STRUCTURE:")
remarks_sample = mongo_db.voyages.find_one({"remarks": {"$ne": []}})
if remarks_sample and 'remarks' in remarks_sample:
    if len(remarks_sample['remarks']) > 0:
        print(f"  Total remarks in sample: {len(remarks_sample['remarks'])}")
        print(f"  First remark keys: {list(remarks_sample['remarks'][0].keys())}")
        print(f"  Sample remark:")
        print(f"    {json.dumps(remarks_sample['remarks'][0], indent=4, default=str)}")

# Analyze fixtures
print("\n📌 FIXTURES STRUCTURE:")
if 'fixtures' in sample_voyage and len(sample_voyage.get('fixtures', [])) > 0:
    print(f"  Fixtures count: {len(sample_voyage['fixtures'])}")
    print(f"  First fixture keys: {list(sample_voyage['fixtures'][0].keys())}")

# Analyze legs
print("\n📌 LEGS STRUCTURE:")
if 'legs' in sample_voyage and len(sample_voyage.get('legs', [])) > 0:
    print(f"  Legs count: {len(sample_voyage['legs'])}")
    print(f"  First leg keys: {list(sample_voyage['legs'][0].keys())}")

# Analyze revenues/expenses
print("\n📌 REVENUES STRUCTURE:")
if 'revenues' in sample_voyage and len(sample_voyage.get('revenues', [])) > 0:
    print(f"  Revenues count: {len(sample_voyage['revenues'])}")
    print(f"  First revenue keys: {list(sample_voyage['revenues'][0].keys())}")

print("\n📌 EXPENSES STRUCTURE:")
if 'expenses' in sample_voyage and len(sample_voyage.get('expenses', [])) > 0:
    print(f"  Expenses count: {len(sample_voyage['expenses'])}")
    print(f"  First expense keys: {list(sample_voyage['expenses'][0].keys())}")


print("\n" + "="*80)
print("2️⃣ MONGODB VESSEL SCHEMA")
print("="*80)

sample_vessel = mongo_db.vessels.find_one({"imo": "9252436"})
if sample_vessel:
    print("\n📋 VESSEL DOCUMENT STRUCTURE:\n")
    for key, value in sample_vessel.items():
        if key != '_id':
            analyze_field(key, value)


print("\n" + "="*80)
print("3️⃣ POSTGRES FINANCE_VOYAGE_KPI SCHEMA")
print("="*80)

pg_cursor.execute("""
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_name = 'finance_voyage_kpi'
    ORDER BY ordinal_position;
""")
finance_cols = pg_cursor.fetchall()

print("\n📋 FINANCE_VOYAGE_KPI COLUMNS:\n")
for col, dtype, nullable in finance_cols:
    print(f"  {col:30s} {dtype:20s} {'NULL' if nullable == 'YES' else 'NOT NULL'}")

# Get data ranges
print("\n📊 DATA RANGES:\n")

pg_cursor.execute("SELECT COUNT(*) FROM finance_voyage_kpi;")
print(f"  Total rows: {pg_cursor.fetchone()[0]}")

pg_cursor.execute("SELECT COUNT(DISTINCT voyage_number) FROM finance_voyage_kpi;")
print(f"  Unique voyages: {pg_cursor.fetchone()[0]}")

pg_cursor.execute("SELECT COUNT(DISTINCT scenario) FROM finance_voyage_kpi;")
print(f"  Scenarios: {pg_cursor.fetchone()[0]}")

pg_cursor.execute("SELECT scenario, COUNT(*) FROM finance_voyage_kpi GROUP BY scenario;")
scenarios = pg_cursor.fetchall()
for scenario, count in scenarios:
    print(f"    {scenario}: {count} rows")

pg_cursor.execute("SELECT MIN(pnl), MAX(pnl), AVG(pnl) FROM finance_voyage_kpi WHERE scenario='ACTUAL';")
min_pnl, max_pnl, avg_pnl = pg_cursor.fetchone()
print(f"\n  PnL range (ACTUAL): ${min_pnl:,.0f} to ${max_pnl:,.0f} (avg: ${avg_pnl:,.0f})")

pg_cursor.execute("SELECT MIN(tce), MAX(tce), AVG(tce) FROM finance_voyage_kpi WHERE scenario='ACTUAL';")
min_tce, max_tce, avg_tce = pg_cursor.fetchone()
print(f"  TCE range (ACTUAL): ${min_tce:,.0f} to ${max_tce:,.0f} (avg: ${avg_tce:,.0f})")

pg_cursor.execute("SELECT MIN(voyage_end_date), MAX(voyage_end_date) FROM finance_voyage_kpi;")
min_date, max_date = pg_cursor.fetchone()
print(f"  Date range: {min_date} to {max_date}")

# Sample row
pg_cursor.execute("""
    SELECT * FROM finance_voyage_kpi 
    WHERE voyage_number = 1901 AND scenario = 'ACTUAL'
    LIMIT 1;
""")
sample_row = pg_cursor.fetchone()
print("\n📄 SAMPLE ROW (voyage 1901, ACTUAL):")
for i, col in enumerate(finance_cols):
    print(f"  {col[0]:30s} = {sample_row[i]}")


print("\n" + "="*80)
print("4️⃣ POSTGRES OPS_VOYAGE_SUMMARY SCHEMA")
print("="*80)

pg_cursor.execute("""
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_name = 'ops_voyage_summary'
    ORDER BY ordinal_position;
""")
ops_cols = pg_cursor.fetchall()

print("\n📋 OPS_VOYAGE_SUMMARY COLUMNS:\n")
for col, dtype, nullable in ops_cols:
    print(f"  {col:30s} {dtype:20s} {'NULL' if nullable == 'YES' else 'NOT NULL'}")

# Get data ranges
print("\n📊 DATA RANGES:\n")

pg_cursor.execute("SELECT COUNT(*) FROM ops_voyage_summary;")
print(f"  Total rows: {pg_cursor.fetchone()[0]}")

pg_cursor.execute("SELECT COUNT(DISTINCT voyage_number) FROM ops_voyage_summary;")
print(f"  Unique voyages: {pg_cursor.fetchone()[0]}")

pg_cursor.execute("SELECT COUNT(*) FROM ops_voyage_summary WHERE is_delayed = TRUE;")
print(f"  Delayed voyages: {pg_cursor.fetchone()[0]}")

# Sample row
pg_cursor.execute("""
    SELECT * FROM ops_voyage_summary 
    WHERE voyage_number = 1901
    LIMIT 1;
""")
sample_ops_row = pg_cursor.fetchone()
print("\n📄 SAMPLE ROW (voyage 1901):")
for i, col in enumerate(ops_cols):
    val = sample_ops_row[i]
    if col[1] == 'jsonb' and val:
        print(f"  {col[0]:30s} = <jsonb> {len(val) if isinstance(val, (dict, list)) else 'N/A'} items")
    else:
        print(f"  {col[0]:30s} = {val}")

# Analyze JSON structures
print("\n📌 JSON FIELD STRUCTURES:")

pg_cursor.execute("SELECT ports_json FROM ops_voyage_summary WHERE ports_json IS NOT NULL LIMIT 1;")
ports_sample = pg_cursor.fetchone()
if ports_sample and ports_sample[0]:
    print(f"\n  ports_json: {len(ports_sample[0])} ports")
    if len(ports_sample[0]) > 0:
        print(f"    First port keys: {list(ports_sample[0][0].keys())}")
        print(f"    Sample: {json.dumps(ports_sample[0][0], indent=6, default=str)}")

pg_cursor.execute("SELECT grades_json FROM ops_voyage_summary WHERE grades_json IS NOT NULL LIMIT 1;")
grades_sample = pg_cursor.fetchone()
if grades_sample and grades_sample[0]:
    print(f"\n  grades_json: {len(grades_sample[0])} grades")
    if len(grades_sample[0]) > 0:
        print(f"    First grade keys: {list(grades_sample[0][0].keys())}")

pg_cursor.execute("SELECT remarks_json FROM ops_voyage_summary WHERE remarks_json IS NOT NULL LIMIT 1;")
remarks_sample = pg_cursor.fetchone()
if remarks_sample and remarks_sample[0]:
    print(f"\n  remarks_json: {len(remarks_sample[0])} remarks")
    if len(remarks_sample[0]) > 0:
        print(f"    First remark keys: {list(remarks_sample[0][0].keys())}")


print("\n" + "="*80)
print("5️⃣ FIELD MAPPINGS BETWEEN SYSTEMS")
print("="*80)

print("""
VOYAGE IDENTIFIERS:
  MongoDB.voyageId        → Postgres.voyage_id       (UUID string)
  MongoDB.voyageNumber    → Postgres.voyage_number   (int, 1901-2409)
  MongoDB.vesselImo       → Postgres.vessel_imo      (string IMO number)

FINANCIAL DATA:
  MongoDB: revenues[], expenses[], bunkers[] lists
  Postgres: finance_voyage_kpi with aggregated totals

OPS DATA:
  MongoDB: fixtures[], legs[], remarks[] as detailed arrays
  Postgres: ops_voyage_summary with JSON aggregations

REMARKS:
  MongoDB: voyage.remarks[] (detailed objects with text, user, date)
  Postgres: ops_voyage_summary.remarks_json (same structure as JSONB)

PORTS:
  MongoDB: voyage.legs[].ports (nested in legs)
  Postgres: ops_voyage_summary.ports_json (flattened array)

CARGO:
  MongoDB: voyage.fixtures[].cargo details
  Postgres: ops_voyage_summary.grades_json
""")


print("\n" + "="*80)
print("6️⃣ DATA QUALITY METRICS")
print("="*80)

# MongoDB metrics
print("\nMONGODB:")
total_voyages = mongo_db.voyages.count_documents({})
print(f"  Total voyages: {total_voyages}")

with_remarks = mongo_db.voyages.count_documents({"remarks": {"$ne": []}})
print(f"  Voyages with remarks: {with_remarks} ({with_remarks/total_voyages*100:.1f}%)")

with_fixtures = mongo_db.voyages.count_documents({"fixtures": {"$ne": []}})
print(f"  Voyages with fixtures: {with_fixtures} ({with_fixtures/total_voyages*100:.1f}%)")

with_legs = mongo_db.voyages.count_documents({"legs": {"$ne": []}})
print(f"  Voyages with legs: {with_legs} ({with_legs/total_voyages*100:.1f}%)")

# Postgres metrics
print("\nPOSTGRES FINANCE:")
pg_cursor.execute("SELECT COUNT(*) FROM finance_voyage_kpi WHERE pnl IS NOT NULL;")
with_pnl = pg_cursor.fetchone()[0]
pg_cursor.execute("SELECT COUNT(*) FROM finance_voyage_kpi;")
total_finance = pg_cursor.fetchone()[0]
print(f"  Rows with PnL: {with_pnl}/{total_finance} ({with_pnl/total_finance*100:.1f}%)")

pg_cursor.execute("SELECT COUNT(*) FROM finance_voyage_kpi WHERE tce IS NOT NULL;")
with_tce = pg_cursor.fetchone()[0]
print(f"  Rows with TCE: {with_tce}/{total_finance} ({with_tce/total_finance*100:.1f}%)")

print("\nPOSTGRES OPS:")
pg_cursor.execute("SELECT COUNT(*) FROM ops_voyage_summary WHERE ports_json IS NOT NULL;")
with_ports = pg_cursor.fetchone()[0]
pg_cursor.execute("SELECT COUNT(*) FROM ops_voyage_summary;")
total_ops = pg_cursor.fetchone()[0]
print(f"  Voyages with ports: {with_ports}/{total_ops} ({with_ports/total_ops*100:.1f}%)")

pg_cursor.execute("SELECT COUNT(*) FROM ops_voyage_summary WHERE remarks_json IS NOT NULL;")
with_remarks_pg = pg_cursor.fetchone()[0]
print(f"  Voyages with remarks: {with_remarks_pg}/{total_ops} ({with_remarks_pg/total_ops*100:.1f}%)")


print("\n" + "="*80)
print("7️⃣ QUERY PLANNING RECOMMENDATIONS")
print("="*80)

print("""
FOR FINANCIAL QUERIES:
  ✅ Use: finance_voyage_kpi table
  ✅ Filter by: scenario='ACTUAL' (default) or 'WHEN_FIXED'
  ✅ Join key: voyage_number (int)
  ✅ Available metrics: pnl, tce, revenue, total_expense, total_commission
  ✅ Date range: voyage_end_date BETWEEN '2019-10-01' AND '2024-12-31'

FOR OPERATIONAL QUERIES:
  ✅ Use: ops_voyage_summary table
  ✅ Join key: voyage_number (int)
  ✅ Delayed voyages: WHERE is_delayed = TRUE
  ✅ Port searches: WHERE ports_json::text ILIKE '%Singapore%'
  ✅ Cargo searches: WHERE grades_json::text ILIKE '%Crude%'

FOR DETAILED REMARKS/FIXTURES:
  ✅ Use: MongoDB voyages collection
  ✅ Query by: voyageNumber (string) or voyageId (UUID)
  ✅ Access: remarks[], fixtures[], legs[], revenues[], expenses[]
  ✅ Full nested structure available

JOINING MONGO + POSTGRES:
  1. Query Postgres for voyage_numbers matching criteria
  2. Convert to strings and query MongoDB: {"voyageNumber": {"$in": [...]}}
  3. Or query MongoDB first, extract voyageNumbers, query Postgres

SAFE VOYAGE NUMBERS FOR TESTING:
  1901, 1902, 1903, 2301, 2306, 2401, 2402, 2409
  (All have complete data in all 3 tables)
""")


print("\n" + "="*80)
print("✅ COMPLETE DATA PROFILE DONE!")
print("="*80)

pg_cursor.close()
pg_conn.close()
mongo_client.close()