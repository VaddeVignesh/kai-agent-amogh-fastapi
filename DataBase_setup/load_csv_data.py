"""
ETL Script: Load CSV Data into PostgreSQL
Transforms 13 CSV files → 2 PostgreSQL tables
Author: VGangadhar
Date: 2026-02-05
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import json
from datetime import datetime

DSN = "postgresql://admin:admin123@localhost:5432/stena_finance_ops"

print("="*100)
print("📊 ETL: LOADING CSV DATA INTO POSTGRESQL")
print("="*100)

# ============================================
# LOAD FINANCE DATA
# ============================================

def load_finance_kpi():
    """Load finance_voyage_results.csv and finance_voyage_results_when_fixed.csv"""
    
    print("\n" + "="*100)
    print("💰 LOADING FINANCE DATA")
    print("="*100)
    
    # Load ops_voyages to get vessel_imo and dates
    print("\n1️⃣ Loading ops_voyages.csv for vessel_imo and dates...")
    ops_voyages = pd.read_csv('data/ops/ops_voyages.csv')
    print(f"   ✅ Loaded {len(ops_voyages)} voyages")
    
    # Load ACTUAL results
    print("\n2️⃣ Loading finance_voyage_results.csv (ACTUAL)...")
    actual = pd.read_csv('data/finance/finance_voyage_results.csv')
    actual['scenario'] = 'ACTUAL'
    print(f"   ✅ Loaded {len(actual)} actual results")
    
    # Load WHEN_FIXED results
    print("\n3️⃣ Loading finance_voyage_results_when_fixed.csv (WHEN_FIXED)...")
    when_fixed = pd.read_csv('data/finance/finance_voyage_results_when_fixed.csv')
    when_fixed['scenario'] = 'WHEN_FIXED'
    print(f"   ✅ Loaded {len(when_fixed)} projected results")
    
    # Combine
    print("\n4️⃣ Combining data...")
    combined = pd.concat([actual, when_fixed], ignore_index=True)
    print(f"   ✅ Total rows: {len(combined)}")
    
    # Merge with ops_voyages to get voyage_number, vessel_imo, dates
    print("\n5️⃣ Merging with ops_voyages...")
    ops_subset = ops_voyages[['voyage_id', 'voyage_number', 'vessel_id', 'start_date_utc', 'end_date_utc']].copy()
    
    # Get vessel_imo from finance_vessels
    finance_vessels = pd.read_csv('data/finance/finance_vessels.csv')
    vessel_imo_map = dict(zip(finance_vessels['vessel_id'], finance_vessels['imo'].astype(str)))
    ops_subset['vessel_imo'] = ops_subset['vessel_id'].map(vessel_imo_map)
    
    merged = combined.merge(ops_subset, on='voyage_id', how='left')
    print(f"   ✅ Merged {len(merged)} rows")
    
    # Transform to match schema
    print("\n6️⃣ Transforming data...")
    
    # Helper function to convert datetime safely
    def safe_date(value):
        """Convert to date, return None if invalid"""
        if pd.isna(value):
            return None
        try:
            dt = pd.to_datetime(value, errors='coerce')
            if pd.isna(dt):
                return None
            return dt.date()
        except:
            return None
    
    def safe_timestamp(value):
        """Convert to timestamp, return None if invalid"""
        if pd.isna(value):
            return None
        try:
            dt = pd.to_datetime(value, errors='coerce')
            if pd.isna(dt):
                return None
            return dt
        except:
            return None
    
    def safe_value(value):
        """Convert NaN/NaT to None"""
        if pd.isna(value):
            return None
        return value
    
    # Build records manually to ensure proper None handling
    records = []
    seen_keys = set()  # Track (voyage_id, scenario) to avoid duplicates
    
    for _, row in merged.iterrows():
        voyage_id = safe_value(row['voyage_id'])
        scenario = safe_value(row['scenario'])
        
        # Skip if missing voyage_id
        if voyage_id is None:
            continue
        
        # Create unique key
        key = (voyage_id, scenario)
        
        # Skip duplicates
        if key in seen_keys:
            continue
        
        seen_keys.add(key)
        
        record = (
            voyage_id,
            safe_value(row['voyage_number']),
            safe_value(row['vessel_imo']),
            scenario,
            safe_value(row['revenue']),
            safe_value(row['expenses']),
            safe_value(row['pnl']),
            safe_value(row['tce']),
            safe_value(row['commission']),
            safe_value(row['bunkers']),
            safe_value(row['port']),
            safe_value(row['days']),
            safe_date(row['start_date_utc']),
            safe_date(row['end_date_utc']),
            safe_value(row['modified_by']),
            safe_timestamp(row['modified_date']),
        )
        
        records.append(record)
    
    print(f"   ✅ Prepared {len(records)} rows for insertion (removed {len(merged) - len(records)} duplicates)")
    
    # Insert into database
    print("\n7️⃣ Inserting into PostgreSQL...")
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    
    # Clear existing data
    cur.execute("DELETE FROM finance_voyage_kpi;")
    
    insert_query = """
        INSERT INTO finance_voyage_kpi (
            voyage_id, voyage_number, vessel_imo, scenario,
            revenue, total_expense, pnl, tce, total_commission,
            bunker_cost, port_cost, voyage_days,
            voyage_start_date, voyage_end_date,
            modified_by, modified_date
        ) VALUES %s;
    """
    
    # Execute in batches
    execute_values(cur, insert_query, records, page_size=100)
    conn.commit()
    
    # Verify
    cur.execute("SELECT COUNT(*) FROM finance_voyage_kpi;")
    count = cur.fetchone()[0]
    print(f"   ✅ Inserted {count} rows into finance_voyage_kpi")
    
    # Show breakdown
    cur.execute("SELECT scenario, COUNT(*) FROM finance_voyage_kpi GROUP BY scenario;")
    for scenario, cnt in cur.fetchall():
        print(f"      - {scenario}: {cnt} rows")
    
    cur.close()
    conn.close()



# ============================================
# LOAD OPS DATA
# ============================================

def load_ops_summary():
    """Load ops data with JSON aggregation"""
    
    print("\n" + "="*100)
    print("⚙️ LOADING OPERATIONS DATA")
    print("="*100)
    
    # Load base voyages
    print("\n1️⃣ Loading ops_voyages.csv...")
    voyages = pd.read_csv('data/ops/ops_voyages.csv')
    print(f"   ✅ Loaded {len(voyages)} voyages")
    
    # Load fixtures
    print("\n2️⃣ Loading ops_fixtures.csv...")
    fixtures = pd.read_csv('data/ops/ops_fixtures.csv')
    print(f"   ✅ Loaded {len(fixtures)} fixtures")
    
    # Load ports
    print("\n3️⃣ Loading ops_fixture_ports.csv...")
    ports = pd.read_csv('data/ops/ops_fixture_ports.csv')
    print(f"   ✅ Loaded {len(ports)} port calls")
    
    # Load grades
    print("\n4️⃣ Loading ops_fixture_grades.csv...")
    grades = pd.read_csv('data/ops/ops_fixture_grades.csv')
    print(f"   ✅ Loaded {len(grades)} cargo grades")
    
    # Get vessel_imo
    print("\n5️⃣ Mapping vessel_imo...")
    finance_vessels = pd.read_csv('data/finance/finance_vessels.csv')
    vessel_imo_map = dict(zip(finance_vessels['vessel_id'], finance_vessels['imo'].astype(str)))
    voyages['vessel_imo'] = voyages['vessel_id'].map(vessel_imo_map)
    
    # Aggregate ports to JSON
    print("\n6️⃣ Aggregating ports to JSON...")
    ports_grouped = {}
    for voyage_id, group in ports.groupby('voyage_id'):
        ports_list = []
        for _, row in group.iterrows():
            port_dict = {
                'port_name': row['port_name'] if pd.notna(row['port_name']) else None,
                'activity_type': row['activity_type'] if pd.notna(row['activity_type']) else None,
                'display_order': int(row['display_order']) if pd.notna(row['display_order']) else None
            }
            ports_list.append(port_dict)
        ports_grouped[voyage_id] = ports_list
    
    # Aggregate grades to JSON
    print("\n7️⃣ Aggregating grades to JSON...")
    grades_grouped = {}
    for voyage_id, group in grades.groupby('voyage_id'):
        grades_list = []
        for _, row in group.iterrows():
            grade_dict = {
                'grade_name': row['grade_name'] if pd.notna(row['grade_name']) else None,
                'display_order': int(row['display_order']) if pd.notna(row['display_order']) else None
            }
            grades_list.append(grade_dict)
        grades_grouped[voyage_id] = grades_list
    
    # Count fixtures per voyage
    print("\n8️⃣ Counting fixtures...")
    fixture_counts = fixtures.groupby('voyage_id').size().to_dict()
    
    # Aggregate fixture remarks
    print("\n9️⃣ Aggregating remarks...")
    remarks_grouped = {}
    for voyage_id, group in fixtures.groupby('voyage_id'):
        remarks = [r for r in group['fixture_remark'].dropna() if r]
        if remarks:
            remarks_grouped[voyage_id] = remarks
    
    # Transform data
    print("\n🔟 Transforming data...")
    ops_data = []
    
    for _, voyage in voyages.iterrows():
        voyage_id = voyage['voyage_id']
        
        row = {
            'voyage_id': voyage_id,
            'voyage_number': int(voyage['voyage_number']) if pd.notna(voyage['voyage_number']) else None,
            'vessel_id': voyage['vessel_id'] if pd.notna(voyage['vessel_id']) else None,
            'vessel_imo': voyage['vessel_imo'] if pd.notna(voyage['vessel_imo']) else None,
            'vessel_name': voyage['vessel_name'] if pd.notna(voyage['vessel_name']) else None,
            'module_type': voyage['module_type'] if pd.notna(voyage['module_type']) else None,
            'fixture_count': fixture_counts.get(voyage_id, 0),
            'offhire_days': float(voyage['offhire_days']) if pd.notna(voyage['offhire_days']) else 0.0,
            'is_delayed': bool(voyage['offhire_days'] > 0) if pd.notna(voyage['offhire_days']) else False,
            'delay_reason': None,
            'voyage_start_date': pd.to_datetime(voyage['start_date_utc'], errors='coerce').date() if pd.notna(voyage['start_date_utc']) else None,
            'voyage_end_date': pd.to_datetime(voyage['end_date_utc'], errors='coerce').date() if pd.notna(voyage['end_date_utc']) else None,
            'ports_json': json.dumps(ports_grouped.get(voyage_id, [])),
            'grades_json': json.dumps(grades_grouped.get(voyage_id, [])),
            'activities_json': None,
            'remarks_json': json.dumps(remarks_grouped.get(voyage_id, [])),
            'tags': voyage['tags'] if pd.notna(voyage['tags']) else None,
            'url': voyage['url'] if pd.notna(voyage['url']) else None,
        }
        ops_data.append(row)
    
    print(f"   ✅ Prepared {len(ops_data)} rows for insertion")
    
    # Insert into database
    print("\n1️⃣1️⃣ Inserting into PostgreSQL...")
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    
    # Clear existing data
    cur.execute("DELETE FROM ops_voyage_summary;")
    
    # Prepare records
    # Build and deduplicate records by voyage_id to avoid duplicate-key batches
    seen_voyage_ids = set()
    records = []
    duplicate_count = 0
    for row in ops_data:
        vid = row['voyage_id']
        if vid in seen_voyage_ids:
            duplicate_count += 1
            continue
        seen_voyage_ids.add(vid)
        records.append((
            row['voyage_id'], row['voyage_number'], row['vessel_id'], row['vessel_imo'], row['vessel_name'],
            row['module_type'], row['fixture_count'], row['offhire_days'], row['is_delayed'], row['delay_reason'],
            row['voyage_start_date'], row['voyage_end_date'],
            row['ports_json'], row['grades_json'], row['activities_json'], row['remarks_json'],
            row['tags'], row['url']
        ))
    
    if duplicate_count:
        print(f"   ✅ Prepared {len(records)} rows for insertion (removed {duplicate_count} duplicate voyage_id rows)")
    else:
        print(f"   ✅ Prepared {len(records)} rows for insertion")
    
    insert_query = """
        INSERT INTO ops_voyage_summary (
            voyage_id, voyage_number, vessel_id, vessel_imo, vessel_name,
            module_type, fixture_count, offhire_days, is_delayed, delay_reason,
            voyage_start_date, voyage_end_date,
            ports_json, grades_json, activities_json, remarks_json,
            tags, url
        ) VALUES %s
        ON CONFLICT (voyage_id) DO UPDATE SET
            fixture_count = EXCLUDED.fixture_count,
            ports_json = EXCLUDED.ports_json,
            grades_json = EXCLUDED.grades_json;
    """
    
    execute_values(cur, insert_query, records, page_size=100)
    conn.commit()
    
    # Verify
    cur.execute("SELECT COUNT(*) FROM ops_voyage_summary;")
    count = cur.fetchone()[0]
    print(f"   ✅ Inserted {count} rows into ops_voyage_summary")
    
    # Show stats
    cur.execute("SELECT COUNT(*) FROM ops_voyage_summary WHERE is_delayed = TRUE;")
    delayed = cur.fetchone()[0]
    print(f"      - Delayed voyages: {delayed}")
    
    cur.close()
    conn.close()


# ============================================
# MAIN
# ============================================

if __name__ == "__main__":
    try:
        # Load finance data
        load_finance_kpi()
        
        # Load ops data
        load_ops_summary()
        
        # Final summary
        print("\n" + "="*100)
        print("📊 FINAL SUMMARY")
        print("="*100)
        
        conn = psycopg2.connect(DSN)
        cur = conn.cursor()
        
        cur.execute("SELECT COUNT(*) FROM finance_voyage_kpi;")
        finance_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM ops_voyage_summary;")
        ops_count = cur.fetchone()[0]
        
        print(f"\n✅ finance_voyage_kpi: {finance_count} rows")
        print(f"✅ ops_voyage_summary: {ops_count} rows")
        
        # Sample queries
        print("\n📊 Sample Data:")
        
        cur.execute("""
            SELECT voyage_number, scenario, revenue, pnl, tce 
            FROM finance_voyage_kpi 
            WHERE scenario = 'ACTUAL' 
            ORDER BY pnl DESC NULLS LAST 
            LIMIT 3;
        """)
        print("\n   Top 3 Profitable Voyages (ACTUAL):")
        for row in cur.fetchall():
            print(f"      Voyage {row[0]}: Revenue=${row[2]:,.0f}, P&L=${row[3]:,.0f}, TCE=${row[4]:,.2f}/day")
        
        cur.execute("""
            SELECT voyage_number, vessel_name, fixture_count 
            FROM ops_voyage_summary 
            ORDER BY fixture_count DESC 
            LIMIT 3;
        """)
        print("\n   Voyages with Most Fixtures:")
        for row in cur.fetchall():
            print(f"      Voyage {row[0]} ({row[1]}): {row[2]} fixtures")
        
        cur.close()
        conn.close()
        
        print("\n" + "="*100)
        print("🎉 ETL COMPLETE! DATA LOADED SUCCESSFULLY!")
        print("="*100)
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
