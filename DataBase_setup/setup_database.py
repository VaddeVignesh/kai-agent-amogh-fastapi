"""
Setup PostgreSQL Database - Create Tables
"""

import psycopg2

# Updated DSN with admin credentials
DSN = "postgresql://admin:admin123@localhost:5432/stena_finance_ops"

print("="*80)
print("🗄️ CREATING DATABASE TABLES")
print("="*80)

try:
    # Connect
    print(f"\nConnecting to: {DSN.replace('admin123', '***')}")
    conn = psycopg2.connect(DSN)
    conn.autocommit = False
    cur = conn.cursor()
    
    # Read SQL file
    print("📄 Reading create_tables.sql...")
    with open('DataBase_setup\create_tables.sql', 'r', encoding='utf-8') as f:
        sql_content = f.read()
    
    # Split by semicolons and execute each statement
    print("⚙️ Executing SQL statements...")
    
    # Remove comments and split
    statements = []
    for line in sql_content.split('\n'):
        if not line.strip().startswith('--') and line.strip():
            statements.append(line)
    
    sql = '\n'.join(statements)
    
    # Execute (PostgreSQL specific commands like \d won't work in psycopg2)
    # So we'll filter those out
    sql_clean = sql.replace('\\d finance_voyage_kpi', '')
    sql_clean = sql_clean.replace('\\d ops_voyage_summary', '')
    
    cur.execute(sql_clean)
    conn.commit()
    
    print("✅ Tables created successfully!")
    
    # Verify tables
    print("\n📊 Verifying tables...")
    cur.execute("""
        SELECT table_name, 
               (SELECT COUNT(*) FROM information_schema.columns 
                WHERE table_schema = 'public' AND table_name = t.table_name) as column_count
        FROM information_schema.tables t
        WHERE table_schema = 'public'
        ORDER BY table_name;
    """)
    
    tables = cur.fetchall()
    print(f"\n✅ Found {len(tables)} tables:")
    for table_name, col_count in tables:
        print(f"   - {table_name:<30} ({col_count} columns)")
    
    # Show row counts
    print("\n📊 Row counts:")
    cur.execute("SELECT COUNT(*) FROM finance_voyage_kpi;")
    finance_count = cur.fetchone()[0]
    print(f"   - finance_voyage_kpi: {finance_count} rows")
    
    cur.execute("SELECT COUNT(*) FROM ops_voyage_summary;")
    ops_count = cur.fetchone()[0]
    print(f"   - ops_voyage_summary: {ops_count} rows")
    
    # Close
    cur.close()
    conn.close()
    
    print("\n" + "="*80)
    print("✅ DATABASE SETUP COMPLETE!")
    print("="*80)
    
except Exception as e:
    print(f"❌ ERROR: {e}")
    import traceback
    traceback.print_exc()
