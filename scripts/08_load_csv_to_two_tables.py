import pandas as pd
import json
from psycopg2 import connect
from psycopg2.extras import execute_batch

# =====================================================
# CONFIG
# =====================================================

DSN = "postgresql://admin:admin123@localhost:5432/pocdb"

# =====================================================
# HELPERS
# =====================================================

def safe_json(val):
    """
    Convert NaN / None to empty list for JSONB columns
    """
    if val is None:
        return []
    if isinstance(val, float) and pd.isna(val):
        return []
    return val


def safe_date(val):
    """
    Convert anything to DATE or NULL
    """
    return pd.to_datetime(val, errors="coerce")


# =====================================================
# LOAD CSVs
# =====================================================

finance_results = pd.read_csv("data/finance/finance_voyage_results.csv")
finance_comm = pd.read_csv("data/finance/finance_fixture_commissions.csv")

ops_voyages = pd.read_csv("data/ops/ops_voyages.csv")
ops_ports = pd.read_csv("data/ops/ops_fixture_ports.csv")
ops_grades = pd.read_csv("data/ops/ops_fixture_grades.csv")

print("✅ CSVs loaded")

# =====================================================
# FINANCE → finance_voyage_kpi
# =====================================================

# Aggregate commission per voyage
commission_sum = (
    finance_comm
    .groupby("voyage_id", dropna=True)["rate"]
    .sum()
    .reset_index(name="total_commission")
)

finance = finance_results.merge(
    commission_sum, on="voyage_id", how="left"
)

finance_rows = []

for _, r in finance.iterrows():
    finance_rows.append((
        r["voyage_id"],                        # voyage_id
        r.get("voyage_number"),                # voyage_number
        r.get("imo"),                          # vessel_imo
        "ACTUAL",                              # scenario (POC default)
        r.get("revenue"),
        r.get("expenses"),
        r.get("pnl"),
        r.get("tce"),
        r.get("total_commission"),
        safe_date(r.get("modified_date")),     # voyage_start_date
        safe_date(r.get("modified_date")),     # voyage_end_date
        "CSV_POC"                              # source_system
    ))

print(f"✅ Prepared {len(finance_rows)} finance rows")

# =====================================================
# OPS → ops_voyage_summary
# =====================================================

ports_json = (
    ops_ports
    .groupby("voyage_id", dropna=True)
    .apply(
        lambda x: x[["port_name", "activity_type"]]
        .dropna()
        .to_dict("records")
    )
    .reset_index(name="ports_json")
)

grades_json = (
    ops_grades
    .groupby("voyage_id", dropna=True)
    .apply(
        lambda x: x[["grade_name"]]
        .dropna()
        .to_dict("records")
    )
    .reset_index(name="grades_json")
)

ops = (
    ops_voyages
    .merge(ports_json, on="voyage_id", how="left")
    .merge(grades_json, on="voyage_id", how="left")
)

ops_rows = []

for _, r in ops.iterrows():
    ops_rows.append((
        r["voyage_id"],                                # voyage_id
        r.get("voyage_number"),                        # voyage_number
        r.get("vessel_id"),                            # vessel_imo
        r.get("module_type"),                          # module_type
        None,                                          # fixture_count
        bool(r.get("offhire_days", 0) > 0),            # is_delayed
        None,                                          # delay_reason
        json.dumps(safe_json(r.get("ports_json"))),    # ports_json
        json.dumps(safe_json(r.get("grades_json"))),   # grades_json
        json.dumps([]),                                # activities_json
        json.dumps([]),                                # remarks_json
        safe_date(r.get("start_date_utc")),            # voyage_start_date
        safe_date(r.get("end_date_utc")),              # voyage_end_date
        "CSV_POC"                                      # source_system
    ))

print(f"✅ Prepared {len(ops_rows)} ops rows")

# =====================================================
# INSERT INTO POSTGRES
# =====================================================

conn = connect(DSN)
cur = conn.cursor()

execute_batch(
    cur,
    """
    INSERT INTO finance_voyage_kpi (
      voyage_id, voyage_number, vessel_imo, scenario,
      revenue, total_expense, pnl, tce, total_commission,
      voyage_start_date, voyage_end_date, source_system
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT DO NOTHING;
    """,
    finance_rows,
)

execute_batch(
    cur,
    """
    INSERT INTO ops_voyage_summary (
      voyage_id, voyage_number, vessel_imo, module_type,
      fixture_count, is_delayed, delay_reason,
      ports_json, grades_json, activities_json, remarks_json,
      voyage_start_date, voyage_end_date, source_system
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT DO NOTHING;
    """,
    ops_rows,
)

conn.commit()
cur.close()
conn.close()

print("🎉 SUCCESS: CSV data loaded into finance_voyage_kpi and ops_voyage_summary")
