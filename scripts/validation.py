# DataBase_setup/audit_mongodb.py
"""
MongoDB Full Audit Script
- Shows all fields/columns in vessels + voyages
- Lists voyage numbers with remarks
- Shows full document structure sample
- Run: python DataBase_setup/audit_mongodb.py
"""

from pymongo import MongoClient
import json
from collections import defaultdict

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB  = "kai_agent"

client = MongoClient(MONGO_URI)
db     = client[MONGO_DB]

SEP = "=" * 80

# ─────────────────────────────────────────────
# HELPER
# ─────────────────────────────────────────────
def flatten_keys(doc, prefix="", result=None):
    """Recursively collect all dot-notation keys."""
    if result is None:
        result = set()
    for k, v in doc.items():
        full = f"{prefix}.{k}" if prefix else k
        result.add(full)
        if isinstance(v, dict):
            flatten_keys(v, full, result)
        elif isinstance(v, list) and v and isinstance(v[0], dict):
            flatten_keys(v[0], f"{full}[]", result)
    return result


# ─────────────────────────────────────────────
# 1. VESSELS AUDIT
# ─────────────────────────────────────────────
print(SEP)
print("🚢  VESSELS COLLECTION AUDIT")
print(SEP)

vessel_count = db.vessels.count_documents({})
print(f"Total vessels: {vessel_count}")

# All keys across all vessels
all_vessel_keys = set()
for doc in db.vessels.find({}, {"_id": 0}):
    all_vessel_keys |= flatten_keys(doc)

print(f"\nAll fields ({len(all_vessel_keys)}):")
for k in sorted(all_vessel_keys):
    print(f"  {k}")

# Sample vessel
print("\nSample vessel document:")
sample = db.vessels.find_one({}, {"_id": 0})
print(json.dumps(sample, indent=2, default=str)[:1500])


# ─────────────────────────────────────────────
# 2. VOYAGES AUDIT
# ─────────────────────────────────────────────
print("\n" + SEP)
print("🛳️  VOYAGES COLLECTION AUDIT")
print(SEP)

voyage_count = db.voyages.count_documents({})
print(f"Total voyages: {voyage_count}")

# Collect keys from first 50 docs (representative sample)
all_voyage_keys = set()
for doc in db.voyages.find({}, {"_id": 0}).limit(50):
    all_voyage_keys |= flatten_keys(doc)

print(f"\nAll top-level + nested fields ({len(all_voyage_keys)}):")
for k in sorted(all_voyage_keys):
    print(f"  {k}")


# ─────────────────────────────────────────────
# 3. REMARKS AUDIT
# ─────────────────────────────────────────────
print("\n" + SEP)
print("💬  REMARKS AUDIT")
print(SEP)

with_remarks   = []
empty_remarks  = []

for doc in db.voyages.find({}, {"voyageNumber": 1, "voyageId": 1, "vesselName": 1, "remarks": 1, "_id": 0}):
    vnum    = doc.get("voyageNumber")
    vessel  = doc.get("vesselName", "")
    remarks = doc.get("remarks", [])
    real    = [r for r in (remarks or []) if isinstance(r, dict) and r.get("remark", "").strip()]
    if real:
        with_remarks.append({
            "voyage_number": vnum,
            "vessel":        vessel,
            "count":         len(real),
            "sample":        real[0].get("remark", "")[:80]
        })
    else:
        empty_remarks.append(vnum)

print(f"Voyages WITH remarks : {len(with_remarks)}")
print(f"Voyages WITHOUT      : {len(empty_remarks)}")

unique_vnums = sorted(set(r["voyage_number"] for r in with_remarks))
print(f"\nUnique voyage numbers with remarks ({len(unique_vnums)}):")
print(" | ".join(str(v) for v in unique_vnums))

print(f"\n{'VOYAGE#':>8} | {'VESSEL':<22} | CNT | SAMPLE REMARK")
print("-" * 80)
for r in sorted(with_remarks, key=lambda x: str(x["voyage_number"]))[:40]:
    print(f"{str(r['voyage_number']):>8} | {str(r['vessel']):<22} | {r['count']:>3} | {r['sample'][:45]}")


# ─────────────────────────────────────────────
# 4. FULL DOCUMENT STRUCTURE — 3 voyages
# ─────────────────────────────────────────────
print("\n" + SEP)
print("📄  FULL DOCUMENT SAMPLE — Voyage with remarks (1901, 2306, 2305)")
print(SEP)

for vnum in ["1901", "2306", "2305"]:
    doc = db.voyages.find_one({"voyageNumber": vnum}, {"_id": 0})
    if doc:
        # Truncate large arrays for readability
        for key in ("legs", "bunkers", "expenses", "revenues", "emissions"):
            if key in doc and isinstance(doc[key], list) and len(doc[key]) > 1:
                doc[key] = doc[key][:1]
                doc[f"_{key}_truncated"] = "...more items"
        print(f"\n--- Voyage #{vnum} ({doc.get('vesselName')}) ---")
        print(json.dumps(doc, indent=2, default=str)[:2000])
    else:
        print(f"\n--- Voyage #{vnum}: NOT FOUND ---")

client.close()
print("\n" + SEP)
print("✅ AUDIT COMPLETE")
print(SEP)