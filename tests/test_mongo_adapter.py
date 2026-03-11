"""
Custom MongoDB Adapter Test Suite (manual).

NOTE:
- This file is a *manual script* and connects to a local MongoDB instance.
- It is NOT safe to run in automated CI/pytest because it executes at import time.

Run manually:
  python tests/test_mongo_adapter.py
"""

# Prevent pytest from importing/executing this module during test discovery.
__test__ = False

from pymongo import MongoClient
import json

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["stena_voyages"]

print("="*80)
print("🧪 CUSTOM MONGODB ADAPTER TEST SUITE")
print("="*80)
print(f"Database: {db.name}")
print(f"Collections: {db.list_collection_names()}")

# Test counters
total_tests = 0
passed_tests = 0
failed_tests = 0

def test_result(test_name, condition, details=""):
    global total_tests, passed_tests, failed_tests
    total_tests += 1
    if condition:
        passed_tests += 1
        print(f"✅ PASS: {test_name}")
        if details:
            print(f"   → {details}")
    else:
        failed_tests += 1
        print(f"❌ FAIL: {test_name}")
        if details:
            print(f"   → {details}")
    print()

print("\n" + "="*80)
print("📊 SECTION 1: DATABASE INTEGRITY CHECKS")
print("="*80 + "\n")

# Test 1: Vessel count
vessel_count = db.vessels.count_documents({})
test_result(
    "Vessel Collection Count",
    vessel_count == 165,
    f"Expected 165, Got {vessel_count}"
)

# Test 2: Voyage count
voyage_count = db.voyages.count_documents({})
test_result(
    "Voyage Collection Count",
    voyage_count == 1500,
    f"Expected 1500, Got {voyage_count}"
)

# Test 3: Check for duplicate IMOs
duplicate_imos = list(db.vessels.aggregate([
    {"$group": {"_id": "$imo", "count": {"$sum": 1}}},
    {"$match": {"count": {"$gt": 1}}}
]))
test_result(
    "No Duplicate IMO Numbers",
    len(duplicate_imos) == 0,
    f"Found {len(duplicate_imos)} duplicate IMOs" if duplicate_imos else "All IMOs are unique"
)

# Test 4: Check for duplicate voyage IDs
duplicate_voyages = list(db.voyages.aggregate([
    {"$group": {"_id": "$voyageId", "count": {"$sum": 1}}},
    {"$match": {"count": {"$gt": 1}}}
]))
test_result(
    "No Duplicate Voyage IDs",
    len(duplicate_voyages) == 0,
    f"Found {len(duplicate_voyages)} duplicate voyage IDs" if duplicate_voyages else "All voyage IDs are unique"
)

print("="*80)
print("🔍 SECTION 2: VESSEL QUERY TESTS")
print("="*80 + "\n")

# Test 5: Find vessel by exact name
vessel = db.vessels.find_one({"name": "Elka Delphi"})
test_result(
    "Find Vessel by Exact Name (Elka Delphi)",
    vessel is not None and vessel['imo'] == '9705902',
    f"Found: {vessel['name']} (IMO: {vessel['imo']})" if vessel else "Not found"
)

# Test 6: Find vessel by IMO
vessel_by_imo = db.vessels.find_one({"imo": "9705902"})
test_result(
    "Find Vessel by IMO (9705902)",
    vessel_by_imo is not None and vessel_by_imo['name'] == 'Elka Delphi',
    f"Found: {vessel_by_imo['name']}" if vessel_by_imo else "Not found"
)

# Test 7: Case-insensitive vessel search
vessel_case_insensitive = db.vessels.find_one({"name": {"$regex": "^elka delphi$", "$options": "i"}})
test_result(
    "Case-Insensitive Vessel Search",
    vessel_case_insensitive is not None,
    f"Found: {vessel_case_insensitive['name']}" if vessel_case_insensitive else "Not found"
)

# Test 8: Partial vessel name search
partial_vessels = list(db.vessels.find({"name": {"$regex": "Stena", "$options": "i"}}).limit(5))
test_result(
    "Partial Name Search (vessels containing 'Stena')",
    len(partial_vessels) > 0,
    f"Found {len(partial_vessels)} vessels"
)

# Test 9: Get vessels with scrubbers
scrubber_vessels = list(db.vessels.find({"scrubber": "Yes"}).limit(3))
test_result(
    "Find Vessels with Scrubbers",
    len(scrubber_vessels) > 0,
    f"Found {db.vessels.count_documents({'scrubber': 'Yes'})} vessels with scrubbers"
)

# Test 10: Get vessels by market type
tanker_vessels = db.vessels.count_documents({"marketType": "Tanker"})
test_result(
    "Count Vessels by Market Type (Tanker)",
    tanker_vessels > 0,
    f"Found {tanker_vessels} tanker vessels"
)

print("="*80)
print("🚢 SECTION 3: VOYAGE QUERY TESTS")
print("="*80 + "\n")

# Test 11: Find voyage by voyage number
voyage = db.voyages.find_one({"voyageNumber": "2306"})
test_result(
    "Find Voyage by Number (2306)",
    voyage is not None and voyage['voyageId'] == 'ABCD2C938DC82BD236B0161F5E88E263',
    f"Found: Voyage {voyage['voyageNumber']} for {voyage['vesselName']}" if voyage else "Not found"
)

# Test 12: Find voyage by ID
voyage_by_id = db.voyages.find_one({"voyageId": "ABCD2C938DC82BD236B0161F5E88E263"})
test_result(
    "Find Voyage by ID",
    voyage_by_id is not None and voyage_by_id['voyageNumber'] == '2306',
    f"Found: Voyage {voyage_by_id['voyageNumber']}" if voyage_by_id else "Not found"
)

# Test 13: Find voyages by vessel name
voyages_by_vessel = list(db.voyages.find({"vesselName": "Stena Imperial"}).limit(3))
test_result(
    "Find Voyages by Vessel Name (Stena Imperial)",
    len(voyages_by_vessel) > 0,
    f"Found {len(voyages_by_vessel)} voyages"
)

# Test 14: Find voyages with fixture data
voyages_with_fixtures = db.voyages.count_documents({"fixtures": {"$exists": True, "$ne": []}})
test_result(
    "Count Voyages with Fixture Data",
    voyages_with_fixtures > 0,
    f"Found {voyages_with_fixtures} voyages with fixtures"
)

# Test 15: Find voyages with emissions data
voyages_with_emissions = db.voyages.count_documents({"emissions": {"$exists": True}})
test_result(
    "Count Voyages with Emissions Data",
    voyages_with_emissions > 0,
    f"Found {voyages_with_emissions} voyages with emissions"
)

print("="*80)
print("⚡ SECTION 4: ADVANCED QUERY TESTS")
print("="*80 + "\n")

# Test 16: Aggregation - Average hire rate
avg_hire = list(db.vessels.aggregate([
    {"$group": {"_id": None, "avgHireRate": {"$avg": "$hireRate"}}}
]))
test_result(
    "Calculate Average Hire Rate",
    len(avg_hire) > 0 and avg_hire[0]['avgHireRate'] > 0,
    f"Average hire rate: ${avg_hire[0]['avgHireRate']:,.2f}/day" if avg_hire else "Failed"
)

# Test 17: Count vessels by pool
vessels_by_pool = list(db.vessels.aggregate([
    {"$unwind": "$tags"},
    {"$match": {"tags.category": "Pool"}},
    {"$group": {"_id": "$tags.value", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 3}
]))
test_result(
    "Group Vessels by Pool",
    len(vessels_by_pool) > 0,
    f"Found {len(vessels_by_pool)} pools"
)

# Test 18: Find high-value voyages (revenue > $500k)
high_value_voyages = db.voyages.count_documents({
    "projected_results.revenue": {"$gt": 500000}
})
test_result(
    "Count High-Value Voyages (Revenue > $500K)",
    high_value_voyages > 0,
    f"Found {high_value_voyages} high-value voyages"
)

# Test 19: Find voyages with bunker consumption data
voyages_with_bunkers = db.voyages.count_documents({
    "bunkers": {"$exists": True, "$ne": []}
})
test_result(
    "Count Voyages with Bunker Data",
    voyages_with_bunkers > 0,
    f"Found {voyages_with_bunkers} voyages with bunker consumption"
)

# Test 20: Projection test - Get specific fields only
vessel_projection = db.vessels.find_one(
    {"imo": "9705902"},
    {"_id": 0, "name": 1, "imo": 1, "hireRate": 1}
)
test_result(
    "Projection Query (Specific Fields Only)",
    vessel_projection is not None and len(vessel_projection.keys()) <= 3,
    f"Retrieved fields: {list(vessel_projection.keys())}" if vessel_projection else "Failed"
)

print("="*80)
print("🔗 SECTION 5: DATA RELATIONSHIP TESTS")
print("="*80 + "\n")

# Test 21: Verify vessel-voyage relationship
test_voyage = db.voyages.find_one({"voyageNumber": "2306"})
if test_voyage:
    vessel_imo = test_voyage.get('vesselImo')
    related_vessel = db.vessels.find_one({"imo": vessel_imo})
    test_result(
        "Vessel-Voyage Relationship Integrity",
        related_vessel is not None,
        f"Voyage 2306 → Vessel IMO {vessel_imo} → Found: {related_vessel['name']}" if related_vessel else "Relationship broken"
    )
else:
    test_result("Vessel-Voyage Relationship Integrity", False, "Test voyage not found")

# Test 22: Count voyages per vessel
voyages_per_vessel = list(db.voyages.aggregate([
    {"$group": {"_id": "$vesselName", "voyageCount": {"$sum": 1}}},
    {"$sort": {"voyageCount": -1}},
    {"$limit": 3}
]))
test_result(
    "Count Voyages per Vessel (Top 3)",
    len(voyages_per_vessel) > 0,
    f"Top vessel: {voyages_per_vessel[0]['_id']} with {voyages_per_vessel[0]['voyageCount']} voyages" if voyages_per_vessel else "Failed"
)

print("="*80)
print("📈 SECTION 6: INDEX PERFORMANCE TESTS")
print("="*80 + "\n")

# Test 23: Check vessel indexes
vessel_indexes = db.vessels.index_information()
required_vessel_indexes = ['imo_1', 'name_1', 'vesselId_1']
indexes_exist = all(idx in vessel_indexes for idx in required_vessel_indexes)
test_result(
    "Vessel Collection Indexes",
    indexes_exist,
    f"Found indexes: {list(vessel_indexes.keys())}"
)

# Test 24: Check voyage indexes
voyage_indexes = db.voyages.index_information()
required_voyage_indexes = ['voyageId_1', 'voyageNumber_1', 'vesselImo_1']
indexes_exist = all(idx in voyage_indexes for idx in required_voyage_indexes)
test_result(
    "Voyage Collection Indexes",
    indexes_exist,
    f"Found indexes: {list(voyage_indexes.keys())}"
)

print("="*80)
print("🎯 SECTION 7: EDGE CASE TESTS")
print("="*80 + "\n")

# Test 25: Query non-existent vessel
non_existent_vessel = db.vessels.find_one({"imo": "0000000"})
test_result(
    "Query Non-Existent Vessel (Should Return None)",
    non_existent_vessel is None,
    "Correctly returned None"
)

# Test 26: Query non-existent voyage
non_existent_voyage = db.voyages.find_one({"voyageNumber": "XXXX9999"})
test_result(
    "Query Non-Existent Voyage (Should Return None)",
    non_existent_voyage is None,
    "Correctly returned None"
)

# Test 27: Empty filter query
all_vessels = db.vessels.count_documents({})
test_result(
    "Query All Vessels (Empty Filter)",
    all_vessels == 165,
    f"Retrieved {all_vessels} vessels"
)

# Test 28: Null/None field handling
vessels_with_null_fields = db.vessels.count_documents({"scrubber": None})
test_result(
    "Handle Null Field Values",
    True,  # Should not crash
    f"Found {vessels_with_null_fields} vessels with null scrubber field"
)

print("\n" + "="*80)
print("📊 TEST SUMMARY")
print("="*80)
print(f"\n✅ Passed: {passed_tests}/{total_tests}")
print(f"❌ Failed: {failed_tests}/{total_tests}")
print(f"📈 Success Rate: {(passed_tests/total_tests*100):.1f}%")

if failed_tests == 0:
    print("\nAll tests passed.")
else:
    print(f"\n⚠️  {failed_tests} test(s) failed. Please review the failures above.")

print("\n" + "="*80)
print("✅ CUSTOM TEST SUITE COMPLETED")
print("="*80)
