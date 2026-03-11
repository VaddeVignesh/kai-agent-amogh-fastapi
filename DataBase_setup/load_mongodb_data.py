"""
Load Vessel and Voyage JSON data into MongoDB
"""

import json
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

print("="*100)
print("📊 LOADING MONGODB DATA")
print("="*100)

# File paths
VESSEL_FILE = r'D:\Downloads\vessel_nosql_data_fixed (1).json'
VOYAGE_FILE = r'D:\Downloads\voyage_nosql_data_fixed (1).json'

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["kai_agent"]

# Collections
vessels_collection = db["vessels"]
voyages_collection = db["voyages"]

print("\n✅ Connected to MongoDB")

# ============================================
# LOAD VESSELS
# ============================================

print("\n" + "="*100)
print("🚢 LOADING VESSELS")
print("="*100)

print(f"\n1️⃣ Reading {VESSEL_FILE}...")
with open(VESSEL_FILE, 'r', encoding='utf-8') as f:
    vessels_data = json.load(f)

print(f"   ✅ Loaded {len(vessels_data)} vessels from file")

print("\n2️⃣ Clearing existing vessels...")
vessels_collection.delete_many({})
print("   ✅ Cleared")

print("\n3️⃣ Inserting vessels into MongoDB...")
try:
    if isinstance(vessels_data, list):
        result = vessels_collection.insert_many(vessels_data, ordered=False)
        print(f"   ✅ Inserted {len(result.inserted_ids)} vessels")
    else:
        result = vessels_collection.insert_one(vessels_data)
        print(f"   ✅ Inserted 1 vessel")
except BulkWriteError as e:
    print(f"   ⚠️ Partial insert: {e.details['nInserted']} succeeded, {len(e.details['writeErrors'])} failed")

# Verify
count = vessels_collection.count_documents({})
print(f"\n4️⃣ Verification: {count} vessels in collection")

# Sample vessel
sample = vessels_collection.find_one({}, {"_id": 0, "name": 1, "imo": 1})
if sample:
    print(f"   Sample: {sample}")

# Create index on imo and name
print("\n5️⃣ Creating indexes...")
vessels_collection.create_index("imo")
vessels_collection.create_index("name")
print("   ✅ Indexes created (imo, name)")

# ============================================
# LOAD VOYAGES
# ============================================

print("\n" + "="*100)
print("🛳️ LOADING VOYAGES")
print("="*100)

print(f"\n1️⃣ Reading {VOYAGE_FILE}...")
print("   ⚠️ Large file (44MB), this may take a minute...")

with open(VOYAGE_FILE, 'r', encoding='utf-8') as f:
    voyages_data = json.load(f)

print(f"   ✅ Loaded {len(voyages_data)} voyages from file")

print("\n2️⃣ Clearing existing voyages...")
voyages_collection.delete_many({})
print("   ✅ Cleared")

print("\n3️⃣ Inserting voyages into MongoDB...")
print("   ⚠️ This will take a few minutes for large dataset...")

# Insert in batches for better performance
batch_size = 1000
total_inserted = 0

if isinstance(voyages_data, list):
    for i in range(0, len(voyages_data), batch_size):
        batch = voyages_data[i:i+batch_size]
        try:
            result = voyages_collection.insert_many(batch, ordered=False)
            total_inserted += len(result.inserted_ids)
            print(f"   Progress: {total_inserted}/{len(voyages_data)} voyages inserted...")
        except BulkWriteError as e:
            total_inserted += e.details['nInserted']
            print(f"   ⚠️ Batch {i//batch_size + 1}: {e.details['nInserted']} succeeded")
    
    print(f"\n   ✅ Inserted {total_inserted} voyages")
else:
    result = voyages_collection.insert_one(voyages_data)
    print(f"   ✅ Inserted 1 voyage")

# Verify
count = voyages_collection.count_documents({})
print(f"\n4️⃣ Verification: {count} voyages in collection")

# Sample voyage
sample = voyages_collection.find_one({}, {"_id": 0, "voyageId": 1, "voyageNumber": 1, "vesselName": 1})
if sample:
    print(f"   Sample: {sample}")

# Create indexes
print("\n5️⃣ Creating indexes...")
voyages_collection.create_index("voyageId")
voyages_collection.create_index("voyageNumber")
voyages_collection.create_index("vesselImo")
print("   ✅ Indexes created (voyageId, voyageNumber, vesselImo)")

# ============================================
# FINAL SUMMARY
# ============================================

print("\n" + "="*100)
print("📊 FINAL SUMMARY")
print("="*100)

vessels_count = vessels_collection.count_documents({})
voyages_count = voyages_collection.count_documents({})

print(f"\n✅ Vessels: {vessels_count} documents")
print(f"✅ Voyages: {voyages_count} documents")

if vessels_count > 0 and voyages_count > 0:
    print("\n" + "="*100)
    print("🎉 MONGODB DATA LOADED SUCCESSFULLY!")
    print("="*100)
else:
    print("\n⚠️ Warning: Some collections are empty!")

# Cleanup
client.close()
print("\n✅ MongoDB connection closed")
