"""
Simple script to insert vessel and voyage data into MongoDB.
"""

import sys
import os
import json
from pathlib import Path

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config.database import get_mongo_db


def main():
    # Get data directory
    data_dir = Path(__file__).parent.parent / "app" / "data" / "VMS DATA LAKE - 09 Sep 2024"
    
    # Connect to MongoDB
    db = get_mongo_db()
    vessels_collection = db["vessels"]
    voyages_collection = db["voyages"]
    
    # Load vessels
    vessel_dir = data_dir / "VesselRegisterDetailRange2022-01-01_2024-09-06"
    vessel_files = list(vessel_dir.glob("vessel_*.json"))
    
    print(f"Loading {len(vessel_files)} vessels...")
    for vessel_file in vessel_files:
        with open(vessel_file, 'r', encoding='utf-8') as f:
            vessel_data = json.load(f)
        vessels_collection.insert_one(vessel_data)
    print(f"✅ Inserted {len(vessel_files)} vessels")
    
    # Load voyages
    voyage_dir = data_dir / "VoyageRange2022-01-01_2024-09-06"
    voyage_files = list(voyage_dir.glob("voyage_*.json"))
    
    print(f"Loading {len(voyage_files)} voyages...")
    for voyage_file in voyage_files:
        with open(voyage_file, 'r', encoding='utf-8') as f:
            voyage_data = json.load(f)
        voyages_collection.insert_one(voyage_data)
    print(f"✅ Inserted {len(voyage_files)} voyages")
    
    print("Done!")


if __name__ == "__main__":
    main()
