"""
MongoDB adapter for vessel and voyage data access.

This adapter provides read-only access to MongoDB collections for vessels and voyages,
following the kai-agent architecture constraints.
"""

from typing import Optional, Dict, Any, List
import difflib
import re
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
import os


class MongoAdapter:
    """
    Adapter for MongoDB operations on vessel and voyage collections.
    
    Provides read-only access to vessel and voyage documents as per
    the kai-agent architecture.
    """
    
    def __init__(self, mongo_client: MongoClient, db_name: Optional[str] = None):
        """
        Initialize the MongoAdapter.
        
        Args:
            mongo_client: PyMongo MongoClient instance
            db_name: Database name. If None, uses environment variable MONGO_DB_NAME
        """
        # Get database name from parameter, environment, or default
        if db_name is None:
            db_name = os.getenv("MONGO_DB_NAME", "kai_agent")
        
        self.db: Database = mongo_client[db_name]
        self.vessels: Collection = self.db["vessels"]
        self.voyages: Collection = self.db["voyages"]
    
    # --- Anchor resolution helpers ---
    
    def get_vessel_imo_by_name(self, vessel_name: str) -> Optional[str]:
        """
        Resolve vessel IMO number by vessel name.
        
        Input: vessel_name (string)
        Output: imo (string) or None if not found
        
        Args:
            vessel_name: Name of the vessel to look up
            
        Returns:
            IMO number as string, or None if vessel not found
        """
        if not vessel_name:
            return None

        projection: Dict[str, int] = {"imo": 1, "name": 1}

        # 1) Exact match
        query: Dict[str, str] = {"name": vessel_name}
        result = self.vessels.find_one(query, projection)
        if result and "imo" in result:
            return str(result["imo"])

        # 2) Case-insensitive exact match
        query = {"name": {"$regex": f"^{re.escape(vessel_name)}$", "$options": "i"}}
        result = self.vessels.find_one(query, projection)
        if result and "imo" in result:
            return str(result["imo"])

        # 3) Case-insensitive substring match (partial)
        query = {"name": {"$regex": re.escape(vessel_name), "$options": "i"}}
        result = self.vessels.find_one(query, projection)
        if result and "imo" in result:
            return str(result["imo"])

        # 4) Fuzzy match using difflib
        token = vessel_name.strip().split()[0][:4]
        cand_query = {"name": {"$regex": re.escape(token), "$options": "i"}}
        cursor = self.vessels.find(cand_query, projection).limit(1000)
        candidates = []
        name_to_imo = {}
        for doc in cursor:
            name = doc.get("name")
            imo = doc.get("imo")
            if name:
                candidates.append(name)
                name_to_imo[name] = imo

        # If no candidates found, try broader pull
        if not candidates:
            cursor = self.vessels.find({}, projection).limit(2000)
            for doc in cursor:
                name = doc.get("name")
                imo = doc.get("imo")
                if name:
                    candidates.append(name)
                    name_to_imo[name] = imo

        if candidates:
            best = difflib.get_close_matches(vessel_name, candidates, n=1, cutoff=0.75)
            if best:
                matched_name = best[0]
                imo_val = name_to_imo.get(matched_name)
                if imo_val:
                    return str(imo_val)

        return None
    
    def get_voyage_id_by_number(self, voyage_number: str | int) -> Optional[str]:
        """
        Resolve voyage ID by voyage number.
        
        Input: voyage_number (string/int)
        Output: voyageId (string) or None if not found
        
        Args:
            voyage_number: Voyage number to look up (can be string or int)
            
        Returns:
            Voyage ID as string, or None if voyage not found
        """
        # Convert to string since MongoDB stores voyageNumber as string
        voyage_number_str = str(voyage_number)
        
        query: Dict[str, str] = {"voyageNumber": voyage_number_str}
        projection: Dict[str, int] = {"voyageId": 1}
        result = self.voyages.find_one(query, projection)
        
        if result and "voyageId" in result:
            return str(result["voyageId"])
        return None
    
    # --- Entity fetch ---
    
    def fetch_vessel(self, imo: str, projection: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Fetch full vessel document by IMO.
        
        Args:
            imo: IMO number of the vessel
            projection: Optional MongoDB projection dictionary to limit fields
            
        Returns:
            Vessel document as dictionary, or None if not found
        """
        query: Dict[str, str] = {"imo": imo}
        doc = self.vessels.find_one(query, projection)
        return doc
    
    def fetch_voyage(self, voyage_id: str, projection: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Fetch full voyage document by voyage ID.
        
        Args:
            voyage_id: Voyage ID to fetch
            projection: Optional MongoDB projection dictionary to limit fields
            
        Returns:
            Voyage document as dictionary, or None if not found
        """
        query: Dict[str, str] = {"voyageId": voyage_id}
        
        # Inject $slice for remarks to limit size
        if projection:
            if "remarks" in projection and projection["remarks"] == 1:
                projection["remarks"] = {"$slice": 5}
            if "remarkList" in projection and projection["remarkList"] == 1:
                projection["remarkList"] = {"$slice": 5}
                
        doc = self.voyages.find_one(query, projection)
        
        # Convert 'remarks' to 'remarkList' format for compatibility
        if doc:
            doc = self._normalize_remarks(doc)
        
        return doc
    
    def get_voyage_by_number(self, voyage_number: int, projection: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Get voyage by voyage number (convenience method).
        
        Args:
            voyage_number: Voyage number
            projection: Optional MongoDB projection
            
        Returns:
            Voyage document or None
        """
        voyage_number_str = str(voyage_number)
        query = {"voyageNumber": voyage_number_str}
        
        # Inject $slice for remarks to limit size
        if projection:
            if "remarks" in projection and projection["remarks"] == 1:
                projection["remarks"] = {"$slice": 5}
            if "remarkList" in projection and projection["remarkList"] == 1:
                projection["remarkList"] = {"$slice": 5}
                
        doc = self.voyages.find_one(query, projection)
        
        # Normalize remarks
        if doc:
            doc = self._normalize_remarks(doc)
        
        return doc
    
    # --- Helper method ---
    
    def _normalize_remarks(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert 'remarks' field to 'remarkList' format for backward compatibility.
        
        Data format:
            remarks: [
                {
                    "modifiedDate": "2023-11-24T08:08:51.467",
                    "modifiedByFull": "Hasbo, Gustav",
                    "remark": "FDA Antwerp approved."
                },
                ...
            ]
        
        Converts to:
            remarkList: [
                {
                    "title": "Hasbo, Gustav",
                    "text": "FDA Antwerp approved.",
                    "date": "2023-11-24T08:08:51.467"
                },
                ...
            ]
        
        Args:
            doc: Voyage document
            
        Returns:
            Document with normalized remarks
        """
        # If document has 'remarks' but not 'remarkList', convert
        if 'remarks' in doc and isinstance(doc['remarks'], list):
            if not doc.get('remarkList'):
                doc['remarkList'] = [
                    {
                        'title': remark.get('modifiedByFull', 'Unknown'),
                        'text': remark.get('remark', ''),
                        'date': remark.get('modifiedDate', '')
                    }
                    for remark in doc['remarks']
                ]
        
        # If remarkList doesn't exist but remarks is empty/None, set remarkList to empty
        if 'remarkList' not in doc:
            doc['remarkList'] = []
        
        return doc

    def find_many(
        self,
        *,
        collection: str,
        filt: Dict[str, Any],
        projection: Optional[Dict[str, int]] = None,
        sort: Optional[Dict[str, int]] = None,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        col = self.db[collection]
        cur = col.find(filt or {}, projection or {"_id": 0})
        if sort:
            cur = cur.sort(list(sort.items()))
        cur = cur.limit(int(limit))
        return list(cur)