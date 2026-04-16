"""
MongoDB adapter for vessel and voyage data access.

This adapter provides read-only access to MongoDB collections for vessels and voyages,
following the kai-agent architecture constraints.
"""

from typing import Optional, Dict, Any, List
import difflib
import re
from pymongo import MongoClient


def narrow_voyage_rows_by_entity_slots(
    rows: List[Dict[str, Any]],
    slots: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    When several Mongo voyage rows share the same voyageNumber, keep the full list
    or reduce to a single row using only structured slot anchors (vessel_name, imo).

    No user-text heuristics: matching is against voyage document fields only.
    """
    if not rows or len(rows) <= 1:
        return rows
    sl = slots if isinstance(slots, dict) else {}

    def _imo_digits(x: Any) -> str:
        s = str(x or "").strip().replace(".0", "")
        return "".join(ch for ch in s if ch.isdigit())

    slot_imo = _imo_digits(sl.get("imo") or sl.get("vessel_imo"))
    slot_vn = str(sl.get("vessel_name") or "").strip()
    slot_cf = slot_vn.casefold() if slot_vn else ""

    def _doc_vessel_name(d: Dict[str, Any]) -> str:
        nm = d.get("vesselName") or d.get("vessel_name")
        return str(nm).strip() if nm not in (None, "") else ""

    def _doc_imo(d: Dict[str, Any]) -> str:
        return _imo_digits(d.get("vesselImo") or d.get("vessel_imo") or d.get("imo"))

    pool = list(rows)
    if slot_imo:
        imo_hits = [r for r in pool if _doc_imo(r) == slot_imo]
        if len(imo_hits) == 1:
            return imo_hits
        if imo_hits:
            pool = imo_hits
    if slot_cf:
        name_hits = []
        for r in pool:
            dcf = _doc_vessel_name(r).casefold()
            if not dcf:
                continue
            if dcf == slot_cf or slot_cf in dcf or dcf in slot_cf:
                name_hits.append(r)
        if len(name_hits) == 1:
            return name_hits
    return pool
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

        def _imo_from_result(result: Optional[Dict[str, Any]]) -> Optional[str]:
            if not result or "imo" not in result:
                return None
            s = str(result.get("imo") or "").strip()
            return s or None

        # 1) Exact match
        query: Dict[str, str] = {"name": vessel_name}
        result = self.vessels.find_one(query, projection)
        imo_s = _imo_from_result(result)
        if imo_s is not None:
            return imo_s

        # 2) Case-insensitive exact match
        query = {"name": {"$regex": f"^{re.escape(vessel_name)}$", "$options": "i"}}
        result = self.vessels.find_one(query, projection)
        imo_s = _imo_from_result(result)
        if imo_s is not None:
            return imo_s

        # 3) Case-insensitive substring match (partial)
        query = {"name": {"$regex": re.escape(vessel_name), "$options": "i"}}
        result = self.vessels.find_one(query, projection)
        imo_s = _imo_from_result(result)
        if imo_s is not None:
            return imo_s

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
                if imo_val is not None:
                    s = str(imo_val).strip()
                    if s:
                        return s

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
        try:
            voyage_number_str = str(int(float(voyage_number)))
        except Exception:
            voyage_number_str = str(voyage_number or "").strip()
        
        query: Dict[str, str] = {"voyageNumber": voyage_number_str}
        projection: Dict[str, int] = {"voyageId": 1}
        try:
            n = int(self.voyages.count_documents(query))
        except Exception:
            n = 0
        if n != 1:
            return None
        result = self.voyages.find_one(query, projection)
        if result and "voyageId" in result:
            return str(result["voyageId"])
        return None

    def count_voyages_by_number(self, voyage_number: int | str) -> int:
        """Count Mongo voyage documents for this voyageNumber (may exceed 1)."""
        try:
            vn = str(int(float(voyage_number)))
        except Exception:
            return 0
        try:
            return int(self.voyages.count_documents({"voyageNumber": vn}))
        except Exception:
            return 0

    def list_voyages_by_number(
        self,
        voyage_number: int,
        projection: Optional[Dict[str, Any]] = None,
        *,
        limit: int = 40,
    ) -> List[Dict[str, Any]]:
        """
        All voyage documents sharing this voyageNumber (non-unique in real data).

        Callers should disambiguate with voyageId and/or vessel anchors from slots.
        """
        voyage_number_str = str(voyage_number)
        query = {"voyageNumber": voyage_number_str}
        proj = dict(projection) if projection else None
        if proj:
            if "remarks" in proj and proj.get("remarks") == 1:
                proj["remarks"] = {"$slice": 5}
            if "remarkList" in proj and proj.get("remarkList") == 1:
                proj["remarkList"] = {"$slice": 5}
        cap = max(1, min(int(limit), 100))
        cur = self.voyages.find(query, proj).limit(cap)
        out: List[Dict[str, Any]] = []
        for doc in cur:
            if isinstance(doc, dict):
                out.append(self._normalize_remarks(dict(doc)))
        return out

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

    def fetch_vessel_by_name(self, vessel_name: str, projection: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Fetch vessel document by vessel name.

        Tries exact, case-insensitive exact, then case-insensitive substring matching.
        """
        value = str(vessel_name or "").strip()
        if not value:
            return None

        queries: List[Dict[str, Any]] = [
            {"name": value},
            {"name": {"$regex": f"^{re.escape(value)}$", "$options": "i"}},
            {"name": {"$regex": re.escape(value), "$options": "i"}},
        ]
        for query in queries:
            doc = self.vessels.find_one(query, projection)
            if doc:
                return doc
        return None
    
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
                
        docs = self.list_voyages_by_number(voyage_number, projection=projection, limit=2)
        if not docs:
            return None
        if len(docs) == 1:
            return docs[0]
        return None

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