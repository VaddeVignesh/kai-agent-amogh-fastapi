# app/agents/mongo_agent.py
"""
MongoDB Agent - Handles entity resolution and document fetching.
Uses minimal projections to reduce token usage.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Literal

from app.adapters.mongo_adapter import MongoAdapter, narrow_voyage_rows_by_entity_slots
from app.llm.mongo_query_builder import MongoQueryBuilder
from app.mongo.mongo_guard import validate_mongo_spec
from app.orchestration.mongo_schema import mongo_schema_hint

# ---------------------------------------
# Types
# ---------------------------------------
AnchorType = Literal["VESSEL", "VOYAGE"]


@dataclass
class MongoAgentResponse:
    anchor_type: AnchorType
    anchor_id: str
    document: Dict[str, Any]


class MongoAgent:
    """
    MongoAgent - Entity resolution and document fetching with minimal projections.
    """

    def __init__(self, mongo_adapter: MongoAdapter, llm_client=None):
        self.mongo = mongo_adapter
        self.adapter = mongo_adapter  # Alias for new context fetching approach
        self.llm = llm_client
        self.builder = MongoQueryBuilder(llm_client) if llm_client else None

    # -------------------------------------------------------
    # Resolve + Fetch full voyage context
    # -------------------------------------------------------
    def fetch_full_voyage_context(
        self,
        *,
        voyage_number: Optional[int] = None,
        voyage_id: Optional[str] = None,
        entity_slots: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:

        doc = None

        if voyage_number is not None:
            try:
                vn_int = int(float(voyage_number))
            except Exception:
                vn_int = None
        else:
            vn_int = None

        if vn_int is not None:
            proj = {
                "voyageId": 1,
                "voyageNumber": 1,
                "vesselName": 1,
                "vesselImo": 1,
                "startDateUtc": 1,
                "extracted_at": 1,
                "remarks": 1,
                "remarkList": 1,
                "fixtures": 1,
                "legs": 1,
                "revenues": 1,
                "expenses": 1,
            }
            doc = self.adapter.get_voyage_by_number(vn_int, projection=proj)
            if not doc:
                batch = self.adapter.list_voyages_by_number(
                    vn_int, projection=proj, limit=40
                )
                narrowed = narrow_voyage_rows_by_entity_slots(batch, entity_slots or {})
                if len(narrowed) == 1:
                    doc = narrowed[0]
                elif narrowed:
                    # Ambiguous voyageNumber: pick a single coherent doc by anchor completeness.
                    def _score(d: Dict[str, Any]) -> tuple[str, int, int, str]:
                        if not isinstance(d, dict):
                            return ("", -1, -1, "")
                        # Prefer more recent voyage snapshots first (ISO-8601 strings are lex-sortable).
                        recency = str(d.get("startDateUtc") or d.get("extracted_at") or "")
                        anchor = int(bool(str(d.get("vesselImo") or "").strip())) + int(bool(str(d.get("vesselName") or "").strip()))
                        payload = int(bool(d.get("fixtures"))) + int(bool(d.get("legs"))) + int(bool(d.get("expenses"))) + int(bool(d.get("revenues")))
                        vid = str(d.get("voyageId") or "")
                        return (recency, anchor, payload, vid)
                    doc = sorted(narrowed, key=_score, reverse=True)[0]

        elif voyage_id:
            doc = self.adapter.fetch_voyage(
                voyage_id,
                projection={
                    "voyageId": 1,
                    "voyageNumber": 1,
                    "vesselName": 1,
                    "vesselImo": 1,
                    "remarks": 1,
                    "remarkList": 1,
                    "fixtures": 1,
                    "legs": 1,
                    "revenues": 1,
                    "expenses": 1,
                },
            )

        if not doc:
            return {}

        return {
            "voyage_id": doc.get("voyageId"),
            "voyage_number": doc.get("voyageNumber"),
            "vessel_name": doc.get("vesselName"),
            "vessel_imo": doc.get("vesselImo"),
            "remarks": doc.get("remarks") or doc.get("remarkList", []),
            "fixtures": doc.get("fixtures", []),
            "legs": doc.get("legs", []),
            "revenues": doc.get("revenues", []),
            "expenses": doc.get("expenses", []),
        }

    def run_llm_find(
        self,
        *,
        question: str,
        slots: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        LLM -> (collection, filter, projection) -> execute -> return SMALL payload.
        """
        if self.builder is None:
            raise ValueError("MongoAgent.run_llm_find requires llm_client")

        hint = mongo_schema_hint()

        # Normalize slots for Mongo conventions without disturbing the SQL pipeline:
        # - Mongo stores voyageNumber as STRING (e.g. "1901")
        # - IDs should be strings for stable matching
        slots_for_llm = dict(slots or {})

        def _voyage_num_to_str(v: Any) -> Any:
            if v is None:
                return None
            if isinstance(v, bool):
                return str(v)
            if isinstance(v, (int, float)):
                try:
                    return str(int(v))
                except Exception:
                    return str(v)
            if isinstance(v, str):
                s = v.strip()
                # Handle "1901.0" style strings
                try:
                    if "." in s:
                        return str(int(float(s)))
                except Exception:
                    pass
                return s
            return str(v)

        if "voyage_number" in slots_for_llm:
            slots_for_llm["voyage_number"] = _voyage_num_to_str(slots_for_llm.get("voyage_number"))
        if "voyage_numbers" in slots_for_llm:
            vns = slots_for_llm.get("voyage_numbers")
            if isinstance(vns, list):
                slots_for_llm["voyage_numbers"] = [_voyage_num_to_str(x) for x in vns]
            else:
                slots_for_llm["voyage_numbers"] = _voyage_num_to_str(vns)
        if "voyage_id" in slots_for_llm:
            slots_for_llm["voyage_id"] = str(slots_for_llm["voyage_id"])
        if "voyage_ids" in slots_for_llm and isinstance(slots_for_llm.get("voyage_ids"), list):
            slots_for_llm["voyage_ids"] = [str(x) for x in slots_for_llm["voyage_ids"] if x is not None]

        spec = self.builder.build(question=question, schema_hint=hint, slots=slots_for_llm)

        guard = validate_mongo_spec(
            collection=spec.collection,
            filt=spec.filter,
            projection=spec.projection,
            sort=spec.sort,
            limit=spec.limit,
            allowed_collections=set(hint["collections"].keys()),
            allowed_ops=set(hint["allowed_operators"]),
        )

        if not guard.ok:
            return {"mode": "mongo_llm", "ok": False, "reason": guard.reason, "rows": []}

        rows = self.mongo.find_many(
            collection=guard.collection,
            filt=guard.filter,
            projection=guard.projection,
            sort=guard.sort,
            limit=guard.limit,
        )

        return {
            "mode": "mongo_llm",
            "ok": True,
            "collection": guard.collection,
            "filter": guard.filter,
            "projection": guard.projection,
            "limit": guard.limit,
            "rows": rows,
        }

    # =========================================================
    # Public entry
    # =========================================================
    def run(
        self,
        intent_key: str,
        slots: Dict[str, Any],
        projection: Optional[Dict[str, int]] = None,
        session_context: Optional[Dict[str, Any]] = None,
    ) -> MongoAgentResponse:
        """
        Main entry point for mongo agent.
        
        intent_key:
          - "vessel.entity"
          - "voyage.entity"
          - "entity.auto"
          - "entity.skip"
          - "vessel.list_all"
          - "voyage.by_vessel"
        
        Args:
            intent_key: Type of query to perform
            slots: Query parameters (voyage_number, vessel_name, etc.)
            projection: MongoDB projection dict (if None, uses minimal default)
            session_context: Session state for follow-up queries
        
        Returns:
            MongoAgentResponse with anchor_type, anchor_id, and document
        """
        session_context = session_context or {}
        
        # Skip mongo entirely
        if intent_key == "entity.skip":
            return MongoAgentResponse(
                anchor_type="VOYAGE",
                anchor_id="SKIP",
                document={}
            )

        anchor_type, anchor_id = self._resolve_anchor(
            intent_key=intent_key,
            slots=slots,
            session_context=session_context,
        )

        document = self._fetch_document(
            intent_key=intent_key,
            anchor_type=anchor_type,
            anchor_id=anchor_id,
            projection=projection,
        )

        return MongoAgentResponse(
            anchor_type=anchor_type,
            anchor_id=anchor_id,
            document=document,
        )

    # =========================================================
    # Safe printing (Windows console encoding)
    # =========================================================
    @staticmethod
    def _safe_print(msg: str) -> None:
        try:
            print(msg)
        except UnicodeEncodeError:
            try:
                print(str(msg).encode("ascii", errors="backslashreplace").decode("ascii"))
            except Exception:
                return

    # =========================================================
    # Anchor resolution
    # =========================================================
    def _resolve_anchor(
        self,
        intent_key: str,
        slots: Dict[str, Any],
        session_context: Dict[str, Any],
    ) -> tuple[AnchorType, str]:
        """
        Resolve the anchor (entity type and ID) for the query.
        """
        # Fleet-wide vessel list
        if intent_key == "vessel.list_all":
            return "VESSEL", "ALL"

        # Voyage by vessel query
        if intent_key == "voyage.by_vessel":
            vessel_name = slots.get("vessel_name")
            if vessel_name:
                return "VESSEL", vessel_name
            raise ValueError("Missing vessel_name for voyage.by_vessel query")

        # ---- Explicit voyage intent ----
        if intent_key.startswith("voyage."):
            voyage_id = self._resolve_voyage_id(slots)
            if voyage_id:
                return "VOYAGE", voyage_id
            raise ValueError("Missing voyage identifier (voyage_id or voyage_number).")

        # ---- Explicit vessel intent (registry uses entity.vessel as well as vessel.*) ----
        if intent_key == "entity.vessel" or intent_key.startswith("vessel."):
            imo = self._resolve_vessel_imo(slots)
            if imo and str(imo).strip():
                return "VESSEL", str(imo).strip()
            vn = slots.get("vessel_name")
            if vn and str(vn).strip():
                return "VESSEL", str(vn).strip()
            raise ValueError("Missing vessel identifier (imo or vessel_name).")

        # ---- AUTO detection ----
        voyage_id = self._resolve_voyage_id(slots)
        if voyage_id:
            return "VOYAGE", voyage_id

        imo = self._resolve_vessel_imo(slots)
        if imo and str(imo).strip():
            return "VESSEL", str(imo).strip()

        # ---- Follow-up: use session context ----
        if (
            session_context.get("anchor_type") in ("VESSEL", "VOYAGE")
            and session_context.get("anchor_id")
        ):
            return (
                session_context["anchor_type"],
                session_context["anchor_id"],
            )

        raise ValueError("Unable to resolve vessel or voyage anchor.")

    # =========================================================
    # Identifier resolution
    # =========================================================
    def _resolve_vessel_imo(self, slots: Dict[str, Any]) -> Optional[str]:
        """Resolve vessel IMO from slots"""
        # Direct IMO
        raw_imo = slots.get("imo")
        if raw_imo is not None and str(raw_imo).strip():
            return str(raw_imo).strip()

        # Resolve via vessel name
        if slots.get("vessel_name"):
            return self.mongo.get_vessel_imo_by_name(
                str(slots["vessel_name"])
            )

        return None

    def _resolve_voyage_id(self, slots: Dict[str, Any]) -> Optional[str]:
        """Resolve voyage ID from slots"""
        # Direct voyage_id
        if slots.get("voyage_id"):
            return str(slots["voyage_id"])

        # Resolve via voyage number
        if slots.get("voyage_number"):
            return self.mongo.get_voyage_id_by_number(
                slots["voyage_number"]
            )

        return None

    # Fetch document with minimal projections
    def _fetch_document(
        self,
        intent_key: str,
        anchor_type: AnchorType,
        anchor_id: str,
        projection: Optional[Dict[str, int]] = None,
    ) -> Dict[str, Any]:
        """
        Fetch document from MongoDB with intelligent projection handling.
        
        If no projection specified, uses minimal default projection.
        instead of fetching entire document (which can be 50,000+ characters!)
        
        Args:
            intent_key: Query intent
            anchor_type: VESSEL or VOYAGE
            anchor_id: Entity identifier
            projection: MongoDB projection (if None, uses minimal default)
        
        Returns:
            Document dictionary
        """
        
        # Use minimal projection if none specified
        if projection is None:
            # Default to MINIMAL fields only - NOT everything!
            proj = {
                "_id": 0,
                # Voyage fields
                "voyageId": 1,
                "voyageNumber": 1,
                "vesselName": 1,
                "voyageStatus": 1,
                # Vessel fields
                "imo": 1,
                "name": 1,
                "vesselStatus": 1,
                # ⚠️ DO NOT include by default:
                # - itinerary (huge array of ports)
                # - cargoDetails (massive nested objects)
                # - portCalls (100+ events)
                # - events (thousands of entries)
                # - financials (large nested structures)
                # - operations (large nested data)
            }
            self._safe_print("   🔍 Using MINIMAL projection (6 fields)")
        else:
            # Use provided projection
            proj = projection
            self._safe_print(f"   🔍 Using custom projection ({len(projection)} fields)")

        # Handle fleet-wide list
        if anchor_type == "VESSEL" and anchor_id == "ALL":
            vessels = list(self.mongo.vessels.find({}, proj).limit(200))
            self._safe_print(f"   ✅ Fetched {len(vessels)} vessels")
            return {"vessels": vessels, "count": len(vessels)}

        # Handle vessel-based voyage query (name anchor, not IMO)
        if anchor_type == "VESSEL" and not str(anchor_id).isdigit():
            if intent_key == "voyage.by_vessel":
                voyages = list(
                    self.mongo.voyages.find(
                        {"vesselName": {"$regex": anchor_id, "$options": "i"}},
                        proj,
                    ).limit(50)
                )
                self._safe_print(f"   ✅ Fetched {len(voyages)} voyages for vessel {anchor_id}")
                return {"voyages": voyages, "vessel_name": anchor_id, "count": len(voyages)}
            doc = self.mongo.fetch_vessel_by_name(str(anchor_id), projection=proj)
            if doc:
                self._safe_print(f"   ✅ Vessel doc (by name): {anchor_id}")
                return doc
            raise ValueError(f"Vessel not found for name={anchor_id}")

        # Fetch single vessel
        if anchor_type == "VESSEL":
            doc = self.mongo.fetch_vessel(
                imo=str(anchor_id),
                projection=proj,
            )
            if not doc:
                raise ValueError(f"Vessel not found for IMO={anchor_id}")
            
            # Track document size
            doc_size = len(str(doc))
            doc_tokens = doc_size // 4
            
            if doc_size > 5000:
                self._safe_print(f"   ⚠️  Large vessel doc: {doc_size:,} chars (~{doc_tokens:,} tokens)")
            else:
                self._safe_print(f"   ✅ Vessel doc: {doc_size:,} chars (~{doc_tokens:,} tokens)")
            
            return doc

        # Fetch single voyage
        if anchor_type == "VOYAGE":
            doc = self.mongo.fetch_voyage(
                voyage_id=str(anchor_id),
                projection=proj,
            )
            if not doc:
                raise ValueError(f"Voyage not found for voyage_id={anchor_id}")
            
            # Track document size
            doc_size = len(str(doc))
            doc_tokens = doc_size // 4
            
            if doc_size > 5000:
                self._safe_print(f"   ⚠️  Large voyage doc: {doc_size:,} chars (~{doc_tokens:,} tokens)")
            else:
                self._safe_print(f"   ✅ Voyage doc: {doc_size:,} chars (~{doc_tokens:,} tokens)")
            
            return doc

        raise ValueError(f"Unsupported anchor_type={anchor_type}")