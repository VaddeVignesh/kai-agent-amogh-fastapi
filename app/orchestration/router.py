from __future__ import annotations

from typing import Dict, Any, List, Optional

from app.registries.intent_registry import INTENT_REGISTRY, SUPPORTED_INTENTS
from app.agents.mongo_agent import MongoAgent
from app.agents.finance_agent import FinanceAgent
from app.agents.ops_agent import OpsAgent
from app.adapters.redis_store import RedisStore
from app.llm.llm_client import LLMClient

class Router:
    """
    Router (POC)
    - LLM: intent+slot extraction
    - Required slots validation + clarification loop
    - Agent orchestration (mongo/finance/ops)
    - Merge outputs
    - LLM: natural-language answer generation from merged outputs
    """

    def __init__(
        self,
        llm: LLMClient,
        mongo_agent: MongoAgent,
        finance_agent: FinanceAgent,
        ops_agent: OpsAgent,
        redis_store: RedisStore,
    ):
        self.llm = llm
        self.mongo_agent = mongo_agent
        self.finance_agent = finance_agent
        self.ops_agent = ops_agent
        self.redis = redis_store

    # --------------------------------------------------
    # Entry point
    # --------------------------------------------------
    def handle(self, session_id: str, user_input: str) -> Dict[str, Any]:
        session = self.redis.load_session(session_id)

        # ==================================================
        # A) Clarification follow-up handling
        # ==================================================
        pending = session.get("pending_clarification")
        if pending and isinstance(pending, dict):
            pending_intent = pending.get("intent_key")
            missing_keys = pending.get("missing_keys") or []
            if pending_intent in INTENT_REGISTRY and missing_keys:
                extracted = self.llm.extract_missing_slots(
                    text=user_input,
                    missing_keys=missing_keys,
                )

                slots = self._merge_slots(session.get("slots", {}), extracted)
                intent_cfg = INTENT_REGISTRY[pending_intent]
                still_missing = self._missing_required_slots(intent_cfg, slots)

                if still_missing:
                    question = self.llm.make_clarification_question(
                        intent_key=pending_intent,
                        missing_keys=still_missing,
                    )
                    self.redis.save_session(session_id, {
                        "slots": slots,
                        "last_intent": pending_intent,
                        "pending_clarification": {
                            "intent_key": pending_intent,
                            "missing_keys": still_missing
                        },
                    })
                    return {"clarification": question}

                session["slots"] = slots
                session["last_intent"] = pending_intent
                session["pending_clarification"] = None

                return self._execute_intent(
                    session_id=session_id,
                    user_input=user_input,
                    intent_key=pending_intent,
                    slots=slots,
                    session=session,
                )

        # ==================================================
        # B) Normal flow: intent extraction via LLM
        # ==================================================
        extraction = self.llm.extract_intent_slots(
            text=user_input,
            supported_intents=list(SUPPORTED_INTENTS),
            schema_hint=self._schema_hint(),
        )

        intent_key = extraction.get("intent_key", "out_of_scope")
        slots = extraction.get("slots", {}) or {}

        if intent_key not in INTENT_REGISTRY:
            answer = self.llm.generate_answer(
                user_input=user_input,
                intent="out_of_scope",
                slots=slots,
                merged_data={"intent": "out_of_scope", "slots": slots},
            )
            return {"intent": "out_of_scope", "slots": slots, "answer": answer}

        slots = self._merge_slots(session.get("slots", {}), slots)
        intent_cfg = INTENT_REGISTRY[intent_key]
        missing = self._missing_required_slots(intent_cfg, slots)

        if missing:
            question = self.llm.make_clarification_question(intent_key=intent_key, missing_keys=missing)
            self.redis.save_session(session_id, {
                "slots": slots,
                "last_intent": intent_key,
                "pending_clarification": {"intent_key": intent_key, "missing_keys": missing},
            })
            return {"clarification": question}

        return self._execute_intent(
            session_id=session_id,
            user_input=user_input,
            intent_key=intent_key,
            slots=slots,
            session=session,
        )

    # --------------------------------------------------
    # Execute a supported intent
    # --------------------------------------------------
    def _execute_intent(
        self,
        *,
        session_id: str,
        user_input: str,
        intent_key: str,
        slots: Dict[str, Any],
        session: Dict[str, Any],
    ) -> Dict[str, Any]:

        intent_cfg = INTENT_REGISTRY[intent_key]
        needs = intent_cfg.get("needs", {})

        session_context = {
            "anchor_type": session.get("anchor_type"),
            "anchor_id": session.get("anchor_id"),
        }

        result: Dict[str, Any] = {"intent": intent_key, "slots": slots}

        # -----------------------------
        # 1) Mongo (only for specific intents that need entity lookup)
        # -----------------------------
        mongo_anchor_type: Optional[str] = None
        mongo_anchor_id: Optional[str] = None

        # Skip Mongo for aggregate analysis queries that don't need entity lookup
        SKIP_MONGO_INTENTS = {
            "analysis.cargo_profitability",  # Q6, Q7, Q8 - aggregate cargo analysis
            "analysis.segment_performance",  # Q9, Q11, Q13 - aggregate segment analysis
            "ranking.voyages",               # Q12 - port-based ranking
        }

        if needs.get("mongo") and intent_key not in SKIP_MONGO_INTENTS:
            # Only call Mongo if we have a specific entity to look up
            if slots.get("voyage_number") or slots.get("voyage_id") or slots.get("vessel_name") or slots.get("imo"):
                try:
                    mongo_resp = self.mongo_agent.run(
                        intent_key=intent_cfg.get("mongo_intent") or "entity.auto",
                        slots=slots,
                        projection=intent_cfg.get("mongo_projection"),
                        session_context=session_context,
                    )
                    result["mongo"] = mongo_resp.document
                    mongo_anchor_type = mongo_resp.anchor_type
                    mongo_anchor_id = mongo_resp.anchor_id

                    slots["anchor_type"] = mongo_anchor_type
                    slots["anchor_id"] = mongo_anchor_id

                    if mongo_anchor_type == "VOYAGE":
                        slots["voyage_id"] = mongo_anchor_id
                    if mongo_anchor_type == "VESSEL":
                        slots["imo"] = mongo_anchor_id
                except Exception as e:
                    # If Mongo fails, continue with finance/ops
                    print(f"⚠️ Mongo lookup failed: {e}")
                    pass

        # -----------------------------
        # 2) Finance
        # -----------------------------
        if needs.get("finance"):
            fin = self.finance_agent.run(intent_key=intent_key, slots=slots)
            result["finance"] = fin.rows if hasattr(fin, "rows") else getattr(fin, "rows", fin)

        # -----------------------------
        # 3) Ops
        # -----------------------------
        if needs.get("ops"):
            ops = self.ops_agent.run(intent_key=intent_key, slots=slots)
            result["ops"] = ops.rows if hasattr(ops, "rows") else getattr(ops, "rows", ops)

        # -----------------------------
        # 3.5) Fetch remarks. Check Mongo document for remarks/remarkList; for aggregate queries, fetch remarks for all voyages.
        # -----------------------------

        # Case A: Single voyage from Mongo - extract remarks from the document itself!
        if result.get("mongo") and isinstance(result["mongo"], dict):
            mongo_doc = result["mongo"]
            # Check both remarkList (normalized) and remarks (raw)
            if mongo_doc.get("remarkList") or mongo_doc.get("remarks"):
                voyage_num = mongo_doc.get("voyageNumber")
                if voyage_num:
                    remarks = mongo_doc.get("remarkList") or mongo_doc.get("remarks") or []
                    if remarks:
                        result["voyage_remarks"] = {str(voyage_num): remarks}

        # Case B: Aggregate finance queries - fetch remarks for all voyage_numbers
        if isinstance(result.get("finance"), list) and len(result["finance"]) > 0:
            voyage_numbers = [row.get("voyage_number") for row in result["finance"] if row.get("voyage_number")]

            if voyage_numbers:
                remarks_map = result.get("voyage_remarks", {})  # Keep existing if any
                for vnum in voyage_numbers[:20]:  # Limit to 20 to avoid overload
                    if str(vnum) in remarks_map:
                        continue  # Skip if already fetched
                    try:
                        doc = self.mongo_agent.mongo.get_voyage_by_number(
                            vnum,
                            projection={"voyageNumber": 1, "remarkList": 1, "remarks": 1}
                        )
                        if doc:
                            remarks = doc.get("remarkList") or doc.get("remarks") or []
                            if remarks:
                                remarks_map[str(vnum)] = remarks
                    except Exception as e:
                        print(f"⚠️ Could not fetch remarks for voyage {vnum}: {e}")
                        pass

                if remarks_map:
                    result["voyage_remarks"] = remarks_map

        # -----------------------------
        # 4) LLM natural-language answer
        # -----------------------------
        answer = self.llm.generate_answer(
            user_input=user_input,
            intent=intent_key,
            slots=slots,
            merged_data=result,
        )
        result["answer"] = answer

        # -----------------------------
        # 5) Save session
        # -----------------------------
        self.redis.save_session(session_id, {
            "slots": slots,
            "last_intent": intent_key,
            "pending_clarification": None,
            "anchor_type": slots.get("anchor_type"),
            "anchor_id": slots.get("anchor_id"),
        })

        return result

    # --------------------------------------------------
    # Helpers
    # --------------------------------------------------
    @staticmethod
    def _merge_slots(old: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
        merged = dict(old or {})
        for k, v in (new or {}).items():
            if v not in (None, "", {}):
                merged[k] = v
        return merged

    @staticmethod
    def _missing_required_slots(intent_cfg: Dict[str, Any], slots: Dict[str, Any]) -> List[str]:
        required = intent_cfg.get("required_slots", [])
        return [k for k in required if k not in slots or slots[k] in (None, "", {})]

    @staticmethod
    def _schema_hint() -> Dict[str, Any]:
        return {
            "intents": {k: v.get("description", "") for k, v in INTENT_REGISTRY.items()},
            "slots": [
                "vessel_name", "imo",
                "voyage_number", "voyage_id",
                "date_from", "date_to",
                "limit",
                "port_name",
            ],
        }
