from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional

from app.llm.llm_client import LLMClient


@dataclass(frozen=True)
class MongoQuerySpec:
    collection: str  # "voyages" | "vessels"
    filter: Dict[str, Any]
    projection: Dict[str, int]
    sort: Optional[Dict[str, int]]
    limit: int


class MongoQueryBuilder:
    def __init__(self, llm: LLMClient):
        self.llm = llm

    def build(self, *, question: str, schema_hint: Dict[str, Any], slots: Dict[str, Any]) -> MongoQuerySpec:
        system = (
            "You generate SAFE MongoDB find() specs.\n"
            "Return ONLY valid JSON. No markdown. No extra text.\n"
            "Hard rules:\n"
            "- Use only collections in schema_hint.\n"
            "- Use only allowed operators.\n"
            "- Always set projection to minimal required fields.\n"
            "- Always include _id: 0 in projection.\n"
            "- Limit must be <= 50.\n"
        )
        payload = {
            "task": "mongo_find_spec",
            "question": question,
            "slots": slots,
            "schema_hint": schema_hint,
            "output_format": {
                "collection": "string",
                "filter": "object",
                "projection": "object",
                "sort": "object|null",
                "limit": "int",
            },
        }

        raw = self.llm._groq_chat(
            system=system,
            user=json.dumps(payload, ensure_ascii=False),
            temperature=0,
        )
        data = self.llm._safe_json_load(raw, fallback={})

        collection = str(data.get("collection") or "").strip()
        filt = data.get("filter") if isinstance(data.get("filter"), dict) else {}
        proj = data.get("projection") if isinstance(data.get("projection"), dict) else {"_id": 0}
        sort = data.get("sort") if isinstance(data.get("sort"), dict) else None
        limit = int(data.get("limit") or 10)

        proj["_id"] = 0
        limit = max(1, min(limit, 50))

        return MongoQuerySpec(collection=collection, filter=filt, projection=proj, sort=sort, limit=limit)

