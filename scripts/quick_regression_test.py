from __future__ import annotations

import os
import sys
import uuid

from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.memory_test import build_router


def main() -> None:
    os.environ["DYNAMIC_SQL_ENABLED"] = "true"

    router = build_router()
    session_id = f"quick-{uuid.uuid4().hex[:6]}"

    queries = [
        "show top 5 voyages by PnL in the last 12 months, and include 1-2 key remarks for each voyage.",
        "Same, but make it top 10 and rank by commission instead.",
        "Find voyages with Rotterdam in the route and summarize the most common cargo grades; include any remarks that mention delays or offhire.",
        "What is the weather today in Singapore?",
    ]

    print("Session:", session_id)
    for q in queries:
        r = router.handle(session_id=session_id, user_input=q)
        data = r.get("data") or {}
        mongo = data.get("mongo") or {}
        print("\nQ:", q)
        print("intent:", r.get("intent_key"))
        print("slots:", r.get("slots") or {})
        if isinstance(mongo, dict):
            print("mongo:", {k: mongo.get(k) for k in ("mode", "ok", "collection", "limit")})
        else:
            print("mongo: <none>")


if __name__ == "__main__":
    main()

