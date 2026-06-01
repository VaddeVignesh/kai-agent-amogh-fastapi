"""Operational voyage diagnostics should route to voyage.summary, not Mongo-only metadata."""

from app.config.routing_rules_loader import (
    is_voyage_operational_profile_query,
)
from app.llm.llm_client import LLMClient, LLMConfig
from app.orchestration.graph_router import GraphRouter


EMMA_GRACE_QUERY = (
    "For Emma Grace voyage 2305, summarize what went wrong operationally "
    "using delay fields, ports, grades, and remarks only."
)


def test_operational_profile_detected_for_emma_grace_voyage():
    slots = {"voyage_number": 2305, "voyage_numbers": [2305]}
    assert is_voyage_operational_profile_query(EMMA_GRACE_QUERY, slots)


def test_explicit_vessel_mention_for_name_before_voyage():
    mentions = GraphRouter._explicit_vessel_mentions(EMMA_GRACE_QUERY)
    assert mentions == ["Emma Grace"]


def test_deterministic_intent_is_voyage_summary_not_metadata():
    client = LLMClient(LLMConfig(api_key="test-key"))
    out = client.extract_intent_slots(text=EMMA_GRACE_QUERY, supported_intents=["voyage.summary", "voyage.metadata"])
    assert out.get("intent_key") == "voyage.summary"
    slots = out.get("slots") or {}
    assert slots.get("voyage_number") == 2305 or slots.get("voyage_numbers") == [2305]
    assert str(slots.get("vessel_name") or "").casefold() == "emma grace"
