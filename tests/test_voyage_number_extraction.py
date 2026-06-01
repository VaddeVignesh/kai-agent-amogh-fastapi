"""Tests for multi-voyage number extraction from user queries."""

from app.utils.voyage_slot_extract import extract_voyage_numbers_from_text


def test_voyage_labeled_and_pair():
    q = "tell me about voyage 1901 and 1902"
    assert extract_voyage_numbers_from_text(q) == [1901, 1902]


def test_about_without_voyage_word():
    q = "tell me about 2302 and 2306"
    assert extract_voyage_numbers_from_text(q) == [2302, 2306]


def test_voyage_labeled_ops_customer_style():
    q = "tell me about voyage 2117 and 2312?"
    assert extract_voyage_numbers_from_text(q) == [2117, 2312]


def test_single_voyage_still_one():
    q = "financial summary for voyage 2306"
    assert extract_voyage_numbers_from_text(q) == [2306]


def test_comma_separated_list():
    q = "compare voyages 1901, 1902, 1903"
    assert extract_voyage_numbers_from_text(q) == [1901, 1902, 1903]


def test_comma_and_list_without_space():
    q = "Compare actual vs when-fixed results for voyages 1901, 1902,and 2301."
    assert extract_voyage_numbers_from_text(q) == [1901, 1902, 2301]


def test_deterministic_does_not_short_circuit_multi_voyage():
    """Multi-voyage asks must reach the LLM classifier (not deterministic voyage.summary)."""
    from app.llm.llm_client import LLMClient, LLMConfig

    client = LLMClient(LLMConfig(api_key="test-key"))
    assert client._deterministic_intent("tell me about voyage 1901 and 1902") is None
    assert client._deterministic_intent("tell me about 2302 and 2306") is None


def test_merge_voyage_slots_llm_primary():
    from app.utils.voyage_slot_extract import merge_voyage_slot_fields

    llm_slots = {"voyage_numbers": [1901, 1902], "vessel_name": "Stena"}
    merged = merge_voyage_slot_fields(llm_slots, "tell me about voyage 1901 and 1902")
    assert merged["voyage_numbers"] == [1901, 1902]
    assert "voyage_number" not in merged


def test_merge_voyage_slots_regex_fallback_when_llm_omits():
    from app.utils.voyage_slot_extract import merge_voyage_slot_fields

    merged = merge_voyage_slot_fields({"voyage_number": 1901}, "tell me about voyage 1901 and 1902")
    assert merged["voyage_numbers"] == [1901, 1902]


def test_unique_voyage_numbers_from_slots():
    from app.orchestration.graph_router import GraphRouter

    slots = {"voyage_numbers": [1901, 1902, 1901]}
    assert GraphRouter._unique_voyage_numbers_from_slots(slots) == [1901, 1902]


def test_voyage_summary_uses_ports_json_when_key_ports_empty():
    from app.llm.llm_client import LLMClient, LLMConfig
    from app.services.response_merger import compact_payload

    client = LLMClient(LLMConfig(api_key="test-key"))
    merged = {
        "finance": {
            "rows": [
                {
                    "voyage_number": 2306,
                    "vessel_name": "Emma Grace",
                    "revenue": 1324975.01,
                    "total_expense": 465363.59,
                    "pnl": 419181.96,
                }
            ]
        },
        "ops": {
            "rows": [
                {
                    "voyage_number": 2306,
                    "vessel_name": "Emma Grace",
                    "ports_json": [
                        {"port_name": "Bilbao"},
                        {"port_name": "Tarragona"},
                        {"port_name": "Amsterdam"},
                    ],
                }
            ]
        },
        "mongo": {"rows": []},
        "artifacts": {"intent_key": "voyage.summary", "slots": {}},
    }

    answer = client._build_deterministic_voyage_summary(
        question="For voyage 2306, give route and financial result.",
        merged_safe=client._convert_to_json_safe(client._truncate_merged_data(compact_payload(merged), max_rows=10)),
        style={"ask_ports": True, "ask_remarks": False},
    )

    assert "Bilbao" in answer
    assert "Tarragona" in answer
    assert "Amsterdam" in answer
    assert "Not available in dataset" not in answer


def test_merge_fills_voyage_numbers_before_validation():
    from app.utils.voyage_slot_extract import merge_voyage_slot_fields

    merged = merge_voyage_slot_fields({}, "tell me about voyage 1901 and 1902")
    assert merged.get("voyage_numbers") == [1901, 1902]
    assert "voyage_number" not in merged
