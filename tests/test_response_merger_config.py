from app.services.response_merger import compact_payload


def test_compact_payload_uses_response_rules_for_caps_and_labels() -> None:
    merged = {
        "finance": {"mode": "dynamic_sql", "rows": [{"i": i} for i in range(60)]},
        "ops": {"mode": "dynamic_sql", "rows": [{"i": i} for i in range(60)]},
        "mongo": {"mode": "mongo_llm", "collection": "voyages", "rows": [{"i": i} for i in range(60)]},
        "artifacts": {
            "voyage_ids": list(range(60)),
            "merged_rows": [
                {
                    "voyage_number": "1901",
                    "vessel_imo": "1234567",
                    "revenue": 1000,
                    "pnl": 50,
                    "total_expense": 900,
                    "total_commission": 25,
                    "offhire_days": 2,
                    "delay_reason": "weather",
                    "is_delayed": True,
                    "finance": {"voyage_id": "V1901", "vessel_imo": "1234567"},
                    "ops": [{"voyage_id": "V1901", "vessel_imo": "1234567.0"}],
                    "key_ports": [{"portName": f"Port {i}", "activityType": "L"} for i in range(12)],
                    "cargo_grades": ["NHC", "none", "NHC", "null", "ULSD"] + [f"G{i}" for i in range(20)],
                    "remarks": [f"remark {i}" for i in range(7)],
                }
            ]
            + [{"voyage_number": str(i)} for i in range(60)],
        },
    }

    compacted = compact_payload(merged)

    assert len(compacted["finance"]["rows"]) == 5
    assert compacted["ops"]["rows"] == []
    assert compacted["mongo"]["rows"] == []
    assert len(compacted["artifacts"]["voyage_ids"]) == 50
    assert len(compacted["artifacts"]["merged_rows"]) == 50

    row = compacted["artifacts"]["merged_rows"][0]
    assert row["vessel_name"] == "IMO:1234567"
    assert len(row["key_ports"]) == 10
    assert len(row["cargo_grades"]) == 10
    assert "none" not in {str(value).lower() for value in row["cargo_grades"]}
    assert len(row["remarks"]) == 5
    assert row["margin"] == 0.05
    assert row["cost_ratio"] == 0.9
    assert row["commission_ratio"] == 0.025
    assert row["offhire_days"] == 2
    assert row["delay_reason"] == "weather"
    signal_names = {signal["name"] for signal in row["business_reasoning"]["signals"]}
    assert {"inefficient_revenue", "delay_exposure", "profitable_but_operationally_risky"} <= signal_names
    assert row["source_reconciliation"]["status"] == "aligned"
    assert row["source_reconciliation"]["severity"] == "info"


def test_compact_payload_preserves_finance_kpi_unavailable_flag() -> None:
    merged = {
        "finance": {"mode": "registry_sql", "rows": []},
        "ops": {"mode": "registry_sql", "rows": [{"voyage_number": 1}]},
        "mongo": {"mode": "mongo_llm", "rows": []},
        "artifacts": {"intent_key": "voyage.summary", "slots": {}, "finance_kpi_unavailable": True},
    }
    out = compact_payload(merged)
    assert out["artifacts"].get("finance_kpi_unavailable") is True
