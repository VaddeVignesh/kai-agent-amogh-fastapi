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
