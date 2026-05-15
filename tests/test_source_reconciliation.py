from app.config.business_rules_loader import get_reconciliation_rules
from app.services.source_reconciliation import reconcile_merged_row, reconcile_sources


def test_reconciliation_rules_load_from_yaml() -> None:
    rules = get_reconciliation_rules()

    assert "voyage_id" in rules["primary_identity_fields"]
    assert "vessel_imo" in rules["fallback_identity_fields"]
    assert "finance" in rules["source_sections"]


def test_reconcile_sources_marks_aligned_rows() -> None:
    result = reconcile_sources(
        {
            "voyage_id": "V001",
            "vessel_imo": "9884837.0",
            "finance": {"voyage_id": "V001", "vessel_imo": "9884837"},
            "ops": [{"voyage_id": "V001", "vessel_imo": 9884837}],
        }
    )

    assert result["status"] == "aligned"
    assert result["severity"] == "info"
    assert "voyage_id" in result["matched_fields"]
    assert result["canonical_fields"]["voyage_id"] == "V001"
    assert result["caveats"]
    assert result["mismatches"] == []


def test_reconcile_sources_marks_mismatches() -> None:
    result = reconcile_sources(
        {
            "voyage_id": "V001",
            "finance": {"voyage_id": "V001", "vessel_name": "Stena Conquest"},
            "ops": [{"voyage_id": "V999", "vessel_name": "Other Vessel"}],
        }
    )

    assert result["status"] == "mismatch"
    assert result["severity"] == "blocking"
    assert result["canonical_fields"]["voyage_id"] == "V001"
    assert result["caveats"]
    fields = {item["field"] for item in result["mismatches"]}
    assert {"voyage_id", "vessel_name"} <= fields


def test_reconcile_merged_row_preserves_original_fields() -> None:
    row = reconcile_merged_row({"pnl": 10, "finance": {"pnl": 10}})

    assert row["pnl"] == 10
    assert row["source_reconciliation"]["status"] == "partial"
    assert row["source_reconciliation"]["severity"] == "warning"


def test_reconcile_sources_prefers_configured_canonical_source() -> None:
    result = reconcile_sources(
        {
            "vessel_name": "Merged Name",
            "finance": {"vessel_name": "Finance Name"},
            "ops": {"vessel_name": "Ops Name"},
        }
    )

    assert result["status"] == "mismatch"
    assert result["canonical_fields"]["vessel_name"] == "Ops Name"
