"""Tests for ops JSON shrinking before LLM prompts."""

from __future__ import annotations

import json

from app.utils.ops_llm_shrink import grade_strings_from_grades_json, port_names_from_ports_json, shrink_ops_row_json_fields


def test_port_names_from_json_string() -> None:
    blob = json.dumps(
        [
            {"portName": "Houston"},
            {"port_name": "Singapore"},
            {"name": "Rotterdam"},
            {"portName": "Stena Sunrise"},
        ]
    )
    names = port_names_from_ports_json(blob, max_unique=10)
    assert "Houston" in names
    assert "Singapore" in names
    assert "Rotterdam" in names
    assert "Stena Sunrise" not in names


def test_shrink_ops_row_drops_json_blobs() -> None:
    row = {
        "voyage_number": 2302,
        "ports_json": [{"portName": "A"}, {"portName": "B"}],
        "grades_json": [{"grade_name": " LNG "}],
        "activities_json": [{"x": "y"}],
        "remarks_json": [{"remark": "late arrival"}],
    }
    shrink_ops_row_json_fields(row, voyage_summary=True)
    assert "ports_json" not in row
    assert row["ports"] == ["A", "B"]
    assert row["cargo_grade_names"] == ["LNG"]
    assert "activities_json" not in row
    assert "remarks_preview" in row
