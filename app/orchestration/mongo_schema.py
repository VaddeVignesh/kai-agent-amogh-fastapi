from __future__ import annotations

from typing import Any, Dict


def mongo_schema_hint() -> Dict[str, Any]:
    """
    Keep this stable and explicit so the LLM does NOT invent keys.
    Use dot-paths for nested fields.

    Audited field facts (Feb 2026):
    - voyageNumber is stored as STRING (e.g. "1901")
    - voyages use vesselName (no top-level vesselImo on voyages)
    - remarks array is "remarks" (not "remarkList")
    - fixtures array is "fixtures" (not "fixtureList")
    - financials live under projected_results.*
    """
    return {
        "collections": {
            "vessels": {
                "id_fields": ["imo", "name", "vesselId"],
                "fields": [
                    "vesselId",
                    "imo",
                    "name",
                    "accountCode",
                    "hireRate",
                    "scrubber",
                    "marketType",
                    "isVesselOperating",
                    "tags.category",
                    "tags.value",
                ],
                "examples": {
                    "by_imo": {"imo": "9705902"},
                    "by_name": {"name": "Elka Delphi"},
                },
            },
            "voyages": {
                "id_fields": ["voyageId", "voyageNumber", "vesselName"],
                "fields": [
                    "voyageId",
                    "voyageNumber",
                    "vesselName",
                    "url",
                    "tags",
                    "startDateUtc",
                    # remarks (native field per audit)
                    "remarks",
                    "remarks.remark",
                    "remarks.modifiedByFull",
                    "remarks.modifiedDate",
                    # fixtures / ports / grades (native field per audit)
                    "fixtures",
                    "fixtures.cpDate",
                    "fixtures.displayOrder",
                    "fixtures.grades",
                    "fixtures.fixtureGrades.gradeName",
                    "fixtures.fixturePorts.portName",
                    "fixtures.fixturePorts.activityType",
                    "fixtures.fixturePorts.displayOrder",
                    "fixtures.fixtureBillsOfLading.description",
                    "fixtures.fixtureBillsOfLading.fixtureGradeName",
                    "fixtures.fixtureBillsOfLading.portName",
                    # legs
                    "legs",
                    "legs.portName",
                    "legs.type",
                    "legs.displayOrder",
                    # projected results (if present)
                    "projected_results.pnl",
                    "projected_results.revenue",
                    "projected_results.expenses",
                    "projected_results.expense",
                    "projected_results.tce",
                ],
                "examples": {
                    "by_voyageId": {"voyageId": "BDEA0AD71D00DE30244B96E0D474F30A"},
                    "by_voyageNumber": {"voyageNumber": "1901"},
                    "by_vesselName_exact": {"vesselName": "Stena Conquest"},
                    "by_vesselName_regex": {"vesselName": {"$regex": "(?i)stena"}},
                    "remarks_for_voyage_ids": {"voyageId": {"$in": ["BDEA0AD71D00DE30244B96E0D474F30A"]}},
                    "voyages_by_port_regex": {"fixtures.fixturePorts.portName": {"$regex": "(?i)rotterdam"}},
                },
            },
        },
        "rules": [
            "Return only fields that are needed for the question.",
            "Always include minimal identifiers (voyageId + voyageNumber, or imo + name).",
            "Prefer projection to reduce payload.",
            'voyageNumber is stored as STRING in Mongo; use strings like "1901".',
            "Use remarks (not remarkList) and fixtures (not fixtureList).",
            "Financials live under projected_results.* (not results.financialSummary.*).",
            "Use projected_results.expenses for total expense (projected_results.expense is not total).",
            "Use only allowed operators from allowed_operators.",
        ],
        "allowed_operators": [
            "$and",
            "$or",
            "$in",
            "$nin",
            "$eq",
            "$ne",
            "$gt",
            "$gte",
            "$lt",
            "$lte",
            "$regex",
            "$options",
            "$exists",
            "$size",
            "$not",
            "$elemMatch",
        ],
    }

