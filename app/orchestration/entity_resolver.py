import logging
from typing import Optional, Callable

from app.config.schema_loader import (
    load_entity_catalog,
    get_scenario_aliases,
    get_scenario_values,
    get_imo_postgres_fix,
)

logger = logging.getLogger(__name__)


def resolve_canonical_keys(
    entity_type: str,
    identifier_field: str,
    identifier_value: str,
    mongo_lookup_fn: Callable,
    postgres_lookup_fn: Callable,
) -> dict:
    fallback = {identifier_field: identifier_value}
    catalog = load_entity_catalog()
    entity_config = catalog.get("entities", {}).get(entity_type) if isinstance(catalog, dict) else None

    if not isinstance(entity_config, dict):
        return fallback

    anchor = entity_config.get("cross_source_anchor")
    if not isinstance(anchor, dict):
        return fallback

    strategy = anchor.get("resolution", "direct")
    resolved = dict(fallback)

    try:
        if strategy == "mongo_first":
            collection = anchor.get("mongo_collection")
            mongo_field = anchor.get("mongo_field")
            postgres_field = anchor.get("postgres_field")
            link_field = anchor.get("link_field")

            if not (collection and mongo_field and postgres_field):
                return resolved

            projection = {mongo_field: 1, "_id": 0}
            if link_field:
                projection[link_field] = 1

            result = mongo_lookup_fn(
                collection=collection,
                filter_dict={identifier_field: identifier_value},
                projection=projection,
            )

            if isinstance(result, dict) and result:
                if mongo_field in result:
                    resolved[postgres_field] = result[mongo_field]
                if link_field and link_field in result:
                    resolved[link_field] = result[link_field]

    except Exception as e:
        logger.error(f"EntityResolver failed: {e}")

    return resolved


def resolve_scenario(raw_scenario: Optional[str]) -> Optional[str]:
    if not raw_scenario:
        return None
    normalised = raw_scenario.strip().upper()
    valid = [str(v).upper() for v in get_scenario_values()]
    if normalised in valid:
        return normalised
    aliases = get_scenario_aliases()
    alias_key = raw_scenario.strip().lower()
    if alias_key in aliases:
        return aliases[alias_key]
    return None


def get_imo_join_expression(alias: str = "") -> str:
    fix = get_imo_postgres_fix()
    return fix

