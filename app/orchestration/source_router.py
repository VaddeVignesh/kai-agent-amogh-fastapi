import logging

from app.config.routing_rules_loader import get_planner_default_primary_source
from app.config.schema_loader import build_field_to_source_index

logger = logging.getLogger(__name__)


def resolve_required_sources(requested_fields: list) -> list:
    if not requested_fields:
        return []

    index = build_field_to_source_index()
    sources: set = set()
    for field in requested_fields:
        entry = index.get(field)
        if entry and entry.get("source"):
            sources.add(entry["source"])
        else:
            logger.debug("SourceRouter: field missing from map")

    result = sorted(sources)
    logger.info(f"SourceRouter: {len(requested_fields)} fields -> {result}")
    return result


def is_composite_required(requested_fields: list) -> bool:
    return len(resolve_required_sources(requested_fields)) > 1


def get_primary_source(requested_fields: list) -> str:
    sources = resolve_required_sources(requested_fields)
    if len(sources) == 1:
        return sources[0]
    return get_planner_default_primary_source()

