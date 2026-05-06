"""Deprecated compatibility facade for the YAML-backed intent registry.

Runtime intent configuration lives in ``config/intent_registry.yaml``.  This
module remains only for older imports and tests that still expect the previous
Python module shape.
"""

from __future__ import annotations

from app.registries.intent_loader import (
    get_intent_aliases_from_yaml,
    get_intent_registry_from_yaml,
    get_supported_intents_from_yaml,
    resolve_intent_from_yaml,
)


SUPPORTED_INTENTS = get_supported_intents_from_yaml()
INTENT_REGISTRY = get_intent_registry_from_yaml()
INTENT_ALIASES = get_intent_aliases_from_yaml()


def resolve_intent(intent_key: str) -> str:
    return resolve_intent_from_yaml(intent_key)
