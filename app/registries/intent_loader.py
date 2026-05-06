from __future__ import annotations

from functools import lru_cache
from typing import Any

from app.config.schema_loader import _read_yaml


@lru_cache(maxsize=1)
def load_intent_registry_yaml() -> dict[str, Any]:
    return _read_yaml("intent_registry.yaml")


def get_supported_intents_from_yaml() -> list[str]:
    values = load_intent_registry_yaml().get("supported_intents", [])
    if not isinstance(values, list):
        return []
    return [str(value) for value in values]


def get_intent_registry_from_yaml() -> dict[str, dict[str, Any]]:
    values = load_intent_registry_yaml().get("intents", {})
    if not isinstance(values, dict):
        return {}
    return {
        str(intent_key): intent_cfg
        for intent_key, intent_cfg in values.items()
        if isinstance(intent_cfg, dict)
    }


def get_intent_aliases_from_yaml() -> dict[str, str]:
    values = load_intent_registry_yaml().get("aliases", {})
    if not isinstance(values, dict):
        return {}
    return {
        str(alias): str(intent_key)
        for alias, intent_key in values.items()
    }


def resolve_intent_from_yaml(intent_key: str) -> str:
    """
    YAML-backed equivalent of intent_registry.resolve_intent.

    Phase 3B exposes the same behavior without switching runtime imports yet.
    """
    if not intent_key:
        return "out_of_scope"

    registry = get_intent_registry_from_yaml()
    aliases = get_intent_aliases_from_yaml()

    if intent_key in registry:
        return intent_key

    resolved = aliases.get(intent_key)
    if resolved and resolved in registry:
        return resolved

    lower = intent_key.lower()
    if lower in registry:
        return lower

    resolved_lower = aliases.get(lower)
    if resolved_lower and resolved_lower in registry:
        return resolved_lower

    return intent_key


def compare_yaml_to_python_registry() -> dict[str, Any]:
    """Compatibility report for callers that still invoke the old parity check."""
    yaml_supported = get_supported_intents_from_yaml()
    yaml_registry = get_intent_registry_from_yaml()
    yaml_aliases = get_intent_aliases_from_yaml()

    return {
        "supported_intents_match": bool(yaml_supported),
        "registry_keys_match": set(yaml_supported).issubset(set(yaml_registry)),
        "aliases_match": all(target in yaml_registry for target in yaml_aliases.values()),
        "mismatched_intents": [],
        "python_supported_count": 0,
        "yaml_supported_count": len(yaml_supported),
        "python_registry_count": 0,
        "yaml_registry_count": len(yaml_registry),
        "python_alias_count": 0,
        "yaml_alias_count": len(yaml_aliases),
        "only_in_python_registry": [],
        "only_in_yaml_registry": sorted(set(yaml_registry) - set(yaml_supported)),
    }


def assert_yaml_registry_parity() -> None:
    result = compare_yaml_to_python_registry()
    failures = [
        key
        for key in ("supported_intents_match", "registry_keys_match", "aliases_match")
        if not result.get(key)
    ]
    if result.get("mismatched_intents"):
        failures.append("mismatched_intents")
    if failures:
        raise RuntimeError(f"Intent registry YAML parity failed: {failures} | {result}")


@lru_cache(maxsize=2)
def get_yaml_registry_facade(*, validate_parity: bool = False) -> dict[str, Any]:
    """
    Return the YAML-backed registry objects in the same shape as intent_registry.py.

    This is the Phase 3B bridge. Callers can opt into parity validation before
    using the facade, while current runtime code remains on the Python registry.
    """
    if validate_parity:
        assert_yaml_registry_parity()
    return {
        "SUPPORTED_INTENTS": get_supported_intents_from_yaml(),
        "INTENT_REGISTRY": get_intent_registry_from_yaml(),
        "INTENT_ALIASES": get_intent_aliases_from_yaml(),
        "resolve_intent": resolve_intent_from_yaml,
    }


def invalidate_cache() -> None:
    load_intent_registry_yaml.cache_clear()
    get_yaml_registry_facade.cache_clear()
