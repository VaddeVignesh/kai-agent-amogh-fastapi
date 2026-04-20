import logging
from functools import lru_cache
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)
CONFIG_DIR = Path("config")


def _read_yaml(name: str) -> dict:
    path = CONFIG_DIR / name
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


@lru_cache(maxsize=1)
def load_schema() -> dict:
    return _read_yaml("schema.yaml")


@lru_cache(maxsize=1)
def load_source_map() -> dict:
    return _read_yaml("source_map.yaml")


@lru_cache(maxsize=1)
def load_entity_catalog() -> dict:
    return _read_yaml("entity_catalog.yaml")


@lru_cache(maxsize=1)
def load_intent_catalog() -> dict:
    return _read_yaml("intent_catalog.yaml")


@lru_cache(maxsize=1)
def build_scenario_alias_map() -> dict:
    """
    Inverted scenario alias map from entity_catalog.yaml.
    Keys are lowercase alias strings. Values are canonical scenario codes.
    """
    catalog = load_entity_catalog()
    result: dict[str, str] = {}
    raw = catalog.get("scenarios", {}) if isinstance(catalog, dict) else {}
    if isinstance(raw, dict):
        for canonical, aliases in raw.items():
            if not isinstance(aliases, list):
                continue
            can = str(canonical).strip()
            for alias in aliases or []:
                result[str(alias).lower().strip()] = can
    sc = catalog.get("scenario_enum", {}) if isinstance(catalog, dict) else {}
    al = sc.get("aliases", {}) if isinstance(sc, dict) else {}
    if isinstance(al, dict):
        for ak, canonical in al.items():
            c = str(canonical).strip()
            lk = str(ak).lower().strip()
            result[lk] = c
            if "_" in lk:
                result[lk.replace("_", " ")] = c
                result[lk.replace("_", "-")] = c
    return result


def get_postgres_schema_str() -> str:
    schema = load_schema()
    lines: list[str] = []
    tables = schema.get("postgres", {}).get("tables", {}) if isinstance(schema, dict) else {}
    for table, meta in (tables or {}).items():
        if not isinstance(meta, dict):
            continue
        lines.append(f"TABLE {table}: {meta.get('description', '')}")
        pk = meta.get("primary_key") or meta.get("composite_key")
        if pk:
            lines.append(f"  PRIMARY KEY: {pk}")
        join = meta.get("join_to")
        if join:
            lines.append(f"  JOINS TO: {join}")
        fields = meta.get("fields", {})
        if isinstance(fields, dict):
            for field, props in fields.items():
                if not isinstance(props, dict):
                    lines.append(f"  {field}")
                    continue
                ftype = props.get("type", "")
                fdesc = props.get("description", "")
                enum_vals = props.get("enum") or []
                enum_str = f" (enum: {enum_vals})" if enum_vals else ""
                lines.append(f"  {field} [{ftype}]{enum_str} — {fdesc}")
    return "\n".join(lines)


def get_mongo_schema_str() -> str:
    schema = load_schema()
    lines: list[str] = []
    colls = schema.get("mongo", {}).get("collections", {}) if isinstance(schema, dict) else {}
    for coll, meta in (colls or {}).items():
        if not isinstance(meta, dict):
            continue
        lines.append(f"COLLECTION {coll}: {meta.get('description', '')}")
        lk = meta.get("lookup_key")
        if lk:
            lines.append(f"  LOOKUP KEY: {lk}")
        for field, props in (meta.get("fields", {}) or {}).items():
            if not isinstance(props, dict):
                lines.append(f"  {field}")
                continue
            ftype = props.get("type", "")
            fdesc = props.get("description", "")
            lines.append(f"  {field} [{ftype}] — {fdesc}")
    return "\n".join(lines)


def get_source_map_str() -> str:
    sm = load_source_map()
    lines: list[str] = []
    if not isinstance(sm, dict):
        return ""
    for group, meta in sm.items():
        if not isinstance(meta, dict):
            continue
        src = meta.get("source", "unknown")
        origin = meta.get("table") or meta.get("collection") or ""
        fields = meta.get("fields") or []
        fields_s = ", ".join(fields) if isinstance(fields, list) else ""
        lines.append(f"{str(group).upper()} -> {src} ({origin}): [{fields_s}]")
    return "\n".join(lines)


def get_intent_catalog_str() -> str:
    catalog = load_intent_catalog()
    ops = catalog.get("operations", []) if isinstance(catalog, dict) else []
    lines: list[str] = []
    for op in ops:
        if not isinstance(op, dict):
            continue
        oid = op.get("id", "")
        desc = op.get("description", "")
        lines.append(f"- {oid}: {desc}")
        lines.append(f"  requires: {op.get('requires', [])}")
        lines.append(f"  sources: {op.get('sources', 'dynamic')}")
    return "\n".join(lines)


def get_entity_catalog_str() -> str:
    catalog = load_entity_catalog()
    lines: list[str] = []
    entities = catalog.get("entities", {}) if isinstance(catalog, dict) else {}
    if isinstance(entities, dict):
        for etype, meta in entities.items():
            if not isinstance(meta, dict):
                continue
            lines.append(
                f"- {etype}: canonical_key={meta.get('canonical_key')}, human_key={meta.get('human_key')}"
            )
    sc = catalog.get("scenario_enum", {}) if isinstance(catalog, dict) else {}
    if isinstance(sc, dict) and sc:
        lines.append(f"- scenario values: {sc.get('values', [])}")
        lines.append(f"  default: {sc.get('default')}")
        lines.append(f"  aliases: {sc.get('aliases', {})}")
    norm = catalog.get("imo_normalization", {}) if isinstance(catalog, dict) else {}
    if isinstance(norm, dict) and norm:
        lines.append(f"- normalization: {norm.get('issue', '')}")
        lines.append(f"  postgres fix: {norm.get('postgres_fix', '')}")
    return "\n".join(lines)


def build_field_to_source_index() -> dict:
    sm = load_source_map()
    index: dict = {}
    if not isinstance(sm, dict):
        return index
    for group, meta in sm.items():
        if not isinstance(meta, dict):
            continue
        source = meta.get("source")
        origin = meta.get("table") or meta.get("collection")
        fields = meta.get("fields", [])
        if not isinstance(fields, list):
            continue
        for field in fields:
            index[field] = {
                "source": source,
                "group": group,
                "table_or_collection": origin,
            }
    return index


def get_all_schema_field_names() -> set:
    schema = load_schema()
    fields: set = set()
    if not isinstance(schema, dict):
        return fields
    for table_meta in (schema.get("postgres", {}).get("tables", {}) or {}).values():
        if isinstance(table_meta, dict):
            f = table_meta.get("fields", {})
            if isinstance(f, dict):
                fields.update(f.keys())
    for coll_meta in (schema.get("mongo", {}).get("collections", {}) or {}).values():
        if isinstance(coll_meta, dict):
            f = coll_meta.get("fields", {})
            if isinstance(f, dict):
                fields.update(f.keys())
    return fields


def get_scenario_values() -> list:
    catalog = load_entity_catalog()
    if not isinstance(catalog, dict):
        return []
    sc = catalog.get("scenario_enum", {})
    return list(sc.get("values", [])) if isinstance(sc, dict) else []


def get_scenario_aliases() -> dict:
    catalog = load_entity_catalog()
    if not isinstance(catalog, dict):
        return {}
    sc = catalog.get("scenario_enum", {})
    return dict(sc.get("aliases", {})) if isinstance(sc, dict) else {}


def get_imo_postgres_fix() -> str:
    catalog = load_entity_catalog()
    if not isinstance(catalog, dict):
        return "field"
    norm = catalog.get("imo_normalization", {})
    if not isinstance(norm, dict):
        return "field"
    return str(norm.get("postgres_fix") or "field")


def invalidate_cache() -> None:
    load_schema.cache_clear()
    load_source_map.cache_clear()
    load_entity_catalog.cache_clear()
    load_intent_catalog.cache_clear()
    build_scenario_alias_map.cache_clear()
