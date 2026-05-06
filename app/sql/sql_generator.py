# app/sql/sql_generator.py

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from app.llm.llm_client import LLMClient
from app.config.prompt_rules_loader import (
    get_sql_generator_finance_system_prompt,
    get_sql_generator_ops_system_prompt,
)
from app.config.sql_rules_loader import (
    get_sql_generator_agent_table_scopes,
    get_sql_generator_allowed_literals,
    get_sql_generator_composite_query_nature,
    get_sql_generator_constraints,
    get_sql_generator_default_intent_key,
    get_sql_generator_default_limit,
    get_sql_generator_empty_sql,
    get_sql_generator_error_retry_suffix,
    get_sql_generator_forbidden_columns,
    get_sql_generator_hardcoded_limit_pattern,
    get_sql_generator_hardcoded_string_pattern,
    get_sql_generator_join_hints,
    get_sql_generator_optional_placeholder_slots,
    get_sql_generator_param_conventions,
    get_sql_generator_required_param_slots,
    get_sql_generator_retryable_pg_errors,
    get_sql_generator_validation_messages,
)
from app.registries.intent_loader import get_yaml_registry_facade
from app.sql.sql_allowlist import DEFAULT_ALLOWLIST, SQLAllowlist

INTENT_REGISTRY = get_yaml_registry_facade(validate_parity=True)["INTENT_REGISTRY"]


FINANCE_SQL_SYSTEM_PROMPT = get_sql_generator_finance_system_prompt()
OPS_SQL_SYSTEM_PROMPT = get_sql_generator_ops_system_prompt()


# Postgres execution error patterns that are retryable
_RETRYABLE_PG_ERRORS = get_sql_generator_retryable_pg_errors()

_DEFAULT_LIMIT = int(os.getenv("SQL_DEFAULT_LIMIT", str(get_sql_generator_default_limit())))

# LIMIT followed by a bare integer — must use %(limit)s instead
_HARDCODED_LIMIT_RE = re.compile(get_sql_generator_hardcoded_limit_pattern(), re.IGNORECASE)

# String literal in a filter position (=, >=, ILIKE, IN, etc.)
_HARDCODED_STRING_RE = re.compile(
    get_sql_generator_hardcoded_string_pattern(), re.IGNORECASE
)

# These string literals are genuinely structural — not data values
# '.0'  → REPLACE(x::TEXT, '.0', '') normalization
# ''    → trim/empty guard  AND trim(elem->>'grade_name') != ''
# ' '   → whitespace guard
_ALLOWED_LITERALS = get_sql_generator_allowed_literals()
_AGENT_TABLE_SCOPES = get_sql_generator_agent_table_scopes()
_JOIN_HINTS = get_sql_generator_join_hints()
_PARAM_CONVENTIONS = get_sql_generator_param_conventions()
_CONSTRAINTS = get_sql_generator_constraints()
_COMPOSITE_QUERY_NATURE = get_sql_generator_composite_query_nature()
_ERROR_RETRY_SUFFIX = get_sql_generator_error_retry_suffix()
_VALIDATION_MESSAGES = get_sql_generator_validation_messages()
_FORBIDDEN_COLUMNS = get_sql_generator_forbidden_columns()
_REQUIRED_PARAM_SLOTS = get_sql_generator_required_param_slots()
_OPTIONAL_PLACEHOLDER_SLOTS = get_sql_generator_optional_placeholder_slots()
_DEFAULT_INTENT_KEY = get_sql_generator_default_intent_key()
_EMPTY_SQL = get_sql_generator_empty_sql()


@dataclass
class SQLGenOutput:
    sql: str
    params: Dict[str, Any]
    tables: List[str]
    confidence: float


class SQLGenerator:
    def __init__(self, llm: LLMClient):
        self.llm = llm
        self.sql_max_tokens = int(os.getenv("SQL_MAX_TOKENS", "1024"))
        self.sql_max_retries = int(os.getenv("SQL_MAX_RETRIES", "2"))

    @staticmethod
    def _schema_hint_for_agent(
        *,
        agent: str,
        allowlist: SQLAllowlist,
        intent_key: str = "",
    ) -> Dict[str, Any]:
        agent = (agent or "").strip().lower()
        tables = sorted(list(allowlist.allowed_tables))
        cols = {
            t: sorted(list(allowlist.allowed_columns.get(t, set())))
            for t in tables
        }

        scoped_tables = _AGENT_TABLE_SCOPES.get(agent)
        if scoped_tables:
            allowed_scope = set(scoped_tables)
            tables = [t for t in tables if t in allowed_scope]

        cols = {t: cols.get(t, []) for t in tables}

        hint: Dict[str, Any] = {
            "allowed_tables": tables,
            "allowed_columns": cols,
            "join_hints": _JOIN_HINTS,
            "param_conventions": _PARAM_CONVENTIONS,
            "constraints": _CONSTRAINTS,
        }

        intent_cfg = INTENT_REGISTRY.get(intent_key, {})
        if intent_cfg.get("route") == "composite":
            hint["query_nature"] = _COMPOSITE_QUERY_NATURE

        return hint

    def _inject_required_params(
        self, sql: str, params: Dict[str, Any], slots: Dict[str, Any]
    ) -> Dict[str, Any]:
        merged = dict(params)

        for placeholder, config in _REQUIRED_PARAM_SLOTS.items():
            key = str(config.get("slot") or "")
            if placeholder in sql and key and key not in merged:
                try:
                    value = slots.get(key)
                    if value is None and config.get("default") == "generator_default_limit":
                        value = _DEFAULT_LIMIT
                    elif value is None and "default" in config:
                        value = config.get("default")
                    if config.get("type") == "int" and value is not None:
                        value = int(value)
                    merged[key] = value
                except (TypeError, ValueError, KeyError):
                    merged[key] = None

        # Always inject limit and scenario — they are unconditional
        if "limit" not in merged:
            merged["limit"] = int(slots.get("limit") or _DEFAULT_LIMIT)
        if "scenario" not in merged:
            merged["scenario"] = slots.get("scenario")

        return merged

    def _build_system_prompt(
        self,
        agent: str,
        intent_key: str,
        error_hint: Optional[str],
    ) -> str:
        base = FINANCE_SQL_SYSTEM_PROMPT if agent == "finance" else OPS_SQL_SYSTEM_PROMPT

        intent_cfg = INTENT_REGISTRY.get(intent_key, {})
        intent_rules = intent_cfg.get("sql_hints", {}).get(agent, "")
        if intent_rules:
            base = base + f"\n\nINTENT RULES:\n{intent_rules}"

        if error_hint:
            base = base + "\n\n" + _ERROR_RETRY_SUFFIX.format(error_hint=error_hint)

        return base

    def _call_llm(
        self,
        question: str,
        intent_key: str,
        slots: Dict[str, Any],
        schema_hint: Dict[str, Any],
        agent: str,
        system_prompt: str,
    ) -> Dict[str, Any]:
        return self.llm.generate_sql(
            question=question,
            intent_key=intent_key,
            slots=slots,
            schema_hint=schema_hint,
            agent=agent,
            system_prompt=system_prompt,
            temperature=0,         # deterministic — never change this
            max_tokens=self.sql_max_tokens,
        )

    def generate(
        self,
        question: str,
        agent: str,
        slots: Optional[Dict[str, Any]] = None,
        intent_key: Optional[str] = None,
        error_hint: Optional[str] = None,
    ) -> SQLGenOutput:
        slots = slots or {}
        intent_key = (intent_key or _DEFAULT_INTENT_KEY).strip()
        agent = (agent or "").strip().lower()

        schema_hint = self._schema_hint_for_agent(
            agent=agent,
            allowlist=DEFAULT_ALLOWLIST,
            intent_key=intent_key,
        )

        last_error: Optional[str] = error_hint

        for attempt in range(self.sql_max_retries + 1):
            system_prompt = self._build_system_prompt(
                agent=agent,
                intent_key=intent_key,
                error_hint=last_error,
            )

            result = self._call_llm(
                question=question,
                intent_key=intent_key,
                slots=slots,
                schema_hint=schema_hint,
                agent=agent,
                system_prompt=system_prompt,
            )

            sql = str(result.get("sql") or "").strip()
            params = result.get("params") or {}
            tables = result.get("tables") or []
            confidence = float(result.get("confidence") or 0.0)

            if not sql:
                return self._empty()

            params = self._inject_required_params(sql, params, slots)

            output = SQLGenOutput(
                sql=sql,
                params=params,
                tables=[str(t) for t in tables] if isinstance(tables, list) else [],
                confidence=confidence,
            )

            try:
                self._validate_sql(output, slots)
                return output

            except ValueError as exc:
                last_error = str(exc)
                if attempt < self.sql_max_retries:
                    continue
                return output  # best effort after exhausting retries

            except Exception as exc:
                err_msg = str(exc)
                is_retryable = any(k in err_msg for k in _RETRYABLE_PG_ERRORS)
                if is_retryable and attempt < self.sql_max_retries:
                    last_error = err_msg
                    continue
                return output

        return self._empty()

    def _validate_sql(self, output: SQLGenOutput, slots: Dict[str, Any]) -> None:
        sql = output.sql
        sql_lower = sql.lower()

        if not sql_lower.strip().startswith("select"):
            raise ValueError(_VALIDATION_MESSAGES["non_select"])

        if "limit" not in sql_lower:
            raise ValueError(_VALIDATION_MESSAGES["missing_limit"])

        if _HARDCODED_LIMIT_RE.search(sql_lower):
            raise ValueError(_VALIDATION_MESSAGES["hardcoded_limit"])

        for pattern in ("scenario = 'actual'", 'scenario = "actual"'):
            if pattern in sql_lower:
                raise ValueError(_VALIDATION_MESSAGES["hardcoded_scenario"])

        # Strip REPLACE('.0','') before checking for hardcoded strings
        # so the structural normalization pattern doesn't trigger
        sql_for_check = re.sub(
            r"REPLACE\s*\([^)]+\)", "", sql, flags=re.IGNORECASE
        )
        # Also strip COALESCE(%(scenario)s,'ACTUAL') — the 'ACTUAL' default
        # is a fallback value, not a hardcoded filter
        sql_for_check = re.sub(
            r"COALESCE\s*\([^)]+\)", "", sql_for_check, flags=re.IGNORECASE
        )
        matches = _HARDCODED_STRING_RE.findall(sql_for_check)
        suspicious = [
            m for m in matches
            if not any(allowed in m for allowed in _ALLOWED_LITERALS)
        ]
        if suspicious:
            raise ValueError(
                _VALIDATION_MESSAGES["hardcoded_literals"].format(suspicious=suspicious)
            )

        if re.search(r"select\s+\*", sql_lower):
            raise ValueError(_VALIDATION_MESSAGES["select_star"])

        if re.search(r"count\s*\(\s*\*\s*\)", sql_lower):
            raise ValueError(_VALIDATION_MESSAGES["count_star"])

        for item in _FORBIDDEN_COLUMNS:
            col = item["column"]
            reason = item.get("reason", "")
            if col in sql_lower:
                raise ValueError(
                    _VALIDATION_MESSAGES["forbidden_column"].format(column=col, reason=reason)
                )

        # Optional placeholder hygiene: only allow entity/filter placeholders
        # when the corresponding slot is actually present for this request.
        for placeholder, slot_key in _OPTIONAL_PLACEHOLDER_SLOTS.items():
            if placeholder in sql:
                slot_val = slots.get(slot_key)
                if slot_val in (None, "", [], {}):
                    raise ValueError(
                        _VALIDATION_MESSAGES["missing_slot_placeholder"].format(
                            placeholder=placeholder,
                            slot_key=slot_key,
                        )
                    )

    def _empty(self) -> SQLGenOutput:
        return SQLGenOutput(
            sql=_EMPTY_SQL,
            params={},
            tables=[],
            confidence=0.0,
        )