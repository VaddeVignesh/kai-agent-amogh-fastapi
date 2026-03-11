# app/adapters/postgres_adapter.py
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

import psycopg2
from psycopg2 import OperationalError
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import RealDictCursor

from app.registries.sql_registry import SQL_REGISTRY, QuerySpec


# =========================================================
# Debug logging
# =========================================================

def _debug_enabled() -> bool:
    return (os.getenv("KAI_DEBUG") or "").strip().lower() in ("1", "true", "yes", "y", "on")


def _dprint(*args: Any, **kwargs: Any) -> None:
    if _debug_enabled():
        print(*args, **kwargs)


# =========================================================
# Config
# =========================================================

@dataclass(frozen=True)
class PostgresConfig:
    dsn: str
    minconn: int = 1
    maxconn: int = 5

    @classmethod
    def from_env(cls, minconn: int = 1, maxconn: int = 5) -> "PostgresConfig":
        dsn = os.getenv("POSTGRES_DSN")
        if dsn:
            # Ensure we fail fast when Postgres is down.
            # libpq supports connect_timeout (seconds) as a connection parameter.
            timeout_sec = str(int(os.getenv("POSTGRES_CONNECT_TIMEOUT_SEC", "2")))
            if "connect_timeout=" not in dsn:
                if "?" in dsn:
                    dsn = f"{dsn}&connect_timeout={timeout_sec}"
                else:
                    dsn = f"{dsn}?connect_timeout={timeout_sec}"
            return cls(dsn=dsn, minconn=minconn, maxconn=maxconn)

        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        user = os.getenv("POSTGRES_USER", "admin")
        password = os.getenv("POSTGRES_PASSWORD", "admin123")
        database = os.getenv("POSTGRES_DB", "postgres")

        dsn = f"postgresql://{user}:{quote_plus(password)}@{host}:{port}/{database}"
        return cls(dsn=dsn, minconn=minconn, maxconn=maxconn)


class PostgresQueryError(RuntimeError):
    pass


# =========================================================
# Adapter
# =========================================================

class PostgresAdapter:

    MAX_ROWS: int = 500
    DEFAULT_LIMIT: int = 10
    MAX_LIMIT: int = 200

    def __init__(self, cfg: PostgresConfig):
        self.cfg = cfg
        # Lazy-init pool so the chatbot can start even if Postgres is down.
        # We only attempt a connection when a query actually runs.
        self.pool: Optional[SimpleConnectionPool] = None
        self._unavailable_until_ts: float = 0.0

    def _ensure_pool(self) -> SimpleConnectionPool:
        # If Postgres was recently unavailable, fail fast instead of repeatedly waiting on connect.
        import time
        now = time.time()
        if self._unavailable_until_ts and now < self._unavailable_until_ts:
            raise PostgresQueryError(
                "Postgres is not available (recent connection failure). "
                "Please start Postgres on localhost:5432 or set POSTGRES_DSN/POSTGRES_HOST/POSTGRES_PORT."
            )
        if self.pool is not None:
            return self.pool
        try:
            self.pool = SimpleConnectionPool(self.cfg.minconn, self.cfg.maxconn, dsn=self.cfg.dsn)
            return self.pool
        except OperationalError as e:
            # Backoff window prevents repeated long waits per request (finance + ops).
            backoff_sec = float(os.getenv("POSTGRES_UNAVAILABLE_BACKOFF_SEC", "5"))
            self._unavailable_until_ts = time.time() + max(0.0, backoff_sec)
            raise PostgresQueryError(
                "Postgres is not available (connection refused). "
                "Please start Postgres on localhost:5432 or set POSTGRES_DSN/POSTGRES_HOST/POSTGRES_PORT."
            ) from e

    # =========================================================
    # Registry Execution
    # =========================================================

    def fetch_all(
        self,
        query_key: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        spec = self._get_spec(query_key)
        final_params = self._prepare_params(spec, params or {})
        return self._execute_fetch_all(spec.sql, final_params)

    def fetch_one(
        self,
        query_key: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        rows = self.fetch_all(query_key, params)
        return rows[0] if rows else None

    # =========================================================
    # Dynamic SQL Execution (LLM SAFE)
    # =========================================================

    def execute_dynamic_select(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:

        if not sql:
            raise PostgresQueryError("Empty SQL provided.")

        sql_stripped = sql.strip()
        sql_lower = sql_stripped.lower()

        # -----------------------------------------------------
        # 🔐 STRICT SECURITY
        # -----------------------------------------------------

        # Allow CTE-based selects (WITH ... SELECT ...)
        if not (sql_lower.startswith("select") or sql_lower.startswith("with")):
            raise PostgresQueryError("Only SELECT queries are allowed.")

        forbidden = ["insert", "update", "delete", "drop", "alter", "truncate"]
        if any(word in sql_lower for word in forbidden):
            raise PostgresQueryError("Unsafe SQL detected.")

        # 🚨 BLOCK positional parameters ($1, $2, ...)
        if re.search(r"\$\d+", sql_stripped):
            raise PostgresQueryError(
                "Positional parameters ($1, $2...) are not supported. "
                "Use named parameters %(param)s instead."
            )

        # -----------------------------------------------------
        # Normalize :param → %(param)s
        # -----------------------------------------------------

        sql = self._normalize_param_format(sql_stripped)

        # -----------------------------------------------------
        # Enforce LIMIT if missing
        # -----------------------------------------------------

        if "limit" not in sql_lower:
            sql = f"{sql.rstrip(';')} LIMIT {self.DEFAULT_LIMIT}"

        params = params or {}

        # -----------------------------------------------------
        # Filter params to only those used in SQL
        # -----------------------------------------------------

        sql_param_names = re.findall(r"%\((\w+)\)s", sql)
        filtered_params = {k: v for k, v in params.items() if k in sql_param_names}

        _dprint(f"🔍 Params filter: {len(params)} → {len(filtered_params)} (SQL has: {sql_param_names})")

        return self._execute_fetch_all(sql, filtered_params)

    # =========================================================
    # Param Normalization
    # =========================================================

    def _normalize_param_format(self, sql: str) -> str:
        """
        Converts:
            :limit → %(limit)s
            :voyage_id → %(voyage_id)s

        Preserves:
            ::text
            ::date
        """
        pattern = r"(?<!:):(\w+)(?!:)"
        return re.sub(pattern, lambda m: f"%({m.group(1)})s", sql)

    # =========================================================
    # Registry Helpers
    # =========================================================

    def _get_spec(self, query_key: str) -> QuerySpec:
        if query_key not in SQL_REGISTRY:
            raise PostgresQueryError(f"Unknown query_key: {query_key}")
        return SQL_REGISTRY[query_key]

    def _prepare_params(self, spec: QuerySpec, params: Dict[str, Any]) -> Dict[str, Any]:
        p = dict(params or {})

        # Validate required params
        missing = [
            k for k in getattr(spec, "required_params", [])
            if k not in p or p[k] in (None, "", {})
        ]
        if missing:
            raise PostgresQueryError(f"Missing required params: {missing}")

        # Auto-detect placeholders
        placeholders = re.findall(r"%\((\w+)\)s", spec.sql)
        for ph in placeholders:
            if ph not in p:
                p[ph] = None

        # Enforce LIMIT bounds
        if "limit" in p:
            try:
                p["limit"] = int(p["limit"])
            except Exception:
                p["limit"] = self.DEFAULT_LIMIT
            p["limit"] = max(1, min(p["limit"], self.MAX_LIMIT))

        return p

    # =========================================================
    # Core executor
    # =========================================================

    def _execute_fetch_all(self, sql: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        conn = None
        pool = self._ensure_pool()
        try:
            conn = pool.getconn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:

                _dprint(f"🔍 DEBUG _execute_fetch_all: params={params}")

                # ALWAYS pass params
                cur.execute(sql, params or {})

                rows: List[Dict[str, Any]] = []

                while True:
                    chunk = cur.fetchmany(self.MAX_ROWS - len(rows))
                    if not chunk:
                        break
                    rows.extend(chunk)
                    if len(rows) >= self.MAX_ROWS:
                        break

            return [dict(r) for r in rows]

        except Exception as e:
            raise PostgresQueryError(f"Postgres query failed: {e}") from e

        finally:
            if conn is not None:
                try:
                    pool.putconn(conn)
                except Exception:
                    pass

    # =========================================================
    # Cleanup
    # =========================================================

    def close(self) -> None:
        if self.pool:
            self.pool.closeall()
