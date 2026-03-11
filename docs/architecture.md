# kai-agent – Reference Architecture (Implemented)

This document describes the **implemented** architecture of `kai-agent` as it exists in this repository.
It covers how the system routes natural-language questions, safely queries Postgres/Mongo, merges results, and generates narrative answers.

---

## 1) System overview

`kai-agent` is a **schema-guarded, multi-agent maritime analytics assistant**.

It answers questions using:
- **PostgreSQL** for analytics KPIs (finance + ops)
- **MongoDB** for rich voyage/vessel context (remarks, fixtures, ports, grades, legs, etc.)
- **Redis** for session memory (slots + last intent + turn counter)
- **LLMs** for:
  - intent + slot extraction (with deterministic overrides)
  - dynamic query generation (guarded SQL and guarded Mongo find specs)
  - narrative response generation (draft + “polish/editor” rewrite)

**Hard constraint**: LLMs do not connect to databases directly. All database access is performed by adapters/agents.

---

## 2) High-level request flow

```
User input
  ↓
GraphRouter.handle(session_id, user_input)
  ↓
Load session (Redis)
  ↓
Intent + slot extraction (LLMClient.extract_intent_slots + deterministic rules + regex)
  ↓
Planning (Planner.build_plan → single vs composite)
  ↓
Execution (agents: finance / ops / mongo)
  ↓
Merge results (artifacts.merged_rows)
  ↓
Compaction + sanitization (response_merger.compact_payload + GraphRouter._sanitize_for_llm)
  ↓
Narrative answer generation (LLMClient.summarize_answer + polish pass)
  ↓
Return { intent_key, slots, answer, data, plan, dynamic_sql_used, dynamic_sql_agents }
```

---

## 3) Core orchestration

### 3.1 `GraphRouter` (LangGraph-style orchestration)
**File**: `app/orchestration/graph_router.py`

Responsibilities:
- maintains GraphState: `intent_key`, `slots`, `plan`, `step_index`, `artifacts`, `finance`, `ops`, `mongo`, `merged`, `answer`
- executes:
  - **single** path (entity-style questions)
  - **composite** path (multi-step analysis/ranking)
- performs deterministic merge into `artifacts.merged_rows`
- compacts/sanitizes payload before LLM summarization
- persists session state back to Redis

Public API:
- `handle(session_id, user_input)` → returns the final response object

### 3.2 `Planner` (single vs composite planning)
**File**: `app/orchestration/planner.py`

Planner is deterministic and uses:
- extracted intent_key
- extracted slots
- phrase heuristics (ranking/trend/offhire/compare)

Outputs:
- `ExecutionPlan(plan_type="single")` with no steps, OR
- `ExecutionPlan(plan_type="composite")` with steps like:

Composite pattern:
1. (optional) `mongo.resolve_anchor`
2. `finance.dynamic_sql`
3. `ops.dynamic_sql` (skipped for scenario comparison)
4. (optional) `mongo.fetch_remarks`
5. `llm.merge`

---

## 4) Intent model and configuration

**File**: `app/registries/intent_registry.py`

- `SUPPORTED_INTENTS`: full list of supported intents
- `INTENT_REGISTRY`: per-intent needs + slot requirements

Each intent config declares:
- `required_slots`, `optional_slots`
- `needs: { mongo, finance, ops }`
- mongo anchor intent + default projection, when applicable

---

## 5) Agents

### 5.1 FinanceAgent (Postgres)
**File**: `app/agents/finance_agent.py`

Role:
- produces finance KPI rows from Postgres (`finance_voyage_kpi`)

Modes:
- `registry_sql`: runs a known SQL registry query
- `dynamic_sql`: generates SQL via `SQLGenerator`, validates via `sql_guard`, executes via `PostgresAdapter.execute_dynamic_select`

**Dynamic SQL enablement / initialization**
- Dynamic SQL is invoked **only in composite plans** (multi-step analysis/ranking).
- In composite execution, the router calls `FinanceAgent.run_dynamic(...)` directly:
  - `SQLGenerator.generate(question, intent_key, slots, agent="finance")`
  - `sql_guard.validate_and_prepare_sql(sql, params, allowlist, enforce_limit=True)`
  - `PostgresAdapter.execute_dynamic_select(guard.sql, guard.params)`
- Output contract:
  - `mode: "dynamic_sql"`
  - `sql`: the final guarded SQL string (for debugging)
  - `rows`: query results (capped upstream in orchestration)

### 5.2 OpsAgent (Postgres)
**File**: `app/agents/ops_agent.py`

Role:
- produces operational rows from Postgres (`ops_voyage_summary`) including JSON aggregates:
  - `ports_json`, `grades_json`, `remarks_json`

Modes:
- `registry_sql`
- `dynamic_sql` with strict guardrails:
  - canonical query paths when `voyage_ids` or `voyage_number` exist
  - deterministic `vessel.summary` query path (ensures ports/grades/remarks coverage)
  - blocks finance columns from leaking into ops queries

**Dynamic SQL behavior (ops)**
- Dynamic SQL is invoked **only in composite plans** (multi-step analysis/ranking).
- In composite execution, the router calls `OpsAgent.run_dynamic(...)` directly.
- For data completeness, `OpsAgent.run_dynamic()` prefers deterministic “canonical” SQL paths when possible:
  - If `slots.voyage_ids` exists → fetch from `ops_voyage_summary` including `ports_json`, `grades_json`, `remarks_json`.
  - If `slots.voyage_number` exists → resolve `voyage_id` then re-run with `voyage_ids`.
  - If `intent_key == "vessel.summary"` → deterministic query by IMO and/or vessel name (ensures ports/grades/remarks are available for narrative vessel summaries).

### 5.3 MongoAgent (MongoDB)
**File**: `app/agents/mongo_agent.py`

Role:
- resolves vessel/voyage anchors and fetches documents with minimal projection
- supports **Dynamic NoSQL**:
  - `run_llm_find(question, slots)` → LLM generates a safe `find()` spec using `mongo_schema_hint()`
  - spec is validated via `mongo_guard.validate_mongo_spec`
  - query is executed via `MongoAdapter.find_many`

Notes:
- Mongo stores `voyageNumber` as **string**, so slots are normalized to string for Mongo find specs.

**Dynamic NoSQL enablement / initialization**
- Dynamic NoSQL is available when `MongoAgent` is constructed with an `LLMClient` (so it can create a `MongoQueryBuilder`).
- Execution path:
  - `mongo_schema_hint()` provides a stable list of collections/fields + allowed operators.
  - `MongoQueryBuilder.build(...)` prompts the LLM to output a strict JSON `MongoQuerySpec`.
  - `mongo_guard.validate_mongo_spec(...)` enforces allowed collections/operators, projection constraints, and `limit <= 50`.
  - `MongoAdapter.find_many(...)` executes the safe Mongo query.
- Output contract:
  - `mode: "mongo_llm"`
  - `ok: true|false`, with `reason` when false
  - `collection`, `filter`, `projection`, `limit`
  - `rows`: projected documents (capped upstream in orchestration)

---

## 6) Adapters (infrastructure boundary)

### 6.1 PostgresAdapter
**File**: `app/adapters/postgres_adapter.py`

- pooled connections (psycopg2)
- supports:
  - registry queries (`fetch_all`)
  - dynamic select (`execute_dynamic_select`)

Dynamic safety:
- only allows `SELECT` or `WITH ... SELECT`
- blocks DML keywords
- normalizes `:param` → `%(param)s`
- enforces `LIMIT` if missing
- filters parameters to those used in SQL

### 6.2 MongoAdapter
**File**: `app/adapters/mongo_adapter.py`

Read-only access to:
- `vessels`
- `voyages`

Provides:
- anchor helpers (vessel name → IMO, voyage number → voyageId)
- document fetch with projections and remark slicing
- `find_many()` for dynamic NoSQL execution

### 6.3 RedisStore
**File**: `app/adapters/redis_store.py`

Provides:
- `load_session()`, `save_session()` (router-facing session memory)
- optional idempotency + distributed lock utilities

---

## 7) Query generation and guards

### 7.1 SQL generation
**File**: `app/sql/sql_generator.py`

Generates SQL for known intents and patterns.
Uses safe metric mapping and deterministic templates for key intents.

### 7.2 SQL guard (validation)
**Files**:
- `app/sql/sql_guard.py`
- `app/sql/sql_allowlist.py`

Validates:
- allowed tables only (`finance_voyage_kpi`, `ops_voyage_summary`)
- blocks forbidden patterns
- validates select list for invalid columns
- handles CTE names, ignores `LATERAL`, enforces LIMIT
- sanitizes parameters (lists for `ANY(...)`, preserves normal strings)

### 7.3 Mongo schema hint + guard (Dynamic NoSQL)
**Files**:
- `app/orchestration/mongo_schema.py` (schema hint, audited field names, allowed operators)
- `app/llm/mongo_query_builder.py` (LLM JSON spec builder)
- `app/mongo/mongo_guard.py` (operator/projection/limit validation)

### 7.4 How orchestration uses Dynamic SQL / Dynamic NoSQL

Dynamic SQL / NoSQL are invoked by the orchestrator depending on plan type:

- **Single**:
  - `FinanceAgent.run()` and `OpsAgent.run()` use **registry SQL** (safe, deterministic query mapping) for entity summaries.
  - `voyage.summary` single path also fetches Mongo context; the router normalizes the mongo payload into a `mode: mongo_llm`-like shape for a consistent summarizer/validator contract.

- **Composite**:
  - Finance step uses `FinanceAgent.run_dynamic(...)` (forced in composite execution).
  - Ops step uses `OpsAgent.run_dynamic(...)` (forced in composite execution, typically with `voyage_ids` from finance).
  - Mongo enrichment step uses `MongoAgent.run_llm_find(...)` to fetch remarks/ports/grades/commissions for the composite set of voyage IDs.
  - Merge step creates `artifacts.merged_rows` as the primary joined dataset for the summarizer.

---

## 8) Merging, compaction, and summarization

### 8.1 Deterministic merge
**File**: `app/orchestration/graph_router.py`

Composite merges by `voyage_id` into:
- `artifacts.merged_rows`: one item per voyage containing:
  - finance row
  - ops row(s)
  - remarks / ports / grades derived from Mongo

### 8.2 Compaction
**File**: `app/services/response_merger.py`

`compact_payload()` caps:
- finance rows
- ops rows
- mongo rows
- artifacts.merged_rows

This prevents token blowups.

### 8.3 Narrative answer generation (draft + polish)
**File**: `app/llm/llm_client.py`

Pipeline:
1. Draft answer from compact JSON, guided by intent templates and style flags.
2. “Polish pass”: send {query + JSON + draft} to an editor prompt to rewrite into a question-driven narrative without inventing facts.
3. Postprocess: normalize bullets, remove repeated headings/duplicate lines.

---

## 9) Data sources and mapping rules

### 9.1 MongoDB key fields
- `voyages.voyageId` (string UUID)
- `voyages.voyageNumber` (string)
- `voyages.vesselName`
- `voyages.remarks[]`, `voyages.fixtures[]`, `voyages.legs[]`, etc.

### 9.2 Postgres key fields
- `finance_voyage_kpi.voyage_id` (string UUID)
- `finance_voyage_kpi.voyage_number` (int)
- `ops_voyage_summary.voyage_id` (string UUID)
- `ops_voyage_summary.voyage_number` (int)
- `ops_voyage_summary.ports_json`, `grades_json`, `remarks_json` (JSONB arrays)

### 9.3 Join rules
- Preferred join: `Mongo.voyageId` ↔ `Postgres.voyage_id`
- Voyage number conversion:
  - Mongo expects `"1901"` (string)
  - Postgres expects `1901` (int)

---

## 10) Operational constraints and guardrails
- Read-only DB access (SELECT/find only)
- SQL allowlist + forbidden pattern checks
- Mongo operator allowlist
- LIMIT enforcement + row caps
- token compaction before summarization
- Redis session TTL

---

## 11) Testing
**File**: `scripts/C.py`

Interactive test harness that:
- runs `GraphRouter.handle()`
- prints intent/plan/agent execution
- validates dynamic SQL usage and dynamic NoSQL usage
- logs results to `test_validation_log.json`
