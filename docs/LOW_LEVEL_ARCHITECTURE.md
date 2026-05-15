# KAI Agent Low-Level Architecture

This LLD maps the current runtime implementation to concrete modules, functions, data contracts, and execution responsibilities.

---

## 1. Runtime File Map

```text
app/
  main.py
  auth.py
  orchestration/
    graph_router.py
    planner.py
    source_router.py
    mongo_schema.py
  agents/
    finance_agent.py
    ops_agent.py
    mongo_agent.py
  adapters/
    postgres_adapter.py
    mongo_adapter.py
    redis_store.py
  llm/
    llm_client.py
    mongo_query_builder.py
  sql/
    sql_generator.py
    sql_guard.py
    sql_allowlist.py
  mongo/
    mongo_guard.py
  services/
    response_merger.py
    business_reasoning.py
    source_reconciliation.py
  config/
    *_loader.py
    database.py
  registries/
    intent_loader.py
    intent_registry.py
    sql_registry.py

frontend/digital-sales-agent-main/
  src/App.tsx
  src/pages/LoginPage.tsx
  src/pages/AssistantPage.tsx
  src/pages/AdminPage.tsx
  src/components/chat/AnalyticsChat.tsx
```

---

## 2. Backend Initialization

`app/main.py` creates the runtime graph once at import/startup.

Initialization sequence:

1. Load `.env`.
2. Configure LangChain tracing environment flags.
3. Create FastAPI app and CORS middleware.
4. Create `LLMClient`.
5. Create Mongo database client through `get_mongo_db()`.
6. Create `MongoAdapter` and `MongoAgent`.
7. Create `PostgresAdapter`.
8. Create `FinanceAgent` and `OpsAgent`.
9. Create `RedisStore`.
10. Create `GraphRouter`.

The query endpoint then calls:

```python
router.handle(session_id=session_id, user_input=req.query)
```

The endpoint returns `QueryResponse` and schedules background side effects for metrics/audit/history.

---

## 3. API Data Contracts

### QueryRequest

Fields:

- `query: str`
- `session_id: str | None`
- `request_id: str | None`
- `chat_history: list[dict]`

### QueryResponse

Fields:

- `session_id`
- `answer`
- `clarification`
- `trace`
- `intent_key`
- `slots`
- `dynamic_sql_used`
- `dynamic_sql_agents`

### Admin/Auth Models

`app/main.py` also defines models for:

- login
- admin metrics
- admin users
- audit events
- system health

RBAC behavior is backed by `app/auth.py` and session/role checks.

---

## 4. GraphState

`GraphState` in `graph_router.py` is the per-request state object passed through the graph.

Important fields:

| Field | Purpose |
| --- | --- |
| `session_id` | Current conversation/session key |
| `user_input` | Effective query used for planning/execution |
| `raw_user_input` | Actual user text this turn |
| `session_ctx` | Redis-loaded session state |
| `intent_key` | Canonical intent |
| `slots` | Extracted and cleaned parameters |
| `missing_keys` | Required missing slots |
| `clarification` | Clarification message, if execution stops early |
| `plan_type` | `single` or `composite` |
| `plan` | Serialized execution plan |
| `step_index` | Current composite step index |
| `finance` | Finance agent result |
| `ops` | Ops agent result |
| `mongo` | Mongo agent result |
| `data` | Normalized source payload |
| `merged` | Final merged payload |
| `answer` | Final answer |
| `artifacts` | Trace, merged rows, ids, coverage, dynamic SQL flags |

---

## 5. Graph Nodes

### `n_load_session`

Calls:

```python
redis_store.load_session(session_id)
```

Loads:

- previous slots
- last intent
- pending clarification
- result-set memory
- focus slots
- previous user input

### `n_extract_intent`

Responsibilities:

- exact incomplete-entity fast paths
- simple voyage fast paths
- pending clarification resolution
- fresh question vs follow-up detection
- result-set follow-up handling
- deterministic intent overrides
- LLM intent extraction when needed
- slot cleanup
- placeholder removal
- scenario detection
- trace event emission

Examples of config-driven incomplete entity handling:

- `tell me about vessel`
- `tell me about vesssl`
- `tell me about vessels`
- `tell me about voyage`
- `tell me about voyages`
- `tell me about voyge`
- `tell me about port`

These should route to entity summary/detail intents and then trigger clarification if the anchor is missing.

### `n_validate_slots`

Checks required slots from `INTENT_REGISTRY`.

Special behavior:

- fleet-wide composite intents skip entity clarification
- placeholder values such as generic `vessel`, `ship`, `voyage`, `port`, typo variants, and plural variants are treated as missing
- incomplete `vessel.summary`, `voyage.summary`, and `port.details` asks become clarification turns

### `n_make_clarification`

Builds clarification text and suggestions.

Persists to Redis:

- `pending_intent`
- `missing_keys`
- `clarification_options`
- `pending_question`
- `pending_slots`

### `n_plan`

Calls `Planner.build_plan()`.

Stores:

- `plan_type`
- serialized plan
- `step_index = 0`

### `n_run_single`

Handles direct execution for:

- anchored voyage summary
- anchored vessel summary
- voyage metadata
- vessel metadata
- ranking vessel metadata
- port details
- some follow-up actions
- out-of-scope responses

It can route to merge if multiple source payloads need final summarization.

### `n_execute_step`

Runs one composite step at a time.

Step types include:

- `mongo.resolveAnchor`
- `finance.dynamicSQL`
- `ops.dynamicSQL`
- `mongo.fetchRemarks`
- `llm.merge`

Important behavior:

- resolves `$finance.voyage_ids` before ops execution
- records trace per step
- extracts voyage ids from finance rows
- skips ops enrichment when aggregate finance rows have no voyage ids
- preserves dynamic SQL metadata

### `n_merge`

Builds `state["merged"]` from source sections and artifacts.

Marks:

- `dynamic_sql_used`
- `dynamic_sql_agents`

### `n_summarize`

Calls:

```python
compact_payload(merged)
llm.summarize_answer(question, plan, compacted_payload)
```

Then saves updated session context and result-set memory back to Redis.

---

## 6. Planner Internals

`app/orchestration/planner.py`

Core models:

- `ExecutionStep`
  - `agent`
  - `operation`
  - `inputs`

- `ExecutionPlan`
  - `plan_type`
  - `intent_key`
  - `required_slots`
  - `confidence`
  - `steps`

Planner rules:

- entity-anchored queries usually stay `single`
- `ranking.*`, `analysis.*`, `aggregation.*`, and decision-style queries usually become `composite`
- some metadata intents remain single Mongo-backed paths
- zero-row or wrong-shape single results can be escalated to composite

Composite default pattern:

1. optional Mongo anchor
2. finance query
3. ops enrichment
4. optional Mongo enrichment
5. merge

---

## 7. Agent Contracts

### FinanceAgent

File:

`app/agents/finance_agent.py`

Methods:

- `run(...)`: registry SQL path
- `run_dynamic(...)`: LLM SQL path plus guard

Outputs:

- `mode`
- `rows`
- `sql` for dynamic SQL
- `voyage_ids`
- `ok`/error metadata

### OpsAgent

File:

`app/agents/ops_agent.py`

Methods:

- `run(...)`: registry SQL path
- `run_dynamic(...)`: canonical ops or guarded dynamic SQL path

Canonical paths:

- by `voyage_ids`
- by `voyage_number` after resolving voyage id
- vessel summary
- cargo profitability grade enrichment

OpsAgent blocks finance-only fields from ops dynamic SQL.

### MongoAgent

File:

`app/agents/mongo_agent.py`

Methods:

- `run(...)`
- `run_llm_find(...)`
- `fetch_full_voyage_context(...)`

Responsibilities:

- resolve voyage/vessel anchors
- fetch vessel metadata
- fetch voyage metadata
- run guarded dynamic Mongo find specs

---

## 8. Adapter Contracts

### PostgresAdapter

File:

`app/adapters/postgres_adapter.py`

Responsibilities:

- connection pool
- registry query execution through query keys
- dynamic SELECT execution
- param preparation
- max row cap
- read-only SQL enforcement

### MongoAdapter

File:

`app/adapters/mongo_adapter.py`

Responsibilities:

- fetch voyage documents
- fetch vessel documents
- resolve vessel name to IMO
- resolve voyage number to voyage id
- run read-only `find_many`

### RedisStore

File:

`app/adapters/redis_store.py`

Responsibilities:

- load/save sessions
- in-memory fallback
- idempotency get/set
- locks
- metrics/audit/history helpers where used

---

## 9. SQL Generation And Guard

### SQLGenerator

File:

`app/sql/sql_generator.py`

Uses:

- schema hint
- intent-specific SQL hints
- SQL rules
- slots
- agent type
- LLM SQL generation

Returns `SQLGenOutput`.

### SQL Guard

Files:

- `app/sql/sql_guard.py`
- `app/sql/sql_allowlist.py`

Validation includes:

- allowed tables
- allowed columns
- no DML
- no unsafe patterns
- valid select shape
- required LIMIT
- param cleanup
- safe rewrites such as `IN %(x)s` to `= ANY(%(x)s)`

---

## 10. Mongo Query Generation And Guard

### MongoQueryBuilder

File:

`app/llm/mongo_query_builder.py`

Builds a strict Mongo query spec using schema hints.

### Mongo Guard

File:

`app/mongo/mongo_guard.py`

Validates:

- collection
- filter object
- allowed operators
- projection
- sort
- limit

---

## 11. Merge And Business Enrichment

### response_merger

File:

`app/services/response_merger.py`

Main function:

```python
compact_payload(merged)
```

It:

- trims large source sections
- builds light `merged_rows`
- caps ports/grades/remarks
- preserves finance and ops KPIs
- preserves decision fields
- applies reconciliation
- applies business reasoning

### source_reconciliation

File:

`app/services/source_reconciliation.py`

Functions:

- `reconcile_merged_row(row)`
- `reconcile_sources(row)`

Outputs:

- `status`
- `severity`
- `canonical_fields`
- `caveats`
- `matched_fields`
- `missing_or_single_source_fields`
- `mismatches`

### business_reasoning

File:

`app/services/business_reasoning.py`

Functions:

- `enrich_row_with_business_reasoning(row)`
- derived metric evaluation
- signal evaluation

Supported condition language:

- `all`
- `any`
- numeric ops
- boolean ops
- exists/missing
- field-to-field comparisons

---

## 12. Config Loader Pattern

Config files live in `config/`.

Loaders live in `app/config/`.

Pattern:

- YAML owns policy.
- loader reads/caches YAML.
- Python modules consume loader functions.

Important loaders:

- `routing_rules_loader.py`
- `prompt_rules_loader.py`
- `business_rules_loader.py`
- `sql_rules_loader.py`
- `response_rules_loader.py`
- `mongo_rules_loader.py`

---

## 13. Prompt And Answer Flow

`LLMClient.summarize_answer()`:

1. Receives question, plan, and compacted data.
2. Adds answer style flags.
3. Adds `business_answer_contract`.
4. Sends system prompt from `prompt_rules.yaml`.
5. Produces draft answer.
6. Optionally polishes.
7. Applies markdown post-processing and configured replacements.

Answer prompt rules include:

- use only provided JSON
- prefer `artifacts.merged_rows`
- include PnL/revenue for ranking rows
- include margin/cost ratio when available and relevant
- mention blocking reconciliation caveats
- use business reasoning signals for impact
- avoid irrelevant unavailable-field caveats

---

## 14. Frontend Runtime Details

React frontend:

- `AnalyticsChat.tsx` creates `request_id`
- posts to API
- renders `clarification` before empty answers
- renders markdown with GFM
- maps trace phases and agents to user-friendly labels
- includes admin diagnostics tabs

Admin pages use login/session role information from backend RBAC.

---

## 15. Test Coverage Map

Important tests:

- `tests/test_slot_clarification.py`
- `tests/test_routing_rules_loader.py`
- `tests/test_prompt_rules_loader.py`
- `tests/test_sql_registry_loader.py`
- `tests/test_sql_rules_loader.py`
- `tests/test_business_reasoning.py`
- `tests/test_source_reconciliation.py`
- `tests/test_response_merger_config.py`
- `tests/test_golden_config_suite.py`
- dynamic SQL and adapter regression tests

Golden files:

- `scripts/golden_config_suite.json`
- `scripts/golden_config_baseline.json`
- `business_decision_current.json` when captured locally

Runner:

```powershell
python scripts/run_golden_config_suite.py capture --category business_decision --base-url http://127.0.0.1:8010/query --output business_decision_current.json
```

---

## 16. Low-Level Summary

At low level, the system is a state machine around safe data retrieval:

1. FastAPI receives query.
2. GraphRouter creates GraphState.
3. Redis session is loaded.
4. Intent/slots are extracted.
5. Missing slots trigger clarification.
6. Planner creates single/composite plan.
7. Agents run registry or guarded dynamic queries.
8. Results merge by stable identity.
9. Reconciliation and reasoning enrich rows.
10. Payload is compacted.
11. LLM writes final answer.
12. Session, trace, metrics, and history are persisted.
