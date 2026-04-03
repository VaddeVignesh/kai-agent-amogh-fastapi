# KAI Agent Low-Level Architecture

## Scope
This document describes the current low-level runtime architecture after the recent routing, metadata, follow-up, and dynamic SQL fixes.

It focuses on the code paths that actually execute in production and maps the implementation to the main runtime files.

## Runtime File Map
```text
app/
  main.py
  UI/UX/streamlit_app.py
  orchestration/
    graph_router.py
    planner.py
  registries/
    intent_registry.py
    sql_registry.py
  agents/
    finance_agent.py
    ops_agent.py
    mongo_agent.py
  adapters/
    postgres_adapter.py
    mongo_adapter.py
    redis_store.py
  services/
    response_merger.py
  sql/
    sql_generator.py
    sql_guard.py
    sql_allowlist.py
  llm/
    llm_client.py
```

## Entry Points
### `app/main.py`
The FastAPI backend is initialized in `app/main.py`.

Responsibilities:
- load environment variables
- build `LLMClient`
- build Mongo, Postgres, and Redis dependencies
- create `GraphRouter`
- expose:
  - `GET /`
  - `POST /query`
  - `POST /session/clear`

Response payload from `/query` includes:
- `session_id`
- `answer`
- `clarification`
- `trace`
- `intent_key`
- `slots`
- `dynamic_sql_used`
- `dynamic_sql_agents`

### `app/UI/UX/streamlit_app.py`
The Streamlit client is a thin frontend over the API.

Important behavior:
- stores `session_id` in session state
- calls `/query`
- renders final answer and trace
- renders SQL snippets from trace
- can clear backend session state via `/session/clear`

## Runtime State Model
### `GraphState`
Defined in `app/orchestration/graph_router.py`.

Important fields:
- `session_id`
- `user_input`
- `raw_user_input`
- `session_ctx`
- `intent_key`
- `slots`
- `missing_keys`
- `clarification`
- `plan_type`
- `plan`
- `step_index`
- `mongo`
- `finance`
- `ops`
- `data`
- `merged`
- `answer`
- `artifacts`

`artifacts` is the main transient execution container and carries:
- `trace`
- `merged_rows`
- `coverage`
- extracted `voyage_ids`
- dynamic SQL metadata
- result-set memory candidates

## Orchestration Graph
The graph is assembled in `GraphRouter._build_graph()`.

Core nodes:
- `load_session` -> `n_load_session`
- `extract` -> `n_extract_intent`
- `validate` -> `n_validate_slots`
- `clarify` -> `n_make_clarification`
- `build_plan` -> `n_plan`
- `run_single` -> `n_run_single`
- `execute_step` -> `n_execute_step`
- `merge` -> `n_merge`
- `summarize` -> `n_summarize`

Conditional routing:
- after validation:
  - `clarify`
  - `plan`
  - or early `summarize` for unsupported edge cases
- after planning:
  - `single`
  - or `composite`
- after single:
  - `done`
  - `merge`
  - or `escalate` into composite after zero-row escalation
- after composite step execution:
  - continue next step
  - or move to merge

## Detailed Request Lifecycle
### 1. Session load
`n_load_session` calls `RedisStore.load_session(session_id)`.

Loaded session context may contain:
- previous slots
- previous intent
- `last_result_set`
- `last_focus_slots`
- pending clarification state
- previous user input

### 2. Turn classification and intent extraction
`n_extract_intent` is now more than plain LLM classification.

Current behavior includes:
- clarification-follow-up resolution
- result-set follow-up fast paths
- placeholder slot cleanup
- deterministic overrides for common intent families
- session-aware slot carry-forward
- optional LLM intent extraction through `LLMClient.extract_intent_slots`

Important recent runtime improvements reflected here:
- stronger fresh-question vs follow-up detection
- explicit result-set follow-up handling
- voyage-linked vessel metadata recovery
- metadata-first routing for correct source selection

### 3. Slot validation
`n_validate_slots` checks `INTENT_REGISTRY[intent]["required_slots"]`.

Special handling includes:
- treating placeholders such as `vessel`, `voyage`, and `port` as missing
- skipping clarification for fleet-wide composite intents
- preserving clarification behavior for incomplete entity-style questions

### 4. Clarification generation
If required slots are missing, `n_make_clarification`:
- builds the clarification message
- persists clarification context to Redis
- stops execution for this turn

### 5. Planning
`Planner.build_plan()` converts:
- `intent_key`
- `slots`
- `user_input`
- `force_composite`

into an `ExecutionPlan`.

Plan models:
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

### 6. Execution modes
The system executes in one of three practical patterns:
- `single`
- `composite`
- `single -> escalate -> composite`

#### Single mode
`n_run_single` is used for:
- anchored voyage questions
- anchored vessel questions
- metadata requests
- some follow-up actions

Typical characteristics:
- registry SQL preferred where applicable
- direct Mongo fetch for metadata-heavy intents
- lower latency
- simpler merge path

#### Composite mode
`n_execute_step` runs plan steps one at a time.

Typical composite sequence:
1. optional `mongo.resolveAnchor`
2. `finance.dynamicSQL`
3. `ops.dynamicSQL`
4. optional `mongo.fetchRemarks`
5. deterministic merge step

Even when one step is named `llm.merge`, the row joining itself is deterministic orchestration logic.

#### Zero-row escalation
If a question is misclassified as `single` and returns no rows without an entity anchor:
- router flips plan type to `composite`
- planner remaps certain entity intents to fleet-wide equivalents
- execution retries through the composite path

This is the current recovery path for misclassified fleet-wide questions.

## Planner Internals
### `app/orchestration/planner.py`
The planner is deterministic.

Important behavior:
- resolves aliases via `resolve_intent`
- keeps strongly entity-anchored questions on `single`
- routes registry-declared composite intents to `composite`
- supports `force_composite` zero-row escalation
- recognizes textual composite triggers such as:
  - `trend`
  - `over time`
  - `offhire + pnl`
  - `delayed + expense`
  - `cargo + port`

Composite plans are built in `_build_composite()` and generally include:
- finance first
- ops second
- Mongo enrichment when allowed
- merge last

Important current behavior:
- finance composite queries use dynamic SQL
- ops enrichment is skipped for some scenario-comparison paths
- Mongo enrichment is disabled for some aggregate-only intents

## Data Access Layer
### PostgreSQL
#### `app/adapters/postgres_adapter.py`
Responsibilities:
- connection pooling
- registry query execution
- dynamic SQL execution

Important behavior:
- lazy pool initialization
- SELECT-only enforcement
- write statement rejection
- `:param` normalization to `%(param)s`
- `LIMIT` injection if missing
- params filtered to only those referenced in SQL

### MongoDB
#### `app/adapters/mongo_adapter.py`
Responsibilities:
- fetch voyage or vessel by identifier
- resolve vessel name to IMO
- resolve voyage number to voyage id
- fetch voyage by number
- apply projections
- normalize remarks shape
- run safe `find_many` queries

### Redis
#### `app/adapters/redis_store.py`
Responsibilities:
- session memory
- clarification state persistence
- result-set memory persistence
- session clearing

Important persisted fields:
- `slots`
- `last_intent_key`
- `last_user_input`
- `last_result_set`
- `last_focus_slots`
- `anchor_type`
- `anchor_id`

Important behavior:
- sticky slots such as `scenario` and `limit` can persist safely
- volatile entity anchors are cleared when conversation family changes
- in-memory fallback exists when Redis is unavailable

## Agent Layer
### FinanceAgent
#### `app/agents/finance_agent.py`
Modes:
- `run()` -> registry SQL
- `run_dynamic()` -> generated SQL

`run_dynamic()` pipeline:
1. `SQLGenerator.generate(...)`
2. agent-level parameter injection
3. validation through `validate_and_prepare_sql`
4. execution through `PostgresAdapter.execute_dynamic_select`
5. repair loop for recoverable SQL problems
6. intent-specific post-checks and fallback repairs

Common guardrails:
- `finance_no_ops_join`
- `require_kpi_columns`
- `verify_scenario_variance_columns`
- `inject_voyage_numbers_param`
- `inject_filter_port_param`

### OpsAgent
#### `app/agents/ops_agent.py`
Modes:
- `run()` -> registry SQL
- `run_dynamic()` -> canonical shortcuts plus guarded dynamic SQL

Important deterministic branches in `run_dynamic()`:
- canonical fetch by `voyage_ids`
- cargo-grade profitability support
- voyage-number lookup to voyage-id path
- deterministic vessel-summary support

Important low-level behavior:
- protects against placeholder inputs
- normalizes cargo grades
- strips noisy/null grade values
- enforces ops-only SQL discipline

### MongoAgent
#### `app/agents/mongo_agent.py`
Responsibilities:
- resolve anchors
- fetch voyage and vessel documents
- fetch full voyage context
- optionally execute safe dynamic Mongo find

Mongo is now central for:
- `vessel.metadata`
- `voyage.metadata`
- `ranking.vessel_metadata`
- rich remarks and nested voyage context

## Registry Layer
### `app/registries/intent_registry.py`
This is the main semantic contract of the system.

Each intent can define:
- `description`
- `route`
- `required_slots`
- `optional_slots`
- `needs`
- `mongo_intent`
- `mongo_projection`
- `sql_hints`
- `guardrails`

This powers:
- planner routing
- classifier context
- SQL generation hints
- execution constraints

Notable updated intent families:
- `ranking.vessel_metadata`
- `vessel.metadata`
- cargo profitability and variance intents
- ranking and aggregation families used in composite mode

### `app/registries/sql_registry.py`
Defines `QuerySpec` entries for static SQL.

Used by:
- `FinanceAgent.run()`
- `OpsAgent.run()`
- registry-based single-path execution

## SQL Generation And Validation
### `app/sql/sql_generator.py`
Responsibilities:
- create schema hints by agent
- append intent-specific SQL hints from the registry
- enforce aggregate-pattern guidance
- call `LLMClient.generate_sql()`

Important prompt behavior:
- schema-aware prompt
- PostgreSQL-only discipline
- strict parameter format
- hard rules for JSONB access
- no invented columns
- current guidance for `NULLS LAST` ordering on ranked numeric metrics

### `app/sql/sql_guard.py`
Responsibilities:
- validate generated SQL against the allowlist
- reject forbidden patterns
- enforce select-only behavior
- normalize params for `ANY(...)`
- inject `LIMIT` when needed
- reject unsupported tables or columns

### `app/sql/sql_allowlist.py`
Responsibilities:
- define allowed tables
- define allowed columns per table
- define forbidden patterns

This constrains dynamic SQL to the intended schema subset.

## Merge And Summarization
### Merge in `GraphRouter`
The router performs the main cross-source merge.

Typical merged outputs include:
- `finance`
- `ops`
- `mongo`
- `artifacts.merged_rows`
- `artifacts.coverage`
- `dynamic_sql_used`
- `dynamic_sql_agents`

### Payload compaction
#### `app/services/response_merger.py`
`compact_payload()` is a post-merge compactor for summarization.

Responsibilities:
- flatten merged rows
- keep relevant top-level KPIs
- compact ports, grades, and remarks
- reduce token load
- preserve important aggregate metrics for final narration

### Final answer generation
#### `app/llm/llm_client.py`
Important entry points:
- `extract_intent_slots(...)`
- `summarize_answer(...)`

Current answer generation pipeline:
1. identify `intent_key`
2. prepare JSON-safe compact payload
3. build strict summarization prompt
4. ask the LLM for the narrative/table answer
5. apply deterministic output guards and markdown cleanup

Current deterministic answer guards include:
- `_ensure_ranking_voyages_answer(...)`
- `_ensure_ranking_vessels_answer(...)`
- `_ensure_cargo_profitability_answer(...)`

These exist to stabilize ranking and aggregate responses when the raw LLM answer is too generic or misses available metrics.

## Follow-Up And Session Logic
### Result-set follow-ups
The router supports follow-up actions over prior results such as:
- top/bottom filters
- compare extremes
- select one row from a prior result set
- project remarks, ports, or grades from the selected row

This is powered by:
- `last_result_set`
- `last_focus_slots`
- turn classification
- result-set follow-up parsing

### Clarification persistence
Pending clarification state is saved so the next user message can be interpreted as:
- a slot answer
- or a fresh question that cancels the clarification

## Execution Trace Model
The router emits a compact trace into `artifacts.trace`.

Trace events include:
- `intent_extraction`
- `planning`
- `composite_step_start`
- `composite_step_result`
- `token_usage`

The UI renders:
- agent
- operation
- inputs
- SQL where available
- row counts
- extracted voyage ids
- skip/escalation reasons

## Important Design Constraints
### 1. LLMs do not access databases directly
All database access goes through adapters and agents.

### 2. Dynamic SQL exists only inside a guarded path
Prompting alone is not trusted.
Generated SQL must pass validation and, for finance paths, may also go through repair/fallback logic.

### 3. Composite answers are staged
The system first retrieves and merges data, then narrates it.
It does not ask one monolithic prompt to both query and answer.

### 4. Session memory is selective
Useful follow-up context is kept.
High-risk entity anchors are cleared when the conversation family changes.

## Updated Complexity Concentration Points
The highest behavioral complexity remains in:
- `app/orchestration/graph_router.py`
- `app/llm/llm_client.py`
- `app/registries/intent_registry.py`
- `app/agents/finance_agent.py`

Reasons:
- orchestration now includes clarification, follow-up, planning, execution, merge, and escalation
- answer generation mixes prompt rules with deterministic post-processing
- registry centralizes semantics and source constraints
- finance dynamic SQL remains the heaviest guarded generation path

## Recommended Extension Pattern
When extending the system:
- add new intent definitions to `INTENT_REGISTRY`
- add new static queries to `SQL_REGISTRY`
- prefer intent-specific SQL hints over agent-local hardcoded behavior
- keep new dynamic SQL inside `sql_generator.py` + `sql_guard.py` + agent guardrails
- add new deterministic answer guards only where repeated failure patterns justify them

## Summary
The updated low-level architecture is a controlled execution pipeline:
- API receives a query
- router restores session state
- turn type, intent, and slots are normalized
- planner selects `single` or `composite`
- agents query Postgres and Mongo through adapters
- router merges results deterministically
- summarizer formats the final answer under strict prompt and code rules
- Redis stores memory for the next turn

This design keeps the system flexible enough for complex analytics while still remaining debuggable through traces, registries, validation, and explicit orchestration.
