# KAI Agent Low-Level Architecture

## Scope
This document describes the implemented low-level architecture of the request pipeline, data flow, agent behavior, session model, and answer generation stack.

It focuses on the code paths that actually run in production and references the concrete files where behavior lives.

## Repository Structure Relevant to Runtime
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
The API is initialized in `app/main.py`.

Key responsibilities:
- load environment variables
- create `LLMClient`
- create Mongo, Postgres, and Redis dependencies
- create `GraphRouter`
- expose:
  - `GET /`
  - `POST /query`
  - `POST /session/clear`

Main request and response models:
- `QueryRequest`
  - `query`
  - `session_id`
- `QueryResponse`
  - `session_id`
  - `answer`
  - `clarification`
  - `trace`
  - `intent_key`
  - `slots`
  - `dynamic_sql_used`
  - `dynamic_sql_agents`

### `app/UI/UX/streamlit_app.py`
The Streamlit client is a thin UI around the API.

Important implementation details:
- `call_api(query, session_id)` posts to `KAI_API_URL`
- `clear_session_cache(session_id)` posts to `/session/clear`
- `st.session_state["session_id"]` is the client-side conversation identifier
- execution trace is rendered from `trace`
- SQL is reformatted for readability through `_format_sql_for_trace`

## Runtime State Model
### `GraphState` in `app/orchestration/graph_router.py`
The router passes a typed state object through the graph.

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

`artifacts` is especially important because it carries:
- `trace`
- merged intermediate rows
- coverage hints
- voyage ids
- dynamic SQL metadata

## Orchestration Graph
### Node sequence
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

### Routing decisions
Conditional routers determine the next stage:
- after validation:
  - clarify if required slots are missing
  - summarize immediately for unsupported intents
  - otherwise plan
- after planning:
  - run `single`
  - or loop through `composite`
- after single:
  - finish
  - clarify
  - or escalate to composite
- after composite steps:
  - continue next step
  - or merge

## Detailed Request Lifecycle
### 1. Session load
`n_load_session` calls `RedisStore.load_session(session_id)`.

Loaded session data may include:
- prior slots
- prior intent
- result-set memory
- clarification state
- turn metadata

### 2. Turn classification and intent extraction
`n_extract_intent` does much more than plain LLM classification.

It includes:
- clarification follow-up handling
- follow-up result-set fast paths
- placeholder slot cleanup
- deterministic overrides for voyage, vessel, and port patterns
- session-aware slot carry-forward
- optional LLM intent extraction via `LLMClient.extract_intent_slots`

This node is the main place where conversational behavior is normalized before planning.

### 3. Slot validation
`n_validate_slots` checks `INTENT_REGISTRY[intent]["required_slots"]`.

Special handling includes:
- treating placeholders like `vessel`, `port`, or `voyage` as effectively missing
- skipping clarification for composite intents
- preserving better behavior for incomplete entity-style questions

### 4. Clarification generation
If required slots are missing, `n_make_clarification`:
- builds a clarification message
- persists pending clarification context to Redis
- returns immediately without continuing to execution

### 5. Planning
`Planner.build_plan()` in `app/orchestration/planner.py` converts:
- `intent_key`
- `slots`
- `text`
- `force_composite`

into `ExecutionPlan`.

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

### 6. Execution
Execution splits into two modes.

#### Single mode
`n_run_single` is used when the planner believes the question is entity-scoped or simple enough for direct execution.

Typical characteristics:
- registry SQL preferred
- direct Mongo lookup for vessel/voyage metadata
- lower latency
- simpler merge path

#### Composite mode
`n_execute_step` runs the step list one-by-one.

Typical step sequence:
1. optional `mongo.resolveAnchor`
2. `finance.dynamicSQL`
3. `ops.dynamicSQL`
4. optional `mongo.fetchRemarks`
5. deterministic `llm.merge`

Even though one step is named `llm.merge`, the join itself is deterministic orchestration logic rather than a generative merge done by the LLM.

## Planner Internals
### `app/orchestration/planner.py`
The planner is deterministic.

Important behaviors:
- resolves aliases through `resolve_intent`
- supports `force_composite` escalation
- keeps strongly entity-anchored questions on `single`
- routes registry-declared `composite` intents to dynamic execution
- uses textual hints like `trend`, `over time`, `offhire + pnl`, and `cargo + port`

Composite plans are built by `_build_composite()` and usually include:
- finance first
- ops second
- Mongo enrichment
- merge last

## Data Access Layer
### PostgreSQL
#### `app/adapters/postgres_adapter.py`
Responsibilities:
- connection pooling through `SimpleConnectionPool`
- registry query execution via `fetch_all` / `fetch_one`
- dynamic SQL execution via `execute_dynamic_select`

Important behaviors:
- lazy pool initialization
- connection backoff when Postgres is unavailable
- allows only `SELECT` or `WITH ... SELECT`
- blocks write statements
- rejects positional parameters like `$1`
- normalizes `:param` to `%(param)s`
- injects `LIMIT` when missing
- filters params to only those referenced in SQL

### MongoDB
#### `app/adapters/mongo_adapter.py`
Responsibilities:
- fetch vessel or voyage by identifier
- resolve vessel name to IMO
- resolve voyage number to voyage id
- apply projections
- normalize remarks shape
- run safe `find_many` queries

### Redis
#### `app/adapters/redis_store.py`
Responsibilities:
- session memory
- optional idempotency cache
- optional distributed lock
- session clearing

Key persisted session fields:
- `slots`
- `last_intent_key`
- `anchor_type`
- `anchor_id`
- `last_user_input`
- `turn`
- `updated_at`
- `last_updated_ts`

Important low-level behavior:
- `STICKY_SLOTS` retain safe preferences such as `scenario` and `limit`
- `VOLATILE_SLOTS` are cleared on intent change to avoid stale anchor pollution
- in-memory fallback is used if Redis is disabled or unavailable

## Agent Layer
### FinanceAgent
#### `app/agents/finance_agent.py`
Modes:
- `run()` -> registry SQL
- `run_dynamic()` -> generated SQL

`run_dynamic()` performs:
1. SQL generation through `SQLGenerator.generate(...)`
2. agent-specific parameter injection
3. allowlist/guard validation through `validate_and_prepare_sql`
4. execution through `PostgresAdapter.execute_dynamic_select`
5. retry/repair loop for repairable SQL errors
6. intent-specific post-checks and fallback repairs

Guardrail examples:
- `finance_no_ops_join`
- `require_kpi_columns`
- `verify_scenario_variance_columns`
- `segment_performance_fallback`
- `inject_voyage_numbers_param`
- `inject_filter_port_param`

This makes `FinanceAgent` the main defensive layer for dynamic finance SQL.

### OpsAgent
#### `app/agents/ops_agent.py`
Modes:
- `run()` -> registry SQL
- `run_dynamic()` -> generated SQL plus several deterministic shortcuts

Important deterministic branches in `run_dynamic()`:
- canonical fetch by `voyage_ids`
- cargo profitability support using `cargo_grades`
- deterministic vessel summary lookup

Important low-level behavior:
- protects against bad placeholder list inputs
- normalizes cargo grades
- removes null/JSON-like grade values via `_clean_grade_name`
- uses ops-only SQL discipline

### MongoAgent
#### `app/agents/mongo_agent.py`
Responsibilities:
- resolve anchors
- fetch voyage and vessel documents
- fetch full voyage context
- optionally execute safe dynamic Mongo find

Mongo is primarily used for:
- anchor resolution
- metadata answers
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
- `mongo_projection`
- `sql_hints`
- `guardrails`

This powers:
- planner routing
- classifier prompt context
- SQL generation hints
- execution constraints

### `app/registries/sql_registry.py`
This file defines `QuerySpec` entries for static SQL.

Typical contents:
- description
- required parameters
- parameterized SQL string

Used by:
- `PostgresAdapter.fetch_all()`
- `FinanceAgent.run()`
- `OpsAgent.run()`

## SQL Generation and Validation
### `app/sql/sql_generator.py`
Responsibilities:
- create schema hints by agent
- inject aggregate patterns for composite intents
- append intent-specific SQL hints from registry
- call `LLMClient.generate_sql()`

Important low-level prompt behavior:
- schema-aware prompt
- PostgreSQL-only
- strict param format
- hard rules for JSONB access
- aggregate pattern guidance
- no invented columns
- explicit table/column notes for finance and ops schema

### `app/sql/sql_guard.py`
Responsibilities:
- validate generated SQL against allowlist
- reject forbidden patterns
- enforce select-only behavior
- clean unsupported table or column references
- normalize list params for `ANY(...)`
- add `LIMIT` when needed

This is the main structural SQL safety layer after prompt generation.

### `app/sql/sql_allowlist.py`
Responsibilities:
- define allowed tables
- define allowed columns per table
- define forbidden patterns

The allowlist constrains dynamic SQL to the intended schema subset.

## Merge and Summarization
### Merge in `GraphRouter`
The router performs the main cross-source merge.

Outputs usually include:
- `finance`
- `ops`
- `mongo`
- `artifacts.merged_rows`
- `artifacts.coverage`
- `dynamic_sql_used`
- `dynamic_sql_agents`

### Payload compaction
#### `app/services/response_merger.py`
`compact_payload()` is not the primary merge algorithm.
It is a post-merge compactor for summarization.

Responsibilities:
- flatten merged rows
- extract top-level KPIs
- compact ports, grades, and remarks
- dedupe some repeated grade values
- fallback to nested finance fields
- reduce token load for final summarization

Important low-level result shape:
- `voyage_number`
- `pnl`
- `revenue`
- `total_expense`
- `tce`
- `total_commission`
- `key_ports`
- `cargo_grades`
- `remarks`
- vessel-level fields when applicable

### Final answer generation
#### `app/llm/llm_client.py`
`summarize_answer()` is the final answer composer.

Its pipeline:
1. identify `intent_key`
2. handle `out_of_scope` cases gracefully
3. truncate merged data
4. convert payload to JSON-safe shape
5. build a strict system prompt
6. ask the LLM for final narrative/table answer
7. apply deterministic post-processing and table cleanup

Important answer constraints implemented in prompt + code:
- verdict-first responses
- archetype-specific structure
- remarks classification
- no raw UUID exposure as identifiers
- ambiguity handling for same voyage number across multiple vessels
- table hygiene and empty-column dropping

## Follow-Up and Session Logic
### Result-set follow-ups
The router supports follow-up actions over prior results, such as:
- top/bottom filters
- compare extremes
- explain remarks
- project ports or grades

This is powered by:
- `last_result_set` in Redis session
- turn classification
- fast-path follow-up parsing

### Clarification persistence
Pending clarification state is saved so the next user message can be interpreted as:
- a slot answer
- or a fresh question that should cancel the clarification path

## Execution Trace Model
The router emits a compact trace into `artifacts.trace`.

Trace events include:
- `intent_extraction`
- `planning`
- `composite_step_start`
- `composite_step_result`
- `token_usage`

The UI renders this trace inside expandable sections and shows:
- agent
- operation
- inputs
- SQL when available
- row counts
- extracted voyage ids

## Important Design Constraints
### 1. LLMs do not query the database directly
All DB access goes through adapters and agents.

### 2. Dynamic SQL is allowed only within a guarded path
Prompting alone is never trusted.
Generated SQL still passes through validation and, in finance, additional repair logic.

### 3. Composite answers are intentionally staged
The system does not ask one monolithic prompt to answer everything.
It first resolves data, then merges, then summarizes.

### 4. Session memory is intentionally selective
Only useful conversational context is kept.
High-risk entity anchors are cleared when the intent family changes.

## Known Complexity Concentration Points
These files carry most of the behavioral complexity:
- `app/orchestration/graph_router.py`
- `app/llm/llm_client.py`
- `app/registries/intent_registry.py`
- `app/agents/finance_agent.py`

Reasons:
- orchestration combines intent, follow-up, planning, execution, and merge
- answer generation mixes prompt rules with deterministic repairs
- registry centralizes domain semantics and guardrails
- finance dynamic SQL needs the most repair and fallback logic

## Extension Points
If the system is expanded later, the safest extension points are:
- add new intent definitions in `INTENT_REGISTRY`
- add new static queries to `SQL_REGISTRY`
- add new SQL hints in `INTENT_REGISTRY` rather than hardcoding in agents
- add new post-merge formatting rules in `response_merger.py`
- keep new dynamic SQL within `sql_generator.py` + `sql_guard.py` + agent guardrails

## Practical Reading Path for Engineers
For implementation-level onboarding:

1. `app/main.py`
2. `app/orchestration/graph_router.py`
3. `app/orchestration/planner.py`
4. `app/registries/intent_registry.py`
5. `app/agents/finance_agent.py`
6. `app/agents/ops_agent.py`
7. `app/adapters/postgres_adapter.py`
8. `app/services/response_merger.py`
9. `app/llm/llm_client.py`
10. `app/sql/sql_generator.py`
11. `app/sql/sql_guard.py`

## Summary
The low-level architecture is a controlled execution pipeline:
- API receives a query
- router restores conversational state
- intent and slots are extracted with deterministic and LLM-assisted logic
- planner selects single or composite execution
- agents query Postgres and Mongo through adapters
- the router merges data deterministically
- the summarizer formats the final answer under strict prompt and code rules
- Redis stores session memory for the next turn

This design gives the project enough flexibility for complex business analytics while still remaining debuggable through traces, registries, and guardrails.
