# Low-Level Design: End-to-End Query Execution

![LLD Query Execution Flow](assets/LLD_QUERY_EXECUTION.png)

This document describes the current end-to-end execution flow from HTTP request to HTTP response, based on the updated runtime behavior now implemented in the system.

---

## 1. Scope

- request entry
- session load
- turn classification
- intent and slot extraction
- validation and clarification
- single or composite planning
- finance / ops / mongo execution
- deterministic merge
- summarization
- response and session persistence

---

## 2. Runtime Overview

- **Role**: maritime analytics chatbot for voyages, vessels, ports, cargo grades, profitability, delays, offhire, metadata, rankings, and scenario comparisons
- **Data stores**:
  - PostgreSQL: `finance_voyage_kpi`, `ops_voyage_summary`
  - MongoDB: voyages, vessels, nested remarks/fixtures/metadata
  - Redis: session memory, clarification state, result-set follow-up state
- **LLM responsibilities**:
  - intent and slot extraction
  - guarded dynamic SQL generation
  - final answer drafting
- **Constraint**: the LLM never talks directly to the databases

---

## 3. Entry Points

### API
`app/main.py`

- `POST /query`
- `POST /session/clear`

`POST /query` calls:
- `router.handle(session_id=..., user_input=req.query)`

It returns:
- `answer`
- `clarification`
- `trace`
- `intent_key`
- `slots`
- `dynamic_sql_used`
- `dynamic_sql_agents`

### UI
`app/UI/UX/streamlit_app.py`

The Streamlit UI:
- keeps a stable `session_id`
- sends user text to `/query`
- renders answer and execution trace

---

## 4. Graph Invocation

### `GraphRouter.handle()`
Location:
- `app/orchestration/graph_router.py`

Runtime action:
- invokes the compiled graph with initial state:
  - `session_id`
  - `user_input`
  - `raw_user_input`

After graph completion:
- if `clarification` exists, return it immediately
- otherwise return:
  - final `answer`
  - `trace`
  - `intent_key`
  - `slots`
  - `dynamic_sql_used`
  - `dynamic_sql_agents`

---

## 5. GraphState

Main fields carried through the graph:
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

Important `artifacts` contents:
- `trace`
- `merged_rows`
- `coverage`
- `voyage_ids`
- result-set metadata for follow-ups

---

## 6. Graph Structure

Nodes:
- `load_session`
- `extract`
- `validate`
- `clarify`
- `build_plan`
- `run_single`
- `execute_step`
- `merge`
- `summarize`

Conditional routing:
- after validate:
  - `clarify`
  - `plan`
  - or edge-case `summarize`
- after plan:
  - `single`
  - `composite`
- after single:
  - `done`
  - `merge`
  - `escalate` to composite
- after composite execution:
  - `more`
  - `done`

---

## 7. Node-by-Node Lifecycle

### 7.1 `n_load_session`
Loads prior session memory from Redis.

Typical data restored:
- previous slots
- previous intent
- pending clarification state
- `last_result_set`
- `last_focus_slots`
- previous user input

### 7.2 `n_extract_intent`
This is the main normalization stage before planning.

It currently handles:
- clarification-follow-up resolution
- follow-up result-set fast paths
- placeholder slot cleanup
- deterministic routing overrides
- session-aware slot carry-forward
- `LLMClient.extract_intent_slots(...)`

Important recent behavior now reflected in code:
- stronger fresh-question vs follow-up classification
- explicit `followup.result_set` fast path
- voyage-linked vessel metadata detection
- metadata-first routing for correct source selection

### 7.3 `n_validate_slots`
Checks required slots from `INTENT_REGISTRY`.

Important behavior:
- treats placeholders such as generic `vessel`, `voyage`, or `port` as missing
- avoids unnecessary clarification for fleet-wide composite intents

### 7.4 `n_make_clarification`
Builds the clarification message and persists pending clarification context to Redis.

Execution stops after this node for that turn.

### 7.5 `n_plan`
Calls `Planner.build_plan(...)`.

Planner output:
- `plan_type`
- `intent_key`
- `confidence`
- ordered execution `steps`

### 7.6 `n_run_single`
Used for direct entity or metadata questions.

Typical intents handled here:
- `voyage.summary`
- `vessel.summary`
- `voyage.metadata`
- `vessel.metadata`
- `ranking.vessel_metadata`
- `followup.result_set`

Important updated behavior:
- single-path result-set persistence
- dedicated Mongo-backed metadata handlers
- single-path clarification support
- zero-row escalation trigger if a fleet-style question was misclassified as single

### 7.7 `n_execute_step`
Runs one composite step at a time.

Typical step sequence:
1. optional `mongo.resolveAnchor`
2. `finance.dynamicSQL`
3. `ops.dynamicSQL`
4. optional `mongo.fetchRemarks`
5. deterministic merge step

Important behavior:
- normalizes operation names such as `dynamicSQL -> dynamic_sql`
- extracts `voyage_ids` from finance output for downstream ops/mongo steps
- records trace per step

### 7.8 `n_merge`
Copies normalized data into `state["merged"]` and marks:
- `dynamic_sql_used`
- `dynamic_sql_agents`

### 7.9 `n_summarize`
Builds the final answer using:
- merged data
- `compact_payload(...)`
- payload sanitization
- `LLMClient.summarize_answer(...)`

It also persists:
- updated session memory
- result-set memory for follow-ups

---

## 8. Planner Details

Planner file:
- `app/orchestration/planner.py`

Important current behaviors:
- deterministic planner, no second planning LLM call
- alias resolution through `resolve_intent(...)`
- entity-anchored questions stay on `single`
- registry-declared analytical intents go to `composite`
- `force_composite` supports zero-row escalation
- text-driven composite triggers include:
  - `offhire + pnl`
  - `delayed + expense`
  - `trend`
  - `over time`
  - `cargo + port`

Composite builder now generally produces:
- finance first
- ops second
- Mongo enrichment when allowed
- merge last

---

## 9. Single Path Details

Single path is used for:
- direct voyage summaries
- direct vessel summaries
- metadata-heavy entity questions
- Mongo-backed metadata views
- result-set follow-up actions

Single path sources can include:
- registry SQL
- direct Mongo document fetch
- mixed finance + ops + Mongo assembly for summary-style responses

Updated single-path special handling:
- `vessel.metadata`
- `voyage.metadata`
- `ranking.vessel_metadata`
- single-path result-set persistence
- zero-row escalation into composite when needed

---

## 10. Composite Path Details

Composite path is used for:
- rankings
- aggregates
- trends
- scenario comparisons
- high-revenue/low-PnL analytics
- offhire and delay analysis

### Finance step
`FinanceAgent.run_dynamic(...)`

Pipeline:
1. generate SQL through `SQLGenerator`
2. validate through `sql_guard`
3. execute through `PostgresAdapter.execute_dynamic_select`
4. attempt repair if recoverable
5. store rows and `voyage_ids`

### Ops step
`OpsAgent.run_dynamic(...)`

Pipeline:
1. consume `voyage_ids` from finance when available
2. use canonical ops fetches where possible
3. otherwise use guarded ops SQL
4. return ports, grades, offhire, delays, remarks-related context

### Mongo enrichment
Mongo can be used for:
- anchor resolution
- remarks enrichment
- metadata context when enabled by intent

### Deterministic merge
The row-level merge is handled by orchestration logic and produces:
- `merged_rows`
- `coverage`

This is where finance, ops, and Mongo-derived fields are combined before summarization.

---

## 11. Metadata Paths

Current low-level metadata handling is more explicit than before.

### `vessel.metadata`
Mongo-backed entity metadata for:
- hire rate
- scrubber
- market type
- operating status
- account code
- passage types
- consumption profiles
- tags
- contract history

### `voyage.metadata`
Voyage-anchored Mongo metadata for:
- fixture/commercial fields
- ports and route details
- cargo details
- bunkers and emissions fields
- remarks and projected fields

### `ranking.vessel_metadata`
Fleet-wide vessel metadata ranking/listing from Mongo only.

Examples:
- currently operating vessels
- scrubber / non-scrubber vessels
- highest hire rate
- ballast/laden speed
- short/long pool
- contract duration

---

## 12. Follow-Up Handling

The router now has a dedicated result-set follow-up layer.

Supported actions include:
- top/bottom filtering
- compare extremes
- choose selected row
- show remarks for selected row
- project ports or grades from selected row
- filter rows having a field

This depends on session fields such as:
- `last_result_set`
- `last_focus_slots`
- `last_user_input`

This is one of the major architectural updates relative to the earlier flow.

---

## 13. SQL Layer

### `sql_generator.py`
Responsible for:
- building schema-aware prompts
- appending registry-driven SQL hints
- producing finance or ops SQL

Updated guidance now includes:
- stronger aggregate patterns
- `NULLS LAST` guidance for ranked numeric metrics
- better intent-specific hints for vessel ranking and cargo profitability

### `sql_guard.py`
Responsible for:
- SELECT-only enforcement
- allowlist validation
- param sanitation
- `LIMIT` enforcement
- rejection of unsupported tables or columns

### `sql_allowlist.py`
Constrains dynamic SQL to allowed tables and columns.

---

## 14. Summarization Layer

Final answer generation happens in `LLMClient.summarize_answer(...)`.

Pipeline:
1. prepare compact JSON-safe merged payload
2. build strict answer-format prompt
3. get LLM draft answer
4. apply deterministic post-processing and answer guards

Important current answer guards:
- ranking-voyage answer repair
- ranking-vessel answer repair
- cargo-profitability answer repair

These are used to stabilize aggregate outputs when the raw LLM summary is weaker than the available data.

---

## 15. Execution Trace

Trace is emitted into `artifacts.trace`.

Current trace events include:
- `intent_extraction`
- `planning`
- `composite_step_start`
- `composite_step_result`
- `token_usage`

UI-visible trace information includes:
- agent
- operation
- inputs
- SQL when present
- row counts
- voyage-id extraction
- skip/escalation reasons

---

## 16. Design Constraints

### 1. The LLM never queries databases directly
All DB access is routed through agents and adapters.

### 2. Dynamic SQL is always guarded
Prompt generation alone is never trusted.

### 3. Merge happens before narration
The system first retrieves and joins data, then explains it.

### 4. Session memory is explicit
Only useful conversational state is preserved, and stale anchors are cleared when intent families change.

---

## 17. Summary

The updated low-level request flow is:

1. request enters FastAPI
2. `GraphRouter` loads session state
3. turn is classified
4. intent and slots are extracted
5. required slots are validated
6. clarification is generated if needed
7. planner chooses `single` or `composite`
8. agents query Postgres and Mongo
9. router merges results deterministically
10. summarizer drafts the final answer
11. session state and result-set memory are saved
12. response plus trace are returned

This is the current implemented flow and should be used as the authoritative LLD for end-to-end request execution.
