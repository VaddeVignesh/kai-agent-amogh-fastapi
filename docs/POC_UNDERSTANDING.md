# KAI Agent POC Understanding

## Why this document exists
This document is a full-context handoff for another LLM or external assistant such as Perplexity, ChatGPT, Claude, or Gemini.

The goal is simple:
- give the model enough context to reason about the current system end to end
- explain what the POC does today
- explain how the code is organized
- explain how requests flow through the system
- explain where the important logic lives
- explain what is already validated
- explain what the model should and should not assume

If you provide only one document to another LLM before asking for help on this repository, this should be the one.

---

## 1. What this project is
`kai-agent-amogh-fastapi` is a maritime analytics assistant focused on voyages, vessels, ports, cargo grades, delays/offhire, remarks, and finance KPIs.

It is a POC/demo-oriented system, but it already has meaningful runtime structure:
- `FastAPI` backend
- `Streamlit` frontend
- `LangGraph-style` orchestrator in `GraphRouter`
- `PostgreSQL` for finance and operational summary data
- `MongoDB` for rich voyage and vessel documents
- `Redis` for session memory and follow-up continuity
- `Groq-hosted LLM` for intent extraction, dynamic SQL generation, and final answer generation

This is not a general-purpose chatbot. It is a domain-specific analytics system with strong prompt rules, SQL guardrails, and deterministic orchestration.

---

## 2. Current business objective of the POC
The current POC is designed to answer business questions such as:
- single-voyage summaries
- vessel performance summaries
- rankings of voyages or vessels
- cargo profitability
- port-based voyage analysis
- delay/offhire impact analysis
- scenario comparison such as `ACTUAL` vs `WHEN_FIXED`

The target user experience is:
- user asks a natural-language question
- system routes it to the right path
- finance, ops, and Mongo context are joined when needed
- user gets a concise narrative or table answer
- execution trace shows how the answer was produced

This system is currently optimized for demo quality, observability, and controlled flexibility rather than deep autonomous reasoning.

---

## 3. Current validation status
An evaluation pipeline now exists under `eval/`.

Files:
- `eval/benchmark_queries.json`
- `eval/run_eval.py`
- `eval/score_eval.py`
- `eval/make_report.py`
- `eval/results_raw.json`
- `eval/results_scored.json`
- `eval/report.md`

Latest benchmark summary:
- `15/15` benchmark queries completed successfully
- overall score: `94.0 / 100`
- target threshold: `80`
- overall status: `PASS`

Bucket scores:
- `voyage_ops_finance`: `95.0`
- `vessel_metadata`: `86.25`
- `single_composite_mix`: `100.0`
- `ranking_analytics`: `100.0`
- `scenario_compare`: `95.0`
- `aggregate_business`: `90.0`

Difficulty scores:
- `simple`: `90.0`
- `medium`: `88.33`
- `complex`: `97.22`

This means the current system is already in a strong demo-ready state, but there are still some precision and maintainability gaps.

---

## 4. High-level architecture
There are five main runtime layers:

1. `Presentation`
   - `app/UI/UX/streamlit_app.py`
   - captures user input
   - maintains frontend session id
   - calls backend API
   - renders answer and execution trace

2. `API`
   - `app/main.py`
   - exposes `/query`
   - exposes `/session/clear`
   - wires all dependencies

3. `Orchestration`
   - `app/orchestration/graph_router.py`
   - owns request lifecycle
   - session load
   - intent extraction
   - slot validation
   - clarification
   - planning
   - execution
   - merge
   - summarization

4. `Domain agents`
   - `app/agents/finance_agent.py`
   - `app/agents/ops_agent.py`
   - `app/agents/mongo_agent.py`

5. `Infrastructure and data`
   - `app/adapters/postgres_adapter.py`
   - `app/adapters/mongo_adapter.py`
   - `app/adapters/redis_store.py`
   - Postgres, MongoDB, Redis

Two additional support layers matter heavily:
- `app/registries/*` for intent and SQL metadata
- `app/sql/*` for dynamic SQL generation and safety

---

## 5. End-to-end request flow
The real request path is:

1. User enters a query in Streamlit
2. Streamlit calls FastAPI `POST /query`
3. FastAPI calls `GraphRouter.handle(session_id, user_input)`
4. `GraphRouter` loads Redis session
5. `GraphRouter` extracts intent and slots
6. `GraphRouter` validates required slots
7. If slots are missing, it returns a clarification immediately
8. Otherwise `Planner` chooses either:
   - `single`
   - `composite`
9. `single` path uses direct agent logic and mostly registry SQL / direct Mongo access
10. `composite` path runs step-by-step:
    - optional anchor resolution
    - finance dynamic SQL
    - ops dynamic SQL
    - optional Mongo enrichment
    - deterministic merge
11. Merge output is compacted
12. `LLMClient.summarize_answer()` creates the final user-facing answer
13. Redis session is updated
14. Response is returned with answer, intent, slots, and execution trace

Important note:
The LLM does not directly query databases. It only helps with:
- intent extraction
- SQL generation
- final wording

All DB calls go through adapters and agents.

---

## 6. Single vs composite execution model
This distinction is one of the most important concepts in the whole system.

### Single plan
Use case:
- a specific voyage
- a specific vessel
- a specific named port
- a metadata lookup

Typical behavior:
- low step count
- registry SQL or direct Mongo lookup
- faster
- lower prompt complexity

Examples:
- `For voyage 2306, give me revenue, expense, PnL, ports, and remarks`
- `Tell me about vessel Elka Delphi`

### Composite plan
Use case:
- rankings
- top/bottom analysis
- fleet-wide aggregation
- trend analysis
- scenario comparison
- cross-domain analytics

Typical behavior:
- multi-step
- finance dynamic SQL
- ops dynamic SQL
- deterministic row merge
- richer trace

Examples:
- `Show me the top 5 most profitable voyages with cargo grades and ports`
- `Which cargo grades have the highest average PnL?`
- `Compare actual vs when-fixed results for voyages 1901, 1902, and 2301`

---

## 7. Core runtime files and what they do
### `app/main.py`
Backend entry point.

Responsibilities:
- loads environment variables
- creates `LLMClient`
- creates Mongo, Postgres, Redis objects
- creates `GraphRouter`
- exposes API endpoints

This is the top-level dependency assembly file.

### `app/UI/UX/streamlit_app.py`
Frontend entry point.

Responsibilities:
- stores UI `session_id`
- calls backend API
- formats execution trace
- supports cache clear, Redis session clear, and new chat

Important detail:
- frontend session continuity is driven by a stable `session_id`

### `app/orchestration/graph_router.py`
The main brain of runtime orchestration.

Responsibilities:
- load session
- classify turns
- extract intent and slots
- detect follow-ups
- issue clarifications
- plan execution
- run agents
- perform deterministic merge
- compact and sanitize payload
- summarize answer
- persist session
- emit trace events

This is the most behavior-dense file in the project.

### `app/orchestration/planner.py`
Determines whether a query should run in:
- `single`
- `composite`

It is deterministic and registry-driven.

### `app/agents/finance_agent.py`
Handles finance-side Postgres queries.

Responsibilities:
- map intent to registry SQL
- generate finance dynamic SQL
- validate and repair SQL
- enforce finance-specific guardrails

This agent carries much of the complexity for dynamic SQL recovery.

### `app/agents/ops_agent.py`
Handles ops-side Postgres queries.

Responsibilities:
- registry SQL for ops intents
- deterministic canonical fetch by voyage ids
- dynamic SQL for ops analysis
- grade/port/remarks enrichment

### `app/agents/mongo_agent.py`
Handles Mongo-based entity resolution and rich context retrieval.

Responsibilities:
- resolve anchors
- fetch vessel/voyage documents
- fetch remarks and nested voyage context
- optionally execute guarded dynamic Mongo find

### `app/registries/intent_registry.py`
Central semantic registry of supported intents.

Defines:
- intent descriptions
- required and optional slots
- data-store needs
- route type (`single` or `composite`)
- SQL hints
- execution guardrails

This file is the main declarative contract of the system.

### `app/registries/sql_registry.py`
Registry of fixed, parameterized Postgres SQL.

Used for:
- stable single-path queries
- auditable query definitions
- lower-risk execution where dynamic SQL is not needed

### `app/sql/sql_generator.py`
Builds schema hints and system prompts for SQL generation.

Important role:
- tells the LLM what tables/columns exist
- provides aggregate SQL pattern hints
- passes agent-specific prompt rules

### `app/sql/sql_guard.py`
Validates generated SQL before execution.

Important role:
- enforce allowlist
- reject non-SELECT SQL
- reject unsupported columns/tables
- normalize params
- enforce `LIMIT`

### `app/sql/sql_allowlist.py`
Defines allowed tables, columns, and forbidden patterns for dynamic SQL.

### `app/services/response_merger.py`
This is not the primary merge engine.
It is the payload compactor that prepares merged data for summarization.

### `app/llm/llm_client.py`
LLM interaction layer.

Responsibilities:
- intent extraction
- SQL generation wrapper
- answer summarization
- deterministic answer cleanup and safety formatting

This is the second most behavior-dense file after `graph_router.py`.

---

## 8. Data sources and their real roles
### PostgreSQL
Main analytical source.

Observed key tables:
- `finance_voyage_kpi`
- `ops_voyage_summary`

From schema inspection:

`finance_voyage_kpi` contains fields such as:
- `voyage_id`
- `voyage_number`
- `vessel_imo`
- `scenario`
- `revenue`
- `total_expense`
- `pnl`
- `tce`
- `total_commission`
- `bunker_cost`
- `port_cost`
- `voyage_days`
- `voyage_start_date`
- `voyage_end_date`

`ops_voyage_summary` contains fields such as:
- `voyage_id`
- `voyage_number`
- `vessel_id`
- `vessel_imo`
- `vessel_name`
- `module_type`
- `fixture_count`
- `offhire_days`
- `is_delayed`
- `delay_reason`
- `voyage_start_date`
- `voyage_end_date`
- `ports_json`
- `grades_json`
- `activities_json`
- `remarks_json`
- `tags`

Important actual data-shape observation:
- `ports_json` is a JSONB array of objects
- `grades_json` is a JSONB array of objects
- `remarks_json` is a JSONB array
- `activities_json` appears to be effectively null / not useful in current data

### MongoDB
Main context source.

Used for:
- vessel metadata
- voyage documents
- fixtures
- legs
- revenue/expense detail
- remarks
- rich operational context not always flattened in Postgres

Mongo is especially important for:
- `vessel.metadata`
- document-driven narrative enrichments
- anchor resolution

### Redis
Main short-term conversation state store.

Used for:
- last intent
- slots
- prior result set
- clarification state
- follow-up continuity

Redis is not used for primary analytics; it is used for conversational orchestration.

---

## 9. Session and follow-up model
The system is not just stateless Q&A.
It supports lightweight follow-up memory.

Stored session concepts include:
- `slots`
- `last_intent_key`
- anchor information
- `last_result_set`
- clarification options
- turn counters

Behavior:
- if the next user query looks like a follow-up, the router can reuse prior context
- if the intent changes, volatile slots are cleared
- if user is answering a clarification, the reply is interpreted as slot input
- if user asks a clearly new question, the router should stop reusing old context

This area is implemented mainly in:
- `app/orchestration/graph_router.py`
- `app/adapters/redis_store.py`

---

## 10. Prompt-driven parts of the system
The system uses prompts in three different ways.

### Intent extraction prompt
Used when deterministic rules are insufficient.
Driven by:
- supported intents
- intent descriptions
- slot extraction instructions

### SQL generation prompt
Built in `sql_generator.py`.
Guided by:
- schema hint
- allowed tables/columns
- aggregate patterns
- intent-specific SQL hints
- hard restrictions such as `SELECT` only

### Final answer prompt
Built in `llm_client.py`.
Guided by:
- answer archetype rules
- formatting rules
- table rules
- ambiguity rules
- remarks classification rules
- vessel/voyage identifier rules

The final answer is not raw LLM output only.
It is also shaped by deterministic post-processing.

---

## 11. Safety and control model
This system is not “LLM free-form”.
It is more accurately described as a controlled hybrid.

Safety/control layers:
- deterministic intent shortcuts
- registry-driven routing
- SQL allowlist
- SQL guard validation
- finance-agent repair loops
- deterministic merge before summarization
- strict answer prompts
- deterministic answer cleanup

This means the system is flexible, but not uncontrolled.

---

## 12. Execution trace model
The execution trace is an important debugging and demo artifact.

The backend emits structured trace events such as:
- intent extraction
- planning
- composite step start
- composite step result
- token usage

The UI renders them in expandable sections.

Trace typically shows:
- step number
- agent
- operation
- whether it succeeded
- SQL if available
- row counts
- extracted voyage ids

This is a major strength of the POC because it makes the runtime explainable.

---

## 13. Current evaluation pipeline
The repository now contains a benchmark-driven evaluation pipeline.

Files:
- `eval/benchmark_queries.json`
- `eval/run_eval.py`
- `eval/score_eval.py`
- `eval/make_report.py`

Current use:
- run benchmark queries through live API
- capture answers, intent, trace, and dynamic SQL detection
- score responses against required terms, forbidden terms, and numeric tolerance
- generate markdown report

Current benchmark result:
- overall score: `94`
- target: `80`
- pass: `true`

This means the POC is already validated against a controlled benchmark pack.

---

## 14. Known limitations and caveats
This system is strong for demo, but not perfect.

Current known caveats include:
- architecture complexity is concentrated heavily in `graph_router.py`
- some behavior is heuristic and prompt-sensitive
- dynamic SQL remains dependent on prompt quality and schema hints
- summarization quality can still drift when merged data is ambiguous
- same `voyage_number` can exist across multiple vessels in dataset and must be handled carefully
- JSONB-heavy queries are a frequent source of complexity
- there are multiple docs in `docs/` with overlapping architecture context, so future updates should keep them aligned

For formal limitation references, see:
- `docs/PHASE1_LIMITATIONS.md`

---

## 15. Current working assumptions external LLMs should use
If another LLM is helping with this repo, it should assume:

1. `GraphRouter` is the true runtime orchestrator.
2. `INTENT_REGISTRY` is the source of truth for supported intent behavior.
3. `single` vs `composite` is the first major architectural split.
4. Postgres handles KPI analytics; Mongo handles rich entity context.
5. Dynamic SQL must always respect:
   - `sql_generator.py`
   - `sql_guard.py`
   - `sql_allowlist.py`
   - agent-level guardrails
6. `response_merger.py` compacts merged data; it does not own the full orchestration merge logic.
7. Redis memory matters for follow-up behavior and clarification behavior.
8. Demo quality matters, so answer clarity and traceability are important, not only correctness.

External LLMs should not assume:
- that the system is stateless
- that the LLM directly talks to databases
- that all answers come from one single prompt
- that every query uses dynamic SQL
- that `voyage_number` is globally unique across vessels

---

## 16. Recommended reading order for a new engineer or LLM
If starting from scratch, read in this order:

1. `app/main.py`
2. `app/UI/UX/streamlit_app.py`
3. `app/orchestration/graph_router.py`
4. `app/orchestration/planner.py`
5. `app/registries/intent_registry.py`
6. `app/agents/finance_agent.py`
7. `app/agents/ops_agent.py`
8. `app/agents/mongo_agent.py`
9. `app/adapters/postgres_adapter.py`
10. `app/adapters/redis_store.py`
11. `app/services/response_merger.py`
12. `app/llm/llm_client.py`
13. `app/sql/sql_generator.py`
14. `app/sql/sql_guard.py`
15. `eval/benchmark_queries.json`
16. `eval/report.md`

---

## 17. Recommended prompts to give another LLM with this doc
When using another LLM, pair this document with prompts like:

- `Read this repo context and explain how a query flows from API to final answer.`
- `Based on this architecture, suggest how to improve maintainability without changing behavior.`
- `Using this system context, review whether a new intent should be single or composite.`
- `Given this architecture, identify where a bug in follow-up routing is most likely to live.`
- `Given this context, suggest safer dynamic SQL patterns for cargo and port aggregation.`

---

## 18. Short final summary
This POC is a maritime analytics assistant with:
- a real backend and frontend
- multi-source data retrieval
- session-aware orchestration
- registry-driven intent handling
- guarded dynamic SQL
- deterministic merge
- structured execution trace
- benchmark-based validation already passing strongly

The most important implementation concept is this:
the system does not answer questions in one LLM jump. It stages the work through intent extraction, planning, agent execution, merge, and summarization.

That staged design is the key to understanding the whole codebase.
