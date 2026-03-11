# kai-agent — Project Notes (Flow, Architecture, Design)

Notes from a full pass over the codebase: structure, workflows, functions, and design.

---

## 1. Project structure

```
kai-agent/
├── app/
│   ├── UI/UX/
│   │   └── streamlit_app.py      # Entry point: Streamlit chat UI
│   ├── orchestration/
│   │   ├── graph_router.py       # LangGraph router (main flow)
│   │   ├── planner.py            # Single vs composite planning
│   │   ├── router.py             # (legacy/supplementary?)
│   │   └── mongo_schema.py       # Mongo schema hint for LLM
│   ├── agents/
│   │   ├── finance_agent.py      # Postgres finance KPIs
│   │   ├── ops_agent.py          # Postgres ops (ports/grades/remarks)
│   │   ├── mongo_agent.py        # MongoDB entity resolution + dynamic find
│   │   └── kpi_agent.py          # Legacy KPI helper (raw SQL, different schema)
│   ├── registries/
│   │   ├── intent_registry.py     # Supported intents + INTENT_REGISTRY + aliases
│   │   └── sql_registry.py       # Named SQL queries (QuerySpec)
│   ├── sql/
│   │   ├── sql_generator.py      # LLM-based SQL generation
│   │   ├── sql_guard.py          # Validate + prepare SQL (allowlist, fixes)
│   │   └── sql_allowlist.py      # Allowed tables/columns + forbidden patterns
│   ├── mongo/
│   │   └── mongo_guard.py         # Validate Mongo find spec (collections, operators)
│   ├── llm/
│   │   ├── llm_client.py         # Groq client: intent/slots, SQL, summarization
│   │   └── mongo_query_builder.py # LLM → Mongo find spec
│   ├── services/
│   │   └── response_merger.py    # compact_payload for summarizer
│   ├── adapters/
│   │   ├── postgres_adapter.py    # Pooled Postgres (registry + dynamic SELECT)
│   │   ├── mongo_adapter.py      # Vessels/voyages read-only
│   │   └── redis_store.py        # Session memory (slots, intent, turn)
│   └── config/
│       └── database.py            # get_mongo_db(), get_postgres_connection(), get_redis_client()
├── tests/
│   ├── test_sql_guard.py
│   ├── test_slot_clarification.py
│   ├── test_dynamic_sql_regressions.py
│   ├── test_postgres_adapter.py
│   └── test_mongo_adapter.py
├── docs/
│   ├── architecture.md           # Reference architecture (implemented)
│   ├── PHASE1_LIMITATIONS.md     # Phase-1 limitations (from query responses)
│   └── PROJECT_NOTES.md          # This file
└── requirements.txt
```

---

## 2. High-level request flow

1. **Entry**: `streamlit_app.py` → user message → `_build_router_cached()` builds `GraphRouter` (LLM, Redis, MongoAgent, FinanceAgent, OpsAgent) → `router.handle(session_id, user_text)`.
2. **Router**: `GraphRouter.handle()` invokes the LangGraph with initial state `{ session_id, user_input, raw_user_input }`. The graph runs nodes in sequence/conditional order.
3. **Graph nodes (simplified)**:
   - **load_session** → load Redis session into `session_ctx`.
   - **extract** → intent + slots (LLM + deterministic overrides + regex; clarification follow-up handling).
   - **validate** → slot validation; may set `missing_keys` and route to **clarify** or **build_plan**.
   - **clarify** → return clarification message and END (no DB/LLM summarization).
   - **build_plan** → `Planner.build_plan()` → `plan_type`: `"single"` or `"composite"`.
   - **run_single** (single path) → run one agent (finance/ops/mongo as needed), then either END or **merge**.
   - **execute_step** (composite path) → loop: run one step (mongo resolveAnchor, finance dynamicSQL, ops dynamicSQL, mongo fetchRemarks, llm merge), then **merge** when no more steps.
   - **merge** → deterministic merge by `voyage_id` into `artifacts.merged_rows`; compact/sanitize payload; set `merged` (with `dynamic_sql_used`, `dynamic_sql_agents`, etc.).
   - **summarize** → `compact_payload(merged)` then `LLMClient.summarize_answer()` (draft + optional polish + postprocess) → `answer` in state.
4. **Exit**: `handle()` reads final state; if `clarification` set, returns it; else returns `intent_key`, `slots`, `answer`, `data` (= merged), `plan`, `dynamic_sql_used`, `dynamic_sql_agents`, `trace`.

---

## 3. Orchestration (graph_router.py)

- **GraphState** (TypedDict): `session_id`, `user_input`, `raw_user_input`, `session_ctx`, `intent_key`, `slots`, `missing_keys`, `clarification`, `plan_type`, `plan`, `step_index`, `artifacts`, `mongo`, `finance`, `ops`, `data`, `merged`, `answer`.
- **Trace**: `_trace()` appends to `artifacts.trace`; `_compact_for_trace()` truncates large values for UI.
- **Scenario detection**: `_detect_scenario_comparison()` sets scenario-related slots from user text.
- **Slot merge**: `_merge_slots()` merges session + current slots per intent.
- **Nodes**:
  - **n_load_session**: `state["session_ctx"] = redis.load_session(session_id)`.
  - **n_extract_intent**: Handles pending clarification (resolve from user text), chitchat fast-path (`out_of_scope`), result-set follow-ups, deterministic overrides + LLM `extract_intent_slots`; writes `intent_key`, `slots`, `artifacts`.
  - **n_validate_slots**: Checks required slots from intent registry; if missing → `missing_keys` + `clarification` and route to clarify; else to build_plan (or summarize in edge cases).
  - **n_plan**: `Planner.build_plan(text, session_ctx, intent_key, slots)` → `ExecutionPlan` (plan_type, steps); stores in state.
  - **n_run_single**: Dispatches to finance/ops/mongo by intent and slots; loads registry SQL or runs dynamic/canonical paths; writes `finance`/`ops`/`mongo` into state; may route to merge (e.g. voyage.summary with mongo) or END.
  - **n_execute_step**: Runs one composite step: resolveAnchor (mongo), dynamicSQL (finance/ops), fetchRemarks (mongo), merge (no-op step); **resolves** `$finance.voyage_ids` from artifacts before calling ops; appends trace; increments `step_index`.
  - **n_merge**: Merges finance/ops/mongo rows by `voyage_id` into `artifacts.merged_rows`; builds `merged` (with `dynamic_sql_used`, `dynamic_sql_agents`, plan); calls `_sanitize_for_llm` on payload.
  - **n_summarize**: `compact_payload(merged)` then `llm.summarize_answer(question, plan, merged)` → `state["answer"]`; saves session back to Redis.
- **Conditional edges**:
  - **r_after_validate**: "clarify" | "plan" | "summarize".
  - **r_plan_path**: "single" → run_single, "composite" → execute_step.
  - **r_has_more_steps**: "more" → execute_step again, "done" → merge.
  - **r_after_run_single**: "done" → END, "merge" → merge.

---

## 4. Planner (planner.py)

- **ExecutionPlan**: `plan_type` ("single" | "composite"), `intent_key`, `required_slots`, `confidence`, `steps` (list of `ExecutionStep(agent, operation, inputs)`).
- **ExecutionStep**: `agent` ("mongo"|"finance"|"ops"|"llm"), `operation` (e.g. "resolveAnchor", "dynamicSQL", "fetchRemarks", "merge"), `inputs` dict.
- **build_plan**:
  - Uses provided `intent_key` and `slots` (no second LLM).
  - `out_of_scope` → single, no steps.
  - Single-voyage / single-vessel / single-port entity → single with appropriate intent.
  - Hard composite intents (e.g. ranking.*, analysis.*, comparison.*) and phrase heuristics (offhire+pnl, delayed+expense, "over time", "trend", cargo+port) → `_build_composite()`.
  - Default → single.
- **_build_composite**:
  - Optional step 0: mongo resolveAnchor if entity hints and use_mongo.
  - Step 1: finance — registrySQL for cargo_profitability, else dynamicSQL.
  - Step 2: ops dynamicSQL (voyage_ids from finance) — skipped for analysis.scenario_comparison.
  - Step 3: mongo fetchRemarks (if use_mongo).
  - Step 4: llm merge.
  - `no_mongo_intents`: scenario_comparison, by_module_type, cargo_profitability, ranking.vessels.

---

## 4.1 Phase-1 limitations and execution-trace notes

- **“Inputs” in execution trace**: For composite steps, the trace “Inputs” show the **plan’s step inputs** (e.g. `"voyage_ids": "$finance.voyage_ids"`). They are **not** the runtime-resolved values. The router resolves placeholders before calling the agent; resolved counts are emitted in a separate trace event `composite_step_inputs_resolved` (e.g. `resolved_voyage_ids_count`, `resolved_cargo_grades_count`). Phase-2 can surface “Resolved: voyage_ids=0, cargo_grades=12” in the UI.
- **Cargo profitability / “argument formats can’t be mixed”**: This was an **execution bug**, not a design limitation. For `analysis.cargo_profitability`, finance returns aggregate rows by cargo grade (no `voyage_id`s). The ops step must receive **resolved** `voyage_ids` (empty list) and **cargo_grades** from artifacts and use the canonical PATH 1b (ports + delay remarks per grade). Fixes in place: (1) graph_router resolves `$finance.voyage_ids` from artifacts and overwrites `slots["voyage_ids"]` before calling the ops agent; (2) ops_agent defensively treats non-list `voyage_ids` (e.g. placeholder string) as `[]` so the DB never receives a string. If the error persists after deploy, confirm the trace shows `resolved_cargo_grades_count > 0` and that the latest graph_router + ops_agent are in use.

---

## 5. Intent and registries

- **intent_registry.py**:
  - **SUPPORTED_INTENTS**: list of canonical intent keys.
  - **INTENT_REGISTRY**: per-intent config: `description`, `required_slots`, `optional_slots`, `needs: { mongo, finance, ops }`, `mongo_intent`, `mongo_projection`.
  - **INTENT_ALIASES**: map LLM/variant keys → canonical (e.g. `analysis.cargoprofitability` → `analysis.cargo_profitability`).
  - **resolve_intent(key)**: direct → alias → lowercase → alias lowercase → else return key (caller treats unknown as out_of_scope).
- **sql_registry.py**:
  - **QuerySpec**: `description`, `required_params`, `sql` (with `%(param)s`).
  - **SQL_REGISTRY**: named queries e.g. `kpi.voyage_by_reference`, `kpi.voyages_by_flexible_filters`, `finance.rank_voyages_safe`, `kpi.voyages_by_cargo_grade`, `kpi.vessel_voyages_by_reference`, `kpi.cargo_profitability_analysis`, `kpi.port_performance_analysis`, `kpi.delayed_voyages_analysis`, `finance.compare_scenarios`, etc.
  - **Note**: `finance_agent` references `finance.compare_scenarios_aggregated`; registry has `finance.compare_scenarios`. If aggregated variant is required, it should be added to SQL_REGISTRY or key aligned.

---

## 6. Agents

- **FinanceAgent** (finance_agent.py):
  - **run(intent_key, slots, ...)**: `_map_intent()` → (query_key, params) → `pg.fetch_all(query_key, params)`; returns dict with `mode: "registry_sql"`, rows, etc.
  - **run_dynamic(question, intent_key, slots)**: `SQLGenerator.generate(question, intent_key, slots, agent="finance")` → `validate_and_prepare_sql()` → `pg.execute_dynamic_select()`; special handling for ops.port_query (filter_port), ranking.voyages (no ops table, required columns); returns `FinanceAgentResult` (mode `dynamic_sql`, sql, rows).
  - **_map_intent**: Maps voyage.summary, vessel.summary, analysis.scenario_comparison, ranking.*, analysis.segment_performance, analysis.cargo_profitability, ops.voyages_by_cargo_grade to registry query keys and params.
- **OpsAgent** (ops_agent.py):
  - **run**: Same pattern: `map_intent()` → `pg.fetch_all()`.
  - **run_dynamic**: (1) If `voyage_ids` list → canonical SELECT from ops_voyage_summary with ports_json, grades_json, remarks_json, etc. (2) If cargo_profitability + cargo_grades → canonical CTE for grade stats, ports, delay remarks. (3) If voyage_number → lookup voyage_id then recurse with voyage_ids. (4) If vessel.summary → deterministic vessel query by imo/vessel_name. (5) Else LLM SQL via SQLGenerator; **block** if SQL contains finance columns (pnl, revenue, total_expense, total_commission, tce). Then guard and execute.
  - **map_intent**: voyage.summary, vessel.summary, ops.delayed_voyages, ops.voyages_by_port / ops.port_query, ops.voyages_by_cargo_grade, port.details → registry keys.
- **MongoAgent** (mongo_agent.py):
  - **run(intent_key, slots, projection, session_context)**: entity.skip → skip; else `_resolve_anchor()` → (anchor_type, anchor_id); `_fetch_document()`; return MongoAgentResponse.
  - **fetch_full_voyage_context(voyage_number, voyage_id)**: get voyage doc with projection (remarks, fixtures, legs, revenues, expenses) → normalized dict.
  - **run_llm_find(question, slots)**: `mongo_schema_hint()` → normalize slots (voyage_number string, voyage_ids list of strings) → `MongoQueryBuilder.build()` → `validate_mongo_spec()` → `mongo.find_many()`; returns dict with mode `mongo_llm`, ok, collection, filter, projection, limit, rows.
  - **_resolve_anchor**: vessel.list_all, voyage.by_vessel, voyage.*, vessel.*, entity.auto, session context.
  - **_resolve_vessel_imo**, **_resolve_voyage_id**: use MongoAdapter get_vessel_imo_by_name, get_voyage_id_by_number.

---

## 7. SQL layer

- **sql_allowlist.py**: **SQLAllowlist** — `allowed_tables` (finance_voyage_kpi, ops_voyage_summary), `allowed_columns` per table, `forbidden_patterns` (DML, comments, etc.). **is_table_allowed**, **is_column_allowed**, **get_allowed_tables**, **get_allowed_columns**.
- **sql_guard.py**:
  - **ValidationResult**: ok, sql, params, reason.
  - **_sanitize_params**: string lists `"[...]"` → ast.literal_eval → wrap in tuple for ANY; list → tuple; else keep.
  - **_apply_simple_fixes**: column/table renames (e.g. costs→total_expense, ops_voyage→ops_voyage_summary), JSONB @> → ::text ILIKE, IN %(x)s → = ANY(%(x)s).
  - **_clean_order_by**: drop ORDER BY columns that are finance-only or ops-only when the other table set is not present.
  - **validate_and_prepare_sql**: apply fixes, sanitize params, extract tables (FROM/JOIN, exclude CTEs, skip LATERAL), allowlist tables, clean ORDER BY, parse SELECT list and reject _INVALID_COLUMNS, check forbidden_patterns, enforce LIMIT, filter params to those used in SQL; return ValidationResult.
- **sql_generator.py**:
  - **SQLGenerator**: uses **LLMClient**.
  - **_schema_hint_for_agent(agent, allowlist)**: for finance/ops restricts tables/columns; returns allowed_tables, allowed_columns, join_hints, param_conventions, constraints.
  - **generate(question, agent, slots, intent_key)**: schema_hint + intent_rules (e.g. ranking.voyages finance-only), system prompt, `llm.generate_sql()` → parse sql, params, tables, confidence; inject limit from slots if `%(limit)s` in sql; return **SQLGenOutput** (sql, params, tables, confidence).

---

## 8. Mongo dynamic path

- **mongo_schema.py**: **mongo_schema_hint()** — collections (vessels, voyages) with id_fields, fields (incl. dot-paths), examples; rules; **allowed_operators** ($and, $or, $in, $eq, $regex, etc.).
- **mongo_query_builder.py**: **MongoQueryBuilder.build(question, schema_hint, slots)** → LLM chat → parse JSON → MongoQuerySpec(collection, filter, projection, sort, limit); projection _id:0, limit capped 1–50.
- **mongo_guard.py**: **validate_mongo_spec(collection, filt, projection, sort, limit, allowed_collections, allowed_ops)** → allow collection, dict filter, _walk() to disallow $ operators not in allowed_ops ($options only with $regex, "i"); projection non-empty, limit 1–50; **MongoGuardResult** (ok, reason, collection, filter, projection, sort, limit).

---

## 9. LLM client (llm_client.py)

- **LLMConfig**: api_key, model, temperature. **LLMClient** uses Groq.
- **_deterministic_intent(text)**: regex/phrase rules for voyage.summary, vessel.summary, ranking.voyages_by_commission, ranking.vessels, ranking.voyages, scenario comparison, port call+profit, offhire, loss-making, cargo+profit, high revenue low pnl, module type, etc.; returns intent or None.
- **extract_intent_slots(text, supported_intents, schema_hint)**: normalize text; run deterministic intent; regex slots (voyage_numbers, vessel_name, limit, port_name); port_name + "visited" → ops.port_query; vessel_name + narrative phrases → vessel.summary; if deterministic → return without LLM; else LLM call, merge slots (regex overrides), sanitize; out_of_scope recovery if vessel_name/voyage_numbers present.
- **_sanitize_slots**: voyage_numbers → int list; limit 1–50; vessel_name 2–60 chars; port_name 1–80 chars.
- **generate_sql(question, intent_key, slots, schema_hint, agent, system_prompt)**: Groq chat, parse JSON; default empty SQL to SELECT 1 WHERE 1=0 LIMIT 1.
- **summarize_answer(question, plan, merged)**: out_of_scope → friendly templates (greeting, identity, weather, generic); else truncate/convert merged to JSON-safe, get merged_rows from artifacts; **_derive_answer_style** (narrative_summary, financial_first, ask_ports, ask_grades, ask_remarks); system prompt with DATA PRIORITY (merged_rows primary), STYLE, TEMPLATES by intent (voyage.summary, ranking.*, analysis.*, vessel.summary); draft answer; **_polish_answer_if_needed** (voyage/vessel narrative); **_postprocess_answer_markdown** (bullets, dedup lines, dedup headings); return cleaned or "Not available in dataset."
- **_call_with_retry**: Groq chat, strip code fences, json.loads; retry on exception; return_string for raw string.

---

## 10. Adapters

- **PostgresAdapter**: **PostgresConfig.from_env()** (DSN or host/port/user/password/db, connect_timeout). Lazy pool; backoff on failure. **fetch_all(query_key, params)** / **fetch_one**: get QuerySpec from SQL_REGISTRY, _prepare_params (required, placeholders, limit bounds), _execute_fetch_all. **execute_dynamic_select(sql, params)**: only SELECT/WITH; no DML; no $1/$2; _normalize_param_format (:x → %(x)s); add LIMIT if missing; filter params to those in SQL; _execute_fetch_all. **MAX_ROWS** cap per query.
- **MongoAdapter**: db = client[db_name], vessels, voyages. **get_vessel_imo_by_name**, **get_voyage_id_by_number**; **fetch_vessel**, **fetch_voyage**, **get_voyage_by_number** (with optional projection, $slice remarks); **_normalize_remarks** (remarks → remarkList); **find_many(collection, filt, projection, sort, limit)**.
- **RedisStore**: **RedisConfig** (host, port, db, timeouts, session_ttl_sec, lock/idem settings). **load_session(session_id)** → dict (slots, last_intent, anchor_type, anchor_id, last_user_input, turn, updated_at); fallback in-memory if Redis disabled/down. **save_session(session_id, patch)** merge patch, increment turn, setex. Optional idem_get/idem_set, acquire_lock/release_lock.

---

## 11. Services

- **response_merger.compact_payload(merged)**: Builds payload for summarizer: finance/ops/mongo with mode and rows[:50]; artifacts.merged_rows[:50] with **_light_merged_row** (voyage_id, voyage_number, pnl, revenue, total_expense, tce, total_commission, key_ports, cargo_grades, remarks capped); if merged_rows present, trim finance rows to 5 and clear ops/mongo rows; set dynamic_sql_used, dynamic_sql_agents.

---

## 12. UI (streamlit_app.py)

- **APP_TITLE / APP_SUBTITLE**; project root and dotenv from repo root.
- **_inject_global_css(theme)**: Light/Dark CSS variables and shared styles (app background, sidebar, expanders, tables, chat input, buttons).
- **_build_router_cached(groq_model, groq_temperature, env_fingerprint, code_fingerprint)**: cache_resource; builds LLMClient, get_mongo_db(), PostgresAdapter, RedisStore, MongoAgent, FinanceAgent, OpsAgent, GraphRouter.
- **Session**: session_id (ui-{uuid}), messages, last_result, ui_theme.
- **_sidebar_settings**: Session id, GROQ_API_KEY check, Connections (Postgres/Mongo/Redis), Model expander, Clear cache, New chat, Theme toggle; returns UiSettings (groq_model, groq_temperature). **_apply_runtime_env** sets GROQ_MODEL, GROQ_TEMPERATURE.
- **main**: set_page_config, init session, CSS, sidebar, header; if no API key show info; else build router, render messages; if last message has "Quick question" and suggestions, show buttons; chat_input → **_run_turn** (append user, router.handle(), append assistant with answer and meta, rerun). **_render_trace**: expanders for intent/slots/dynamic_sql, per-step trace (goal, inputs, result, sql, mongo_query).

---

## 13. Config and tests

- **config/database.py**: **get_mongo_db()** (MONGO_URI, MONGO_DB_NAME, timeouts), **get_postgres_connection()**, **get_redis_client()** (legacy helpers).
- **Tests**: test_sql_guard, test_slot_clarification, test_dynamic_sql_regressions, test_postgres_adapter, test_mongo_adapter.

---

## 14. Data and join rules

- **Mongo**: voyageId (string), voyageNumber (string), vesselName; remarks, fixtures, legs; projected_results.*.
- **Postgres**: finance_voyage_kpi (voyage_id, voyage_number, scenario, revenue, pnl, tce, …); ops_voyage_summary (voyage_id, voyage_number, ports_json, grades_json, remarks_json, …). Join by voyage_id or (voyage_number + vessel_imo normalized).
- **Merge**: by voyage_id; merged_rows carry finance + ops + mongo-derived key_ports, cargo_grades, remarks.

---

## 15. Design summary

- **Schema-guarded**: All DB access via adapters; SQL allowlist + guard; Mongo allowlist + guard; LIMIT and row caps.
- **Intent-first**: Deterministic overrides and regex reduce LLM drift; intent registry drives slots and plan.
- **Single vs composite**: Planner chooses single (one agent path) or composite (multi-step with finance → ops → mongo → merge).
- **Dynamic SQL/NoSQL**: Used in composite steps; finance/ops use SQLGenerator + sql_guard; mongo uses MongoQueryBuilder + mongo_guard; canonical paths when voyage_ids/vessel summary to avoid bad LLM SQL.
- **Session**: Redis (or in-memory fallback) for slots, intent, clarification, result-set follow-ups.
- **Answer quality**: compact_payload → summarize (draft) → polish for voyage/vessel narrative → postprocess markdown; style flags (narrative_summary, financial_first) and intent-specific templates.

---

## 16. Dynamic SQL & Dynamic NoSQL — Logic in detail

This section spells out **when** and **how** LLM-generated queries are used, validated, and executed.

### 16.1 When are dynamic queries used?

| Path | Dynamic SQL (Postgres) | Dynamic NoSQL (Mongo) |
|------|------------------------|------------------------|
| **Single** | No. Single path uses **registry SQL** only (`_map_intent()` → `query_key` → `SQL_REGISTRY`). | No. Single path uses **anchor resolution** + fixed projections (`_fetch_document`). |
| **Composite** | Yes. Finance step = `dynamicSQL` (except cargo_profitability → registry). Ops step = `dynamicSQL` (or canonical SQL when possible). | Optional. `run_llm_find` is used when the plan includes a Mongo step that needs a find spec (e.g. fetch remarks by voyage_ids). |

So: **dynamic SQL** = only in composite plans. **Dynamic NoSQL** = when MongoAgent is called with `run_llm_find` (question + slots → LLM builds a find spec).

---

### 16.2 Dynamic SQL — End-to-end logic

**1) Trigger**  
Composite step says `agent=finance, operation=dynamicSQL` or `agent=ops, operation=dynamicSQL`. Router calls `FinanceAgent.run_dynamic(...)` or `OpsAgent.run_dynamic(...)` with `question`, `intent_key`, `slots`.

**2) Schema hint (for the LLM)**  
`SQLGenerator._schema_hint_for_agent(agent, allowlist)` builds a strict hint:

- **Allowed tables**: from allowlist; for `finance` only finance_voyage_kpi (and join hints); for `ops` only ops_voyage_summary.
- **Allowed columns**: per-table from allowlist (no invented columns).
- **Param conventions**: use `%(param)s`; for lists use `= ANY(%(voyage_ids)s)`; scenario filter `COALESCE(%(scenario)s,'ACTUAL')`.
- **Constraints**: SELECT only, must have LIMIT.

**3) Intent-specific rules (finance)**  
For `ranking.voyages*` the generator adds extra rules: query only `finance_voyage_kpi`, no ops join, required columns (voyage_id, voyage_number, pnl, revenue, total_expense, …), rank by pnl DESC.

**4) LLM call**  
`LLMClient.generate_sql(question, intent_key, slots, schema_hint, agent, system_prompt)`:

- Sends: question, intent, slots, schema_hint, agent.
- Asks for **only** valid JSON: `{ "sql": "...", "params": {...}, "tables": [...], "confidence": 0.x }`.
- LLM returns a single SELECT (or WITH ... SELECT) using allowed tables/columns and named params.

**5) OpsAgent: prefer canonical, then LLM**  
Before calling the generator, OpsAgent tries in order:

- If **voyage_ids** list exists (from upstream finance step) → use **fixed SQL**: `SELECT ... FROM ops_voyage_summary WHERE voyage_id = ANY(%(voyage_ids)s) LIMIT %(limit)s`. No LLM.
- If **cargo_profitability** + **cargo_grades** → use **fixed CTE** (grade stats, ports, delay remarks). No LLM.
- If **voyage_number** only → lookup voyage_id in Postgres, then re-call `run_dynamic` with voyage_ids. No LLM.
- If **vessel.summary** → **fixed SQL** by imo/vessel_name (ports_json, grades_json, remarks_json). No LLM.
- Otherwise → call **SQLGenerator.generate(agent="ops")**. Then **hard block** if generated SQL contains any of: pnl, revenue, total_expense, total_commission, tce (ops must not touch finance columns).

**6) Guard (validate + prepare)**  
`validate_and_prepare_sql(sql, params, allowlist, enforce_limit=True)`:

- **Fixes**: column/table renames (e.g. costs→total_expense, ops_voyage→ops_voyage_summary), JSONB `@>` → `::text ILIKE`, `IN %(x)s` → `= ANY(%(x)s)`.
- **Params**: string that looks like list `"[...]"` → parsed and wrapped in tuple for psycopg2 `ANY()`; lists → tuple.
- **Tables**: regex FROM/JOIN, exclude CTE names and LATERAL; every table must be in allowlist.
- **ORDER BY**: drop columns that are finance-only when only ops table is used (and vice versa).
- **SELECT list**: reject known invalid column names (e.g. cargo_grade, port_name as column).
- **Forbidden patterns**: no DML, no comments, no pg_catalog, etc.
- **LIMIT**: if missing, append `LIMIT 50`.
- **Params**: keep only params that appear in the SQL (`%(name)s` or `:name`).

Returns `ValidationResult(ok, sql, params, reason)`.

**7) FinanceAgent repair (ranking only)**  
If intent is `ranking.voyages*` and (a) SQL joins ops table, or (b) guard fails, or (c) rows lack required columns → one repair: re-prompt LLM with "finance_voyage_kpi only, return voyage_id, voyage_number, pnl, revenue, total_expense, …" and re-run guard/execute.

**8) Execution**  
`PostgresAdapter.execute_dynamic_select(guard.sql, guard.params)`:

- Only SELECT or WITH ... SELECT; no DML; no `$1`/`$2` (only `%(name)s`).
- Converts `:param` → `%(param)s` in SQL (for compatibility).
- Adds default LIMIT if still missing; filters params to those in SQL; runs query and returns list of dicts.

So the **logic**: **Question + intent + slots + schema (and for ops, canonical paths first) → LLM generates SQL → guard fixes and validates → adapter runs only SELECT with filtered params.**

---

### 16.3 Dynamic NoSQL — End-to-end logic

**1) Trigger**  
When the plan has a Mongo step that needs a find spec (e.g. "fetch remarks for these voyage IDs"), the router calls `MongoAgent.run_llm_find(question, slots)`.

**2) Schema hint**  
`mongo_schema_hint()` returns a **stable** description:

- **Collections**: `vessels`, `voyages` with `id_fields`, `fields` (including dot-paths like `remarks.remark`, `fixtures.fixturePorts.portName`), and **examples** (by_voyageId, by_voyageNumber, by_vesselName_regex, voyageId $in, etc.).
- **Rules**: e.g. voyageNumber is string in Mongo; use `remarks` not remarkList; only allowed operators.
- **allowed_operators**: `$and`, `$or`, `$in`, `$nin`, `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$regex`, `$options`, `$exists`, `$size`, `$not`, `$elemMatch`.

**3) Slot normalization**  
Slots are normalized for Mongo conventions: `voyage_number` → string; `voyage_numbers` / `voyage_ids` → list of strings (Mongo uses string voyageNumber and voyageId).

**4) LLM call**  
`MongoQueryBuilder.build(question, schema_hint, slots)`:

- Sends: task `mongo_find_spec`, question, slots, schema_hint, output_format (collection, filter, projection, sort, limit).
- Asks for **only** valid JSON; no markdown.
- LLM returns: `collection`, `filter` (query), `projection` (fields to return), `sort`, `limit`.
- Builder forces `_id: 0` in projection and caps limit 1–50.

**5) Guard**  
`validate_mongo_spec(collection, filter, projection, sort, limit, allowed_collections, allowed_ops)`:

- **Collection** must be in allowed set (vessels, voyages).
- **Filter** must be a dict; every `$` key in the filter (recursively) must be in `allowed_ops`; `$options` only allowed next to `$regex` with value `"i"`.
- **Projection** non-empty; limit clamped to 1–50.
- Returns **MongoGuardResult** (ok, reason, collection, filter, projection, sort, limit).

**6) Execution**  
`MongoAdapter.find_many(collection, filter, projection, sort, limit)` runs the find and returns a list of documents.

So the **logic**: **Question + slots + schema hint (collections, fields, operators, examples) → LLM generates a find spec (collection, filter, projection, sort, limit) → guard ensures collection and operators are allowed and projection/limit safe → adapter runs find_many.**

---

### 16.4 Summary table

| Step | Dynamic SQL | Dynamic NoSQL |
|------|-------------|----------------|
| **When** | Composite plan: finance/ops step with operation=dynamicSQL | Plan step that uses Mongo find (e.g. fetch by voyage_ids) |
| **Input** | question, intent_key, slots (+ for ops: voyage_ids / voyage_number / vessel) | question, slots (voyage_ids, voyage_number, etc.) |
| **Schema** | Allowlist tables/columns, param conventions, intent rules | Collections, fields, examples, allowed_operators |
| **LLM output** | JSON: sql, params, tables, confidence | JSON: collection, filter, projection, sort, limit |
| **Guard** | Fixes (renames, IN→ANY, param wrap), allowlist, ORDER BY, invalid cols, forbidden patterns, LIMIT, param filter | Collection allowlist, $ operator allowlist, projection, limit 1–50 |
| **Execute** | PostgresAdapter.execute_dynamic_select(sql, params) | MongoAdapter.find_many(collection, filter, projection, sort, limit) |

**Design idea**: The LLM only proposes a query; it never runs it. The **guard** enforces schema and safety; the **adapter** runs a single, validated operation. For Postgres, ops avoids bad LLM SQL by using **canonical SQL** whenever voyage_ids, voyage_number, vessel.summary, or cargo_profitability context is available.
