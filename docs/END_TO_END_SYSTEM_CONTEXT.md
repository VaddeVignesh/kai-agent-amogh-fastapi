# KAI-Agent — End-to-End System Context

This document summarizes the **full architecture, execution flow, and module roles** of the KAI-Agent maritime analytics system after a complete pass over the codebase.

---

## 1. What the System Is

**KAI-Agent** is a **schema-guarded, multi-agent maritime analytics chatbot** that:

- Answers natural-language questions about **voyages**, **vessels**, **ports**, **PnL**, **revenue**, **delays**, **scenarios**, and **rankings**.
- Uses **PostgreSQL** (finance + ops KPIs), **MongoDB** (voyage/vessel context, remarks, fixtures), and **Redis** (session memory).
- Uses an **LLM** (Groq) for: intent/slot extraction (with deterministic overrides), dynamic SQL/Mongo find-spec generation (both guarded), and narrative answer generation (draft + optional polish).
- **Constraint**: The LLM never talks to databases directly. All DB access goes through adapters and agents.

---

## 2. Entry Points and Request Flow

### 2.1 Two Entry Points

| Entry | File | Role |
|-------|------|------|
| **API** | `app/main.py` | FastAPI app: `POST /query` with `{ query, session_id? }` → `router.handle(session_id, user_input)` → `QueryResponse`. |
| **UI** | `app/UI/UX/streamlit_app.py` | Streamlit chat: user types message → `call_api(query, session_id)` → `POST http://127.0.0.1:8000/query` → displays `answer`, `clarification`, `trace`, etc. |

So: **Streamlit is the frontend; FastAPI is the backend.** The UI always calls the same `/query` endpoint.

### 2.2 Single Request Flow (High Level)

```
User input (or API payload)
    ↓
GraphRouter.handle(session_id, user_input)
    ↓
LangGraph INVOKE with initial state { session_id, user_input, raw_user_input }
    ↓
load_session → extract (intent+slots) → validate → [clarify | build_plan]
    ↓
[clarify] → END (return clarification only)
[build_plan] → plan_type: "single" | "composite"
    ↓
SINGLE: run_single → [done → END | merge → summarize → END]
COMPOSITE: execute_step (loop) → merge → summarize → END
    ↓
handle() reads final state → returns { intent_key, slots, answer?, clarification?, data, plan, dynamic_sql_used, dynamic_sql_agents, trace }
```

---

## 3. Project Structure (App-Only)

```
app/
├── main.py                    # FastAPI app, /query endpoint, wires router + agents + adapters
├── UI/UX/
│   └── streamlit_app.py       # Chat UI: call_api(), session, theme, trace expanders
├── orchestration/
│   ├── graph_router.py        # GraphState, LangGraph (nodes + edges), handle()
│   ├── planner.py             # ExecutionPlan, single vs composite, _build_composite()
│   ├── mongo_schema.py        # mongo_schema_hint() for LLM Mongo find specs
│   └── router.py              # (legacy/supplementary)
├── registries/
│   ├── intent_registry.py     # SUPPORTED_INTENTS, INTENT_REGISTRY, INTENT_ALIASES, resolve_intent()
│   └── sql_registry.py        # QuerySpec, SQL_REGISTRY (named queries for Postgres)
├── agents/
│   ├── finance_agent.py       # FinanceAgent: run() registry, run_dynamic() LLM SQL
│   ├── ops_agent.py           # OpsAgent: run() registry, run_dynamic() canonical + LLM SQL
│   ├── mongo_agent.py         # MongoAgent: run() anchor + fetch, run_llm_find() dynamic Mongo
│   └── kpi_agent.py           # Legacy KPI helper (different schema)
├── adapters/
│   ├── postgres_adapter.py    # PostgresAdapter: fetch_all(query_key, params), execute_dynamic_select()
│   ├── mongo_adapter.py       # MongoAdapter: vessels/voyages, get_vessel_imo_by_name, get_voyage_id_by_number, find_many()
│   └── redis_store.py         # RedisStore: load_session(), save_session(), in-memory fallback
├── llm/
│   ├── llm_client.py          # LLMClient: extract_intent_slots(), generate_sql(), summarize_answer(), Groq
│   └── mongo_query_builder.py # MongoQueryBuilder: build() → MongoQuerySpec (collection, filter, projection, sort, limit)
├── sql/
│   ├── sql_allowlist.py       # DEFAULT_ALLOWLIST: allowed_tables, allowed_columns, forbidden_patterns
│   ├── sql_guard.py           # validate_and_prepare_sql(): fixes, allowlist, LIMIT, params
│   └── sql_generator.py       # SQLGenerator: schema hint, generate() → SQLGenOutput (sql, params, tables, confidence)
├── mongo/
│   └── mongo_guard.py         # validate_mongo_spec(): collection, $ operators, projection, limit 1–50
├── services/
│   └── response_merger.py     # compact_payload(merged): light merged_rows, cap rows, for summarizer
└── config/
    └── database.py            # get_mongo_db(), get_postgres_connection(), get_redis_client()
```

---

## 4. Orchestration (LangGraph)

### 4.1 GraphState (TypedDict)

Carries through the graph: `session_id`, `user_input`, `raw_user_input`, `session_ctx`, `intent_key`, `slots`, `missing_keys`, `clarification`, `plan_type`, `plan`, `step_index`, `artifacts`, `mongo`, `finance`, `ops`, `data`, `merged`, `answer`.

### 4.2 Nodes (in order of use)

| Node | Function | Role |
|------|----------|------|
| **load_session** | `n_load_session` | `session_ctx = redis.load_session(session_id)` |
| **extract** | `n_extract_intent` | Pending clarification resolution; chitchat/out_of_scope; regex slots; deterministic intent; LLM `extract_intent_slots`; scenario detection; slot cleaning; trace |
| **validate** | `n_validate_slots` | Required slots from INTENT_REGISTRY; missing → clarification + route to clarify; else to build_plan (or summarize for edge cases) |
| **clarify** | `n_make_clarification` | Return clarification message; END |
| **build_plan** | `n_plan` | `Planner.build_plan(text, session_ctx, intent_key, slots)` → plan_type single/composite, steps; store plan, step_index=0, init artifacts |
| **run_single** | `n_run_single` | Dispatch by intent: followup.result_set, voyage.summary (finance+ops+mongo), vessel.summary, out_of_scope, ops.port_query, etc.; registry only; write finance/ops/mongo; route done | merge |
| **execute_step** | `n_execute_step` | Run one step: finance (registrySQL or dynamicSQL), ops (dynamicSQL, voyage_ids from finance), mongo (resolveAnchor, fetchRemarks/run_llm_find); increment step_index; trace |
| **merge** | `n_merge` | Merge state.data (finance, ops, mongo, artifacts) into state.merged; set dynamic_sql_used, dynamic_sql_agents |
| **summarize** | `n_summarize` | compact_payload(merged); _sanitize_for_llm; llm.summarize_answer(question, plan, merged) → state["answer"]; redis.save_session(); trace |

### 4.3 Conditional Edges

- **r_after_validate**: "clarify" | "plan" | "summarize"
- **r_plan_path**: "single" → run_single, "composite" → execute_step
- **r_has_more_steps**: "more" → execute_step again, "done" → merge
- **r_after_run_single**: "done" → END, "merge" → merge

### 4.4 Merge Logic (inside graph, before summarize)

Merge builds `state["merged"]` from `state["data"]` (finance, ops, mongo, artifacts). The actual **row-level merge by voyage_id** into `artifacts.merged_rows` is done inside the composite path (in `n_execute_step` or a dedicated merge step depending on code). Summarize then uses `compact_payload(merged)` and sends that to the LLM.

---

## 5. Planner (Single vs Composite)

- **Planner.build_plan(text, session_context, intent_key, slots)** is deterministic (no second LLM).
- **out_of_scope** → single, no steps.
- **Single voyage/vessel/port** (e.g. one voyage_number, vessel.summary, ops.port_query with port_name) → single.
- **Hard composite intents** (e.g. ranking.*, analysis.scenario_comparison, analysis.cargo_profitability, ops.delayed_voyages) and phrase heuristics ("offhire+pnl", "delayed+expense", "over time", "trend", "cargo+port") → `_build_composite()`.
- **_build_composite** steps (conceptually):
  - (Optional) mongo resolveAnchor
  - finance: registrySQL for cargo_profitability / high_revenue_low_pnl / ranking.vessels, else dynamicSQL
  - ops: dynamicSQL (skipped for analysis.scenario_comparison); inputs `$finance.voyage_ids`
  - mongo fetchRemarks (if use_mongo)
  - llm merge
- **no_mongo_intents**: scenario_comparison, by_module_type, cargo_profitability, ranking.vessels.

---

## 6. Agents

### 6.1 FinanceAgent

- **run(intent_key, slots, ...)** → `_map_intent()` → (query_key, params) → `pg.fetch_all(query_key, params)` → registry_sql result.
- **run_dynamic(question, intent_key, slots)** → SQLGenerator.generate(agent="finance") → validate_and_prepare_sql → pg.execute_dynamic_select(); repair for ranking.voyages* (finance-only, required columns); returns FinanceAgentResult (mode=dynamic_sql, sql, rows).

### 6.2 OpsAgent

- **run** → map_intent() → pg.fetch_all().
- **run_dynamic**: (1) If voyage_ids → canonical SELECT ops_voyage_summary WHERE voyage_id = ANY(%(voyage_ids)s). (2) If cargo_profitability + cargo_grades → canonical CTE (grade stats, ports, delay remarks). (3) If voyage_number → lookup voyage_id, recurse with voyage_ids. (4) If vessel.summary → deterministic vessel SQL. (5) Else LLM SQL via SQLGenerator; **block** if SQL contains finance columns (pnl, revenue, total_expense, total_commission, tce). Then guard and execute.

### 6.3 MongoAgent

- **run(intent_key, slots, projection, session_context)** → entity.skip | _resolve_anchor → _fetch_document → MongoAgentResponse(anchor_type, anchor_id, document).
- **fetch_full_voyage_context(voyage_number, voyage_id)** → voyage doc with remarks, fixtures, legs, revenues, expenses.
- **run_llm_find(question, slots)** → mongo_schema_hint() → normalize slots (voyage_number string, voyage_ids list of strings) → MongoQueryBuilder.build() → validate_mongo_spec → mongo.find_many() → { mode: mongo_llm, ok, collection, filter, projection, limit, rows }.

---

## 7. Adapters

- **PostgresAdapter**: PostgresConfig.from_env(); lazy pool; fetch_all(query_key, params) from SQL_REGISTRY; execute_dynamic_select(sql, params): SELECT/WITH only, no DML, :param → %(param)s, LIMIT enforced, params filtered to SQL; MAX_ROWS cap.
- **MongoAdapter**: db[vessels], db[voyages]; get_vessel_imo_by_name, get_voyage_id_by_number; fetch_vessel, fetch_voyage, get_voyage_by_number; _normalize_remarks; find_many(collection, filt, projection, sort, limit).
- **RedisStore**: load_session(session_id), save_session(session_id, patch); in-memory fallback if Redis disabled/down; optional idem_get/idem_set, acquire_lock/release_lock.

---

## 8. LLM Client (llm_client.py)

- **extract_intent_slots(text, supported_intents, schema_hint)**: normalize text; _deterministic_intent(); regex slots (voyage_numbers, vessel_name, limit, port_name); port_name + "visited" → ops.port_query; vessel narrative → vessel.summary; if deterministic return; else Groq call; merge slots; sanitize; out_of_scope recovery if vessel/voyage present.
- **generate_sql(question, intent_key, slots, schema_hint, agent, system_prompt)**: Groq chat, JSON { sql, params, tables, confidence }; default empty → SELECT 1 WHERE 1=0 LIMIT 1.
- **summarize_answer(question, plan, merged)**: out_of_scope → templates; else compact/truncate merged, merged_rows from artifacts; _derive_answer_style; draft; _polish_answer_if_needed (voyage/vessel narrative); _postprocess_answer_markdown; return cleaned or "Not available in dataset."

---

## 9. SQL Layer

- **sql_allowlist**: DEFAULT_ALLOWLIST (finance_voyage_kpi, ops_voyage_summary; allowed_columns per table; forbidden_patterns).
- **sql_guard**: validate_and_prepare_sql — _apply_simple_fixes (column/table renames, IN→ANY, JSONB); _sanitize_params (list→tuple for ANY); extract tables (FROM/JOIN); allowlist; _clean_order_by; reject _INVALID_COLUMNS; forbidden_patterns; LIMIT; filter params to SQL.
- **sql_generator**: _schema_hint_for_agent(agent, allowlist); generate(question, agent, slots, intent_key) → LLM generate_sql → SQLGenOutput.

---

## 10. Mongo Dynamic Path

- **mongo_schema**: mongo_schema_hint() — collections (vessels, voyages), id_fields, fields (dot-paths), examples, rules, allowed_operators.
- **mongo_query_builder**: build(question, schema_hint, slots) → Groq → JSON → MongoQuerySpec(collection, filter, projection, sort, limit); _id:0, limit 1–50.
- **mongo_guard**: validate_mongo_spec(collection, filt, projection, sort, limit, allowed_collections, allowed_ops) → MongoGuardResult; _walk() for $ operators.

---

## 11. Intent and Registries

- **intent_registry**: SUPPORTED_INTENTS list; INTENT_REGISTRY per-intent (description, required_slots, optional_slots, needs mongo/finance/ops, mongo_intent, mongo_projection); INTENT_ALIASES; resolve_intent(key).
- **sql_registry**: QuerySpec(description, required_params, sql); SQL_REGISTRY keys e.g. kpi.voyage_by_reference, kpi.voyages_by_flexible_filters, finance.rank_voyages_safe, finance.compare_scenarios_aggregated, kpi.cargo_profitability_analysis, kpi.port_performance_analysis, etc.

---

## 12. Data and Join Rules

- **Mongo**: voyageId (string), voyageNumber (string), vesselName; remarks, fixtures, legs; projected_results.*.
- **Postgres**: finance_voyage_kpi (voyage_id, voyage_number, scenario, revenue, pnl, tce, …); ops_voyage_summary (voyage_id, voyage_number, ports_json, grades_json, remarks_json, …). Join by voyage_id or (voyage_number + vessel_imo normalized).
- **Merge**: by voyage_id; merged_rows carry finance + ops + mongo-derived key_ports, cargo_grades, remarks.

---

## 13. Execution Flow Summary

1. **Request** hits FastAPI `/query` or Streamlit → `router.handle(session_id, user_input)`.
2. **Graph** runs: load_session → extract (intent + slots) → validate.
3. If **clarify** → return clarification and stop.
4. **build_plan** → single or composite with steps.
5. **Single**: run_single runs one or more agents via registry (voyage.summary: finance + ops + mongo context); then done or merge → summarize.
6. **Composite**: execute_step loop (finance → ops → mongo by plan); merge builds merged (with merged_rows); summarize compacts payload, sanitizes, calls LLM summarize_answer, saves session.
7. **handle()** returns answer (or clarification), intent_key, slots, data (merged), plan, dynamic_sql_used, dynamic_sql_agents, trace.

---

## 14. Configuration and Environment

- **Backend**: GROQ_API_KEY (required), GROQ_MODEL, GROQ_TEMPERATURE; POSTGRES_DSN or POSTGRES_HOST/PORT/USER/PASSWORD/DB; MONGO_URI, MONGO_DB_NAME; REDIS_HOST, REDIS_PORT (or REDIS_DISABLED); KAI_DEBUG for debug logs.
- **Frontend**: Streamlit reads .env from repo root; API_URL = http://127.0.0.1:8000/query.

This is the **full end-to-end context** of how the system is structured and how a single user query flows from the UI or API through the graph, agents, adapters, and back to the response.
