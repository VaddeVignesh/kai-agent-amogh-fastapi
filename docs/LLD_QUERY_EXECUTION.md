# Low-Level Design: End-to-End Query Execution

![LLD Query Execution Flow](assets/LLD_QUERY_EXECUTION.png)

This document describes how a single user query is executed through the KAI-Agent system, with file and code references. It is the **complete** flow from HTTP request to HTTP response.

---

## 1. Scope and Conventions

- **LLD scope**: Request entry → intent extraction → planning → execution (finance / ops / mongo) → merge → summarization → response. No UI layout or deployment.
- **Code refs**: `path/to/file.py` and, where useful, line or function names.
- **State**: `GraphState` is the single state object passed through the LangGraph; it is mutated by each node.

---

## 2. System Overview

- **Role**: Maritime analytics chatbot: natural-language questions about voyages, vessels, PnL, revenue, delays, rankings, scenarios.
- **Data**: PostgreSQL (finance_voyage_kpi, ops_voyage_summary), MongoDB (voyages, vessels, remarks/fixtures), Redis (session memory).
- **LLM**: Groq (configurable model). Used for intent/slot extraction, dynamic SQL generation, dynamic Mongo find-spec generation, and final answer summarization. The LLM never talks to DBs directly; all access is via adapters and agents.

---

## 3. Directory and File Map

| Path | Purpose |
|------|--------|
| `app/main.py` | FastAPI app; `POST /query`; wires router, agents, adapters. |
| `app/orchestration/graph_router.py` | `GraphState`, LangGraph build, all nodes (`n_*`), conditional edges (`r_*`), `handle()`. |
| `app/orchestration/planner.py` | `ExecutionPlan`, `build_plan()`, `_build_composite()`; single vs composite; step list. |
| `app/registries/intent_registry.py` | `SUPPORTED_INTENTS`, `INTENT_REGISTRY`, `INTENT_ALIASES`, `resolve_intent()`. |
| `app/registries/sql_registry.py` | `QuerySpec`, `SQL_REGISTRY` (named Postgres queries). |
| `app/agents/finance_agent.py` | `FinanceAgent`: `run()` (registry), `run_dynamic()` (LLM SQL), `_map_intent()`. |
| `app/agents/ops_agent.py` | `OpsAgent`: `run()`, `run_dynamic()` (canonical + LLM SQL). |
| `app/agents/mongo_agent.py` | `MongoAgent`: `run()`, `fetch_full_voyage_context()`, `run_llm_find()`. |
| `app/adapters/postgres_adapter.py` | `PostgresAdapter`: `fetch_all(query_key, params)`, `execute_dynamic_select(sql, params)`. |
| `app/adapters/mongo_adapter.py` | MongoAdapter: vessels/voyages collections, get_voyage_by_number, find_many. |
| `app/adapters/redis_store.py` | `RedisStore`: `load_session()`, `save_session()`; in-memory fallback. |
| `app/llm/llm_client.py` | `LLMClient`: `extract_intent_slots()`, `generate_sql()`, `summarize_answer()`. |
| `app/llm/mongo_query_builder.py` | Builds Mongo find specs from question + schema (for dynamic Mongo path). |
| `app/sql/sql_generator.py` | `SQLGenerator`: schema hint, `generate()` → LLM → `SQLGenOutput`. |
| `app/sql/sql_allowlist.py` | Allowed tables/columns for Postgres. |
| `app/sql/sql_guard.py` | `validate_and_prepare_sql()`: allowlist, LIMIT, param filter, fixes. |
| `app/mongo/mongo_guard.py` | Validates Mongo find spec (collection, operators, limit). |
| `app/orchestration/mongo_schema.py` | `mongo_schema_hint()` for LLM Mongo specs. |
| `app/services/response_merger.py` | `compact_payload(merged)`: light merged_rows and caps for summarizer. |
| `app/config/database.py` | `get_mongo_db()`, `get_postgres_connection()`, `get_redis_client()`. |
| `app/UI/UX/streamlit_app.py` | Streamlit chat; calls `POST /query`; displays answer, trace, etc. |

---

## 4. Entry Points and Initial Request Flow

### 4.1 Entry Points

- **API**: `app/main.py`. `POST /query` with body `{ "query": str, "session_id": str | null }`. Handler calls `router.handle(session_id=..., user_input=req.query)` and returns `QueryResponse` (answer, clarification, trace, intent_key, slots, dynamic_sql_used, dynamic_sql_agents).
- **UI**: `app/UI/UX/streamlit_app.py`. User types message → `call_api(query, session_id)` → same `POST /query` → render answer, clarification, trace.

### 4.2 handle() and Graph Invocation

- **Location**: `app/orchestration/graph_router.py`, `GraphRouter.handle(session_id, user_input)`.
- **Action**: `self.graph.invoke({"session_id": session_id, "user_input": user_input, "raw_user_input": user_input})`. The graph is a compiled LangGraph (`StateGraph(GraphState)`); invoke runs until END.
- **After invoke**: Read `out["artifacts"]["trace"]`, `out["clarification"]`, `out["merged"]`, `out["answer"]`. If clarification is set, return it and stop. Otherwise return answer, data=merged, dynamic_sql_used, dynamic_sql_agents, plan, trace.

---

## 5. Graph State (GraphState)

- **Defined in**: `app/orchestration/graph_router.py`, `class GraphState(TypedDict, total=False)`.
- **Main keys**:
  - `session_id`, `user_input`, `raw_user_input`, `session_ctx`
  - `intent_key`, `slots`, `missing_keys`, `clarification`
  - `plan_type`, `plan`, `step_index`, `artifacts`
  - `mongo`, `finance`, `ops`, `data`, `merged`, `answer`

---

## 6. Graph Structure (Nodes and Edges)

- **Build**: `GraphRouter._build_graph()` in `graph_router.py` (StateGraph, add_node, add_edge, add_conditional_edges, set_entry_point, compile).

**Nodes:**

| Node name     | Method             | Role |
|---------------|--------------------|------|
| load_session  | `n_load_session`   | Load session from Redis into `state["session_ctx"]`. |
| extract       | `n_extract_intent` | Intent + slots from user input; scenario detection; trace. |
| validate      | `n_validate_slots` | Required slots from INTENT_REGISTRY; set `missing_keys` or leave empty. |
| clarify       | `n_make_clarification` | Set clarification message; then END. |
| build_plan    | `n_plan`           | Planner.build_plan() → plan_type (single/composite), steps; store plan, step_index=0, init artifacts. |
| run_single    | `n_run_single`     | Single-path: dispatch by intent (voyage.summary, vessel.summary, etc.); run agents via registry; set answer or leave for merge. |
| execute_step  | `n_execute_step`   | Run one composite step (finance / ops / mongo / llm.merge); update state.data, artifacts, step_index; trace. |
| merge         | `n_merge`          | Copy state.data into state.merged; set dynamic_sql_used / dynamic_sql_agents. |
| summarize     | `n_summarize`      | compact_payload; sanitize; llm.summarize_answer(); save session; set state["answer"]. |

**Edges:**

- load_session → extract → validate.
- validate → **conditional** `r_after_validate`: "clarify" | "plan" | "summarize".
- clarify → END.
- build_plan → **conditional** `r_plan_path`: "single" → run_single, "composite" → execute_step.
- execute_step → **conditional** `r_has_more_steps`: "more" → execute_step, "done" → merge.
- run_single → **conditional** `r_after_run_single`: "done" → END, "merge" → merge.
- merge → summarize → END.

---

## 7. Node 1: load_session

- **Method**: `n_load_session(state)`.
- **Code**: `state["session_ctx"] = self.redis.load_session(state["session_id"])`. RedisStore returns a dict (e.g. slots, last_intent, anchor_type, anchor_id, last_user_input); if Redis is down, in-memory fallback is used.

---

## 8. Node 2: extract (Intent and Slots)

- **Method**: `n_extract_intent(state)` in `graph_router.py`. Long method: normalizes input, applies deterministic overrides, regex slots, LLM extraction, scenario comparison detection, slot cleaning, trace append.
- **Intent source**: Resolved via `resolve_intent()` from `intent_registry.py` (INTENT_ALIASES map). LLM returns intent + slots; router can override (e.g. voyage summary, vessel summary, out_of_scope recovery).
- **LLM extraction**: `self.llm.extract_intent_slots(text=user_input, supported_intents=SUPPORTED_INTENTS, schema_hint=...)` in `llm_client.py`. Steps: normalize text; `_deterministic_intent()` (e.g. scenario comparison, port calls + profit, offhire, ranking); regex slots (voyage numbers, vessel name, limit, port_name, etc.); optional Groq call; merge slots; sanitize; out_of_scope recovery if entity present.
- **Output**: `state["intent_key"]`, `state["slots"]`; optionally `state["clarification"]`; trace event appended to `state["artifacts"]["trace"]`.

---

## 9. Node 3: validate

- **Method**: `n_validate_slots(state)`.
- **Logic**: intent_key from state; look up `INTENT_REGISTRY[intent_key]`; required_slots list; for each required key check slots (null/empty/placeholder). Set `state["missing_keys"]` to list of missing keys if any; otherwise leave missing_keys empty.
- **No branching here**: branching happens in the conditional edge after validate.

---

## 10. Conditional: r_after_validate

- **Method**: `r_after_validate(state)`.
- **Returns**: "clarify" if `state["missing_keys"]` is non-empty; "summarize" if resolved intent not in INTENT_REGISTRY; else "plan".

---

## 11. Node 4: clarify

- **Method**: `n_make_clarification(state)`. Builds a clarification message (e.g. which voyage, which port) and sets `state["clarification"]`. Next edge: clarify → END.

---

## 12. Node 5: build_plan (Planner)

- **Method**: `n_plan(state)`. Calls `self.planner.build_plan(text=user_input, session_context=session_ctx, intent_key=state["intent_key"], slots=state["slots"])`.
- **Planner logic** (`app/orchestration/planner.py`):
  - `intent_key = resolve_intent(intent_key or "out_of_scope")`.
  - If out_of_scope → return ExecutionPlan(plan_type="single", intent_key=out_of_scope, steps=[]).
  - **Single** (entity anchor): single voyage_number and voyage-like intent → single, intent voyage.summary. Single vessel_name + vessel intent (no trend/ranking) → single, vessel.summary. Single port query with port_name → single, ops.port_query.
  - **Composite**: if intent in HARD_COMPOSITE_INTENTS or composite_targets (e.g. ranking.*, analysis.scenario_comparison, analysis.cargo_profitability, analysis.by_module_type, ops.delayed_voyages, analysis.segment_performance, composite.query) → `_build_composite()`. Also phrase rules: "offhire" + pnl/tce, "delayed" + pnl/expense, "over time"/"trend", "cargo" + "port" → composite.
  - **Default**: single with current intent_key and steps=[].
- **_build_composite**: Builds steps list. Optional step 0: mongo resolveAnchor. Step 1: finance with operation **dynamicSQL** (composite always uses dynamic SQL for finance). Step 2: ops dynamicSQL (skipped if intent is analysis.scenario_comparison); inputs `voyage_ids: "$finance.voyage_ids"`. Step 3: mongo fetchRemarks if use_mongo (excluded for no_mongo_intents). Step 4: llm merge. Plan stored with plan_type="composite", steps=[...].
- **State update**: `state["plan"]` = plan as dict, `state["plan_type"]` = plan.plan_type, `state["step_index"]` = 0, `state["artifacts"]` initialized if needed.

---

## 13. Conditional: r_plan_path

- **Method**: `r_plan_path(state)`. Returns "composite" if `state["plan_type"] == "composite"`, else "single".

---

## 14. Node 6a: run_single (Single Path)

- **Method**: `n_run_single(state)`. Dispatches by intent_key. Examples: followup.result_set, voyage.summary (finance + ops + mongo context, registry), vessel.summary, out_of_scope (template answer), ops.port_query, etc. Uses **registry only** (no dynamic SQL in single path for finance/ops when implemented via run()). Writes to state["finance"], state["ops"], state["mongo"], state["data"]; for voyage.summary/vessel.summary may set state["answer"] directly or leave for merge.
- **Routing after**: `r_after_run_single` → "done" if state["clarification"] or state["answer"], else "merge".

---

## 15. Node 6b: execute_step (Composite Path, One Step per Call)

- **Method**: `n_execute_step(state)`. Runs one step at index `state["step_index"]` from `state["plan"]["steps"]`. Step has `agent`, `operation`, `inputs`. Operation normalized to lowercase with camelCase→snake_case (e.g. dynamicSQL → dynamic_sql).

**Finance step** (agent=="finance", op in ("dynamic_sql", "registry_sql")):

- Merge step inputs into slots. intent_key from state/plan. **use_registry = (op == "registry_sql")** (composite plan currently always sends dynamicSQL for finance, so use_registry is False).
- If use_registry: `self.finance_agent.run(intent_key, slots)` → _map_intent → query_key, params → `pg.fetch_all(query_key, params)` → rows; result mode=registry_sql.
- Else: `self.finance_agent.run_dynamic(question=state["user_input"], intent_key=intent_key, slots=slots)` → SQLGenerator.generate() → LLM generate_sql → validate_and_prepare_sql → pg.execute_dynamic_select(); result mode=dynamic_sql, sql=..., rows. Rows capped (e.g. 20). voyage_ids extracted from rows; stored in artifacts["voyage_ids"], artifacts["finance_rows"], slots["voyage_ids"]. Trace appended.

**Ops step** (agent=="ops", op=="dynamic_sql"):

- Resolve inputs: e.g. voyage_ids from artifacts (from finance). Set slots["voyage_ids"] from artifacts.
- `self.ops_agent.run_dynamic(question=..., intent_key=intent_key, slots=slots)`. Ops uses canonical SQL when voyage_ids present, or LLM SQL when needed; guard blocks finance columns in ops SQL. Result written to state["ops"]; trace appended.

**Mongo steps**:

- resolveAnchor: resolve voyage/vessel anchors for downstream.
- fetchRemarks: voyage_ids from artifacts; fetch remarks (and optional minimal context) per voyage; store in artifacts (e.g. remarks_by_voyage_id, cargo_by_voyage_id, ports_by_voyage_id). Trace appended.

**LLM merge step** (agent=="llm", op=="merge"):

- Builds **merged_rows**: from finance_rows + ops_by_vid + remarks/cargo/ports by voyage_id. For each finance row with voyage_id, build one merged row with voyage_id, voyage_number, pnl, revenue, total_expense, tce, total_commission, key_ports, cargo_grades, remarks, and intent-specific fields (e.g. scenario comparison: pnl_actual, pnl_when_fixed, pnl_variance, tce_*; offhire_ranking: offhire_days, delay_reason; port_calls when key_ports list). Dedupe by voyage_id. Store in artifacts["merged_rows"], artifacts["coverage"]. Trace appended.
- Then increment `state["step_index"]` and return state.

**Conditional**: `r_has_more_steps`: if step_index < len(steps) return "more", else "done".

---

## 16. Node 7: merge (n_merge)

- **Method**: `n_merge(state)`. Ensures state["data"] has finance, ops, mongo, artifacts from state (copy from state.finance/ops/mongo/artifacts if present). Sets `state["merged"]` = { mongo, finance, ops, artifacts, plan, dynamic_sql_used, dynamic_sql_agents }. dynamic_sql_used = True if finance or ops mode is dynamic_sql. Does **not** recompute merged_rows; that was done inside execute_step (llm.merge step).

---

## 17. Node 8: summarize (n_summarize)

- **Method**: `n_summarize(state)`. merged_full = state["merged"]. Optional: for ranking.*, ensure merged_rows have top-level pnl/revenue/total_expense from finance rows. **compact_payload(merged)** (response_merger.py): light merged_rows (flattened fields), cap finance/ops/mongo rows to 50, cap merged_rows to 50. **_sanitize_for_llm**: truncate large payloads, cap nested lists, to avoid rate limits; token estimate trace. **llm.summarize_answer(question=state["user_input"], plan=state["plan"], merged=sanitized_merged)** → LLM returns markdown answer. If summarization fails but has_data, fallback message; else "No data available". Set state["answer"] = answer. Persist compact result-set to session (e.g. last result set for follow-ups). redis.save_session(session_id, session_patch). Append token_usage trace. Return state.

---

## 18. Agents (Detail)

### 18.1 FinanceAgent (`app/agents/finance_agent.py`)

- **run(intent_key, slots, ...)**: _map_intent(intent_key, slots) → (query_key, params). query_key is a key in SQL_REGISTRY (e.g. kpi.voyage_by_reference, finance.compare_scenarios, finance.rank_voyages_safe). pg.fetch_all(query_key, params) → rows. Returns dict with mode="registry_sql", rows, intent_key, query_key, params.
- **run_dynamic(question, intent_key, slots)**: SQLGenerator.generate(question, agent="finance", slots=slots, intent_key=intent_key) → SQLGenOutput(sql, params, tables, confidence). Optional repair for ranking.voyages (no ops tables, required columns). validate_and_prepare_sql(sql, params, allowlist, enforce_limit=True) → guard; if not ok and ranking.voyages, one repair attempt. pg.execute_dynamic_select(guard.sql, guard.params) → rows. Post-check for ranking.voyages required columns; optional second repair. Returns FinanceAgentResult(mode="dynamic_sql", sql=guard.sql, rows=rows, ...).

### 18.2 OpsAgent (`app/agents/ops_agent.py`)

- **run**: Map intent to registry query_key; pg.fetch_all.
- **run_dynamic**: If voyage_ids → canonical SELECT from ops_voyage_summary WHERE voyage_id = ANY(%(voyage_ids)s). Else cargo_profitability + cargo_grades → canonical CTE. Else voyage_number → resolve voyage_id, recurse. Else vessel.summary → deterministic vessel SQL. Else LLM SQL via SQLGenerator; block if SQL contains finance columns; guard and execute.

### 18.3 MongoAgent (`app/agents/mongo_agent.py`)

- **run(intent_key, slots, projection, session_context)**: Entity resolution or skip; fetch document; return MongoAgentResponse.
- **fetch_full_voyage_context(voyage_number, voyage_id)**: get_voyage_by_number or fetch_voyage with projection (remarks, fixtures, legs, revenues, expenses).
- **run_llm_find(question, slots)**: mongo_schema_hint(); MongoQueryBuilder.build(); validate_mongo_spec; mongo.find_many(); returns { mode: mongo_llm, ok, collection, filter, projection, limit, rows }.

---

## 19. Adapters

- **PostgresAdapter** (`app/adapters/postgres_adapter.py`): fetch_all(query_key, params) looks up SQL_REGISTRY[query_key], runs SQL with params (RealDictCursor). execute_dynamic_select(sql, params): SELECT/WITH only, :param → %(param)s, LIMIT enforced, params filtered to those in SQL; MAX_ROWS cap.
- **MongoAdapter**: db[vessels], db[voyages]; get_voyage_by_number, fetch_voyage, get_vessel_imo_by_name, get_voyage_id_by_number; find_many(collection, filter, projection, sort, limit).
- **RedisStore**: load_session(session_id) → dict; save_session(session_id, patch); in-memory fallback if Redis unavailable.

---

## 20. SQL Layer

- **sql_allowlist.py**: DEFAULT_ALLOWLIST (allowed_tables: finance_voyage_kpi, ops_voyage_summary; allowed_columns per table; forbidden_patterns).
- **sql_guard.py**: validate_and_prepare_sql(sql, params, allowlist, enforce_limit): apply simple fixes, sanitize params (list for ANY), extract tables, allowlist check, LIMIT enforcement, filter params to those referenced in SQL.
- **sql_generator.py**: _schema_hint_for_agent(agent, allowlist) → schema hint (tables, columns, join_hints, param_conventions, constraints). generate(question, agent, slots, intent_key): intent_rules for ranking.voyages (finance-only); system_prompt with HARD RULES (Postgres, CTE/JOIN qualify columns, LIMIT, etc.); llm.generate_sql(...) → sql, params, tables, confidence; ensure limit in params if %(limit)s in sql; return SQLGenOutput.

---

## 21. Registries

- **intent_registry.py**: SUPPORTED_INTENTS list; INTENT_REGISTRY per intent (description, required_slots, optional_slots, needs, mongo_intent, mongo_projection); INTENT_ALIASES (e.g. ops.delayed_voyages → ops.offhire_ranking); resolve_intent(key) returns canonical key.
- **sql_registry.py**: SQL_REGISTRY[key] = QuerySpec(description, required_params, sql). Keys include kpi.voyage_by_reference, kpi.voyages_by_flexible_filters, finance.rank_voyages_safe, finance.compare_scenarios, kpi.offhire_ranking, kpi.delayed_voyages_analysis, kpi.cargo_profitability_analysis, finance.high_revenue_low_pnl, kpi.vessel_performance_summary, etc.

---

## 22. Response Merger

- **compact_payload(merged)** in `app/services/response_merger.py`: Builds a payload for the summarizer. finance/ops/mongo: mode + rows[:50]. artifacts: merged_rows[:50] with _light_merged_row (voyage_id, voyage_number, pnl, revenue, total_expense, tce, total_commission, key_ports, cargo_grades, remarks; vessel-level fields if present). Optional coverage. Returns dict with finance, ops, mongo, artifacts.

---

## 23. End-to-End Query Execution Sequence

1. **Request**: POST /query { query, session_id? }.
2. **main.py**: router.handle(session_id, user_input).
3. **handle**: graph.invoke({ session_id, user_input, raw_user_input }).
4. **load_session**: session_ctx = redis.load_session(session_id).
5. **extract**: intent_key, slots = LLM + regex + overrides; scenario detection; append trace.
6. **validate**: missing_keys = required slots not in slots; append trace.
7. **r_after_validate**: if missing_keys → "clarify"; elif intent not in registry → "summarize"; else → "plan".
8. **build_plan**: plan = planner.build_plan(...); plan_type single | composite; steps = []; state.plan, state.step_index = 0.
9. **r_plan_path**: composite → execute_step; single → run_single.

**Composite branch:**

10. **execute_step** (repeated): step = steps[step_index]. If finance: run_dynamic (or run if registry_sql); write state.finance, artifacts.voyage_ids, artifacts.finance_rows. If ops: run_dynamic with voyage_ids; write state.ops. If mongo resolveAnchor/fetchRemarks: write artifacts.remarks_by_voyage_id, etc. If llm merge: build merged_rows from finance_rows + ops + mongo; write artifacts.merged_rows. step_index += 1. Append trace.
11. **r_has_more_steps**: if step_index < len(steps) → execute_step again; else → merge.
12. **merge**: state.merged = { mongo, finance, ops, artifacts, plan, dynamic_sql_used, dynamic_sql_agents }.

**Single branch (alternative to 10–12):**

10'. **run_single**: Dispatch by intent; run finance/ops/mongo via registry; set state.answer or state.finance/ops/mongo/data.
11'. **r_after_run_single**: if answer or clarification → END; else → merge.
12'. **merge**: same as 12.

**Common:**

13. **summarize**: compact_payload(merged); sanitize; llm.summarize_answer(question, plan, merged) → answer; state.answer = answer; save_session; append trace.
14. **END**.
15. **handle**: Read state; if clarification return { clarification, ... }; else return { answer, data=merged, dynamic_sql_used, dynamic_sql_agents, plan, trace }.
16. **main.py**: QueryResponse(answer=..., trace=..., ...).

---

## 24. Configuration (Environment)

- **Backend**: GROQ_API_KEY (required), GROQ_MODEL, GROQ_TEMPERATURE; POSTGRES_DSN or POSTGRES_HOST/PORT/USER/PASSWORD/DB; MONGO_URI, MONGO_DB_NAME; REDIS_HOST, REDIS_PORT, REDIS_DB (or REDIS_DISABLED); KAI_DEBUG.
- **Adapters**: Postgres connect_timeout; Mongo serverSelectionTimeoutMS, connectTimeoutMS, socketTimeoutMS; Redis socket timeouts.

This is the full low-level design for how a query executes through the system.
