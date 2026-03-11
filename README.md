# KAI-Agent (FastAPI + Streamlit)

KAI-Agent is a multi-agent maritime analytics assistant that answers natural-language questions about voyages, vessels, ports, financial KPIs, delays, and scenario comparisons.

It combines:
- FastAPI backend (`/query`)
- Streamlit chat UI
- PostgreSQL + MongoDB + Redis
- LLM-assisted intent extraction, SQL generation, and answer summarization
- Guardrails (allowlists, validators, deterministic routing rules)

## What This System Does

- Converts a user query into `intent + slots`
- Builds an execution plan (single-step or composite multi-step)
- Executes data retrieval through specialized agents:
  - `FinanceAgent` (Postgres finance KPIs)
  - `OpsAgent` (Postgres operational data)
  - `MongoAgent` (voyage/vessel metadata, remarks, fixtures)
- Merges outputs and generates a final answer with execution trace

## High-Level Architecture

```
User (Streamlit / API client)
        |
        v
FastAPI /query  --> GraphRouter (LangGraph orchestration)
        |              |
        |              +--> Intent + Slots (LLM + deterministic rules)
        |              +--> Planner (single/composite)
        |              +--> FinanceAgent (Postgres)
        |              +--> OpsAgent (Postgres)
        |              +--> MongoAgent (MongoDB)
        |              +--> Merge + Summarize (LLM)
        |
        v
Response: answer + trace + plan + metadata
```

## Core Runtime Flow

1. Load session context from Redis.
2. Extract intent and slots from the query.
3. Validate required slots (clarify if missing).
4. Build execution plan:
   - `single` for direct entity queries
   - `composite` for multi-step analytics/ranking/comparison asks
5. Run agents per plan steps.
6. Merge data into normalized artifacts.
7. Summarize answer and return trace/debug metadata.

## Main Components

- `app/main.py`  
  FastAPI entrypoint and `/query` endpoint.

- `app/orchestration/graph_router.py`  
  LangGraph state machine, routing, step execution, merge, summarize.

- `app/orchestration/planner.py`  
  Builds deterministic execution plans (`single` vs `composite`).

- `app/agents/finance_agent.py`  
  Registry SQL + dynamic SQL generation/repair for finance flows.

- `app/agents/ops_agent.py`  
  Registry SQL + dynamic SQL execution for operational queries.

- `app/agents/mongo_agent.py`  
  Mongo anchor resolution, context fetch, metadata retrieval.

- `app/sql/sql_generator.py` + `app/sql/sql_guard.py` + `app/sql/sql_allowlist.py`  
  SQL generation and safety enforcement.

- `app/llm/llm_client.py`  
  Intent extraction, SQL generation calls, and final summarization.

## Why This Is Production-Oriented (Not Hardcoded Query Logic)

- Query results are data-driven from Postgres/Mongo, not hardcoded answers.
- Determinism is used for:
  - Intent/routing guardrails
  - Slot normalization
  - SQL shape and validation constraints
- Dynamic SQL is validated and repaired with guardrails before execution.
- Execution trace is returned for observability and debugging.

## Setup

### 1) Install dependencies

```bash
python -m pip install -r requirements.txt
```

### 2) Configure environment

Set environment variables (or `.env`) for:

- LLM:
  - `GROQ_API_KEY` (required)
  - `GROQ_MODEL` (optional)
- Postgres:
  - `POSTGRES_DSN` (preferred) or host/user/password/db vars
- Mongo:
  - `MONGO_URI`
  - `MONGO_DB_NAME` (if required by your environment)
- Redis:
  - `REDIS_HOST`
  - `REDIS_PORT`
  - `REDIS_DB`
- API/UI:
  - `KAI_API_URL` (used by Streamlit; default `http://127.0.0.1:8000/query`)
  - `KAI_DEBUG=1` (optional verbose/debug mode)

## Run

### Start FastAPI backend

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Start Streamlit UI

From repo root:

```bash
streamlit run app/UI/UX/streamlit_app.py
```

## API Contract

### Request

`POST /query`

```json
{
  "query": "Compare actual vs when-fixed for voyages 1901, 1902, 2301",
  "session_id": "optional-session-id"
}
```

### Response (shape)

- `answer` (final markdown response)
- `clarification` (if slot is missing)
- `intent_key`, `slots`
- `trace` (step-by-step execution details)
- `dynamic_sql_used`, `dynamic_sql_agents`
- `plan` and merged data artifacts

## Common Query Types

- Voyage summary:
  - "For voyage 2306, show financial summary and remarks."
- Vessel metadata:
  - "What is hire rate of vessel Elka Delphi?"
  - "Show default speed and fuel consumption by passage type for vessel X."
- Scenario comparison:
  - "Compare actual vs when-fixed for voyages 1901, 1902, 2301 and show variance in PnL and TCE."
- Ranking/trend:
  - "Top 10 voyages by PnL."
  - "Voyage profitability over time for vessel Stena Conquest."

## Repository Notes

- Current low-level and architecture context:
  - `docs/LLD_QUERY_EXECUTION.md`
  - `docs/END_TO_END_SYSTEM_CONTEXT.md`
- Current known limitations and caveats:
  - `docs/PHASE1_LIMITATIONS.md`
- Presenter/demo support:
  - `docs/PROJECT_NOTES.md`

## Operational Guidance

- Keep runtime changes in `app/**` focused and minimal.
- Avoid committing local audit artifacts or temporary scripts unless intentional.
- Prefer trace-backed debugging over ad-hoc print debugging.
- Validate key flows after changes:
  - `voyage.summary`
  - `vessel.metadata`
  - `analysis.scenario_comparison`


