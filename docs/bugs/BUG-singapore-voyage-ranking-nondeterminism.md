# BUG: Singapore voyage ranking — non-deterministic candidate selection

**Query:** `rank voyages that visited Singapore by PnL`

## Symptom

- Same query, same intent classification, same source routing.
- Different result sets across runs (high-PnL vs low-PnL voyages returned).
- Indicates SQL generation is producing different `ORDER BY` / `WHERE` conditions on different LLM calls for identical input.

## Root cause hypothesis

- Dynamic SQL generator receives same intent slots but produces different SQL depending on LLM sampling or prompt interpretation.
- Possible: `ORDER BY` direction flips (`DESC` vs `ASC`) across runs.
- Possible: filter on port name uses different case/spelling or join scope.

## Not caused by Thing 2 cleanup

Confirmed via A/B revert test: instability reproduces on reverted code and on post-cleanup code with identical intent and trace-level routing.

## Scope

- **Assigned to:** Thing 1 fix scope (`sql_generator` prompt stability / validation of generated SQL for ranking intents).
- **Priority:** Medium — affects ranking queries only; underlying data may be correct but selection/order is unstable.
