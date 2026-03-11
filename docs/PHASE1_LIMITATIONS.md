# Phase-1 limitations

Consolidated **logic/design limitations** of the current system, derived from phase-1 tested query responses and execution steps. Only limitations are listed (not execution bugs, not minor issues). Use for Phase-2 fixes.

---

## L1. Multiple financial values per metric without labels

Revenue, Total Expense, and PnL can appear as **two numbers** (e.g. "X and Y") with no indication of scenario (e.g. ACTUAL vs BUDGET). The user cannot tell which value is which.

**Phase-2 fix:** When multiple finance rows/scenarios exist, label each (e.g. "ACTUAL: … / BUDGET: …") in the summariser or merge output. Or return one scenario by default and mention the other in a footnote.

---

## L2. Fetched data not surfaced in composite answers

When all composite steps succeed (finance, ops, mongo fetch_remarks, merge), the final answer can still state that **financial metrics are not available** even though the finance step returned PnL/revenue/expense. The results table may show **Key ports** but **Cargo grades** as "None" and **Remarks** as "Not available" for every row, despite ops returning grades and mongo returning remarks. Merge or summariser is not consistently passing through and surfacing finance, cargo grade, and remarks in the final answer.

**Phase-2 fix:** Ensure merged payload includes pnl, revenue, total_expense, grades (from ops or mongo), and remarks (from mongo). Adjust summariser prompt or compact_payload so it uses these fields when present. Do not state "financial metrics not available" when finance step succeeded and returned data.

---

## L3. Trace "Inputs" show plan placeholders, not resolved values

For composite steps, the trace **Inputs** show the plan’s placeholders (e.g. `"voyage_ids": "$finance.voyage_ids"`), not the runtime-resolved values. Resolved values are emitted in a separate trace event. This can confuse diagnosis when debugging.

**Phase-2 fix:** Surface resolved inputs in the UI (e.g. "Resolved: voyage_ids=6, cargo_grades=0") alongside or instead of the placeholder in the step Inputs.

---

## L4. Summary can contradict the results table

The summary may state a "top" result or "highest" value that does not match the results table. For example: summary says "Top result: Voyage 2303 with PnL $1,494,334.80" while the table shows another voyage with higher PnL; or summary says "highest offhire 115.28 days for voyage 2201" while the table shows a different offhire value for that voyage. This can happen when multiple rows/scenarios exist or when the summariser does not derive "top"/"highest" from the same sorted data as the table.

**Phase-2 fix:** Ensure summariser derives "top", "highest", "lowest" from the same merged/sorted data as the results table. Optionally sort merged rows by the requested metric before summarisation and pass that order explicitly.

---

## L5. "Most/least" with driving remarks not explicitly structured

When the user asks for the most and least profitable voyage (or similar extremes) and for remarks explaining what drove the result, the answer may show a table with remarks but not explicitly call out "Most profitable: Voyage X — [driving remarks]" and "Least profitable: Voyage Y — [driving remarks]." The user has to infer from the table.

**Phase-2 fix:** When the question asks for most/least (or best/worst) with explanation or remarks, add a summariser instruction to output a dedicated short subsection (e.g. "Most profitable" / "Least profitable") with the chosen voyage and the relevant driving remarks.

---

## L6. Scenario comparison: when-fixed / variance "Not available" for some voyages

When comparing **ACTUAL vs WHEN_FIXED** for specific voyage numbers (e.g. 1901, 1902, 2301), the results table can show **multiple rows per voyage number** (one per `voyage_id`). For voyage_ids that have only ACTUAL data in `finance_voyage_kpi` (no WHEN_FIXED row), the columns **PnL When Fixed**, **TCE When Fixed**, and **PnL/TCE Variance** show as "Not available." This is a **data availability** behaviour: the registry query correctly returns one row per voyage_id with MAX(ACTUAL) and MAX(WHEN_FIXED); when WHEN_FIXED is missing in the source, those fields are null. The user sees a mix of full comparison rows and rows with only actuals.

**Phase-2 fix:** Either (a) filter to only voyage_ids that have both scenarios and mention in the summary when some voyage numbers were omitted for missing when-fixed data, or (b) clearly label in the table/UI when a row is "ACTUAL only" so the user understands why variance is missing.

---

## Quick reference: Phase-2 fixes

| # | Limitation | Phase-2 fix |
|---|------------|-------------|
| L1 | Multiple finance values unlabeled | Label scenarios (ACTUAL/BUDGET) or default to one and footnote the other. |
| L2 | Fetched data not surfaced in composite answers | Include pnl, revenue, expense, grades, remarks in merged payload; summariser must use them; do not say "financial metrics not available" when finance data exists. |
| L3 | Trace Inputs show placeholders | Show resolved inputs (e.g. voyage_ids count) in UI. |
| L4 | Summary contradicts results table | Derive "top"/"highest" from same sorted data as table; align sort before summarisation. |
| L5 | Most/least + driving remarks not explicit | When user asks for most/least with explanation, add subsection "Most profitable" / "Least profitable" with voyage and driving remarks. |
| L6 | Scenario comparison: when-fixed/variance "Not available" for some rows | Filter to voyage_ids with both scenarios and note omissions, or label "ACTUAL only" rows so user knows why variance is missing. |

---

## References

- **Flow and orchestration:** `docs/PROJECT_NOTES.md` (§2–4, §4.1)
- **Architecture:** `docs/architecture.md`
