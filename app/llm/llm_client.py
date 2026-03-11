# app/llm/llm_client.py

"""
LLM Client v7 — Hardened + Deterministic Routing
Feb 18, 2026

Major Fixes:
- Deterministic intent routing BEFORE LLM call
- Removed voyage_ids extraction completely
- Strong regex for vessel_name
- Deterministic loss-making mapping
- Strong slot sanitization
- Safer JSON parsing
- No hallucinated fallback text
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time
from decimal import Decimal
from typing import Any, Dict, List, Optional

from groq import Groq


# =========================================================
# CONFIG
# =========================================================

@dataclass
class LLMConfig:
    api_key: str
    model: str = "openai/oss-gpt-120b"
    temperature: float = 0.0


# =========================================================
# CLIENT
# =========================================================

class LLMClient:

    def __init__(self, config: LLMConfig):
        self.config = config
        self.client = Groq(api_key=config.api_key)

    # =========================================================
    # Deterministic intent router (before LLM)
    # =========================================================

    def _deterministic_intent(self, text: str) -> Optional[str]:
        t = text.lower()

        # FIRST: Delayed voyages with negative PnL / loss / root cause → finance.loss_due_to_delay (avoid mis-classification as port_query)
        if "delayed" in t and ("negative" in t and "pnl" in t or "negative pn" in t or "loss" in t or "root cause" in t):
            return "finance.loss_due_to_delay"

        # 0) "Tell me about voyage 1901" / "voyage 1901 summary" → voyage summary
        if "voyage" in t and any(k in t for k in ("tell me about", "details about", "information about", "summary", "summarize")):
            if re.search(r"\bvoyage\s+\d{3,5}\b", t):
                return "voyage.summary"

        # 0) "Tell me about vessel ..." → vessel summary
        if ("vessel" in t or "ship" in t) and (
            "tell me about" in t
            or "details about" in t
            or "information about" in t
            or "summary" in t
        ):
            return "vessel.summary"

        # 0) Commission ranking
        if ("commission" in t) and ("top" in t) and ("voyage" in t):
            return "ranking.voyages_by_commission"

        # 0b) Vessel screening: high voyage count + above-average profitability
        if ("high voyage count" in t or "many voyages" in t) and ("above-average" in t or "above average" in t) and ("profit" in t or "pnl" in t or "profitability" in t):
            return "ranking.vessels"

        # 1) Top + profitable/pnl (must beat "visited" in "key ports visited")
        if "top" in t and ("most profitable" in t or "profit" in t or "pnl" in t):
            return "ranking.voyages"

        # 2) Scenario comparison (before any voyage-number escape)
        if "when-fixed" in t or "when fixed" in t:
            return "analysis.scenario_comparison"

        # 3) Port calls + profitability/compare → composite
        if ("port call" in t or "port calls" in t) and ("profit" in t or "compare" in t or "most" in t):
            return "composite.query"

        # 4) Offhire + financial impact
        if "offhire" in t:
            return "ops.delayed_voyages"

        # 5) Loss-making
        if "loss-making" in t or "loss making" in t:
            return "analysis.segment_performance"

        # 6) Cargo profitability
        if "cargo" in t and "profit" in t:
            return "analysis.cargo_profitability"

        # 6b) High revenue but low/negative PnL
        if "high revenue" in t and ("low pnl" in t or "negative pnl" in t or ("low" in t and "pnl" in t)):
            return "analysis.high_revenue_low_pnl"

        # 6c) Module type: average PnL, most common cargo grades/ports
        if "module type" in t and ("average pnl" in t or "most common" in t or "cargo grades" in t or "ports" in t or "tc voyage" in t or "spot" in t):
            return "analysis.by_module_type"

        # 7) Top voyages (generic)
        if "top" in t and "voyage" in t:
            return "ranking.voyages"

        return None

    # =========================================================
    # Extract intent and slots
    # =========================================================

    def extract_intent_slots(
        self,
        *,
        text: str,
        supported_intents: List[str],
        schema_hint: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:

        # Normalize common apostrophe variants (incl. mojibake) to improve regex reliability.
        text_norm = (text or "")
        text_norm = (
            text_norm.replace("’", "'")
            .replace("‘", "'")
            .replace("â€™", "'")
            .replace("â€˜", "'")
        )

        # 1️⃣ Deterministic override
        deterministic = self._deterministic_intent(text_norm)

        # 2️⃣ Regex slot extraction (always)
        slots: Dict[str, Any] = {}

        # Voyage numbers
        voyages = re.findall(r"\b\d{3,4}\b", text_norm)
        if voyages:
            slots["voyage_numbers"] = [int(v) for v in voyages]

        # IMO extraction (e.g. "IMO 9667485", "vessel IMO: 9667485")
        imo_match = re.search(r"\b(?:vessel\s+)?imo(?:\s*[:#-]?\s*|\s+)(\d{7,10})\b", text_norm, re.IGNORECASE)
        if imo_match:
            slots["imo"] = imo_match.group(1).strip()

        # Vessel extraction (safer)
        #
        # Common failure modes we must avoid:
        # - "How has vessel Stena Superior been performing recently?"  -> capture "Stena Superior" (NOT trailing words)
        # - "Stena Superior’s last 3 voyages"                          -> capture "Stena Superior" (handle ’s / 's)
        #
        # Use a bounded, lookahead-terminated match after the word "vessel"/"ship".
        vessel_match = re.search(
            r"(?:vessel|ship)\s+"
            r"("
            r"[A-Za-z0-9][A-Za-z0-9\- ]{2,60}?"
            r")"
            r"(?="
            r"(?:\s+been\b|\s+is\b|\s+has\b|\s+doing\b|\s+performing\b|\s+recently\b|\s+last\b|\s+summary\b|\s+overview\b)"
            r"|[?.!,;:]|$"
            r")",
            text,
            re.IGNORECASE,
        )

        if vessel_match:
            cand = vessel_match.group(1).strip()
            cand = re.sub(r"(?:’s|'s)\s*$", "", cand).strip()
            slots["vessel_name"] = cand
        else:
            # Beginner phrasing often omits the word "vessel", e.g.
            # "How has Stena Superior been performing recently?"
            # "Give me a quick overview of Stena Superior: last/best/worst voyage"
            phr_patterns = [
                r"(?:how\s+has|how\s+is)\s+([A-Za-z0-9][A-Za-z0-9\- ]{2,60}?)\s+been\b",
                # Possessive form: "Stena Superior’s last 3 voyages" / "Stena Superior's last 3 voyages"
                r"\bof\s+([A-Za-z0-9][A-Za-z0-9\- ]{2,60}?)(?:’s|'s)\b",
                # "Is Stena Superior doing well/poorly ..."
                r"\bis\s+(?:vessel\s+)?([A-Za-z0-9][A-Za-z0-9\- ]{2,60}?)(?=\s+(?:doing|performing|good|bad)\b)",
                r"(?:quick\s+overview\s+of|overview\s+of)\s+([A-Za-z0-9][A-Za-z0-9\- ]{2,60}?)(?:[:?]|$)",
                r"(?:tell\s+me\s+about|give\s+me\s+details\s+about)\s+([A-Za-z0-9][A-Za-z0-9\- ]{2,60}?)(?:[?.!]|$)",
            ]
            for pat in phr_patterns:
                m = re.search(pat, text_norm, re.IGNORECASE)
                if m:
                    cand = m.group(1).strip().strip("\"'“”")
                    cand = re.sub(r"(?:’s|'s)\s*$", "", cand).strip()
                    if 2 <= len(cand) <= 60:
                        slots["vessel_name"] = cand
                        break

            # Extra heuristic: many vessel names in this dataset start with "Stena <Name>".
            if "vessel_name" not in slots:
                m = re.search(r"\b(stena\s+[A-Za-z0-9][A-Za-z0-9\-]*(?:\s+[A-Za-z0-9][A-Za-z0-9\-]*){0,2})\b", text_norm, re.IGNORECASE)
                if m:
                    slots["vessel_name"] = m.group(1).strip()

        # Limit (top N, or "N voyages")
        limit_match = re.search(r"top\s+(\d+)", text_norm.lower())
        if limit_match:
            slots["limit"] = int(limit_match.group(1))
        if "limit" not in slots:
            n_voyages = re.search(r"(\d+)\s+voyages", text_norm.lower())
            if n_voyages:
                slots["limit"] = min(int(n_voyages.group(1)), 50)

        # Port name from "visited X" / "called at X"
        port_visited = re.search(
            r"(?:visited|called at)\s+([A-Za-z0-9\-]+(?:\s+[A-Za-z0-9\-]+)*)",
            text_norm,
            re.IGNORECASE,
        )
        if port_visited:
            slots["port_name"] = port_visited.group(1).strip()

        # If user asked about a specific port ("visited Singapore" etc.), use ops.port_query when we have port_name
        if not deterministic and slots.get("port_name"):
            if "visited" in text_norm.lower() or "called at" in text_norm.lower():
                deterministic = "ops.port_query"

        # Commission rankings
        if not deterministic:
            tl = text_norm.lower()
            if ("commission" in tl) and ("top" in tl) and ("voyage" in tl):
                deterministic = "ranking.voyages_by_commission"

        # Loss-making / what went wrong → segment_performance composite (needs breakdown + remarks)
        if not deterministic:
            tl = text_norm.lower()
            if ("loss-making" in tl) or ("loss making" in tl) or ("went wrong" in tl):
                deterministic = "analysis.segment_performance"

        tl = text_norm.lower()
        metadata_keywords = (
            "passage type",
            "passage types",
            "consumption profile",
            "consumption profiles",
            "consumption",
            "default consumption",
            "speed",
            "ifo",
            "mgo",
            "ballast",
            "laden",
            "non passage",
            "non-passage",
            "idle",
            "load",
            "discharge",
            "heat",
            "clean",
            "inert",
            "hire rate",
            "hirerate",
            "hire_rate",
            "hire-rate",
            "scrubber",
            "market type",
            "contract history",
            "contract",
            "tags",
            "account code",
            "is vessel operating",
            "operating status",
            "operational",
            "is operating",
            "owner",
            "duration",
            "cp date",
            "delivery",
            "extracted at",
        )

        # Metadata-first routing for vessel-anchored or small voyage-number anchored questions.
        if not deterministic and any(k in tl for k in metadata_keywords):
            has_vessel_anchor = bool(slots.get("vessel_name") or slots.get("imo"))
            vnums = slots.get("voyage_numbers")
            has_small_voyage_anchor = isinstance(vnums, list) and 1 <= len(vnums) <= 3
            if has_vessel_anchor or has_small_voyage_anchor:
                deterministic = "vessel.metadata"

        # If user asked about a specific vessel and it's not metadata, route to vessel.summary.
        if not deterministic and slots.get("vessel_name"):
            if any(
                k in tl
                for k in (
                    "profitability",
                    "pnl",
                    "tce",
                    "over time",
                    "trend",
                    "performing",
                    "doing well",
                    "poorly",
                    "overall",
                    "best",
                    "worst",
                    "recent",
                    "recently",
                    "last",
                    "captain",
                    "brief",
                    "route pattern",
                    "cargo pattern",
                    "remarks",
                )
            ) or ("voyage" in tl or "voyages" in tl):
                deterministic = "vessel.summary"

        # 3️⃣ If deterministic intent found → skip LLM
        if deterministic:
            return {
                "intent_key": deterministic,
                "slots": self._sanitize_slots(slots),
            }

        # 4️⃣ Otherwise call LLM
        intents_formatted = "\n".join([f"- {i}" for i in supported_intents])

        system = f"""
You are a maritime finance intent classifier.
Return ONLY valid JSON.

SUPPORTED INTENTS:
{intents_formatted}

Rules:
- Extract voyage_numbers (int list)
- Extract vessel_name
- Extract limit
- NEVER invent fields
- NEVER output voyage_ids
"""

        result = self._call_with_retry(
            system=system,
            user=json.dumps({"query": text_norm}),
            operation="intent_extraction",
        )

        if not result or not isinstance(result, dict):
            return {"intent_key": "out_of_scope", "slots": slots}

        intent = result.get("intent_key", "out_of_scope")
        llm_slots = result.get("slots", {}) or {}

        # Merge regex + llm slots (regex wins)
        llm_slots.update({k: v for k, v in slots.items()})
        clean_slots = self._sanitize_slots(llm_slots)

        # 4b) Post-LLM correction: ops.port_query with "negative PnL" (or similar) as port_name is wrong
        if intent == "ops.port_query" and clean_slots.get("port_name"):
            pn = str(clean_slots.get("port_name", "")).strip().lower()
            if "pnl" in pn or "negative" in pn or pn in ("revenue", "expense", "tce"):
                intent = "finance.loss_due_to_delay"
                clean_slots = {k: v for k, v in clean_slots.items() if k != "port_name"}

        # 5️⃣ Recovery for common "false out_of_scope" cases.
        # If we have strong entity slots, do not allow out_of_scope to block a valid answer.
        if intent == "out_of_scope":
            if clean_slots.get("vessel_name"):
                ql = text_norm.lower()
                intent = "vessel.metadata" if any(k in ql for k in metadata_keywords) else "vessel.summary"
            elif clean_slots.get("voyage_numbers"):
                ql = text_norm.lower()
                intent = "vessel.metadata" if any(k in ql for k in metadata_keywords) else "voyage.summary"

        return {
            "intent_key": intent,
            "slots": clean_slots,
        }

    # =========================================================
    # Slot sanitization
    # =========================================================

    def _sanitize_slots(self, slots: Dict[str, Any]) -> Dict[str, Any]:

        clean: Dict[str, Any] = {}

        # voyage_numbers
        if "voyage_numbers" in slots:
            try:
                vns = slots["voyage_numbers"]
                if not isinstance(vns, list):
                    vns = [vns]
                clean["voyage_numbers"] = [
                    int(float(v)) for v in vns if str(v).isdigit()
                ]
            except Exception:
                pass

        # limit
        if "limit" in slots:
            try:
                limit = int(float(slots["limit"]))
                clean["limit"] = max(1, min(limit, 50))
            except Exception:
                pass

        # vessel_name
        if "vessel_name" in slots:
            name = str(slots["vessel_name"]).strip()
            # Trim trailing query phrases accidentally captured as part of vessel name.
            name = re.sub(
                r"\b(?:operating status|operational status|operating|status|passage type|passage types|hire rate|hirerate|account code|market type|scrubber|tags|contract history)\b.*$",
                "",
                name,
                flags=re.IGNORECASE,
            ).strip()
            name = re.sub(r"\s{2,}", " ", name).strip()
            if 2 <= len(name) <= 60:
                clean["vessel_name"] = name

        # imo
        if "imo" in slots:
            imo = str(slots["imo"]).strip()
            if imo.isdigit() and 7 <= len(imo) <= 10:
                clean["imo"] = imo

        # port_name (for ops.port_query) — reject values that are clearly not port names (e.g. "negative PnL")
        if "port_name" in slots:
            name = str(slots["port_name"]).strip()
            name_lower = name.lower()
            # Do not treat PnL/finance phrases as port names
            if name_lower in ("negative pnl", "negative pn", "pnl", "revenue", "expense", "tce"):
                pass  # drop port_name
            elif "pnl" in name_lower or "revenue" in name_lower or "expense" in name_lower:
                pass  # drop
            elif 1 <= len(name) <= 80:
                clean["port_name"] = name

        return clean

    # =========================================================
    # SQL generation (safe wrapper)
    # =========================================================

    def generate_sql(
        self,
        *,
        question: str,
        intent_key: str,
        slots: Dict[str, Any],
        schema_hint: Dict[str, Any],
        agent: str,
        system_prompt: Optional[str] = None,
    ) -> Dict[str, Any]:

        system = system_prompt or "Return SQL JSON only."

        result = self._call_with_retry(
            system=system,
            user=json.dumps({
                "question": question,
                "intent": intent_key,
                "slots": slots,
                "schema_hint": schema_hint,
                "agent": agent,
            }),
            operation=f"sql_generation_{agent}",
        )

        if not result or "sql" not in result:
            return {
                "sql": "SELECT 1 WHERE 1=0 LIMIT 1",
                "params": {},
                "tables": [],
                "confidence": 0.0,
            }

        result.setdefault("params", {})
        result.setdefault("tables", [])
        result.setdefault("confidence", 0.9)

        result["sql"] = result["sql"].strip().rstrip(";")

        return result

    # =========================================================
    # Answer generation
    # =========================================================

    def generate_final_answer(
        self,
        *,
        question: str,
        merged_data: Dict[str, Any],
    ) -> str:
        """Alias function designed specifically for the voyage.summary override logic."""
        return self.summarize_answer(
            question=question,
            plan={"plan_type": "single", "intent_key": "voyage.summary"},
            merged=merged_data
        )

    def summarize_answer(
        self,
        *,
        question: str,
        plan: Dict[str, Any],
        merged: Dict[str, Any],
    ) -> str:

        intent_key = ""
        if isinstance(plan, dict):
            intent_key = str(plan.get("intent_key") or "").strip()

        # Graceful handling for out-of-scope / chit-chat queries.
        # Avoid emitting empty finance/ops tables that look like hallucination.
        if intent_key == "out_of_scope":
            q = (question or "").strip()
            q_lower = q.lower()

            # Friendly greeting / onboarding (no DB needed).
            greeting_exact = {
                "hi", "hello", "hey", "hiya", "yo",
                "good morning", "good afternoon", "good evening",
                "help", "start",
            }
            if q_lower in greeting_exact or any(q_lower.startswith(p) for p in ("hi ", "hello ", "hey ")):
                return (
                    "### Hello\n"
                    "- I’m **Digital Sales Agent**, your maritime finance + operations analytics assistant.\n"
                    "- I can help you analyze **voyages, vessels, ports, cargo grades, delays/offhire, remarks**, and related **financial KPIs** (PnL, revenue, expense, TCE, commissions).\n\n"
                    "### Try asking\n"
                    "- \"For voyage 1901, summarize financials, key ports, cargo grades, and remarks\"\n"
                    "- \"Top 10 voyages by commission and include key ports and cargo grades\"\n"
                    "- \"For port Rotterdam, summarize the most common cargo grades across voyages\"\n"
                    "- \"Tell me about vessel Stena Superior: recent performance, frequent ports, and notable remarks\"\n"
                )

            identity_phrases = (
                "who are you",
                "who r you",
                "who are u",
                "who r u",
                "what are you",
                "what are u",
                "what can you do",
                "what can u do",
                "what do you do",
                "what do u do",
            )
            if any(p in q_lower for p in identity_phrases):
                return (
                    "### About Digital Sales Agent\n"
                    "- I’m **Digital Sales Agent**, a maritime analytics assistant focused on **voyage finance + operations**.\n"
                    "- I can answer questions about **PnL, revenue, expenses, TCE, commissions**, plus **ports/routes, cargo grades, delays/offhire, and voyage remarks**.\n\n"
                    "### Try asking\n"
                    "- \"For voyage 1901, summarize financials, key ports, cargo grades, and remarks\"\n"
                    "- \"Top 10 voyages by commission and include key ports and cargo grades\"\n"
                    "- \"For port Rotterdam, summarize the most common cargo grades across voyages\"\n"
                )

            if any(k in q_lower for k in ["weather", "temperature", "rain", "forecast", "climate"]):
                return (
                    "### Summary\n"
                    "- I can’t provide live weather/forecast data from this system.\n"
                    "- If you want, tell me the **location and date/time**, and I can help you interpret weather impacts on voyages (delays, routing) using your operational/remark data.\n\n"
                    "### What I can help with here\n"
                    "- Voyage / vessel performance (P&L, costs, TCE, commission)\n"
                    "- Routes, ports, cargo grades, delays/offhire, and voyage remarks\n\n"
                    "### Try asking\n"
                    "- \"For voyage 1901, summarize financials, key ports, and remarks\"\n"
                    "- \"Top 5 most profitable voyages with key ports and remarks\"\n"
                )
            return (
                "### Summary\n"
                "- This question is outside the supported dataset/skills for this assistant.\n\n"
                "### What I can help with here\n"
                "- Voyage / vessel performance (P&L, costs, TCE, commission)\n"
                "- Routes, ports, cargo grades, delays/offhire, and voyage remarks\n\n"
                "### Try asking\n"
                "- \"Tell me about vessel Stena Superior: voyage profitability over time and frequent ports\"\n"
                "- \"For voyage 1901, financial summary + main ports + remarks\"\n"
            )

        merged = self._truncate_merged_data(merged, max_rows=10)
        merged_safe = self._convert_to_json_safe(merged)
        merged_rows = None
        if isinstance(merged_safe, dict):
            artifacts = merged_safe.get("artifacts")
            if isinstance(artifacts, dict) and isinstance(artifacts.get("merged_rows"), list):
                merged_rows = artifacts.get("merged_rows") or []

        # Strong hint for ranking intents so the model includes PnL/Revenue in the table
        ranking_hint = None
        if intent_key and str(intent_key).startswith("ranking.") and merged_rows:
            ranking_hint = "Each object in merged_rows has numeric fields pnl, revenue, total_expense at the top level. You MUST include PnL and Revenue (and Total expense when present) as columns in the Results table. Do NOT say financial metrics are not available."

        style = self._derive_answer_style(question=question, intent_key=intent_key)

        system = """
You are a flagship-quality maritime analytics assistant (finance + operations).

HARD RULES:
- Use ONLY the provided JSON. Do NOT invent numbers, entities, or causes.
- If a value is missing/NULL, say "Not available" (do NOT convert to 0.0 unless the JSON explicitly says 0).
- Produce clean, readable Markdown with consistent headings and tables.
- Keep lists short and scannable. Never dump huge raw lists.
- Never repeat the same '###' heading more than once.
- You will receive style flags in data.style. Follow them strictly.
- Do NOT omit rows or metrics for brevity. Include all available data for every voyage/row in the result set. Do not add notes like "other voyages/vessels available in original data but not included here for brevity"—show full metrics for every row (e.g. if there are 3 voyages, show Revenue/PnL/Total commission etc. for all 3).

DATA PRIORITY:
- If data.artifacts.merged_rows exists, it is the PRIMARY joined dataset (one item per voyage).
- Prefer merged_rows over raw mongo/finance/ops sections when available.
- In merged_rows, KPIs may appear at the TOP LEVEL (pnl, revenue, total_expense, tce, total_commission) even if finance.rows is empty.
- In merged_rows, ops enrichment may appear as cargo_grades, key_ports, and remarks (even if ops.rows is empty).
- When grades/ports/remarks exist in the JSON, include them. Do NOT claim they are unavailable.
- If data.artifacts.coverage is present, use it to avoid false "Not available" claims (e.g., if cargo_grades_available>0 then cargo grades are available for at least some voyages).
- For ranking.* intents: each item in merged_rows HAS pnl, revenue, total_expense (and often tce, total_commission) at the top level. You MUST include PnL and Revenue (and Total expense when present) as columns in the Results table. Do NOT say "financial metrics are not available" when merged_rows exist and contain these fields.

STYLE / STRUCTURE (always follow):
- Start with a 2–4 bullet **Summary** of the key result.
- Do NOT create table rows for metrics that are not present in the JSON. Prefer omitting them over showing "Not available" repeatedly.
- Use '-' for bullet points (not '*').
- Use sections with '###' headings only.
- Prefer tables for numeric KPIs; include currency formatting for USD amounts.
- Cap long lists:
  - Ports: show at most 8; if more, add "(+N more)".
  - Grades: show at most 8; if more, add "(+N more)".
  - Remarks: show at most 3 short bullets; if more, add "(+N more)".

STYLE FLAGS (data.style):
- If narrative_summary=true: Summary MUST start with 1–2 narrative bullets (full sentences) BEFORE any KPI/template bullets.
- If narrative_summary=false and financial_first=true: lead with KPI bullets + the Financials table.
- If financial_first=false: keep the response more narrative/operational first, but still include the Financials table.

TEMPLATES BY INTENT:

1) voyage.summary (single voyage):
IMPORTANT: Tailor the emphasis to the user's wording.
- If the question contains phrases like "what happened" or "summarize", write a brief 2–4 sentence narrative in the Summary (still using bullets) describing what stands out operationally and financially, then include the tables/lists.
- If the question asks specifically for "financial summary" first, lead with the KPI line and table.
### Summary
- **Voyage**: <voyage_number>
- **Vessel**: <vessel_name> (IMO: <imo>) when available
- **PnL / Revenue / Expense / TCE**: include if present
- **Key ports**: 5–8 max with (L/D) if present
- **Remarks**: 0–3 bullets; if none, say "No remarks recorded"

### Financials (ACTUAL)
| Metric | Value |
| --- | --- |
| Revenue | ... |
| Total expense | ... |
| PnL | ... |
| TCE | ... |
| Total commission | ... |

### Operational snapshot
- **Key ports**: <comma-separated capped list>
- **Cargo grades** (if present): <capped list>

### Remarks
- <bullet 1>
- <bullet 2>

2) ranking.* (multiple voyages):
- CRITICAL: merged_rows for ranking ALWAYS contain pnl, revenue, total_expense at the top level. Include PnL and Revenue (and Total expense, TCE, Total commission when present) as columns in the Results table. Do NOT state that financial metrics are not available.
- Include ALL rows in the result set: do not show financials for only the first voyage and omit the rest "for brevity". Show Revenue, Total expense, PnL, TCE, Total commission (and any other requested metrics) for every voyage in the table.
- When merged_rows contain offhire_days: include **Offhire days** as a column (and **Delay reason** when present) so the ranking by offhire is visible.
- When the question asks for "most port calls" or merged_rows contain port_calls: include **Port calls** as a column so the ranking by port calls is visible.
### Summary
- **Ranking**: what is being ranked and limit
- **Top result**: voyage_number + key metric value (e.g. PnL)

### Results
| Voyage # | PnL | Revenue | Total expense | Total commission | Key ports | Cargo grades | Remarks |
| --- | --- | --- | --- | --- | --- | --- | --- |
(Only include columns that exist in the JSON and are relevant to the question. For ranking by profitability, PnL and Revenue MUST be included. For offhire ranking, include Offhire days. For "most port calls", include Port calls.)

2b) ranking.vessels (vessels with voyage count + profitability + cargo grades):
- When merged_rows contain vessel_imo, vessel_name, voyage_count, avg_pnl, cargo_grades (no voyage_id), show a **vessel-level** table.
### Summary
- **Vessels**: high voyage count and above-average profitability
- **Count**: how many vessels

### Results
| Vessel (IMO) | Vessel name | Voyage count | Avg PnL / PnL | Cargo grades |
| --- | --- | --- | --- | --- |
- List the most common cargo grades per vessel from the cargo_grades array in each row.

3) analysis.* (aggregates):
### Summary
- **What was grouped by** and **what metric**

### Results
Use a compact table. If a metric is missing for a group, show "Not available" and (optionally) include counts like "available/total" when present in JSON.

FAILSAFE:
- ONLY if the provided JSON is completely empty (no rows anywhere) then output exactly: "Not available in dataset."

4) vessel.summary (single vessel / overview):
- Write a short narrative briefing (2–5 sentences) describing what we know about the vessel's voyage performance (range/volatility, best/worst, recency).
- If the question asks about "recently", include a **Recent voyages** table with the latest 3 voyages by end date (if end date exists).
- If the question asks "good or bad" or "best/worst", include **Best voyage** and **Worst voyage** (by PnL) as a compact 2–3 row table.
- Then (optionally) include one compact table combining recent + best + worst. Avoid dumping all voyages.
- If ports/grades/remarks are present in ops rows (ports_json/grades_json/remarks_json), include these sections:
  - ### Frequent ports: up to 8 ports (add "(+N more)" if needed)
  - ### Common cargo grades: up to 8 grades (add "(+N more)" if needed)
  - ### Recent remarks: up to 3 short bullets; ignore empty/null remarks
- If ports/grades/remarks are missing, state that plainly in one line under a "### Data coverage" section.
"""

        result = self._call_with_retry(
            system=system,
            user=json.dumps(
                {
                    "question": question,
                    "plan": plan,
                    "intent_key": intent_key,
                    "data": {**(merged_safe if isinstance(merged_safe, dict) else {}), "style": style},
                    "merged_rows": merged_rows,
                    **({"instruction": ranking_hint} if ranking_hint else {}),
                }
            ),
            operation="answer_generation",
            return_string=True,
        )

        polished = self._polish_answer_if_needed(
            question=question,
            intent_key=intent_key,
            plan=plan,
            merged_safe=(merged_safe if isinstance(merged_safe, dict) else {}),
            merged_rows=(merged_rows if isinstance(merged_rows, list) else None),
            style=style,
            draft=(result or ""),
        )

        cleaned = self._postprocess_answer_markdown(
            polished,
            intent_key=intent_key,
            style=style,
            merged_safe=(merged_safe if isinstance(merged_safe, dict) else {}),
        )
        return cleaned if cleaned else "Not available in dataset."

    def _polish_answer_if_needed(
        self,
        *,
        question: str,
        intent_key: str,
        plan: Dict[str, Any],
        merged_safe: Dict[str, Any],
        merged_rows: Optional[List[Any]],
        style: Dict[str, Any],
        draft: str,
    ) -> str:
        """
        Second-pass "editor" rewrite for question-driven narrative quality.
        Uses ONLY the provided JSON + the draft answer (no new facts).
        """
        text = (draft or "").strip()
        if not text:
            return ""

        # Gate: polish where narrative quality matters most (keeps latency/cost down).
        should_polish = False
        if intent_key in ("voyage.summary", "vessel.summary"):
            should_polish = True
        if style.get("narrative_summary") is True:
            should_polish = True

        if not should_polish:
            return text

        system = """
You are an expert editor for a maritime analytics chatbot.
Rewrite the DRAFT answer into a final answer that is question-driven and natural (ChatGPT-style),
while staying 100% faithful to the provided JSON data.

HARD RULES:
- Use ONLY the provided JSON data. Do NOT invent or assume anything.
- You MAY rephrase, reorder, and summarize, but you MUST NOT introduce new numbers, ports, grades, dates, or remarks.
- If the draft contains something that is not supported by JSON, remove it.
- Do not repeat sections. Never repeat the same '###' heading more than once.
- Keep it clean and readable. Avoid overly generic filler.

OUTPUT STYLE:
- Use '###' headings only.
- Use '-' bullets (not '*').
- Prefer narrative explanation FIRST when the question asks "what happened", "summarize", "explain", "root cause".
- Include tables ONLY if the question explicitly asks for a "financial summary" / metrics or comparisons; otherwise keep tables minimal.
- If you include a table, include ALL rows (every voyage/vessel in the result set); do not omit rows for brevity. Include all requested metrics for each row.
- Cap long lists (ports/grades/remarks) and add '(+N more)' when needed.
"""

        user = {
            "question": question,
            "intent_key": intent_key,
            "plan": plan,
            "style": style,
            "data": merged_safe,
            "merged_rows": merged_rows,
            "draft_answer": text,
        }

        rewritten = self._call_with_retry(
            system=system,
            user=json.dumps(user, ensure_ascii=False),
            operation="answer_polish",
            return_string=True,
        )

        return (rewritten or text).strip()

    def _derive_answer_style(self, *, question: str, intent_key: str) -> Dict[str, Any]:
        q = (question or "").strip()
        ql = q.lower()

        narrative_triggers = (
            "what happened",
            "summarize",
            "summary of what happened",
            "what went wrong",
            "root cause",
            "brief me",
        )
        narrative_summary = any(t in ql for t in narrative_triggers)

        financial_first = "financial summary" in ql or (
            any(k in ql for k in ("revenue", "expense", "expenses", "pnl", "tce", "commission"))
            and not narrative_summary
        )

        ask_ports = any(k in ql for k in ("port", "ports", "route", "routing"))
        ask_grades = any(k in ql for k in ("grade", "grades", "cargo"))
        ask_remarks = any(k in ql for k in ("remark", "remarks", "issue", "issues", "delay", "delays")) or narrative_summary

        return {
            "intent_key": intent_key,
            "narrative_summary": bool(narrative_summary) if intent_key == "voyage.summary" else False,
            "financial_first": bool(financial_first),
            "ask_ports": bool(ask_ports),
            "ask_grades": bool(ask_grades),
            "ask_remarks": bool(ask_remarks),
        }

    def _postprocess_answer_markdown(
        self,
        text: str,
        *,
        intent_key: str,
        style: Dict[str, Any],
        merged_safe: Dict[str, Any],
    ) -> str:
        s = (text or "").strip()
        if not s:
            return ""

        # Normalize bullets: enforce '-' (not '*')
        s = re.sub(r"(?m)^\*\s+", "- ", s)

        # Drop consecutive duplicate lines (common LLM glitch)
        lines = s.splitlines()
        dedup: List[str] = []
        for line in lines:
            if dedup and line.strip() and line.strip() == dedup[-1].strip():
                continue
            dedup.append(line)

        # Drop repeated sections entirely (another common glitch)
        out: List[str] = []
        seen_headings: set[str] = set()
        i = 0
        while i < len(dedup):
            line = dedup[i]
            if re.match(r"^###\s+\S", line.strip()):
                heading = line.strip()
                if heading in seen_headings:
                    i += 1
                    while i < len(dedup) and not re.match(r"^###\s+\S", dedup[i].strip()):
                        i += 1
                    continue
                seen_headings.add(heading)
            out.append(line)
            i += 1

        s = "\n".join(out).strip()

        # Ensure voyage.summary "what happened" queries don't look identical to KPI-first queries.
        if intent_key == "voyage.summary" and style.get("narrative_summary") is True:
            s = self._ensure_voyage_narrative_summary(s, merged_safe=merged_safe)
        if intent_key == "voyage.summary":
            s = self._ensure_voyage_identity_line(s, merged_safe=merged_safe)

        return s.strip()

    def _ensure_voyage_narrative_summary(self, text: str, *, merged_safe: Dict[str, Any]) -> str:
        lines = (text or "").splitlines()
        try:
            idx = next(i for i, l in enumerate(lines) if l.strip() == "### Summary")
        except StopIteration:
            return text

        j = idx + 1
        while j < len(lines) and not lines[j].strip():
            j += 1

        # If the Summary starts immediately with KPI/template bullets, inject a short narrative bullet.
        if j < len(lines) and lines[j].lstrip().startswith("- **"):
            hint = self._build_voyage_narrative_hint(merged_safe)
            if hint:
                lines.insert(j, hint)
        return "\n".join(lines).strip()

    @staticmethod
    def _fmt_usd(v: Any) -> Optional[str]:
        try:
            if v is None:
                return None
            fv = float(v)
            return f"${fv:,.2f}"
        except Exception:
            return None

    def _build_voyage_narrative_hint(self, merged_safe: Dict[str, Any]) -> str:
        fin = merged_safe.get("finance")
        row = None
        if isinstance(fin, dict) and isinstance(fin.get("rows"), list) and fin["rows"]:
            row = fin["rows"][0] if isinstance(fin["rows"][0], dict) else None

        if not isinstance(row, dict):
            return ""

        pnl = row.get("pnl")
        revenue = row.get("revenue")
        expense = row.get("total_expense")

        pnl_s = self._fmt_usd(pnl) or "Not available"
        rev_s = self._fmt_usd(revenue) or "Not available"
        exp_s = self._fmt_usd(expense) or "Not available"

        direction = ""
        try:
            if pnl is not None and float(pnl) >= 0:
                direction = "positive"
            elif pnl is not None:
                direction = "negative"
        except Exception:
            direction = ""

        if direction:
            return f"- Overall, this voyage finished {direction} (PnL {pnl_s}) on revenue {rev_s} and total expense {exp_s}."
        return f"- Overall, this voyage finished with PnL {pnl_s} on revenue {rev_s} and total expense {exp_s}."

    @staticmethod
    def _norm_imo_text(v: Any) -> str:
        if v in (None, ""):
            return ""
        s = str(v).strip()
        if s.endswith(".0"):
            s = s[:-2]
        return s

    def _extract_voyage_identity(self, merged_safe: Dict[str, Any]) -> tuple[str, str]:
        # Prefer finance row, then ops row, then mongo row.
        fin = merged_safe.get("finance")
        if isinstance(fin, dict) and isinstance(fin.get("rows"), list) and fin.get("rows"):
            r0 = fin["rows"][0]
            if isinstance(r0, dict):
                vname = str(r0.get("vessel_name") or "").strip()
                imo = self._norm_imo_text(r0.get("vessel_imo") or r0.get("imo"))
                if vname or imo:
                    return vname, imo

        ops = merged_safe.get("ops")
        if isinstance(ops, dict) and isinstance(ops.get("rows"), list) and ops.get("rows"):
            r0 = ops["rows"][0]
            if isinstance(r0, dict):
                vname = str(r0.get("vessel_name") or "").strip()
                imo = self._norm_imo_text(r0.get("vessel_imo") or r0.get("imo"))
                if vname or imo:
                    return vname, imo

        mongo = merged_safe.get("mongo")
        if isinstance(mongo, dict):
            rows = mongo.get("rows")
            if isinstance(rows, list) and rows and isinstance(rows[0], dict):
                r0 = rows[0]
                vname = str(r0.get("vesselName") or r0.get("vessel_name") or "").strip()
                imo = self._norm_imo_text(r0.get("vesselImo") or r0.get("vessel_imo") or r0.get("imo"))
                if vname or imo:
                    return vname, imo

        return "", ""

    def _ensure_voyage_identity_line(self, text: str, *, merged_safe: Dict[str, Any]) -> str:
        s = (text or "").strip()
        if not s:
            return s
        if re.search(r"(?im)^\s*-\s*\*\*Vessel\*\*:", s):
            return s

        vname, imo = self._extract_voyage_identity(merged_safe)
        if not vname and not imo:
            return s

        vessel_val = vname if vname else "Not available"
        if imo:
            vessel_val = f"{vessel_val} (IMO: {imo})"
        vessel_line = f"- **Vessel**: {vessel_val}"

        lines = s.splitlines()
        try:
            idx = next(i for i, l in enumerate(lines) if l.strip() == "### Summary")
        except StopIteration:
            return f"### Summary\n{vessel_line}\n\n{s}".strip()

        insert_at = idx + 1
        for i in range(idx + 1, len(lines)):
            line = lines[i].strip()
            if not line:
                continue
            if line.lower().startswith("- **voyage**"):
                insert_at = i + 1
                break
            if line.startswith("### "):
                insert_at = idx + 1
                break
            if line.startswith("- "):
                insert_at = i
                break
        lines.insert(insert_at, vessel_line)
        return "\n".join(lines).strip()

    # =========================================================
    # RETRY + JSON SAFE PARSER
    # =========================================================

    def _call_with_retry(
        self,
        *,
        system: str,
        user: str,
        operation: str,
        max_retries: int = 3,
        return_string: bool = False,
    ):

        for _ in range(max_retries):
            try:
                raw = self._groq_chat(system=system, user=user)

                if return_string:
                    return raw

                cleaned = raw.strip()

                # Remove code fences
                cleaned = re.sub(r"^```.*?\n", "", cleaned)
                cleaned = cleaned.replace("```", "")

                return json.loads(cleaned)

            except Exception:
                time.sleep(0.5)

        return "" if return_string else None

    # =========================================================
    # UTILITIES
    # =========================================================

    def _safe_json_load(self, raw: str, fallback: Any):
        """
        Parse JSON from a model response safely.
        Removes code fences and returns fallback on any error.
        """
        try:
            cleaned = (raw or "").strip()
            cleaned = re.sub(r"^```.*?\n", "", cleaned)
            cleaned = cleaned.replace("```", "")
            return json.loads(cleaned)
        except Exception:
            return fallback

    def _truncate_merged_data(self, merged: Dict[str, Any], max_rows: int):
        if not isinstance(merged, dict):
            return merged

        import copy

        out = copy.deepcopy(merged)

        def cap_rows(section_key: str):
            section = out.get(section_key)
            if isinstance(section, dict) and isinstance(section.get("rows"), list):
                section["rows"] = section["rows"][:max_rows]

        cap_rows("finance")
        cap_rows("ops")
        cap_rows("mongo")

        # Cap nested payloads to avoid token blowups
        def _cap_list(v, n: int):
            return v[:n] if isinstance(v, list) else v

        def _cap_str(v, n: int):
            if isinstance(v, str) and len(v) > n:
                return v[:n] + "…"
            return v

        # Ops rows can contain large json arrays
        ops = out.get("ops")
        if isinstance(ops, dict) and isinstance(ops.get("rows"), list):
            for r in ops["rows"]:
                if not isinstance(r, dict):
                    continue
                r["ports_json"] = _cap_list(r.get("ports_json"), 20)
                r["grades_json"] = _cap_list(r.get("grades_json"), 20)
                r["remarks_json"] = _cap_list(r.get("remarks_json"), 10)

        artifacts = out.get("artifacts")
        if isinstance(artifacts, dict) and isinstance(artifacts.get("merged_rows"), list):
            artifacts["merged_rows"] = artifacts["merged_rows"][:max_rows]
            for mr in artifacts["merged_rows"]:
                if not isinstance(mr, dict):
                    continue
                mr["key_ports"] = _cap_list(mr.get("key_ports"), 10)
                mr["cargo_grades"] = _cap_list(mr.get("cargo_grades"), 10)
                mr["commissions"] = _cap_list(mr.get("commissions"), 10)

                # Remarks can be huge; keep first few and shorten long text
                rem = mr.get("remarks")
                if isinstance(rem, list):
                    rem = rem[:5]
                    cleaned = []
                    for x in rem:
                        if isinstance(x, dict):
                            cleaned.append({
                                "remark": _cap_str(x.get("remark"), 300),
                                "modifiedDate": x.get("modifiedDate"),
                                "modifiedByFull": x.get("modifiedByFull"),
                            })
                        else:
                            cleaned.append(_cap_str(str(x), 300))
                    mr["remarks"] = cleaned
                elif isinstance(rem, str):
                    mr["remarks"] = _cap_str(rem, 300)

        return out

    def _convert_to_json_safe(self, obj: Any):
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, (datetime, date, dt_time)):
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {k: self._convert_to_json_safe(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_to_json_safe(i) for i in obj]
        return obj

    def _groq_chat(self, *, system: str, user: str) -> str:
        completion = self.client.chat.completions.create(
            model=self.config.model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=self.config.temperature,
        )
        return completion.choices[0].message.content or ""