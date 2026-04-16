import json
import time
from pathlib import Path

import requests


BASE_URL = "http://127.0.0.1:8010/query"
OUTPUT_PATH = Path("scripts") / "smoke_results_local_8010.json"


SESSIONS = [
    {
        "name": "voyage_finance_ops",
        "queries": [
            "For voyage 1901 on Stena Conquest, give me the financial summary (revenue, expenses, PnL, TCE), main ports and cargo grades, and any remarks on record.",
            "For voyage 2306, give an executive summary: vessel, route, financial result (PnL, revenue, expense), cargo grade, and any remarks on record.",
            "For voyage 2208, what does the data show? Include PnL, revenue, expense, offhire days, ports, and remarks.",
            "Give me a concise incident-style summary for voyage 2204: offhire days, cost breakdown, and remarks.",
        ],
    },
    {
        "name": "ranking_analysis",
        "queries": [
            "Show me the top 5 most profitable voyages with PnL, revenue, margin %, vessel name, cargo grade, and key ports.",
            "Which voyages had high revenue but low or negative PnL? Show revenue, total expense, PnL, and expense-to-revenue ratio. Include remarks where recorded.",
            "Which cargo grades have the highest average PnL? For each grade show average PnL, average revenue, voyage count, and most common ports.",
            "Show the top 3 voyages by total commission with PnL, revenue, vessel name, and key ports.",
            "For each module type (TC Voyage, Spot, etc), show average PnL, average revenue, voyage count, and most common cargo grade.",
            "Which vessels have the most voyages and above-average PnL? Show voyage count, average PnL, and most common cargo grade.",
            "Show the 10 voyages with the most port calls. For each show vessel name, port count, PnL, and revenue.",
            "For voyages that visited Singapore, rank by PnL and show cargo grades and vessel names. Include remarks if available.",
            "Which voyages had the most offhire days? Show offhire days, PnL, revenue, is_delayed flag, vessel name, and remarks where recorded.",
            "Show the top loss-making voyages with revenue, total expense, PnL, expense ratio, and key ports. Include remarks where recorded.",
            "For voyages flagged as delayed with negative PnL, show offhire days, total expense, PnL, vessel name, and remarks where recorded.",
            "Compare actual vs when-fixed results for voyages 1901, 1902, and 2301. Show PnL variance and TCE variance.",
            "For vessel Stena Conquest, show voyage-by-voyage PnL trend and its top 5 most visited ports.",
            "Is Stena Superior performing well overall? Show its voyage history with PnL, identify best and worst voyage, and include any remarks for those voyages.",
        ],
    },
    {
        "name": "aggregations",
        "queries": [
            "Which vessel has earned the highest total PnL across all voyages?",
            "What is the average TCE per vessel?",
            "Which cargo grade appears most frequently across all voyages?",
            "How many voyages does each vessel have?",
            "What is the most commonly visited port across all voyages?",
            "What is the total revenue across all voyages?",
            "What are the busiest ports in the fleet?",
            "How many voyages were delayed overall?",
            "Which vessel has carried the most diverse set of cargo grades?",
            "Ports with highest average demurrage wait time?",
            "Cargo grades with highest variance between ACTUAL and WHEN_FIXED PnL?",
            "Show average PnL trend month by month across all voyages.",
        ],
    },
    {
        "name": "vessel_metadata",
        "queries": [
            "Show vessel id, IMO, and account code for vessel Elka Delphi.",
            "What is the hire rate, scrubber status, and market type of vessel Elka Delphi?",
            "Is vessel Elka Delphi operational?",
            "Give me commercial metadata for vessel Elka Delphi.",
            "List all passage types for vessel Elka Delphi.",
            "Show passage consumption profile for vessel Elka Delphi.",
            "Show default consumption rows for vessel Elka Delphi.",
            "For vessel Elka Delphi, show Ballast default speed, IFO, and MGO.",
            "For vessel Elka Delphi, show Laden default speed, IFO, and MGO.",
            "Show all Ballast consumption rows for vessel Elka Delphi.",
            "Show all Laden consumption rows for vessel Elka Delphi.",
            "Show non-passage consumption for vessel Elka Delphi.",
            "For vessel Elka Delphi, show ifoLoad, ifoDischarge, ifoIdle, ifoHeat, ifoClean, ifoInert.",
            "For vessel Elka Delphi, show mgoLoad, mgoDischarge, mgoIdle, mgoHeat, mgoClean, mgoInert.",
            "Show tags for vessel Elka Delphi.",
            "Show contract history for vessel Elka Delphi.",
            "Show owner, duration, cp date, and delivery date for vessel Elka Delphi contract history.",
            "For voyages 2301 and 2302, show operating status of corresponding vessels.",
            "For voyages 2301, 2302, 2303 show hire rate and scrubber.",
            "For voyages 2301 and 2302, show passage types for corresponding vessels.",
            "For voyages 2301 and 2302, show account code and market type.",
        ],
    },
    {
        "name": "voyage_metadata",
        "queries": [
            "What is the charterer, broker, and commission rates for voyage 2306?",
            "Show the CP date, CP quantity, demurrage rate, and laytime for voyage 2306.",
            "What cargo was carried on voyage 2306? Show grade, BL quantity, shipper, load port, and discharge port.",
            "Show the full route for voyage 2306 with ports, arrival and departure dates, and distances.",
            "Break down the revenue lines for voyage 2306. Show freight, demurrage, and rebills separately.",
            "What are the expense items for voyage 2306? Include remarks for each line.",
            "Show bunker consumption and cost for voyage 2306 by grade.",
            "What is the CII band, CO2, SOx, and NOx for voyage 2306?",
            "Show the projected financials for voyage 2306 - PnL, revenue, TCE, expenses, bunkers, port cost, and commission.",
            "What remarks are on record for voyage 2306? Show remark text and who added it.",
        ],
    },
    {
        "name": "vessel_ranking",
        "queries": [
            "Which vessels are currently operating?",
            "Which vessels have the highest hire rate?",
            "Which vessels have scrubbers?",
            "How many vessels are non-scrubber?",
            "Which vessel has the highest default ballast speed?",
            "Which vessel has the highest default laden speed?",
            "Which vessels are in the short pool?",
            "Which vessels have the longest current contract duration?",
            "Which vessel has the highest voyage count?",
            "Which voyage had the highest profit or PnL?",
            "Which vessel has the best average TCE?",
            "Show me the top vessels by number of voyages.",
        ],
    },
    {
        "name": "multi_turn_followups",
        "queries": [
            "For voyage 2306, give an executive summary: vessel, route, financial result (PnL, revenue, expense), cargo grade, and any remarks on record.",
            "What about its remarks only?",
            "And who added them?",
            "Now show the expense items for that voyage.",
            "Compare that with 2301 on PnL and TCE.",
            "For vessel Elka Delphi, show hire rate, scrubber, and market type.",
            "What about ballast defaults?",
            "And laden defaults?",
            "Now show contract history for the same vessel.",
            "For voyages 2301 and 2302, show operating status of corresponding vessels.",
            "What about their passage types?",
            "And account code plus market type?",
        ],
    },
]


def main() -> None:
    OUTPUT_PATH.parent.mkdir(exist_ok=True)
    results = []

    for session in SESSIONS:
        session_id = f"smoke-{session['name']}"
        for index, query in enumerate(session["queries"], start=1):
            started = time.time()
            item = {
                "session": session["name"],
                "index": index,
                "query": query,
            }
            try:
                response = requests.post(
                    BASE_URL,
                    json={"query": query, "session_id": session_id},
                    timeout=120,
                )
                data = response.json()
                item.update(
                    {
                        "status_code": response.status_code,
                        "intent_key": data.get("intent_key"),
                        "answer": data.get("answer"),
                        "clarification": data.get("clarification"),
                        "trace": data.get("trace"),
                        "dynamic_sql_used": data.get("dynamic_sql_used"),
                    }
                )
            except Exception as exc:
                item["error"] = str(exc)
            item["elapsed_sec"] = round(time.time() - started, 2)
            results.append(item)
            print(
                json.dumps(
                    {
                        "session": item["session"],
                        "index": item["index"],
                        "intent_key": item.get("intent_key"),
                        "status_code": item.get("status_code"),
                        "error": item.get("error"),
                        "elapsed_sec": item["elapsed_sec"],
                    },
                    ensure_ascii=True,
                ),
                flush=True,
            )

    OUTPUT_PATH.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"WROTE {OUTPUT_PATH} {len(results)} results", flush=True)


if __name__ == "__main__":
    main()
