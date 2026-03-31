# KAI Demo Validation Report

- Overall score: **94.00**
- Target score: **80**
- Pass overall: **True**
- Total queries: **15**
- Failed requests: **0**

## Scores By Set
- aggregate_business: **90.00**
- ranking_analytics: **100.00**
- scenario_compare: **95.00**
- single_composite_mix: **100.00**
- vessel_metadata: **86.25**
- voyage_ops_finance: **95.00**

## Scores By Difficulty
- complex: **97.22**
- medium: **88.33**
- simple: **90.00**

## Low-Scoring Queries (< 80)
- None

## Per Query
| ID | Difficulty | Set | Score | Intent (exp -> actual) | Dynamic |
| --- | --- | --- | ---: | --- | --- |
| Q01 | simple | voyage_ops_finance | 90.00 | voyage.summary -> voyage.summary | N |
| Q02 | simple | vessel_metadata | 80.00 | ranking.vessels -> ranking.vessels | Y |
| Q03 | simple | vessel_metadata | 100.00 | vessel.metadata -> vessel.metadata | N |
| Q04 | medium | vessel_metadata | 85.00 | vessel.metadata -> vessel.metadata | N |
| Q05 | medium | vessel_metadata | 80.00 | analysis.scenario_comparison -> comparison.vessels | Y |
| Q06 | medium | voyage_ops_finance | 100.00 | voyage.summary -> voyage.summary | N |
| Q07 | complex | single_composite_mix | 100.00 | voyage.summary -> voyage.summary | N |
| Q08 | complex | ranking_analytics | 100.00 | ranking.voyages -> ranking.voyages | Y |
| Q09 | complex | ranking_analytics | 100.00 | analysis.segment_performance -> analysis.high_revenue_low_pnl | Y |
| Q10 | complex | ranking_analytics | 100.00 | vessel.summary -> ranking.voyages | Y |
| Q11 | complex | scenario_compare | 95.00 | analysis.scenario_comparison -> analysis.scenario_comparison | N |
| Q12 | complex | ranking_analytics | 100.00 | analysis.cargo_profitability -> ranking.ports | Y |
| Q13 | complex | ranking_analytics | 100.00 | ranking.voyages -> ranking.voyages_by_commission | Y |
| Q14 | complex | aggregate_business | 100.00 | aggregation.average -> analysis.by_module_type | Y |
| Q15 | complex | aggregate_business | 80.00 | finance.loss_due_to_delay -> finance.loss_due_to_delay | Y |
