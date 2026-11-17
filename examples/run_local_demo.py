#!/usr/bin/env python3
"""Run the exact local SPTC-Miner core on synthetic positions, without Spark."""
import json
import sys
from collections import defaultdict
from pathlib import Path

repo = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo / 'src'))
from sptc_miner.mining.growth import mine_one_partition
from sptc_miner.mining.reducers import dedup_one_sid, combine_distinct_sids

inputs = repo / 'sample_data' / 'synthetic' / 'complete_od_summaries.jsonl'
postings = []
for line in inputs.read_text(encoding='utf-8').splitlines():
    record = json.loads(line)
    for entry in record['U']:
        postings.append((record['CardID'], entry['od'], entry['freq'], entry['Pos']))

adjacency = {'Bus_A': ('Subway_B',), 'Subway_B': ('Bus_C',)}
contrib = list(mine_one_partition(0, postings, adjacency, 2, 0, 1,
                                  max_pattern_length=2, candidate_topology='directed'))
by_pattern_sid = {}
for key, value in contrib:
    by_pattern_sid[key] = dedup_one_sid(by_pattern_sid[key], value) if key in by_pattern_sid else value
combined = {}
for (pattern, card_id), value in by_pattern_sid.items():
    combined[pattern] = combine_distinct_sids(combined[pattern], value) if pattern in combined else value
results = {pattern: total for pattern, (total, _) in combined.items() if total >= 2}
expected = ('Bus_A -> Subway_B', 'Subway_B -> Bus_C')
assert results[expected] == 3, results
print('LOCAL DEMO PASS:')
for pattern, total in sorted(results.items(), key=lambda x: (len(x[0]), x[0])):
    print('  {}: {}'.format(' , '.join(pattern), total))
print('This validates local pattern growth and aggregation, not an end-to-end Spark pipeline.')
