'Build a partition-local TPGI including original OD positions.'

from collections import defaultdict
from typing import Iterable, Tuple
from .utils import split_od

def _posting_values(item):
    """Convert Spark Row/dict fields, preserving original integer positions."""
    sid, od, freq, positions = item
    sid, od = str(sid), str(od)
    f = int(freq)
    if positions is None:
        raise ValueError('Missing Positions: sid={}, OD={}'.format(sid, od))
    pos = tuple(int(x) for x in positions)
    if f <= 0 or len(pos) != f or any(a >= b for a, b in zip(pos, pos[1:])):
        raise ValueError('Frequency/Positions mismatch: sid={}, OD={}, freq={}, Pos={}'
                         .format(sid, od, f, pos))
    split_od(od)
    return sid, od, f, pos

def construct_local_tpgi(postings: Iterable[Tuple[str, str, int, list]],
                         minsup: int):
    """Algorithm 2 per physical partition; each (sid,od) is counted once."""
    od_count = defaultdict(int)
    index = defaultdict(dict)
    for raw in postings:
        sid, od, freq, pos = _posting_values(raw)
        old = index[od].get(sid)
        if old is not None:
            if old != (freq, pos):
                raise ValueError('Inconsistent duplicate (CardID, OD): {}, {}'.format(sid, od))
            continue
        index[od][sid] = (freq, pos)
        od_count[od] += freq

    # local frequent OD edges are the ONLY elements allowed for pattern growth.
    W = {e: count for e, count in od_count.items() if count >= minsup}
    for e in tuple(index):
        if e not in W:
            del index[e]

    local_graph = defaultdict(set)
    source_index = defaultdict(list)
    for e in sorted(W):
        source, target = split_od(e)
        local_graph[source].add(target)
        source_index[source].append(e)
    inv_keys = {e: set(by_sid) for e, by_sid in index.items()}
    return W, index, inv_keys, local_graph, source_index
