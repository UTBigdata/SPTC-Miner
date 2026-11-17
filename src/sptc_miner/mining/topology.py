'External directed topology queries and candidate adjacency structures.'

from collections import defaultdict, deque, OrderedDict
from typing import Iterable
from .utils import split_od

def gen_gap_succ(local_graph: dict, global_adj: dict, start: str,
                 min_gap: int, max_gap: int, gap_cache: OrderedDict,
                 topo_cache: dict, gap_cache_limit: int) -> dict:
    """Shortest directed distance buckets, including distance 0 for start.

    Every visited station queries the FULL global adjacency, not only when local
    outgoing edges are empty. 'Ghost' topology edges never enter local SrcIndex.
    """
    if start in gap_cache:
        gap_cache.move_to_end(start)
        distance = gap_cache[start]
    else:
        distance = {start: 0}
        queue = deque([start])
        while queue:
            v = queue.popleft()
            depth = distance[v]
            if depth >= max_gap:
                continue
            if v not in topo_cache:
                neighbors = set(global_adj.get(v, ()))
                neighbors.update(local_graph.get(v, ()))
                topo_cache[v] = tuple(sorted(neighbors))
            for u in topo_cache[v]:
                if u not in distance:
                    distance[u] = depth + 1
                    queue.append(u)
        if gap_cache_limit > 0:
            gap_cache[start] = distance
            gap_cache.move_to_end(start)
            if len(gap_cache) > gap_cache_limit:
                gap_cache.popitem(last=False)
    buckets = defaultdict(list)
    for u, d in distance.items():
        if min_gap <= d <= max_gap:
            buckets[d].append(u)
    return {d: tuple(sorted(nodes)) for d, nodes in buckets.items()}

def construct_incident_index(od_edges: Iterable[str]) -> dict:
    """Index all locally available OD types by either endpoint.

    The normal SrcIndex only indexes OD sources. That misses forked OD patterns
    such as A->B then A->C if there is no directed return path B -> A.
    The incidence index does not add ghost global edges to mining postings.
    """
    incident = defaultdict(list)
    for od in sorted(od_edges):
        source, target = split_od(od)
        incident[source].append(od)
        if target != source:
            incident[target].append(od)
    return incident
