'Topology-constrained prefix growth and position-aware local support.'

from collections import OrderedDict
from typing import Iterable, Tuple
from .utils import split_od
from .index import construct_local_tpgi
from .topology import gen_gap_succ, construct_incident_index
from .support import oneoff_occurrence_count

def mine_one_partition(pid: int, postings: Iterable[Tuple[str, str, int, list]],
                       global_adj: dict, minsup: int, min_gap: int, max_gap: int,
                       max_pattern_length: int = 0, support_cache_limit: int = 2000,
                       gap_cache_limit: int = 1000, candidate_topology: str = 'shared',
                       local_min_support: int = None, candidate_counter=None):
    """Same SPTC output and One-off semantics, with lower repeated Python work.

    The original prefix support H is an upper bound for the support of any
    extension. Therefore, for beta=prefix+(e,), sum_sid min(H[sid], f_sid(e))
    safely bounds exact One-off support of beta. This eliminates repeated
    full-pattern set intersections and min-frequency scans.

    The prefix growth is a tree: once each parent deduplicates its OD choices,
    each ordered tuple beta has exactly one parent. Thus no global `visited`
    set is needed and the original support LRU has no cache hits.
    """
    if candidate_topology not in ('shared', 'directed'):
        raise ValueError('candidate_topology must be "shared" or "directed"')
    if local_min_support is None:
        local_min_support = minsup
    if not (1 <= local_min_support <= minsup):
        raise ValueError('Require 1 <= local_min_support <= global minsup')

    W, index, inv_keys, graph, src_index = construct_local_tpgi(
        postings, local_min_support)
    if not W:
        return
    incident_index = (construct_incident_index(W)
                      if candidate_topology == 'shared' else {})
    endpoints = {e: split_od(e) for e in W}
    gap_cache = OrderedDict()
    topo_cache = {}
    # A successor set depends on the last OD (not on any earlier prefix).
    # The bounded cache also avoids materializing every allowed successor list.
    successor_cache = OrderedDict()
    candidates_seen = 0
    pid_flag = 1 << int(pid)

    def eligible_next(last_od):
        if gap_cache_limit > 0 and last_od in successor_cache:
            successor_cache.move_to_end(last_od)
            return successor_cache[last_od]
        source, last_target = endpoints[last_od]
        buckets = gen_gap_succ(graph, global_adj, last_target,
                               min_gap, max_gap, gap_cache, topo_cache,
                               gap_cache_limit)
        options = set()
        for gap in range(min_gap, max_gap + 1):
            for station in buckets.get(gap, ()):
                options.update(src_index.get(station, ()))
        if candidate_topology == 'shared':
            options.update(incident_index.get(source, ()))
            options.update(incident_index.get(last_target, ()))
        ans = tuple(sorted(options))
        if gap_cache_limit > 0:
            successor_cache[last_od] = ans
            successor_cache.move_to_end(last_od)
            if len(successor_cache) > gap_cache_limit:
                successor_cache.popitem(last=False)
        return ans

    def grow(prefix, H):
        nonlocal candidates_seen
        for sid, count in H.items():
            yield ((prefix, sid), (int(count), pid_flag))
        if max_pattern_length and len(prefix) >= max_pattern_length:
            return
        for e in eligible_next(prefix[-1]):
            if e in prefix:
                continue  # preserve no-duplicate-OD-types policy
            beta = prefix + (e,)
            candidates_seen += 1
            # H only includes sequences with an ORDERED match of prefix.
            # Exact beta matches cannot occur on any other CardID.
            postings_e = index[e]
            upper = 0
            possible_sids = []
            for sid, parent_occ in H.items():
                item = postings_e.get(sid)
                if item is not None:
                    upper += min(parent_occ, item[0])
                    possible_sids.append(sid)
            if upper < local_min_support:
                continue
            new_H = {}
            new_sup = 0
            for sid in possible_sids:
                stage_positions = [index[od][sid][1] for od in beta]
                occ = oneoff_occurrence_count(stage_positions)
                if occ:
                    new_H[sid] = occ
                    new_sup += occ
            if new_sup >= local_min_support:
                yield from grow(beta, new_H)

    for e in sorted(W):
        candidates_seen += 1   # length-1 root, same candidate-count semantics
        H = {sid: record[0] for sid, record in index[e].items()}
        yield from grow((e,), H)
    # Aggregate once per Spark task instead of one Python->Spark add per candidate.
    if candidate_counter is not None:
        candidate_counter.add(candidates_seen)
