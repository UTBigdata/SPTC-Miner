'Exact ordered one-off support using original event-position lists.'

from collections import OrderedDict
from typing import Dict, List, Tuple
from .utils import split_od
Pattern = Tuple[str, ...]

def common_sids(pattern: Pattern, inv_keys: dict) -> set:
    """Candidate trajectory-ID intersection, with smallest lists first."""
    sets = []
    for e in pattern:
        current = inv_keys.get(e)
        if not current:
            return set()
        sets.append(current)
    sets.sort(key=len)
    common = set(sets[0])
    for s in sets[1:]:
        common.intersection_update(s)
        if not common:
            break
    return common

def cal_ub(pattern: Pattern, index: dict, inv_keys: dict) -> int:
    """UB = SUM over shared trajectories of MIN element Frequency, NOT final support."""
    total = 0
    for sid in common_sids(pattern, inv_keys):
        total += min(index[od][sid][0] for od in pattern)
    return total

def oneoff_occurrence_count(positions_by_stage: List[Tuple[int, ...]]) -> int:
    """Maximum non-reusing ordered matches; distinct pattern OD types assumed.

    The chronological scan maintains unmatched partial prefixes at each stage.
    Different completed occurrence time intervals may overlap/cross; individual
    original event positions are consumed no more than once.
    No event-level gap/time window constraint is applied.
    """
    k = len(positions_by_stage)
    if k < 1:
        return 0
    if k == 1:
        return len(positions_by_stage[0])
    if k == 2:
        # Sorted position lists: linear-time matching, with the same One-off
        # semantics as the chronological scan. Reject overlapping positions.
        left, right = positions_by_stage
        i = j = unmatched = complete = 0
        while i < len(left) or j < len(right):
            if j == len(right) or (i < len(left) and left[i] < right[j]):
                unmatched += 1
                i += 1
            elif i == len(left) or right[j] < left[i]:
                if unmatched:
                    unmatched -= 1
                    complete += 1
                j += 1
            else:
                raise ValueError('Duplicate original event position across distinct ODs')
        return complete
    events = []
    for stage, positions in enumerate(positions_by_stage):
        events.extend((int(pos), stage) for pos in positions)
    events.sort(key=lambda x: (x[0], x[1]))
    # The same event position cannot correspond to two distinct OD types.
    if any(events[i][0] == events[i - 1][0] for i in range(1, len(events))):
        raise ValueError('Duplicate original event position across distinct ODs')
    partial = [0] * (k - 1)
    complete = 0
    for _, stage in events:
        if stage == 0:
            partial[0] += 1
        elif partial[stage - 1] > 0:
            partial[stage - 1] -= 1
            if stage == k - 1:
                complete += 1
            else:
                partial[stage] += 1
    return complete

def cal_oneoff_sup(pattern: Pattern, index: dict, inv_keys: dict,
                   cache: OrderedDict, cache_limit: int) -> Tuple[int, Dict[str, int]]:
    """Exact One-off support and H[sid] contributions; paper Algorithm 5."""
    if pattern in cache:
        cache.move_to_end(pattern)
        return cache[pattern]
    support = 0
    H = {}
    for sid in common_sids(pattern, inv_keys):
        stage_positions = [index[e][sid][1] for e in pattern]
        count = oneoff_occurrence_count(stage_positions)
        if count > 0:
            H[sid] = count
            support += count
    if cache_limit > 0:
        cache[pattern] = (support, H)
        cache.move_to_end(pattern)
        if len(cache) > cache_limit:
            cache.popitem(last=False)
    return support, H
