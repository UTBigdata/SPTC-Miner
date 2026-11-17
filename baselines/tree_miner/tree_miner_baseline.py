#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tree-Miner-style baseline for sequential pattern mining on single-item event sequences.

Why "Tree-Miner-style" and not a byte-for-byte reproduction?
- The paper defines SP-Tree / next_link / SMAL / breadth-first pruning conceptually,
  but does not fully expose all low-level implementation details in the manuscript.
- This script therefore provides a *practical baseline reimplementation* that keeps the
  paper's core spirit: compressed input handling, support-based sequential pattern mining,
  projected-growth mining, optional two-item co-occurrence pruning, and weighted support.

Input supported:
1) A normal CSV/TSV with one row per sequence.
2) Your uploaded compact format where one row contains:
      pattern total_support is_single_group group_count
   Example row:
      "\"A , B , C\" 206 true 1"
   In this case the sequence is [A, B, C] and `total_support` is treated as its weight.

Default behavior on your file:
- `pattern` is split by ' , ' into ordered single-item events.
- `total_support` is treated as multiplicity / sequence weight.
- The mining support of a pattern is the sum of weights of sequences containing it.

Output:
- CSV with columns: pattern, support, length

Usage examples:
  python tree_miner_baseline.py \
      --input part-00000-....csv \
      --output baseline_patterns.csv \
      --min-sup-ratio 0.01 \
      --max-len 4

  python tree_miner_baseline.py \
      --input data.csv \
      --pattern-col pattern \
      --weight-col total_support \
      --min-sup-count 100 \
      --max-len 4
"""

from __future__ import annotations

import argparse
import csv
import math
import re
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple


# =========================
# Data structures
# =========================

@dataclass
class WeightedSequence:
    sid: int
    events: List[str]
    weight: int = 1


@dataclass
class ProjectionEntry:
    sid: int
    start_pos: int  # search starts strictly after the last matched position
    weight: int


@dataclass
class SPNode:
    """A lightweight prefix-sharing tree node used to compactly store sequences.

    This is included to keep the baseline close to the paper's spirit, even though
    the mining itself is done with projected occurrences for clarity and robustness.
    """
    item: Optional[str] = None
    count: int = 0
    children: Dict[str, "SPNode"] = field(default_factory=dict)

    def add_child(self, item: str) -> "SPNode":
        if item not in self.children:
            self.children[item] = SPNode(item=item)
        return self.children[item]


class SPTree:
    def __init__(self) -> None:
        self.root = SPNode(item=None)

    def insert(self, events: Sequence[str], weight: int) -> None:
        node = self.root
        for ev in events:
            node = node.add_child(ev)
            node.count += weight


# =========================
# Parsing utilities
# =========================


def open_text_auto(path: Path):
    """Open a text file with a small list of common encodings for Chinese CSV files."""
    encodings = ['utf-8-sig', 'utf-8', 'gb18030', 'gbk', 'latin1']
    last_err = None
    for enc in encodings:
        try:
            f = path.open('r', encoding=enc, newline='')
            f.read(4096)
            f.seek(0)
            return f
        except UnicodeDecodeError as e:
            last_err = e
    raise UnicodeDecodeError(
        'auto', b'', 0, 1,
        f'Failed to decode {path}. Last error: {last_err}' if last_err else 'Unknown decode error'
    )


def split_events(pattern: str, sep_token: str) -> List[str]:
    pattern = _strip_outer_quotes(pattern.strip())
    if not pattern:
        return []
    if sep_token == 'auto':
        parts = [x.strip() for x in pattern.split(',')]
    else:
        parts = [x.strip() for x in pattern.split(sep_token)]
    return [x for x in parts if x]

COMPACT_ROW_RE = re.compile(r'^(.*)\s+(\d+)\s+(true|false)\s+(\d+)$', re.IGNORECASE)


def _strip_outer_quotes(text: str) -> str:
    text = text.strip()
    if len(text) >= 2 and text[0] == text[-1] == '"':
        return text[1:-1]
    return text


def parse_compact_single_column_file(path: Path, sep_token: str = ' , ') -> List[WeightedSequence]:
    sequences: List[WeightedSequence] = []
    with open_text_auto(path) as f:
        header = next(f, None)
        for sid, raw in enumerate(f, start=1):
            line = raw.strip()
            if not line:
                continue
            line = _strip_outer_quotes(line)
            line = line.replace('\\"', '"')
            line = _strip_outer_quotes(line)
            m = COMPACT_ROW_RE.match(line)
            if not m:
                raise ValueError(
                    f"Failed to parse compact row {sid}: {raw[:200]!r}. "
                    "Expected format: pattern total_support is_single_group group_count"
                )
            pattern, total_support, _is_single_group, _group_count = m.groups()
            pattern = _strip_outer_quotes(pattern)
            events = split_events(pattern, sep_token)
            sequences.append(WeightedSequence(sid=sid, events=events, weight=int(total_support)))
    return sequences


def parse_tabular_sequence_file(
    path: Path,
    pattern_col: str,
    weight_col: Optional[str],
    csv_sep: str,
    sep_token: str,
) -> List[WeightedSequence]:
    sequences: List[WeightedSequence] = []
    with open_text_auto(path) as f:
        reader = csv.DictReader(f, delimiter=csv_sep)
        if pattern_col not in (reader.fieldnames or []):
            raise ValueError(f"Column {pattern_col!r} not found. Available: {reader.fieldnames}")
        for sid, row in enumerate(reader, start=1):
            pattern = row[pattern_col].strip()
            pattern = _strip_outer_quotes(pattern)
            events = split_events(pattern, sep_token)
            weight = int(row[weight_col]) if weight_col else 1
            sequences.append(WeightedSequence(sid=sid, events=events, weight=weight))
    return sequences


def load_sequences(
    input_path: Path,
    pattern_col: Optional[str],
    weight_col: Optional[str],
    csv_sep: str,
    sep_token: str,
) -> List[WeightedSequence]:
    """Auto-detect the uploaded compact format; otherwise parse as normal CSV/TSV."""
    with open_text_auto(input_path) as _f:
        first_line = _f.readline().strip()
    if first_line == 'pattern total_support is_single_group group_count' and pattern_col is None:
        return parse_compact_single_column_file(input_path, sep_token=sep_token)

    # fallback: standard CSV/TSV with explicit columns
    if pattern_col is None:
        with open_text_auto(input_path) as f:
            reader = csv.DictReader(f, delimiter=csv_sep)
            fieldnames = reader.fieldnames or []
        if 'Transfer_Trajectory' in fieldnames:
            pattern_col = 'Transfer_Trajectory'
        else:
            raise ValueError(
                "This file is not the compact single-column format, and no --pattern-col was provided. "
                f"Available columns: {fieldnames}"
            )
    return parse_tabular_sequence_file(
        input_path,
        pattern_col=pattern_col,
        weight_col=weight_col,
        csv_sep=csv_sep,
        sep_token=sep_token,
    )


# =========================
# Core mining helpers
# =========================


def build_two_item_cooccurrence(sequences: Sequence[WeightedSequence]) -> Dict[str, set]:
    """Build a simple ordered 2-item co-occurrence table.

    If item b never appears after item a in any sequence, then a->...->b cannot be a valid
    extension under the paper's ordered co-occurrence idea.
    """
    next_items: Dict[str, set] = defaultdict(set)
    for seq in sequences:
        seen_pairs = set()
        evs = seq.events
        n = len(evs)
        for i in range(n):
            a = evs[i]
            for j in range(i + 1, n):
                b = evs[j]
                if (a, b) not in seen_pairs:
                    seen_pairs.add((a, b))
                    next_items[a].add(b)
    return next_items


def first_level_support(sequences: Sequence[WeightedSequence]) -> Counter:
    """Support = number (or weight sum) of sequences containing the item at least once."""
    cnt: Counter = Counter()
    for seq in sequences:
        for item in set(seq.events):
            cnt[item] += seq.weight
    return cnt


class TreeMinerBaseline:
    def __init__(
        self,
        sequences: Sequence[WeightedSequence],
        min_sup_count: int,
        max_len: int = 5,
        enable_cooc_pruning: bool = True,
    ) -> None:
        self.sequences = list(sequences)
        self.min_sup_count = min_sup_count
        self.max_len = max_len
        self.enable_cooc_pruning = enable_cooc_pruning
        self.total_weight = sum(s.weight for s in self.sequences)
        self.cooc = build_two_item_cooccurrence(self.sequences) if enable_cooc_pruning else defaultdict(set)
        self.tree = SPTree()
        for seq in self.sequences:
            self.tree.insert(seq.events, seq.weight)

    def mine(self) -> List[Tuple[Tuple[str, ...], int]]:
        results: List[Tuple[Tuple[str, ...], int]] = []

        lvl1 = first_level_support(self.sequences)
        frequent_items = sorted([it for it, sup in lvl1.items() if sup >= self.min_sup_count])

        for item in frequent_items:
            pattern = (item,)
            support = lvl1[item]
            results.append((pattern, support))
            if self.max_len > 1:
                proj = self._build_initial_projection(item)
                self._grow(pattern, proj, results)

        results.sort(key=lambda x: (len(x[0]), -x[1], x[0]))
        return results

    def _build_initial_projection(self, item: str) -> List[ProjectionEntry]:
        proj: List[ProjectionEntry] = []
        for seq in self.sequences:
            pos = self._first_occurrence_after(seq.events, -1, item)
            if pos != -1:
                proj.append(ProjectionEntry(sid=seq.sid, start_pos=pos + 1, weight=seq.weight))
        return proj

    def _grow(
        self,
        prefix: Tuple[str, ...],
        projection: List[ProjectionEntry],
        results: List[Tuple[Tuple[str, ...], int]],
    ) -> None:
        if len(prefix) >= self.max_len:
            return

        candidates = self._candidate_supports(prefix, projection)
        for item, support, new_proj in candidates:
            new_pattern = prefix + (item,)
            results.append((new_pattern, support))
            self._grow(new_pattern, new_proj, results)

    def _candidate_supports(
        self,
        prefix: Tuple[str, ...],
        projection: List[ProjectionEntry],
    ) -> List[Tuple[str, int, List[ProjectionEntry]]]:
        """Compute candidate supports from a projected DB.

        Support semantics: a sequence contributes its weight at most once to each extension.
        """
        support_counter: Counter = Counter()
        per_candidate_proj: Dict[str, List[ProjectionEntry]] = defaultdict(list)
        last_item = prefix[-1]

        seq_map = {s.sid: s for s in self.sequences}

        for entry in projection:
            seq = seq_map[entry.sid]
            seen_in_this_seq = set()
            allowed = None
            if self.enable_cooc_pruning and last_item in self.cooc:
                allowed = self.cooc[last_item]

            for pos in range(entry.start_pos, len(seq.events)):
                cand = seq.events[pos]

                if allowed is not None and cand not in allowed:
                    continue

                if cand in seen_in_this_seq:
                    continue

                seen_in_this_seq.add(cand)
                support_counter[cand] += entry.weight
                per_candidate_proj[cand].append(
                    ProjectionEntry(sid=seq.sid, start_pos=pos + 1, weight=entry.weight)
                )

        frequent = []
        for cand, sup in support_counter.items():
            if sup >= self.min_sup_count:
                frequent.append((cand, sup, per_candidate_proj[cand]))

        frequent.sort(key=lambda x: (-x[1], x[0]))
        return frequent

    @staticmethod
    def _first_occurrence_after(events: Sequence[str], pos: int, item: str) -> int:
        for i in range(pos + 1, len(events)):
            if events[i] == item:
                return i
        return -1


# =========================
# CLI / output
# =========================


def format_pattern(pattern: Sequence[str], joiner: str = ' , ') -> str:
    return joiner.join(pattern)


def write_results(path: Path, patterns: Sequence[Tuple[Tuple[str, ...], int]]) -> None:
    with path.open('w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['pattern', 'support', 'length'])
        for patt, sup in patterns:
            writer.writerow([format_pattern(patt), sup, len(patt)])


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description='Tree-Miner-style weighted sequential pattern mining baseline')
    p.add_argument('--input', required=True, help='Input CSV/TSV path')
    p.add_argument('--output', required=True, help='Output CSV path')
    p.add_argument('--pattern-col', default=None, help='Column holding the sequence string in a normal CSV/TSV; auto-detects Transfer_Trajectory when omitted')
    p.add_argument('--weight-col', default=None, help='Optional weight column in a normal CSV/TSV')
    p.add_argument('--csv-sep', default=',', help='CSV delimiter for normal tabular files, e.g. , or \t')
    p.add_argument('--event-sep', default='auto', help="Separator between events inside a sequence string; use 'auto' to split by comma")
    p.add_argument('--min-sup-ratio', type=float, default=None, help='Minimum support ratio in [0,1]')
    p.add_argument('--min-sup-count', type=int, default=None, help='Minimum support count (overrides ratio if given)')
    p.add_argument('--max-len', type=int, default=4, help='Maximum pattern length to mine')
    p.add_argument('--disable-cooc-pruning', action='store_true', help='Disable ordered 2-item co-occurrence pruning')
    return p.parse_args()


def main() -> None:
    import time

    start_time = time.perf_counter()

    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)

    sequences = load_sequences(
        input_path=input_path,
        pattern_col=args.pattern_col,
        weight_col=args.weight_col,
        csv_sep=args.csv_sep.encode('utf-8').decode('unicode_escape'),
        sep_token=args.event_sep,
    )
    if not sequences:
        raise ValueError('No sequences were loaded from input.')

    total_weight = sum(s.weight for s in sequences)
    if args.min_sup_count is not None:
        min_sup_count = args.min_sup_count
    elif args.min_sup_ratio is not None:
        min_sup_count = max(1, math.ceil(args.min_sup_ratio * total_weight))
    else:
        raise ValueError('Please provide --min-sup-count or --min-sup-ratio')

    miner = TreeMinerBaseline(
        sequences=sequences,
        min_sup_count=min_sup_count,
        max_len=args.max_len,
        enable_cooc_pruning=not args.disable_cooc_pruning,
    )
    patterns = miner.mine()
    write_results(output_path, patterns)

    end_time = time.perf_counter()
    elapsed_time = end_time - start_time

    print(f'Loaded sequences: {len(sequences)}')
    print(f'Total sequence weight: {total_weight}')
    print(f'Min support count: {min_sup_count}')
    print(f'Frequent patterns mined: {len(patterns)}')
    print(f'Runtime: {elapsed_time:.2f} s')
    print(f'Output written to: {output_path}')


if __name__ == '__main__':
    main()
