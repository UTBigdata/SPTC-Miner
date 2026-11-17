#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Input/output adapter for the USER-SUPPLIED tree_miner_baseline.py.

THE MINER'S ALGORITHM IS NOT MODIFIED.  This program imports and uses the
original TreeMinerBaseline exactly as supplied, including its original SP-Tree,
projection construction, co-occurrence pruning and recursive mining routine.

CAUTION: The supplied algorithm is explicitly a 'Tree-Miner-style' Python
reimplementation; it is not a verified reproduction of Rizvee et al.'s
SP-Tree/next_link/SMAL/breadth-first original Tree-Miner implementation.

Input : HDFS Parquet Transfer_TR_Chronological {CardID, TR: array<string>}
Output: HDFS CSV, original source columns: pattern,support,length
Support: 1 per original passenger trajectory, preserves order/repeated ODs.
Timing: wall-clock from program startup through Spark stop (and /usr/bin/time).
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from collections import Counter
from contextlib import contextmanager

PROCESS_START = time.perf_counter()

# This import is the UNCHANGED user's original algorithm, not an optimized copy.
from tree_miner_baseline import WeightedSequence, TreeMinerBaseline, format_pattern


@contextmanager
def measure(label, timings):
    t0 = time.perf_counter()
    try:
        yield
    finally:
        dt = time.perf_counter() - t0
        timings.append((label, dt))
        print('[TIME] {}: {:.3f} s'.format(label, dt), flush=True)


def read_parquet_as_original_sequences(spark, parquet_path):
    """ONLY change of input: one chronological TR row -> WeightedSequence(...,1)."""
    from pyspark.sql.types import ArrayType, StringType

    df = spark.read.parquet(parquet_path)
    required = {'CardID', 'TR'}
    if not required.issubset(df.columns):
        raise ValueError('Expected CardID,TR; present: {}'.format(df.columns))
    t = df.schema['TR'].dataType
    if not isinstance(t, ArrayType) or not isinstance(t.elementType, StringType):
        raise TypeError('Expected TR: array<string>, got {}'.format(t.simpleString()))

    sequences = []
    card_ids = set()
    for record in df.select('CardID', 'TR').toLocalIterator():
        card_id, tr = record['CardID'], record['TR']
        if card_id is None or tr is None or len(tr) == 0:
            raise ValueError('Missing CardID or empty TR at record {}'.format(len(sequences)))
        if card_id in card_ids:
            raise ValueError('Duplicate CardID {}, cannot guarantee one sequence per ID'.format(card_id))
        card_ids.add(card_id)
        if any(x is None or not isinstance(x, str) or not x.strip() for x in tr):
            raise ValueError('Null/empty OD in CardID {}'.format(card_id))
        # Do NOT sort, deduplicate, compress, cluster, or expand events.
        sequences.append(WeightedSequence(sid=len(sequences), events=list(tr), weight=1))

    if not sequences:
        raise ValueError('Transfer_TR_Chronological contains no sequences')
    return sequences


def save_results(spark, patterns, output):
    """Original output format: pattern,support,length, written to HDFS by Spark."""
    from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType

    schema = StructType([
        StructField('pattern', StringType(), False),
        StructField('support', LongType(), False),
        StructField('length', IntegerType(), False),
    ])
    # Original algorithm already sorts its results; no extra mining/optimization.
    formatted = [(format_pattern(pattern), int(support), len(pattern))
                 for pattern, support in patterns]
    result_df = spark.createDataFrame(formatted, schema=schema)
    result_df.write.mode('overwrite').option('header', 'true').csv(output)


def get_parser():
    p = argparse.ArgumentParser(
        description='Run the EXACT user-supplied Tree-Miner-style Python core on chronological Parquet')
    p.add_argument('--input', required=False, help='HDFS Transfer_TR_Chronological parquet')
    p.add_argument('--output', required=False, help='HDFS directory of CSV outputs')
    p.add_argument('--min-sup-count', type=int, default=200)
    p.add_argument('--max-len', type=int, default=4)
    p.add_argument('--disable-cooc-pruning', action='store_true',
                   help='Original option, default is COOC pruning enabled')
    p.add_argument('--self-test', action='store_true',
                   help='Test original baseline function without starting Spark')
    return p


def self_test():
    sequences = [
        WeightedSequence(sid=0, events=['A', 'B', 'A'], weight=1),
        WeightedSequence(sid=1, events=['B', 'A'], weight=1),
        WeightedSequence(sid=2, events=['A', 'C'], weight=1),
    ]
    actual = dict(TreeMinerBaseline(sequences, min_sup_count=2, max_len=3).mine())
    assert actual[('A',)] == 3
    assert actual[('B',)] == 2
    assert actual[('B', 'A')] == 2
    print('SELF_TEST: PASS (unchanged original algorithm imported)', flush=True)


def run(args):
    if args.self_test:
        self_test()
        return
    if not args.input or not args.output:
        raise ValueError('Both --input and --output are required unless --self-test is used')
    if args.min_sup_count < 1 or args.max_len < 1:
        raise ValueError('min-sup-count and max-len must be >= 1')

    # I/O only: Spark is not used in mining or support computation.
    from pyspark.sql import SparkSession
    spark = None
    timings = []
    status = 'FAILED'
    sequence_count = None
    pattern_count = None
    pattern_lengths = None
    print('ORIGINAL_USER_SOURCE:', os.path.abspath(sys.modules['tree_miner_baseline'].__file__), flush=True)
    print('INPUT_PARQUET:', args.input, flush=True)
    print('OUTPUT_CSV:', args.output, flush=True)
    print('ALGORITHM: UNMODIFIED user-supplied TreeMinerBaseline; NOT certified author implementation', flush=True)
    print('MIN_SUPPORT:', args.min_sup_count, 'MAX_LENGTH:', args.max_len, flush=True)
    try:
        with measure('01 Start SparkSession (HDFS I/O only)', timings):
            spark = SparkSession.builder.appName('Original-TreeMiner-Python-TR-Adapter').getOrCreate()
            spark.sparkContext.setLogLevel('WARN')

        with measure('02 Read Transfer_TR_Chronological Parquet into Python', timings):
            sequences = read_parquet_as_original_sequences(spark, args.input)
            sequence_count = len(sequences)
            print('ORIGINAL_SEQUENCES:', sequence_count, flush=True)

        with measure('03 Construct original TreeMinerBaseline (UNCHANGED)', timings):
            miner = TreeMinerBaseline(
                sequences=sequences,
                min_sup_count=args.min_sup_count,
                max_len=args.max_len,
                enable_cooc_pruning=not args.disable_cooc_pruning)

        with measure('04 Run original TreeMinerBaseline.mine() (UNCHANGED)', timings):
            patterns = miner.mine()
            pattern_count = len(patterns)
            pattern_lengths = dict(sorted(Counter(len(p) for p, _ in patterns).items()))
            print('FREQUENT_PATTERNS:', pattern_count, flush=True)
            print('PATTERNS_BY_LENGTH:', pattern_lengths, flush=True)

        with measure('05 Save original-format results to HDFS CSV', timings):
            save_results(spark, patterns, args.output)
        status = 'SUCCESS'
    finally:
        if spark is not None:
            with measure('06 Stop SparkSession', timings):
                spark.stop()
        total = time.perf_counter() - PROCESS_START
        print('\n========== ORIGINAL PYTHON TREE-MINER BASELINE ==========', flush=True)
        for name, seconds in timings:
            print('{}: {:.3f} s'.format(name, seconds), flush=True)
        print('STATUS:', status, flush=True)
        print('ORIGINAL_SEQUENCES:', sequence_count, flush=True)
        print('FREQUENT_PATTERNS:', pattern_count, flush=True)
        print('PATTERNS_BY_LENGTH:', pattern_lengths, flush=True)
        print('TOTAL_INTERNAL_SECONDS: {:.3f}'.format(total), flush=True)
        print('========================================================\n', flush=True)


if __name__ == '__main__':
    run(get_parser().parse_args())
