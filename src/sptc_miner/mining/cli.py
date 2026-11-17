'PySpark ingestion, distributed local mining and global pattern aggregation.'

import argparse
import time
from collections import defaultdict
from .growth import mine_one_partition
from .reducers import dedup_one_sid, combine_distinct_sids

def make_arg_parser():
    p = argparse.ArgumentParser(description='SPTC-Miner: exact position-aware PySpark miner')
    p.add_argument('--root', default='hdfs://namenode.example.org:9000/project/data/201901/Withpositions/CoverageFixed_m64_sup200',
                   help='HDFS root of outputs from code1 + code2')
    p.add_argument('--input', default=None, help='Override Parquet mining input')
    p.add_argument('--inputMode', choices=('postings', 'fullU'), default='postings',
                   help='postings=code2 Local_TPGI_Postings; fullU=code2 repartition_complete_U')
    p.add_argument('--minSupport', type=int, required=True,
                   help='Final GLOBAL One-off support threshold')
    p.add_argument('--localMinSupport', type=int, default=None,
                   help='Local threshold, default=minSupport (legacy); set to 1 with '
                        '--inputMode fullU to avoid local-minsup false negatives')
    p.add_argument('--candidateTopology', choices=('shared', 'directed'),
                   default='shared',
                   help='shared (default)=directed reachability OR shared endpoint; '
                        'directed=original directed reachability only')
    p.add_argument('--minGap', type=int, required=True)
    p.add_argument('--maxGap', type=int, required=True)
    p.add_argument('--numPartitions', type=int, required=True,
                   help='Physical partition count m used by code2, NOT cluster count')
    p.add_argument('--preparedMinSupport', type=int, default=200,
                   help='The minsup used to write code2 Local_TPGI_Postings and global adjacency')
    p.add_argument('--topologyMode', choices=('directory', 'counts'), default='directory',
                   help='directory=code2 Global_Topology_Adjacency; counts=derive for current minsup from code1 counts')
    p.add_argument('--adjacency', default=None, help='Override code2 adjacency directory Parquet path')
    p.add_argument('--globalCounts', default=None, help='Override code1 Global_OD_Counts Parquet path')
    p.add_argument('--output', required=True, help='Destination directory for final results')
    p.add_argument('--outputFormat', choices=('csv', 'parquet'), default='csv')
    p.add_argument('--localContribOutput', default=None,
                   help='Optional Parquet output of exact local (pattern, partitionID, CardID, count)')
    p.add_argument('--maxPatternLength', type=int, default=0,
                   help='0=unrestricted as defined in paper; positive value truncates patterns (benchmark only)')
    p.add_argument('--supportCacheSize', type=int, default=2000,
                   help='Compatibility flag; prefix-only cache unnecessary after removing duplicate candidate evaluations')
    p.add_argument('--gapCacheSize', type=int, default=1000)
    p.add_argument('--maxBroadcastEdges', type=int, default=2000000,
                   help='Guard against collecting an oversized global topology onto driver')
    p.add_argument('--master', default=None)
    return p

def main(argv=None):
    args = make_arg_parser().parse_args(argv)
    if args.minSupport <= 0 or args.minGap < 0 or args.maxGap < args.minGap:
        raise ValueError('Require minSupport>0 and 0<=minGap<=maxGap')
    if args.numPartitions <= 0 or args.preparedMinSupport <= 0:
        raise ValueError('Partition count and prepared threshold must be positive')
    if args.supportCacheSize < 0 or args.gapCacheSize < 0 or args.maxPatternLength < 0:
        raise ValueError('Cache limits and maxPatternLength must be nonnegative')
    local_min_support = (args.minSupport if args.localMinSupport is None
                         else args.localMinSupport)
    if not (1 <= local_min_support <= args.minSupport):
        raise ValueError('Require 1 <= localMinSupport <= minSupport')
    if args.inputMode == 'postings' and local_min_support < args.preparedMinSupport:
        raise ValueError('Pre-filtered Local_TPGI_Postings cannot recover local ODs below '
                         'preparedMinSupport={}; use --inputMode fullU instead'
                         .format(args.preparedMinSupport))
    if args.inputMode == 'postings' and args.minSupport != args.preparedMinSupport:
        raise ValueError('Local_TPGI_Postings was filtered with preparedMinSupport={}; '
                         'use --inputMode fullU to change minsup'.format(args.preparedMinSupport))
    if args.topologyMode == 'directory' and args.minSupport != args.preparedMinSupport:
        raise ValueError('Global_Topology_Adjacency was built with preparedMinSupport={}; '
                         'use --topologyMode counts to change minsup'.format(args.preparedMinSupport))

    # Lazy PySpark import makes all core algorithms unit-testable with plain Python.
    from pyspark.sql import SparkSession, functions as F
    from pyspark.sql.types import (StructType, StructField, StringType,
                                   LongType, BooleanType, IntegerType)

    t0 = time.time()
    builder = SparkSession.builder.appName('SPTC-Miner-Positions-OneOff')
    if args.master:
        builder = builder.master(args.master)
    spark = builder.getOrCreate()
    sc = spark.sparkContext
    root = args.root.rstrip('/')
    joinpath = lambda name: root + '/' + name
    input_path = args.input or joinpath('Local_TPGI_Postings' if args.inputMode == 'postings'
                                       else 'repartition_complete_U')
    adjacency_path = args.adjacency or joinpath('Global_Topology_Adjacency')
    counts_path = args.globalCounts or joinpath('Global_OD_Counts')

    print('Input mode: {}\nInput: {}\nTopology mode: {}\nCandidate topology: {}'
          '\nGlobal minsup={} local minsup={} gap=[{},{}] m={}'
          .format(args.inputMode, input_path, args.topologyMode, args.candidateTopology,
                  args.minSupport, local_min_support,
                  args.minGap, args.maxGap, args.numPartitions))

    # Global topology: adjacency is global and read-only; it NEVER joins local I.
    if args.topologyMode == 'directory':
        edges_df = (spark.read.parquet(adjacency_path)
                    .select(F.col('A').cast('string').alias('A'), F.explode('Neighbors').alias('B')))
    else:
        global_counts = spark.read.parquet(counts_path)
        edges_df = (global_counts.filter(F.col('GlobalFrequency') >= args.minSupport)
                    .select(F.split('Transfer_Route', ' -> ').getItem(0).alias('A'),
                            F.split('Transfer_Route', ' -> ').getItem(1).alias('B')))
    edge_rows = edges_df.dropDuplicates(['A', 'B']).limit(args.maxBroadcastEdges + 1).collect()
    if len(edge_rows) > args.maxBroadcastEdges:
        raise ValueError('Global topology has too many edges for broadcast; '
                         'increase --maxBroadcastEdges only if driver/executor memory permits')
    adjacency = defaultdict(set)
    for row in edge_rows:
        if row['A'] is None or row['B'] is None:
            raise ValueError('Global adjacency contains a null station')
        adjacency[str(row['A'])].add(str(row['B']))
    global_adj = {k: tuple(sorted(v)) for k, v in adjacency.items()}
    b_adj = sc.broadcast(global_adj)
    candidate_counter = sc.accumulator(0)
    print('Broadcast global topology: {} edges, {} source stations'.format(
        len(edge_rows), len(global_adj)))

    if args.inputMode == 'postings':
        flat = (spark.read.parquet(input_path)
                .select(F.col('partitionID').cast('int').alias('partitionID'),
                        F.col('CardID').cast('string').alias('CardID'),
                        F.col('od').cast('string').alias('od'),
                        F.col('Frequency').cast('long').alias('Frequency'),
                        F.col('Positions').alias('Positions')))
    else:
        flat = (spark.read.parquet(input_path)
                .select('partitionID', 'CardID', F.explode('U').alias('_item'))
                .select(F.col('partitionID').cast('int').alias('partitionID'),
                        F.col('CardID').cast('string').alias('CardID'),
                        F.col('_item.od').alias('od'),
                        F.col('_item.freq').cast('long').alias('Frequency'),
                        F.col('_item.Pos').alias('Positions')))

    # Ensure all postings sharing the same logical physical partition arrive in
    # precisely one Spark task, with no groupByKey(list) memory blow-up.
    def row_to_keyvalue(row):
        pid = int(row['partitionID'])
        if pid < 0 or pid >= args.numPartitions:
            raise ValueError('Invalid partitionID {} for m={}'.format(pid, args.numPartitions))
        return (pid, (row['CardID'], row['od'], row['Frequency'], row['Positions']))

    organized = flat.rdd.map(row_to_keyvalue).partitionBy(
        args.numPartitions, lambda physical_pid: int(physical_pid))

    def run_local_partition(pid, iterator):
        def posting_stream():
            for logical_pid, record in iterator:
                if int(logical_pid) != pid:
                    raise ValueError('Partitioner mismatch: Spark task {}, logical {}'.format(
                        pid, logical_pid))
                yield record
        yield from mine_one_partition(pid, posting_stream(), b_adj.value,
                                      args.minSupport, args.minGap, args.maxGap,
                                      args.maxPatternLength, args.supportCacheSize,
                                      args.gapCacheSize, args.candidateTopology,
                                      local_min_support, candidate_counter)

    # A single RDD action will trigger local mining and final global aggregation.
    local_contributions = organized.mapPartitionsWithIndex(run_local_partition)

    if args.localContribOutput:
        from pyspark import StorageLevel
        local_contributions.persist(StorageLevel.MEMORY_AND_DISK)
        local_schema = StructType([
            StructField('pattern', StringType(), False),
            StructField('partitionID', IntegerType(), False),
            StructField('CardID', StringType(), False),
            StructField('occ', LongType(), False)
        ])
        local_rows = local_contributions.map(lambda kv: (
            ' , '.join(kv[0][0]), (kv[1][1].bit_length() - 1), kv[0][1], int(kv[1][0])))
        spark.createDataFrame(local_rows, schema=local_schema).write.mode('overwrite').parquet(
            args.localContribOutput)

    # Same (pattern, original CardID) from different partitions is counted ONCE.
    by_pattern_sid = local_contributions.reduceByKey(dedup_one_sid)
    totals = (by_pattern_sid
              .map(lambda kv: (kv[0][0], kv[1]))
              .reduceByKey(combine_distinct_sids)
              .filter(lambda kv: kv[1][0] >= args.minSupport))

    result_schema = StructType([
        StructField('pattern', StringType(), False),
        StructField('total_support', LongType(), False),
        StructField('is_single_group', BooleanType(), False),
        StructField('group_count', IntegerType(), False),
        StructField('pattern_length', IntegerType(), False),
    ])
    def to_result_row(kv):
        pattern, (support, group_mask) = kv
        n_groups = bin(group_mask).count('1')
        return (' , '.join(pattern), int(support), n_groups == 1,
                n_groups, len(pattern))
    result_rows = totals.map(to_result_row)
    result_df = spark.createDataFrame(result_rows, schema=result_schema)
    if args.outputFormat == 'csv':
        result_df.write.mode('overwrite').option('header', 'true').csv(args.output)
    else:
        result_df.write.mode('overwrite').parquet(args.output)
    print('Wrote final patterns to {} ({})'.format(args.output, args.outputFormat))
    print('Total candidate patterns generated (including length-1 roots, before pruning): {}'.format(
        candidate_counter.value))
    print('Execution time: {:.2f} seconds'.format(time.time() - t0))
    spark.stop()
