#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CTM baseline: K-Means (all transfer ODs) -> ordered groupID sequences ->
Spark MLlib PrefixSpan.  One spark-submit; end-to-end timing.

Input is the ORIGINAL chronological transfer trajectory, not the sorted
frequency summary U or high-frequency feature F:
    .../201902/Withpositions/Transfer_TR_Chronological (Parquet)
        CardID: string, TR: array<string>

K-Means additionally needs geographic OD coordinates.  By default it reads
Stage-1's *complete-OD* spatial table from the same parent directory:
    .../201902/Withpositions/TransferWithLngAndLa (CSV)
        Transfer_Route, lon1, lon2, lat1, lat2
    Also accepts the legacy columns Longitude, Next_longitude,
    Latitude, Next_latitude for compatibility.
Do NOT use Stage-1 center_point here: it covers only the high-frequency F ODs
and would silently omit some of the original TR events.

Clustering is identical in method to the supplied K-Means.py:
    VectorAssembler(center_lon, center_lat) + Spark ML KMeans(k, seed=42)
    group label 'g' + (prediction + 1).
Mining is identical in method to the supplied PrefixSpan baseline:
    PrefixSpan(minSupport = absolute threshold / ORIGINAL sequence count)
    with one singleton itemset per event, in the original event order.

IMPORTANT: no frequency expansion, no One-off, no topology pruning; output
patterns contain group labels, NOT OD strings; support is *sequence support*.

HDFS outputs:
  --output                final CSV: sequence_str,freq
  --mappingOutput         OD -> groupID mapping CSV
  --sequencesOutput       translated CardID,sequence Parquet
All outputs are written before final internal timing stops; use Linux
/usr/bin/time -p to also count spark-submit launcher start/exit.
"""

import time
PROCESS_START = time.perf_counter()
import argparse
import sys
from contextlib import contextmanager
from datetime import datetime

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.fpm import PrefixSpan
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import ArrayType, StringType, DoubleType
from pyspark import StorageLevel

DEFAULT_INPUT = (
    'hdfs://namenode.example.org:9000/project/data/201902/'
    'Withpositions/Transfer_TR_Chronological'
)


def log(msg):
    print(msg, flush=True)


@contextmanager
def measured(name, records):
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        records.append((name, elapsed))
        log('[TIME] {}: {:.3f} s'.format(name, elapsed))


def run(args):
    if args.minsup_abs < 1:
        raise ValueError('minsup_abs must be >= 1')
    if args.k < 2:
        raise ValueError('k must be >= 2')
    if args.shufflePartitions is not None and args.shufflePartitions < 1:
        raise ValueError('shufflePartitions must be >= 1')

    directory = args.input.rstrip('/').rsplit('/', 1)[0]
    coordinate_path = args.coordinates or (directory + '/TransferWithLngAndLa')
    mapping_output = args.mappingOutput or (args.output.rstrip('/') + '_KMeans_OD_Mapping')
    sequences_output = args.sequencesOutput or (args.output.rstrip('/') + '_KMeans_Sequences')
    if len({args.output.rstrip('/'), mapping_output.rstrip('/'),
            sequences_output.rstrip('/')}) != 3:
        raise ValueError('output, mappingOutput, and sequencesOutput must differ')

    steps = []
    spark = None
    cached = []
    status = 'FAILED'
    total_seqs = None
    total_patterns = None
    try:
        with measured('01 SparkSession startup', steps):
            builder = SparkSession.builder.appName('CTM-KMeans-then-MLlib-PrefixSpan')
            if args.shufflePartitions is not None:
                builder = builder.config('spark.sql.shuffle.partitions', str(args.shufflePartitions))
            spark = builder.getOrCreate()
            spark.sparkContext.setLogLevel('WARN')

        log('\n=== CTM baseline: K-Means -> PrefixSpan ===')
        log('TR input          : ' + args.input)
        log('OD coordinate CSV : ' + coordinate_path)
        log('Output patterns   : ' + args.output)
        log('Mapping output    : ' + mapping_output)
        log('Sequence output   : ' + sequences_output)
        log('K-Means k/seed    : {} / {}'.format(args.k, args.seed))
        log('minsup_abs        : {}'.format(args.minsup_abs))
        log('Algorithm          : native K-Means + native MLlib PrefixSpan')
        log('Support            : native SEQUENCE support')
        log('Event expansion    : disabled')

        # I. Original ordered sequences: one row per CardID, original repeated
        #    ODs and their chronological ordering are retained.
        with measured('02 Read/check/cache chronological TR', steps):
            raw = spark.read.parquet(args.input)
            required = {'CardID', 'TR'}
            missing = required.difference(raw.columns)
            if missing:
                raise ValueError('TR input missing columns: {}'.format(sorted(missing)))
            tr_type = raw.schema['TR'].dataType
            if not (isinstance(tr_type, ArrayType) and
                    isinstance(tr_type.elementType, StringType)):
                raise TypeError('TR must be array<string>, got ' + tr_type.simpleString())

            tr = (raw.select(F.col('CardID').cast('string').alias('CardID'),
                             F.col('TR'))
                  .where(F.col('CardID').isNotNull() &
                         F.col('TR').isNotNull() & (F.size('TR') > 0))
                  .persist(StorageLevel.MEMORY_AND_DISK))
            cached.append(tr)
            total_seqs = tr.count()
            if total_seqs == 0:
                raise ValueError('No nonempty TR sequences found')
            # Null / blank OD events invalidate the exact one-event/one-label map.
            invalid = (tr.select(F.explode('TR').alias('OD'))
                       .where(F.col('OD').isNull() | (F.length(F.trim('OD')) == 0))
                       .limit(1).count())
            if invalid:
                raise ValueError('TR contains null or blank OD events')
            log('Original nonempty TR sequences: {}'.format(total_seqs))

        # II. Build one coordinate vector for each UNIQUE OD in the input TR.
        #     Stage-1 TransferWithLngAndLa is produced from ALL original events,
        #     unlike the high-frequency-only center_point dataset.
        with measured('03 Read complete OD coordinates + coverage validation', steps):
            coordinate_raw = (spark.read.option('header', 'true')
                              .option('encoding', 'UTF-8').csv(coordinate_path))
                    #   Transfer_Route, lon1, lon2, lat1, lat2
            # The earlier CTM script mistakenly expected raw-event column names.
            # Accept either schema while keeping the exact same center-point
            # features and the unchanged K-Means / PrefixSpan algorithms.
            available = {name.strip().lower(): name for name in coordinate_raw.columns}

            def geo_column(*candidates):
                for candidate in candidates:
                    column_name = available.get(candidate.lower())
                    if column_name is not None:
                        return F.col('`' + column_name.replace('`', '``') + '`')
                raise ValueError(
                    'OD coordinate CSV lacks one of {}. Actual columns: {}'.format(
                        candidates, coordinate_raw.columns))

            log('OD coordinate columns: {}'.format(coordinate_raw.columns))
            coord = (coordinate_raw.select(
                F.trim(geo_column('Transfer_Route')).alias('OD'),
                geo_column('lon1', 'Longitude').cast(DoubleType()).alias('lon1'),
                geo_column('lon2', 'Next_longitude').cast(DoubleType()).alias('lon2'),
                geo_column('lat1', 'Latitude').cast(DoubleType()).alias('lat1'),
                geo_column('lat2', 'Next_latitude').cast(DoubleType()).alias('lat2'))
                .where(F.col('OD').isNotNull() & (F.length('OD') > 0))
                .dropna(subset=['lon1', 'lon2', 'lat1', 'lat2'])
                # Original Stage-1 output is unique OD; preserve its center-point
                # representation, with no OD-frequency weighting in Spark KMeans.
                .dropDuplicates(['OD'])
                .withColumn('center_lon', (F.col('lon1') + F.col('lon2')) / F.lit(2.0))
                .withColumn('center_lat', (F.col('lat1') + F.col('lat2')) / F.lit(2.0)))

            observed_ods = tr.select(F.explode('TR').alias('OD')).distinct()
            missing_ods = observed_ods.join(coord.select('OD'), 'OD', 'left_anti')
            examples = [r['OD'] for r in missing_ods.limit(10).collect()]
            if examples:
                raise ValueError(
                    'K-Means coordinate table does not cover all ODs in TR. '
                    'Examples: {}. Use the COMPLETE Stage-1 TransferWithLngAndLa, '
                    'not high-frequency center_point.'.format(examples))

            od_points = (observed_ods.join(coord, 'OD', 'inner')
                         .persist(StorageLevel.MEMORY_AND_DISK))
            cached.append(od_points)
            number_od = od_points.count()
            if number_od < args.k:
                raise ValueError('Only {} distinct ODs, cannot use k={}'.format(number_od, args.k))
            log('Unique ODs clustered (all TR OD types): {}'.format(number_od))

        # III. Exactly original Spark K-Means method, same features and default
        #      fitting parameters, with seed=42 and k configurable.
        with measured('04 Spark ML K-Means model fitting', steps):
            feature_df = VectorAssembler(
                inputCols=['center_lon', 'center_lat'],
                outputCol='features').transform(od_points)
            kmeans = KMeans(
                k=args.k, seed=args.seed, featuresCol='features',
                predictionCol='groupID_raw')
            model = kmeans.fit(feature_df)

        with measured('05 Generate/save complete OD -> groupID mapping', steps):
            predictions = model.transform(feature_df)
            od_groups = (predictions.withColumn(
                'groupID', F.concat(F.lit('g'),
                                    (F.col('groupID_raw') + F.lit(1)).cast('string')))
                         .select('OD', 'groupID', 'lon1', 'lat1', 'lon2', 'lat2',
                                 'center_lon', 'center_lat')
                         .persist(StorageLevel.MEMORY_AND_DISK))
            cached.append(od_groups)
            count_labels = od_groups.count()
            if count_labels != number_od:
                raise AssertionError('OD label count != number of original ODs')
            (od_groups.orderBy('OD').coalesce(1).write.mode('overwrite')
             .option('header', 'true').csv(mapping_output))
            log('OD mapping records: {}'.format(count_labels))

        # IV. Replace each event OD with its K-Means group label.  The position
        #     column in posexplode and sort_array make the temporal order exact;
        #     if two neighboring ODs map to the same cluster, BOTH labels stay.
        with measured('06 Rewrite ordered TR, cache and save group sequences', steps):
            event_rows = tr.select('CardID', F.posexplode('TR').alias('pos', 'OD'))
            mapped_events = event_rows.join(
                F.broadcast(od_groups.select('OD', 'groupID')),
                on='OD', how='left')
            if mapped_events.where(F.col('groupID').isNull()).limit(1).count():
                raise RuntimeError('Unexpected unlabeled OD after K-Means join')

            groups = (mapped_events.groupBy('CardID')
                      .agg(F.sort_array(F.collect_list(
                          F.struct(F.col('pos'), F.col('groupID')))).alias('ordered')))
            sequences = (groups.select(
                'CardID',
                F.expr('transform(ordered, x -> array(x.groupID))').alias('sequence'))
                .persist(StorageLevel.MEMORY_AND_DISK))
            cached.append(sequences)
            reconstructed_count = sequences.count()
            if reconstructed_count != total_seqs:
                raise ValueError('CardID sequences changed from {} to {} during '
                                 'OD-label reconstruction; check duplicate CardIDs'
                                 .format(total_seqs, reconstructed_count))
            sequences.write.mode('overwrite').parquet(sequences_output)
            log('Chronological cluster-ID sequences: {}'.format(reconstructed_count))

        # V. Use stock Spark MLlib PrefixSpan; native sequence support, not One-off.
        min_relative = args.minsup_abs / float(total_seqs)
        log('PrefixSpan relative minSupport: {:.16g}'.format(min_relative))
        if args.minsup_abs > total_seqs:
            log('[WARN] absolute minsup > original TR count: output will be empty')
            with measured('07 Write empty pattern output', steps):
                empty = spark.createDataFrame([], 'sequence_str string, freq long')
                empty.write.mode('overwrite').option('header', 'true').csv(args.output)
            total_patterns = 0
        else:
            with measured('07 Native MLlib PrefixSpan mining/materialization', steps):
                ps = PrefixSpan(minSupport=min_relative, sequenceCol='sequence',
                                maxPatternLength=args.maxPatternLength)
                freq_seq = ps.findFrequentSequentialPatterns(
                    sequences.select('sequence'))
                freq_seq = freq_seq.persist(StorageLevel.MEMORY_AND_DISK)
                cached.append(freq_seq)
                total_patterns = freq_seq.count()
                log('Frequent groupID patterns: {}'.format(total_patterns))

            with measured('08 Sort and write final patterns to HDFS', steps):
                formatted = (freq_seq.withColumn(
                    'sequence_str',
                    F.expr('concat_ws(\',\', transform(sequence, x -> x[0]))'))
                    .select('sequence_str', 'freq'))
                (formatted.orderBy(F.col('freq').desc())
                 .write.mode('overwrite').option('header', 'true').csv(args.output))

        status = 'SUCCESS'
    finally:
        # The total ends AFTER SparkSession stop, not after PrefixSpan call.
        for df in reversed(cached):
            try:
                df.unpersist(blocking=False)
            except Exception as ex:
                log('[WARN] Could not release cache: {}'.format(ex))
        if spark is not None:
            with measured('09 Stop SparkSession', steps):
                spark.stop()
        total_elapsed = time.perf_counter() - PROCESS_START
        log('\n========== CTM END-TO-END TIMING ==========')
        for name, seconds in steps:
            log('{:<61} {:>10.3f} s'.format(name, seconds))
        log('STATUS: {}'.format(status))
        log('ORIGINAL_SEQUENCES: {}'.format(total_seqs))
        log('FREQUENT_PATTERNS: {}'.format(total_patterns))
        log('TOTAL_INTERNAL_SECONDS: {:.3f}'.format(total_elapsed))
        log('TOTAL_INTERNAL_MINUTES: {:.4f}'.format(total_elapsed / 60.0))
        log('===========================================\n')


def parse_args():
    parser = argparse.ArgumentParser(
        description='CTM K-Means + MLlib PrefixSpan on chronological TR, full timing')
    parser.add_argument('--input', default=DEFAULT_INPUT,
                        help='Parquet Transfer_TR_Chronological directory')
    parser.add_argument('--coordinates', default=None,
                        help='Complete OD-coordinate CSV; default sibling TransferWithLngAndLa')
    parser.add_argument('--output', required=True,
                        help='Final PrefixSpan groupID-pattern CSV directory')
    parser.add_argument('--mappingOutput', default=None,
                        help='OD -> KMeans groupID CSV, default <output>_KMeans_OD_Mapping')
    parser.add_argument('--sequencesOutput', default=None,
                        help='Rewritten chronological groupID sequences Parquet, '
                             'default <output>_KMeans_Sequences')
    parser.add_argument('--k', type=int, default=64,
                        help='Number of K-Means clusters (old program used 64)')
    parser.add_argument('--seed', type=int, default=42,
                        help='Spark K-Means seed (old program used 42)')
    parser.add_argument('--maxPatternLength', type=int, default=4,
                        help='Maximum group-label pattern length (paper setting: 4)')
    parser.add_argument('--minsup_abs', type=int, required=True,
                        help='Minimum number of distinct original CardID sequences')
    parser.add_argument('--shufflePartitions', type=int, default=None,
                        help='Optional spark.sql.shuffle.partitions override')
    return parser.parse_args()


if __name__ == '__main__':
    run(parse_args())
