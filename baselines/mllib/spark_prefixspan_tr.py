#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
from datetime import datetime

SCRIPT_START = time.perf_counter()
SCRIPT_START_WALL = datetime.now().astimezone()

import argparse
from contextlib import contextmanager

from pyspark.ml.fpm import PrefixSpan
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType


DEFAULT_INPUT = (
    "hdfs://namenode.example.org:9000/project/data/201901And02/"
    "positions_v2/Transfer_TR_Chronological"
)


@contextmanager
def timed_stage(stage_times, label):
    """Measure actual wall-clock elapsed time for a pipeline stage."""
    started = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - started
        stage_times.append((label, elapsed))
        print(f"[TIME] {label}: {elapsed:.3f} seconds", flush=True)


def print_timing_summary(stage_times, status, original_sequences=None):
    ended_at = datetime.now().astimezone()
    full_elapsed = time.perf_counter() - SCRIPT_START

    print("\n" + "=" * 68, flush=True)
    print("MLlib PrefixSpan TIMING SUMMARY (wall-clock)", flush=True)
    print("=" * 68, flush=True)
    print(f"Status                   : {status}", flush=True)
    print(f"Script timer started     : {SCRIPT_START_WALL.isoformat(timespec='seconds')}", flush=True)
    print(f"Spark stopped / finished : {ended_at.isoformat(timespec='seconds')}", flush=True)
    if original_sequences is not None:
        print(f"Original input sequences : {original_sequences}", flush=True)
    for label, elapsed in stage_times:
        print(f"  {label:<48} {elapsed:>11.3f} s", flush=True)
    print("-" * 68, flush=True)
    print(f"TOTAL INTERNAL ELAPSED   : {full_elapsed:.3f} seconds", flush=True)
    print(f"TOTAL INTERNAL ELAPSED   : {full_elapsed / 60:.3f} minutes", flush=True)
    print("Includes PySpark imports, parameter parsing, SparkSession startup,", flush=True)
    print("input materialization, PrefixSpan, HDFS output write, and spark.stop().", flush=True)
    print("For spark-submit launcher overhead, use /usr/bin/time -p.", flush=True)
    print("=" * 68 + "\n", flush=True)


def main(args):
    stages = []
    spark = None
    status = "FAILED"
    total_sequences = None

    print("========== MLlib PrefixSpan (chronological TR) ==========", flush=True)
    print(f"Input   : {args.input}", flush=True)
    print(f"Output  : {args.output}", flush=True)
    print(f"MinSup  : {args.minsup_abs} original sequences", flush=True)
    print("Support : native sequence-level support; NO event-count expansion", flush=True)

    try:
        if args.minsup_abs <= 0:
            raise ValueError("--minsup_abs must be a positive integer")
        with timed_stage(stages, "01. Create SparkSession"):
            spark = (
                SparkSession.builder
                .appName("Spark-MLlib-PrefixSpan-TR-Chronological-Timed")
                .getOrCreate()
            )
        to_seq_udf = F.udf(
            lambda tr: [[x] for x in tr] if tr is not None else None,
            ArrayType(ArrayType(StringType())),
        )
        seq_to_str_udf = F.udf(
            lambda seq: (
                ",".join(itemset[0] for itemset in seq)
                if seq is not None else None
            ),
            StringType(),
        )
        with timed_stage(stages, "02. Read Parquet metadata and check schema"):
            raw = spark.read.parquet(args.input)
            needed = {"CardID", "TR"}
            missing = needed.difference(raw.columns)
            if missing:
                raise ValueError(
                    "Missing expected Parquet columns: " + ", ".join(sorted(missing))
                )

            tr_type = raw.schema["TR"].dataType
            if not (
                isinstance(tr_type, ArrayType)
                and isinstance(tr_type.elementType, StringType)
            ):
                raise TypeError(
                    "Expected TR: array<string> from Transfer_TR_Chronological, "
                    f"but got {tr_type.simpleString()}"
                )

        with timed_stage(stages, "03. Transform TR (Python UDF) and count sequences"):
            sequences_df = (
                raw
                .where(F.col("TR").isNotNull() & (F.size(F.col("TR")) > 0))
                .select(to_seq_udf(F.col("TR")).alias("sequence"))
            )
            total_sequences = sequences_df.count()

        print(f"Original sequences: {total_sequences}", flush=True)
        if total_sequences == 0:
            raise ValueError("No nonempty TR sequences were found")

        if args.minsup_abs > total_sequences:
            print("[WARN] Absolute minSupport exceeds sequence count; writing empty output.", flush=True)
            with timed_stage(stages, "04. Write empty results to HDFS"):
                empty = spark.createDataFrame([], "sequence_str string, freq long")
                (
                    empty.write
                    .mode("overwrite")
                    .option("header", "true")
                    .csv(args.output)
                )
            status = "SUCCESS (empty result: minsup > sequences)"
            return

        min_support = float(args.minsup_abs) / float(total_sequences)
        print(f"minSupport(relative): {min_support:.16g}", flush=True)
        with timed_stage(stages, "04. Invoke Spark MLlib PrefixSpan"):
            ps = PrefixSpan(
                minSupport=min_support,
                maxPatternLength=4,
                sequenceCol="sequence",
            )
            frequent = ps.findFrequentSequentialPatterns(sequences_df)
        with timed_stage(stages, "05. Format (Python UDF), sort, and write ALL results to HDFS"):
            out_df = (
                frequent
                .withColumn(
                    "sequence_str",
                    seq_to_str_udf(F.col("sequence")),
                )
                .select("sequence_str", "freq")
            )
            (
                out_df.orderBy(F.col("freq").desc())
                .write
                .mode("overwrite")
                .option("header", "true")
                .csv(args.output)
            )

        print(f"[OK] PrefixSpan results completely written to: {args.output}", flush=True)
        status = "SUCCESS"

    finally:
        try:
            if spark is not None:
                with timed_stage(stages, "06. Stop SparkSession"):
                    spark.stop()
        finally:
            print_timing_summary(stages, status, total_sequences)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Run unmodified Spark MLlib PrefixSpan on chronological transfer "
            "sequences and time the full script lifecycle."
        )
    )
    parser.add_argument(
        "--input", default=DEFAULT_INPUT,
        help="HDFS Parquet directory for Transfer_TR_Chronological"
    )
    parser.add_argument(
        "--output", required=True,
        help="HDFS output CSV directory"
    )
    parser.add_argument(
        "--minsup_abs", type=int, required=True,
        help="Minimum number of original sequences containing a pattern"
    )
    main(parser.parse_args())