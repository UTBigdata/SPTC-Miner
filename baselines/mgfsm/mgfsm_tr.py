#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'Documentation for module in baselines/mgfsm/mgfsm_tr.py. Algorithmic behavior is preserved.'

import argparse
import base64
import os
import subprocess
import time

from contextlib import contextmanager
from datetime import datetime


START = time.perf_counter()

DEFAULT_INPUT = (
    "hdfs://namenode.example.org:9000/project/data/201902/"
    "Withpositions/Transfer_TR_Chronological"
)

DEFAULT_OUTPUT = "hdfs://namenode.example.org:9000/project/benchmark/mgfsm_output"


# ============================================================

# ============================================================

def encode_od(od):
    'Encode each complete directed OD as one whitespace-free token.'
    if not isinstance(od, str) or not od.strip():
        raise ValueError("Null/empty transfer OD in TR")

    blob = base64.urlsafe_b64encode(
        od.encode("utf-8")
    ).decode("ascii").rstrip("=")

    return "OD_" + blob


def decode_od(token):
    'Decode an encoded MG-FSM OD token to its original Unicode event.'

    if not token.startswith("OD_") or len(token) <= 3:
        raise ValueError(
            "Unexpected MG-FSM output token: {!r}".format(token)
        )

    b64 = token[3:]

    return base64.urlsafe_b64decode(
        (b64 + "=" * (-len(b64) % 4)).encode("ascii")
    ).decode("utf-8")


def to_mgfsm_line(pair):
    'Encode one time-ordered trajectory into the original MG-FSM text format.'

    tr, numeric_id = pair

    if not tr:
        raise ValueError("Empty input sequence passed conversion")

    return (
        str(numeric_id)
        + " "
        + " ".join(encode_od(item) for item in tr)
    )


def parse_translated_line(line):
    'Parse the original translated frequent sequence output.'

    fields = line.rstrip("\r\n").rsplit("\t", 1)

    if len(fields) != 2:
        raise ValueError(
            "Unexpected translatedFS line: {!r}".format(line[:250])
        )

    tokens = fields[0].split()

    if not tokens:
        raise ValueError(
            "Empty pattern in translatedFS: {!r}".format(line[:250])
        )

    support = int(fields[1].strip())

    ods = [decode_od(token) for token in tokens]

    return (
        " , ".join(ods),
        int(support),
        len(ods)
    )


# ============================================================

# ============================================================

def self_test():

    test = [
        "Subway_益田 -> Bus_石厦南",
        "Bus_宝安人民医院 -> Subway_洪浪北",
        "Subway_益田 -> Bus_石厦南"
    ]

    assert all(
        decode_od(encode_od(x)) == x
        for x in test
    )

    line = to_mgfsm_line((test, 123))

    sid, *items = line.split()

    assert sid == "123"

    assert [decode_od(x) for x in items] == test

    restored = parse_translated_line(
        " ".join(items) + "\t218"
    )

    assert restored == (
        " , ".join(test),
        218,
        3
    )

    assert items[0] == items[2]

    print(
        "SELF_TEST: PASS "
        "(OD encoding, repeated OD, chronology, output parsing)"
    )


# ============================================================

# ============================================================

@contextmanager
def stage(stats, title):

    start = time.perf_counter()

    try:
        yield

    finally:
        elapsed = time.perf_counter() - start

        stats.append((title, elapsed))

        print(
            "[TIME] {:65s} {:12.3f} s".format(
                title, elapsed
            ),
            flush=True
        )


# ============================================================

# ============================================================

def build_parser():

    p = argparse.ArgumentParser(
        description=(
            "Run original MG-FSM MapReduce "
            "on Transfer_TR_Chronological Parquet"
        )
    )

    p.add_argument(
        "--input",
        default=DEFAULT_INPUT
    )

    p.add_argument(
        "--output",
        default=DEFAULT_OUTPUT
    )

    p.add_argument(
        "--mgfsmHome",
        default="/path/to/mgfsm-master/"
    )

    p.add_argument(
        "--minsup_abs",
        type=int,
        default=200
    )

    p.add_argument(
        "--gamma",
        type=int,
        default=3
    )

    p.add_argument(
        "--maxLength",
        type=int,
        default=4
    )

    p.add_argument(
        "--numReducers",
        type=int,
        default=64
    )

    p.add_argument(
        "--inputPartitions",
        type=int,
        default=64
    )

    p.add_argument(
        "--rawTextInput",
        default=None
    )

    p.add_argument(
        "--nativeOutput",
        default=None
    )

    p.add_argument(
        "--mgfsmWorkPath",
        default=None,
        help="HDFS intermediate path for original MG-FSM -k"
    )

    p.add_argument(
        "--autoBuildJar",
        action="store_true"
    )

    p.add_argument(
        "--selfTest",
        action="store_true"
    )

    return p


# ============================================================

# ============================================================

def get_fs(sc, path):

    jpath = sc._jvm.org.apache.hadoop.fs.Path(path)

    fs = jpath.getFileSystem(
        sc._jsc.hadoopConfiguration()
    )

    return fs, jpath


def exists(sc, path):

    fs, jpath = get_fs(sc, path)

    return bool(fs.exists(jpath))


def remove_if_exists(sc, path):

    fs, jpath = get_fs(sc, path)

    if fs.exists(jpath):

        if not fs.delete(jpath, True):
            raise RuntimeError(
                "Unable to remove HDFS directory: " + path
            )


def require_translated_success(sc, native_path):
    'Check Hadoop success markers before accepting native MG-FSM output.'

    target = native_path.rstrip("/") + "/translatedFS"

    success = target + "/_SUCCESS"

    if not exists(sc, success):

        raise RuntimeError(
            "Original MG-FSM did not produce "
            "translatedFS/_SUCCESS: {}\n"
            "Check original Java/Hadoop logs."
            .format(success)
        )

    return target


# ============================================================

# ============================================================

def verify_jar(home, autobuild, stats):

    home = os.path.abspath(home)

    launcher = os.path.join(
        home, "bin", "mgfsm"
    )

    jar = os.path.join(
        home,
        "target",
        "mgfsm-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
    )

    if not os.path.isfile(launcher):

        raise FileNotFoundError(
            "Original MG-FSM launcher not found: "
            + launcher
        )

    if not os.path.isfile(jar):

        if not autobuild:

            raise FileNotFoundError(
                "Missing MG-FSM JAR: {}\n"
                "Please compile original MG-FSM first."
                .format(jar)
            )

        with stage(
            stats,
            "Build original MG-FSM Java JAR"
        ):

            subprocess.run(
                [
                    "mvn",
                    "clean",
                    "package",
                    "-DskipTests"
                ],
                cwd=home,
                check=True
            )

        if not os.path.isfile(jar):

            raise FileNotFoundError(
                "MG-FSM JAR still missing: " + jar
            )

    if not os.environ.get("JAVA_HOME"):

        raise EnvironmentError(
            "JAVA_HOME is not configured"
        )

    if not os.environ.get("HADOOP_HOME"):

        raise EnvironmentError(
            "HADOOP_HOME is not configured"
        )

    return launcher, jar


# ============================================================

# ============================================================

def run(args):

    if args.selfTest:
        self_test()
        return

    if (
        args.minsup_abs < 1
        or args.gamma < 0
        or args.maxLength < 2
        or args.numReducers < 1
        or args.inputPartitions < 1
    ):
        raise ValueError(
            "Invalid MG-FSM parameters"
        )

    source = args.input.rstrip("/")

    output = args.output.rstrip("/")

    raw_input = (
        args.rawTextInput
        or output + "_mgfsm_input"
    ).rstrip("/")

    native = (
        args.nativeOutput
        or output + "_mgfsm_native"
    ).rstrip("/")

    mgfsm_work = (
        args.mgfsmWorkPath
        or output + "_mgfsm_work"
    ).rstrip("/")

  
  
  

    if not mgfsm_work.startswith("hdfs://"):

        raise ValueError(
            "MG-FSM work path must be a full HDFS URI"
        )

    paths = [
        source,
        output,
        raw_input,
        native,
        mgfsm_work
    ]

    if len(set(paths)) != len(paths):

        raise ValueError(
            "Input/output/intermediate HDFS paths "
            "must be different"
        )

    for path in [
        raw_input,
        native,
        output,
        mgfsm_work
    ]:

        if (
            path.startswith(source + "/")
            or source.startswith(path + "/")
        ):
            raise ValueError(
                "Unsafe overlapping HDFS paths"
            )

  
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    from pyspark.sql.types import (
        ArrayType,
        StringType,
        StructType,
        StructField,
        LongType,
        IntegerType
    )

    from pyspark import StorageLevel

    stats = []

    spark = None

    status = "FAILED"

    original_sequences = None
    pattern_count = None
    histogram = None

    print(
        "MG-FSM BASELINE | start = {}".format(
            datetime.now().astimezone().isoformat()
        ),
        flush=True
    )

    print("Input Parquet :", source, flush=True)
    print("Final CSV     :", output, flush=True)
    print("Native Hadoop :", native, flush=True)
    print("Text input    :", raw_input, flush=True)
    print("MG-FSM work   :", mgfsm_work, flush=True)

    print(
        "MG-FSM parameters: "
        "support={} gamma={} lambda={} reducers={}".format(
            args.minsup_abs,
            args.gamma,
            args.maxLength,
            args.numReducers
        ),
        flush=True
    )

    try:

        # ----------------------------------------------------
      
        # ----------------------------------------------------

        with stage(
            stats,
            "01. Validate original MG-FSM launcher and JAR"
        ):

            launcher, jar = verify_jar(
                args.mgfsmHome,
                args.autoBuildJar,
                stats
            )

        # ----------------------------------------------------
      
        # ----------------------------------------------------

        with stage(
            stats,
            "02. Start SparkSession"
        ):

            spark = (
                SparkSession.builder
                .appName("MG-FSM-Original-MapReduce")
                .getOrCreate()
            )

        sc = spark.sparkContext

        # ----------------------------------------------------
      
        # ----------------------------------------------------

        with stage(
            stats,
            "03. Read/check chronological TR Parquet"
        ):

            df = spark.read.parquet(source)

            missing = (
                {"CardID", "TR"} - set(df.columns)
            )

            if missing:
                raise ValueError(
                    "Missing input columns: "
                    + ", ".join(sorted(missing))
                )

            t = df.schema["TR"].dataType

            if not (
                isinstance(t, ArrayType)
                and isinstance(t.elementType, StringType)
            ):

                raise TypeError(
                    "Expected TR array<string>, got "
                    + t.simpleString()
                )

            bad = df.where(
                F.col("CardID").isNull()
                | F.col("TR").isNull()
                | (F.size(F.col("TR")) == 0)
                | F.expr(
                    "exists(TR, x -> "
                    "x is null or length(trim(x)) = 0)"
                )
            ).limit(1).count()

            if bad:
                raise ValueError(
                    "Input contains null/empty CardID or TR"
                )

        # ----------------------------------------------------
      
        # ----------------------------------------------------

        with stage(
            stats,
            "04. Build original ordered MG-FSM sequences"
        ):

            tr_df = (
                df.select("TR")
                .repartition(args.inputPartitions)
            )

          
          

            lines = (
                tr_df.rdd
                .map(lambda row: row[0])
                .zipWithUniqueId()
                .map(to_mgfsm_line)
                .persist(StorageLevel.MEMORY_AND_DISK)
            )

            original_sequences = lines.count()

            if original_sequences == 0:
                raise ValueError(
                    "Input Parquet has zero sequences"
                )

            print(
                "ORIGINAL_SEQUENCES:",
                original_sequences,
                flush=True
            )

            if args.minsup_abs > original_sequences:

                print(
                    "[WARN] minSupport exceeds sequence count",
                    flush=True
                )

        # ----------------------------------------------------
      
        # ----------------------------------------------------

        with stage(
            stats,
            "05. Write MG-FSM text input to HDFS"
        ):

            remove_if_exists(sc, raw_input)

            lines.saveAsTextFile(raw_input)

            lines.unpersist()

        # ----------------------------------------------------
      
        # ----------------------------------------------------

        with stage(
            stats,
            "06. Run ORIGINAL Hadoop MapReduce MG-FSM"
        ):

            cmd = [
                "bash",
                launcher,
                "-m", "d",
                "-i", raw_input,
                "-o", native,
                "-s", str(args.minsup_abs),
                "-g", str(args.gamma),
                "-l", str(args.maxLength),
                "-t", "a",
                "-N", str(args.numReducers),
                "-k", mgfsm_work
            ]

            print(
                "ORIGINAL_MGFSM_COMMAND:",
                " ".join(cmd),
                flush=True
            )

            try:

              
              
                subprocess.run(
                    cmd,
                    check=True
                )

            except subprocess.CalledProcessError as exc:

                print(
                    "[ERROR] Original MG-FSM failed. "
                    "Exit code: {}".format(exc.returncode),
                    flush=True
                )

                print(
                    "[ERROR] Inspect earlier Java/Hadoop "
                    "errors in the log.",
                    flush=True
                )

                print(
                    "[ERROR] Prepared HDFS input is available:",
                    raw_input,
                    flush=True
                )

                print(
                    "[ERROR] You can rerun "
                    "ORIGINAL_MGFSM_COMMAND directly.",
                    flush=True
                )

                raise

          
            translated = require_translated_success(
                sc,
                native
            )

        # ----------------------------------------------------
      
        # ----------------------------------------------------

        with stage(
            stats,
            "07. Read/decode MG-FSM frequent patterns"
        ):

            lines_out = (
                spark.read.text(translated)
                .select("value")
                .rdd
                .map(lambda row: row[0])
            )

            schema = StructType([
                StructField(
                    "pattern",
                    StringType(),
                    False
                ),
                StructField(
                    "total_support",
                    LongType(),
                    False
                ),
                StructField(
                    "pattern_length",
                    IntegerType(),
                    False
                )
            ])

            pattern_rdd = (
                lines_out
                .filter(lambda s: bool(s.strip()))
                .map(parse_translated_line)
            )

            results = (
                spark.createDataFrame(
                    pattern_rdd,
                    schema=schema
                )
                .persist(StorageLevel.MEMORY_AND_DISK)
            )

            pattern_count = results.count()

            length_rows = (
                results
                .groupBy("pattern_length")
                .count()
                .collect()
            )

            histogram = {
                int(r["pattern_length"]): int(r["count"])
                for r in length_rows
            }

        # ----------------------------------------------------
      
        # ----------------------------------------------------

        with stage(
            stats,
            "08. Write decoded frequent patterns to HDFS"
        ):

            (
                results.write
                .mode("overwrite")
                .option("header", "true")
                .csv(output)
            )

            results.unpersist()

        status = "SUCCESS"

        print(
            "[OK] Native MG-FSM output:",
            native + "/translatedFS",
            flush=True
        )

        print(
            "[OK] Final decoded CSV:",
            output,
            flush=True
        )

    finally:

        # ----------------------------------------------------
      
        # ----------------------------------------------------

        if spark is not None:

            with stage(
                stats,
                "09. Stop SparkSession"
            ):

                spark.stop()

        # ----------------------------------------------------
      
        # ----------------------------------------------------

        elapsed = time.perf_counter() - START

        print(
            "\n========== ORIGINAL MG-FSM "
            "END-TO-END TIMING ==========",
            flush=True
        )

        for label, seconds in stats:

            print(
                "{:<69s} {:10.3f} s".format(
                    label,
                    seconds
                ),
                flush=True
            )

        print("STATUS:", status, flush=True)

        print(
            "ORIGINAL_SEQUENCES:",
            original_sequences,
            flush=True
        )

        print(
            "FREQUENT_PATTERNS:",
            pattern_count,
            flush=True
        )

        print(
            "PATTERNS_BY_LENGTH:",
            histogram,
            flush=True
        )

        print(
            "TOTAL_INTERNAL_SECONDS: {:.3f}".format(
                elapsed
            ),
            flush=True
        )

        print(
            "TOTAL_INTERNAL_MINUTES: {:.4f}".format(
                elapsed / 60.0
            ),
            flush=True
        )

        print(
            "MG-FSM gap type: skipped sequence items, "
            "NOT topology",
            flush=True
        )

        print(
            "MG-FSM support type: "
            "native sequence/document frequency",
            flush=True
        )

        print(
            "=====================================================",
            flush=True
        )


# ============================================================

# ============================================================

if __name__ == "__main__":

    run(build_parser().parse_args())
