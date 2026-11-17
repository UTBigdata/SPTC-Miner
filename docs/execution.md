# Reproducing the processing pipeline

## Environment

The manuscript reports **Spark 3.1.3**, 64 cores, 128 GB configured memory, `n=64`, `minsup=200`, graph-hop gap `[0,3]`, and `maxPatternLength=4`. The repository has not been run in that cluster. Install the JVM/Scala versions compatible with your selected Spark build, Hadoop/HDFS, and the matching PySpark version. Python packages: `numpy`, `scikit-learn`, and `pytest` for local checks.

A GeoHash dependency is imported as `ch.hsr.geohash.GeoHash` by preprocessing. Supply a compatible GeoHash JAR in `spark-shell --jars <geohash.jar> ...` and arrange the original road network and FMM-cleaned GPS data before running stage 1.

## Run order

Set HDFS paths, input dates, threshold, and Spark settings in the Scala files before execution. Example hostnames such as `namenode.example.org` are intentionally **not real endpoints**. Some input data contain Chinese schema/value strings, preserved because processing depends on exact source encodings. Most Scala scripts contain original dataset assumptions rather than portable flags.

```bash
# Five independent Scala modules in order, using your own Spark/Hadoop configuration
spark-shell --jars '<geohash-jar>' -i pipeline/01_preprocessing/01_normalize_ticket_and_gps.scala
spark-shell -i pipeline/01_preprocessing/02_clean_and_deduplicate.scala
spark-shell --jars '<geohash-jar>' -i pipeline/01_preprocessing/03_match_bus_gps_to_stops.scala
spark-shell -i pipeline/01_preprocessing/04_integrate_smart_card_records.scala
spark-shell -i pipeline/01_preprocessing/05_clean_station_names.scala
spark-shell -i pipeline/02_alighting_inference/bus_alighting_inference.scala
spark-shell -i pipeline/03_position_summaries/trajectory_summary_builder.scala

python scripts/build_spark_zip.py
spark-submit --py-files dist/sptc_miner_lib.zip scripts/run_clustering.py \
  '<center-point-input>' '<cluster-label-output>' '<groups>' '<clusters-per-group>' '<input-partitions>'

spark-shell -i pipeline/05_label_remapping/trajectory_label_mapper.scala
spark-shell -i pipeline/06_partition_coverage/partition_coverage_repair.scala

spark-submit --py-files dist/sptc_miner_lib.zip scripts/run_mining.py \
  --root '<coverage-repair-output-root>' --inputMode postings \
  --minSupport 200 --preparedMinSupport 200 \
  --minGap 0 --maxGap 3 --numPartitions 64 \
  --maxPatternLength 4 --output '<final-output-directory>'
```

These are **stage launch examples**, not a confirmed fully executable pipeline on the included sample CSVs. If intermediate outputs do not already exist, or the source field names differ, stages must be configured to match the real input data.

## Important dependencies / configuration pitfalls

1. **Stage 1 missing intermediate**: the supplied cleaning module has its `Bus_Data` write commented out while a later module reads that path. Recover that dataset or configure the correct intermediate before chaining stage 1 scripts. This has not been silently fixed in the public release.
2. **Dataset mismatch**: stage 3 defaults to a January data root; stages 5 and 6 default to January+February. Point all stages to the *same actual dataset* and output locations.
3. **Clustering path**: stage 5 must read the exact CSV directory written by the clustering command. Confirm the `clusterPath` value in `trajectory_label_mapper.scala`.
4. **Stage 6 output**: the owner-coverage repair writes a separate directory (`CoverageFixed_m64_sup200` by default). Point `--root` for stage 7 to the **output directory of stage 6**, not stage 5 or stage 3.
5. **Support thresholds**: `postings` is already filtered by `preparedMinSupport`. The default miner rejects a mismatch between `--minSupport` and `--preparedMinSupport`. For mining from full U under alternate thresholds, use `--inputMode fullU`, set `--localMinSupport` deliberately, and use `--topologyMode counts` if computing adjacency from global OD counts.
6. **Topology semantics**: `--candidateTopology shared` includes directed reachable options **or shared endpoint** (matching the supplied code's default). `directed` uses directed reachable paths only. These settings can change the candidate space; report the setting in experiments.
7. **Maximum length**: the code defaults to no maximum (`0`), but the paper's comparison setting is `4`, so specify `--maxPatternLength 4` explicitly.
8. **Data scale**: clustering uses driver-side `collect()`; set executor/driver memory for the data scale. Do not assume the small sample is sufficient to test data pipeline scalability.

## Validation commands

```bash
python -m pip install -e .
python -m pip install -r requirements.txt
python examples/run_local_demo.py
pytest -q
python -m compileall -q src scripts baselines examples
```

A passing local test does not prove end-to-end Spark/HDFS, Java MG-FSM execution, exact paper run times, or whole-dataset result equality.
